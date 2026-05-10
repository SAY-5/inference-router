// Chaos test — the load-bearing piece.
//
// Topology (all in-process):
//   - 1 backend echo (in-process, on its own thread)
//   - 1 router (in-process: acceptor + thread pool + backend pool)
//   - N client threads each sending M requests as fast as the backend will let them
//   - At t=`sigterm-after-ms` the test triggers the same drain protocol that SIGTERM
//     would: acceptor.stop() → wait for in_flight==0 → pool.stop() → backend.shutdown().
//
// Why no real fork() / SIGTERM here?
//   - In-process is hermetic and gives us deterministic counters. The drain protocol
//     under test is the same one main.cpp follows on SIGTERM — main.cpp's signal handler
//     is a thin wrapper that flips the same flag this test toggles directly.
//   - This keeps CI fast and removes the OS-level test flakiness that comes with
//     spawning binaries.
//
// Output:
//   bench/chaos-result.json  with the verdict + counters + timing

#include <netinet/in.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <getopt.h>
#include <mutex>
#include <thread>
#include <vector>

#include "acceptor.h"
#include "backend_pool.h"
#include "connection.h"
#include "handler.h"
#include "log.h"
#include "metrics.h"
#include "thread_pool.h"
#include "tls.h"

namespace {

struct EchoBackend {
    int listen_fd = -1;
    std::uint16_t port = 0;
    std::atomic<bool> stop{false};
    std::thread thread;
    std::vector<std::thread> workers;
    std::mutex workers_mu;

    bool start() {
        listen_fd = ir::listen_tcp("127.0.0.1", 0, 64);
        if (listen_fd < 0) return false;
        struct sockaddr_in sa {};
        socklen_t len = sizeof(sa);
        ::getsockname(listen_fd, reinterpret_cast<struct sockaddr*>(&sa), &len);
        port = ntohs(sa.sin_port);
        thread = std::thread([this] {
            while (!stop.load()) {
                struct pollfd pfd {};
                pfd.fd = listen_fd;
                pfd.events = POLLIN;
                int pr = ::poll(&pfd, 1, 50);
                if (pr <= 0) continue;
                int c = ::accept(listen_fd, nullptr, nullptr);
                if (c < 0) continue;
                std::lock_guard<std::mutex> lk(workers_mu);
                workers.emplace_back([c] {
                    while (true) {
                        std::vector<std::uint8_t> msg;
                        auto rc = ir::read_message(c, msg, 64 * 1024, 5000);
                        if (rc != ir::IoStatus::kOk) break;
                        if (ir::write_message(c, msg, 5000) != ir::IoStatus::kOk) break;
                    }
                    ir::safe_close(c);
                });
            }
            ir::safe_close(listen_fd);
        });
        return true;
    }
    void shutdown() {
        stop.store(true);
        if (thread.joinable()) thread.join();
        std::lock_guard<std::mutex> lk(workers_mu);
        for (auto& w : workers) {
            if (w.joinable()) w.join();
        }
    }
};

struct ClientStats {
    std::atomic<std::uint64_t> sent{0};
    std::atomic<std::uint64_t> got_response{0};
    std::atomic<std::uint64_t> connect_failed{0};
    std::atomic<std::uint64_t> io_failed{0};
};

// Plain-fd client: one request, one response, then close.
void client_main(int id, std::uint16_t router_port, int requests, ClientStats& stats) {
    for (int i = 0; i < requests; ++i) {
        int c = ir::dial_tcp("127.0.0.1", router_port, 2000);
        if (c < 0) {
            // Connect failure happens once the listening socket has closed — these
            // requests were never accepted by the router and don't count toward the
            // no-drop invariant.
            stats.connect_failed.fetch_add(1);
            continue;
        }
        stats.sent.fetch_add(1);
        std::uint8_t payload[16];
        std::memset(payload, id & 0xFF, sizeof(payload));
        auto wr = ir::write_message(c, payload, sizeof(payload), 5000);
        if (wr != ir::IoStatus::kOk) {
            stats.io_failed.fetch_add(1);
            ir::safe_close(c);
            continue;
        }
        std::vector<std::uint8_t> got;
        auto rd = ir::read_message(c, got, 1024, 5000);
        if (rd == ir::IoStatus::kOk) {
            stats.got_response.fetch_add(1);
        } else {
            // PeerClosed before reading a length is an error response by definition —
            // the server told us "no answer". The router's metrics will tally this in
            // errored, not dropped, as long as the request was accepted.
            stats.io_failed.fetch_add(1);
        }
        ir::safe_close(c);
    }
}

// TLS client: same loop but each connection runs a real SSL_connect() over the
// router's TLS-terminating acceptor. Shares one SSL_CTX (thread-safe in 1.1+).
void tls_client_main(int id, std::uint16_t router_port, int requests, SSL_CTX* ctx,
                     ClientStats& stats) {
    for (int i = 0; i < requests; ++i) {
        int c = ir::dial_tcp("127.0.0.1", router_port, 2000);
        if (c < 0) {
            stats.connect_failed.fetch_add(1);
            continue;
        }
        SSL* ssl = SSL_new(ctx);
        if (!ssl) {
            ir::safe_close(c);
            stats.io_failed.fetch_add(1);
            continue;
        }
        SSL_set_fd(ssl, c);
        if (SSL_connect(ssl) <= 0) {
            SSL_free(ssl);
            ir::safe_close(c);
            stats.connect_failed.fetch_add(1);
            continue;
        }
        stats.sent.fetch_add(1);
        std::uint8_t payload[16];
        std::memset(payload, id & 0xFF, sizeof(payload));
        ir::Stream s = ir::Stream::tls(ssl);
        auto wr = ir::write_message(s, payload, sizeof(payload), 5000);
        if (wr != ir::IoStatus::kOk) {
            stats.io_failed.fetch_add(1);
            ir::close_stream(s);
            continue;
        }
        std::vector<std::uint8_t> got;
        auto rd = ir::read_message(s, got, 1024, 5000);
        if (rd == ir::IoStatus::kOk) {
            stats.got_response.fetch_add(1);
        } else {
            stats.io_failed.fetch_add(1);
        }
        ir::close_stream(s);
    }
}

struct Args {
    int clients = 50;
    int requests = 100;
    int sigterm_after_ms = 5000;
    int router_threads = 8;
    int pool_size = 8;
    int shutdown_grace_ms = 30000;
    std::string out_path = "bench/chaos-result.json";
    bool tls = false;
    std::string tls_cert_path;
    std::string tls_key_path;
};

void usage() {
    std::fprintf(stderr,
                 "chaos --clients N --requests N --sigterm-after-ms MS\n"
                 "  --threads N --pool-size N --shutdown-grace MS --out PATH\n"
                 "  --tls --tls-cert PATH --tls-key PATH\n");
}

bool parse_args(int argc, char** argv, Args& out) {
    static struct option longs[] = {
        {"clients", required_argument, nullptr, 'c'},
        {"requests", required_argument, nullptr, 'r'},
        {"sigterm-after-ms", required_argument, nullptr, 's'},
        {"threads", required_argument, nullptr, 't'},
        {"pool-size", required_argument, nullptr, 'p'},
        {"shutdown-grace", required_argument, nullptr, 'g'},
        {"out", required_argument, nullptr, 'o'},
        {"tls", no_argument, nullptr, 'T'},
        {"tls-cert", required_argument, nullptr, 'C'},
        {"tls-key", required_argument, nullptr, 'K'},
        {"help", no_argument, nullptr, 'h'},
        {nullptr, 0, nullptr, 0},
    };
    int opt, idx;
    while ((opt = getopt_long(argc, argv, "", longs, &idx)) != -1) {
        switch (opt) {
            case 'c':
                out.clients = std::atoi(optarg);
                break;
            case 'r':
                out.requests = std::atoi(optarg);
                break;
            case 's':
                out.sigterm_after_ms = std::atoi(optarg);
                break;
            case 't':
                out.router_threads = std::atoi(optarg);
                break;
            case 'p':
                out.pool_size = std::atoi(optarg);
                break;
            case 'g':
                out.shutdown_grace_ms = std::atoi(optarg);
                break;
            case 'o':
                out.out_path = optarg;
                break;
            case 'T':
                out.tls = true;
                break;
            case 'C':
                out.tls_cert_path = optarg;
                break;
            case 'K':
                out.tls_key_path = optarg;
                break;
            case 'h':
                usage();
                std::exit(0);
            default:
                usage();
                return false;
        }
    }
    return true;
}

std::string ts_iso8601(std::chrono::system_clock::time_point tp) {
    auto t = std::chrono::system_clock::to_time_t(tp);
    char buf[40];
    struct tm gm {};
    gmtime_r(&t, &gm);
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &gm);
    return buf;
}

}  // namespace

int main(int argc, char** argv) {
    Args args;
    if (!parse_args(argc, argv, args)) return 2;

    ir::set_log_level(ir::LogLevel::kWarn);

    EchoBackend backend;
    if (!backend.start()) {
        std::fprintf(stderr, "fatal: backend failed to start\n");
        return 1;
    }

    ir::Metrics metrics;
    ir::BackendPool::Options bopts;
    bopts.host = "127.0.0.1";
    bopts.port = backend.port;
    bopts.max_size = static_cast<std::size_t>(args.pool_size);
    bopts.min_idle = 0;
    bopts.enable_health_check = false;
    bopts.borrow_timeout = std::chrono::milliseconds(5000);
    ir::BackendPool bpool(bopts, &metrics);

    ir::ThreadPool::Options topts;
    topts.worker_count = static_cast<std::size_t>(args.router_threads);
    topts.max_queue_depth = 4096;
    ir::ThreadPool pool(topts);

    ir::HandlerOptions hopts;
    hopts.client_io_timeout_ms = 10'000;
    hopts.backend_io_timeout_ms = 10'000;

    // TLS terminates on the acceptor; backend pool stays plaintext (mesh layer's
    // job for prod). Real handshake on every client connection.
    ir::TlsContext tls_ctx;
    SSL_CTX* client_ssl_ctx = nullptr;
    if (args.tls) {
        if (args.tls_cert_path.empty() || args.tls_key_path.empty()) {
            std::fprintf(stderr, "fatal: --tls requires --tls-cert and --tls-key\n");
            return 2;
        }
        ir::TlsContext::Options topts2;
        topts2.cert_path = args.tls_cert_path;
        topts2.key_path = args.tls_key_path;
        if (!tls_ctx.init_server(topts2)) {
            std::fprintf(stderr, "fatal: tls server init failed\n");
            return 1;
        }
        hopts.server_tls = &tls_ctx;

        ir::tls_global_init();
        client_ssl_ctx = SSL_CTX_new(TLS_client_method());
        if (!client_ssl_ctx) {
            std::fprintf(stderr, "fatal: tls client ctx failed\n");
            return 1;
        }
        SSL_CTX_set_verify(client_ssl_ctx, SSL_VERIFY_NONE, nullptr);
    }

    ir::Acceptor::Options aopts;
    aopts.host = "127.0.0.1";
    aopts.port = 0;
    ir::Acceptor acceptor(aopts, &metrics, [&](int fd) {
        bool ok = pool.submit(
            [fd, &bpool, &metrics, hopts] { ir::handle_one(fd, bpool, metrics, hopts); });
        if (!ok) {
            ir::safe_close(fd);
            metrics.inc_errored();
        }
    });
    if (!acceptor.start()) {
        std::fprintf(stderr, "fatal: acceptor start failed\n");
        return 1;
    }
    auto router_port = acceptor.bound_port();
    std::printf("chaos: backend=%u router=%u clients=%d requests=%d sigterm@%dms\n", backend.port,
                router_port, args.clients, args.requests, args.sigterm_after_ms);

    auto run_start_wall = std::chrono::system_clock::now();
    auto run_start = std::chrono::steady_clock::now();

    ClientStats stats;
    std::vector<std::thread> clients;
    clients.reserve(static_cast<std::size_t>(args.clients));
    for (int i = 0; i < args.clients; ++i) {
        if (args.tls) {
            clients.emplace_back(tls_client_main, i, router_port, args.requests, client_ssl_ctx,
                                 std::ref(stats));
        } else {
            clients.emplace_back(client_main, i, router_port, args.requests, std::ref(stats));
        }
    }

    // Sleep until the configured drain trigger.
    std::this_thread::sleep_for(std::chrono::milliseconds(args.sigterm_after_ms));
    auto sigterm_wall = std::chrono::system_clock::now();
    auto sigterm_at = std::chrono::steady_clock::now();
    std::printf(
        "chaos: triggering shutdown at %lldms (accepted=%llu in_flight=%llu)\n",
        static_cast<long long>(
            std::chrono::duration_cast<std::chrono::milliseconds>(sigterm_at - run_start).count()),
        static_cast<unsigned long long>(metrics.accepted()),
        static_cast<unsigned long long>(metrics.in_flight()));

    // Drain protocol — same sequence as main.cpp.
    acceptor.stop();
    auto grace_end =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(args.shutdown_grace_ms);
    while (metrics.in_flight() > 0 && std::chrono::steady_clock::now() < grace_end) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    if (metrics.in_flight() > 0) {
        std::uint64_t remaining = metrics.in_flight();
        for (std::uint64_t i = 0; i < remaining; ++i) {
            metrics.inc_dropped();
        }
    }
    pool.stop();
    bpool.shutdown();

    for (auto& t : clients) t.join();
    backend.shutdown();
    if (client_ssl_ctx) SSL_CTX_free(client_ssl_ctx);

    auto run_end = std::chrono::steady_clock::now();
    auto run_end_wall = std::chrono::system_clock::now();

    auto total_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(run_end - run_start).count();
    auto sigterm_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(sigterm_at - run_start).count();

    std::printf(
        "chaos: result accepted=%llu completed=%llu errored=%llu dropped=%llu "
        "client_sent=%llu client_got_response=%llu client_connect_failed=%llu "
        "client_io_failed=%llu total_ms=%lld\n",
        (unsigned long long)metrics.accepted(), (unsigned long long)metrics.completed(),
        (unsigned long long)metrics.errored(), (unsigned long long)metrics.dropped(),
        (unsigned long long)stats.sent.load(), (unsigned long long)stats.got_response.load(),
        (unsigned long long)stats.connect_failed.load(), (unsigned long long)stats.io_failed.load(),
        (long long)total_ms);

    bool dropped_zero = metrics.dropped() == 0;
    bool invariant_ok =
        metrics.completed() + metrics.errored() == metrics.accepted() && dropped_zero;

    std::ofstream out(args.out_path);
    if (!out) {
        std::fprintf(stderr, "warning: could not write %s\n", args.out_path.c_str());
    } else {
        out << "{\n";
        out << "  \"schema\": \"inference-router.chaos.v1\",\n";
        out << "  \"verdict\": \"" << (invariant_ok ? "pass" : "fail") << "\",\n";
        out << "  \"dropped_total\": " << metrics.dropped() << ",\n";
        out << "  \"accepted_total\": " << metrics.accepted() << ",\n";
        out << "  \"completed_total\": " << metrics.completed() << ",\n";
        out << "  \"errored_total\": " << metrics.errored() << ",\n";
        out << "  \"client_sent\": " << stats.sent.load() << ",\n";
        out << "  \"client_got_response\": " << stats.got_response.load() << ",\n";
        out << "  \"client_connect_failed\": " << stats.connect_failed.load() << ",\n";
        out << "  \"client_io_failed\": " << stats.io_failed.load() << ",\n";
        out << "  \"clients\": " << args.clients << ",\n";
        out << "  \"requests_per_client\": " << args.requests << ",\n";
        out << "  \"router_threads\": " << args.router_threads << ",\n";
        out << "  \"backend_pool_size\": " << args.pool_size << ",\n";
        out << "  \"sigterm_after_ms\": " << sigterm_ms << ",\n";
        out << "  \"total_ms\": " << total_ms << ",\n";
        out << "  \"started_at\": \"" << ts_iso8601(run_start_wall) << "\",\n";
        out << "  \"sigterm_at\": \"" << ts_iso8601(sigterm_wall) << "\",\n";
        out << "  \"finished_at\": \"" << ts_iso8601(run_end_wall) << "\"\n";
        out << "}\n";
    }

    if (!invariant_ok) {
        std::fprintf(stderr, "FAIL: invariant violated (dropped=%llu)\n",
                     (unsigned long long)metrics.dropped());
        return 1;
    }
    std::printf("PASS: dropped == 0 over %llu accepted requests\n",
                (unsigned long long)metrics.accepted());
    return 0;
}
