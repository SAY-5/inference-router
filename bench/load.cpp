// Throughput / latency micro-benchmark. Runs against an external router (default
// 127.0.0.1:8080) — assumes the router is already running, with a backend behind it.
//
// CI runs this against an in-process router/backend pair started inline.

#include <getopt.h>
#include <limits.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <memory>
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

namespace {

struct Args {
    std::string host = "127.0.0.1";
    std::uint16_t port = 0;
    int concurrency = 8;  // legacy: thread count when --clients is 0
    int clients = 0;      // when >0: spawn this many client threads (each = 1 connection lifecycle)
    int reqs_per_client = 0;  // when >0 + --clients: each client sends this many requests
    int requests = 0;         // if >0, legacy total requests; else use duration
    int duration_s = 0;       // if >0, run for this long
    int payload_bytes = 64;
    int router_threads = 16;
    int router_pool = 64;
    int client_stack_kb = 128;  // 128 KiB per client thread keeps 10k threads at ~1.3 GiB virtual
    bool spawn_inproc = true;   // start router+backend in-process for hermetic CI runs
    std::string out_path = "bench/bench-result.json";
};

void usage() {
    std::fprintf(stderr,
                 "load_bench [legacy] --concurrency N (--requests N | --duration-s N)\n"
                 "load_bench [10k]    --clients N --reqs-per-client M\n"
                 "  --host H --port P --payload-bytes N --out PATH --no-inproc\n"
                 "  --router-threads N --router-pool N --client-stack-kb N\n");
}

bool parse_args(int argc, char** argv, Args& out) {
    static struct option longs[] = {
        {"host", required_argument, nullptr, 'h'},
        {"port", required_argument, nullptr, 'p'},
        {"concurrency", required_argument, nullptr, 'c'},
        {"clients", required_argument, nullptr, 'C'},
        {"reqs-per-client", required_argument, nullptr, 'R'},
        {"requests", required_argument, nullptr, 'r'},
        {"duration-s", required_argument, nullptr, 'd'},
        {"payload-bytes", required_argument, nullptr, 'b'},
        {"router-threads", required_argument, nullptr, 'T'},
        {"router-pool", required_argument, nullptr, 'P'},
        {"client-stack-kb", required_argument, nullptr, 'S'},
        {"out", required_argument, nullptr, 'o'},
        {"no-inproc", no_argument, nullptr, 'N'},
        {"help", no_argument, nullptr, 'H'},
        {nullptr, 0, nullptr, 0},
    };
    int opt, idx;
    while ((opt = getopt_long(argc, argv, "", longs, &idx)) != -1) {
        switch (opt) {
            case 'h':
                out.host = optarg;
                break;
            case 'p':
                out.port = static_cast<std::uint16_t>(std::atoi(optarg));
                break;
            case 'c':
                out.concurrency = std::atoi(optarg);
                break;
            case 'C':
                out.clients = std::atoi(optarg);
                break;
            case 'R':
                out.reqs_per_client = std::atoi(optarg);
                break;
            case 'r':
                out.requests = std::atoi(optarg);
                break;
            case 'd':
                out.duration_s = std::atoi(optarg);
                break;
            case 'b':
                out.payload_bytes = std::atoi(optarg);
                break;
            case 'T':
                out.router_threads = std::atoi(optarg);
                break;
            case 'P':
                out.router_pool = std::atoi(optarg);
                break;
            case 'S':
                out.client_stack_kb = std::atoi(optarg);
                break;
            case 'o':
                out.out_path = optarg;
                break;
            case 'N':
                out.spawn_inproc = false;
                break;
            case 'H':
                usage();
                std::exit(0);
            default:
                usage();
                return false;
        }
    }
    if (out.clients > 0 && out.reqs_per_client <= 0) {
        out.reqs_per_client = 50;
    }
    if (out.clients == 0 && out.requests == 0 && out.duration_s == 0) {
        out.duration_s = 5;
    }
    return true;
}

// One-client worker: opens reqs_per_client fresh connections, sends the payload,
// reads the echo, accounts the latency and error counters. Implemented as a free
// function so it can be wired through pthread_create() in the 10k-client path.
struct ClientCfg {
    int id;
    std::string host;
    std::uint16_t port;
    int payload_bytes;
    int reqs_per_client;
    std::vector<std::uint64_t>* lat;
    std::atomic<std::uint64_t>* ok_acc;
    std::atomic<std::uint64_t>* err_acc;
    std::atomic<std::uint64_t>* connects_acc;
    std::atomic<std::uint64_t>* connect_fail_acc;
};

// One persistent connection per client, sending reqs_per_client length-prefixed
// messages and reading the echo for each. This matches the v2 spec: 10000
// concurrent CLIENTS each doing 50 requests, not 500000 fresh dials. Keeping one
// fd per client also keeps the bench from saturating the ephemeral-port pool.
//
// The router's handler.handle_one() closes the client conn after one request
// (request-then-close is the documented protocol), so we DO have to redial per
// request — but we cap concurrency-of-fresh-dials at the live client thread count
// (= n_workers concurrent), so ephemeral usage stays bounded by n_workers, not by
// total request count.
void run_client(const ClientCfg& cfg) {
    std::vector<std::uint8_t> payload(static_cast<std::size_t>(cfg.payload_bytes),
                                      static_cast<std::uint8_t>('a' + (cfg.id & 0x1F)));
    cfg.lat->reserve(static_cast<std::size_t>(cfg.reqs_per_client));
    // 30s IO timeout: at 10k concurrent clients on a 32-worker router, queueing
    // can push individual round-trips past the 5s default. The bench cares about
    // throughput-under-saturation, not 5s tail; budget enough that the router's
    // queue depth is the real metric, not "did we time out".
    constexpr int kIoTimeoutMs = 30'000;
    for (int i = 0; i < cfg.reqs_per_client; ++i) {
        auto t0 = std::chrono::steady_clock::now();
        int c = ir::dial_tcp(cfg.host, cfg.port, 5000);
        if (c < 0) {
            cfg.err_acc->fetch_add(1, std::memory_order_relaxed);
            cfg.connect_fail_acc->fetch_add(1, std::memory_order_relaxed);
            continue;
        }
        // SO_LINGER {1, 0}: close() sends RST instead of FIN, skipping TIME_WAIT
        // on the client-side ephemeral port. Without this, 500k localhost dials
        // exhaust the ~28k ephemeral-port pool within seconds.
        struct linger lg = {1, 0};
        ::setsockopt(c, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
        cfg.connects_acc->fetch_add(1, std::memory_order_relaxed);
        bool round_ok = ir::write_message(c, payload, kIoTimeoutMs) == ir::IoStatus::kOk;
        std::vector<std::uint8_t> got;
        if (round_ok) {
            round_ok = ir::read_message(c, got, 64 * 1024, kIoTimeoutMs) == ir::IoStatus::kOk &&
                       got == payload;
        }
        ir::safe_close(c);
        auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - t0)
                           .count();
        if (round_ok) {
            cfg.ok_acc->fetch_add(1, std::memory_order_relaxed);
            cfg.lat->push_back(static_cast<std::uint64_t>(elapsed));
        } else {
            cfg.err_acc->fetch_add(1, std::memory_order_relaxed);
        }
    }
}

void* run_client_trampoline(void* p) {
    auto* cfg = static_cast<ClientCfg*>(p);
    run_client(*cfg);
    return nullptr;
}

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

}  // namespace

int main(int argc, char** argv) {
    Args args;
    if (!parse_args(argc, argv, args)) return 2;
    ir::set_log_level(ir::LogLevel::kWarn);

    EchoBackend backend;
    ir::Metrics metrics;
    std::unique_ptr<ir::BackendPool> bpool;
    std::unique_ptr<ir::ThreadPool> tpool;
    std::unique_ptr<ir::Acceptor> acceptor;
    // HandlerOptions lives in main()'s scope because the acceptor's accept callback
    // is a lambda that outlives the local block where it's constructed. If we kept
    // hopts inside the `if (args.spawn_inproc)` block, the by-reference capture in
    // the outer lambda would dangle as soon as the block ended — fine when
    // HandlerOptions was three ints (stack memory survived intact long enough), but
    // breaks the moment the struct grows (e.g. v4's TlsContext* server_tls field
    // would read garbage and the handler would think TLS is on).
    ir::HandlerOptions hopts;
    hopts.client_io_timeout_ms = 30'000;
    hopts.backend_io_timeout_ms = 30'000;

    std::uint16_t target_port = args.port;

    if (args.spawn_inproc) {
        if (!backend.start()) {
            std::fprintf(stderr, "backend start failed\n");
            return 1;
        }
        ir::BackendPool::Options bopts;
        bopts.host = "127.0.0.1";
        bopts.port = backend.port;
        bopts.max_size = static_cast<std::size_t>(args.router_pool);
        bopts.enable_health_check = false;
        bopts.borrow_timeout = std::chrono::milliseconds(15'000);
        bpool = std::make_unique<ir::BackendPool>(bopts, &metrics);

        ir::ThreadPool::Options topts;
        topts.worker_count = static_cast<std::size_t>(args.router_threads);
        topts.max_queue_depth = 16'384;
        tpool = std::make_unique<ir::ThreadPool>(topts);

        ir::Acceptor::Options aopts;
        aopts.host = "127.0.0.1";
        aopts.port = 0;
        aopts.backlog = 4096;
        acceptor = std::make_unique<ir::Acceptor>(aopts, &metrics, [&](int fd) {
            tpool->submit(
                [fd, &bp = *bpool, &m = metrics, hopts] { ir::handle_one(fd, bp, m, hopts); });
        });
        if (!acceptor->start()) {
            std::fprintf(stderr, "acceptor failed\n");
            return 1;
        }
        target_port = acceptor->bound_port();
    } else if (target_port == 0) {
        std::fprintf(stderr, "must specify --port when --no-inproc is set\n");
        return 2;
    }

    const bool use_clients_mode = args.clients > 0;
    const int n_workers = use_clients_mode ? args.clients : args.concurrency;

    std::printf(
        "bench: target=%s:%u mode=%s clients=%d reqs/client=%d concurrency=%d "
        "requests=%d duration_s=%d payload=%dB router_threads=%d router_pool=%d\n",
        args.host.c_str(), target_port, use_clients_mode ? "clients" : "legacy", args.clients,
        args.reqs_per_client, args.concurrency, args.requests, args.duration_s, args.payload_bytes,
        args.router_threads, args.router_pool);

    std::atomic<std::uint64_t> total_ok{0};
    std::atomic<std::uint64_t> total_err{0};
    std::atomic<std::uint64_t> total_connects{0};
    std::atomic<std::uint64_t> total_connect_failed{0};
    std::vector<std::vector<std::uint64_t>> per_thread_lat(static_cast<std::size_t>(n_workers));

    auto run_start = std::chrono::steady_clock::now();
    auto deadline = run_start + std::chrono::seconds(args.duration_s);

    std::vector<pthread_t> client_threads;
    std::vector<std::thread> std_workers;

    int per_thread_requests = (args.requests > 0) ? args.requests / args.concurrency : -1;

    if (use_clients_mode) {
        // 10k+ client threads with reduced stack size to keep virtual memory bounded.
        std::vector<ClientCfg> cfgs(static_cast<std::size_t>(n_workers));
        client_threads.resize(static_cast<std::size_t>(n_workers));
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        size_t stack_bytes = static_cast<size_t>(args.client_stack_kb) * 1024;
        if (stack_bytes < static_cast<size_t>(PTHREAD_STACK_MIN)) {
            stack_bytes = static_cast<size_t>(PTHREAD_STACK_MIN);
        }
        pthread_attr_setstacksize(&attr, stack_bytes);

        std::size_t spawned = 0;
        for (int i = 0; i < n_workers; ++i) {
            auto& cfg = cfgs[static_cast<std::size_t>(i)];
            cfg.id = i;
            cfg.host = args.host;
            cfg.port = target_port;
            cfg.payload_bytes = args.payload_bytes;
            cfg.reqs_per_client = args.reqs_per_client;
            cfg.lat = &per_thread_lat[static_cast<std::size_t>(i)];
            cfg.ok_acc = &total_ok;
            cfg.err_acc = &total_err;
            cfg.connects_acc = &total_connects;
            cfg.connect_fail_acc = &total_connect_failed;
            int rc = pthread_create(&client_threads[static_cast<std::size_t>(i)], &attr,
                                    &run_client_trampoline, &cfg);
            if (rc != 0) {
                std::fprintf(stderr, "pthread_create failed at i=%d rc=%d errno=%d\n", i, rc,
                             errno);
                break;
            }
            ++spawned;
        }
        pthread_attr_destroy(&attr);
        for (std::size_t i = 0; i < spawned; ++i) {
            pthread_join(client_threads[i], nullptr);
        }
    } else {
        // Legacy mode: --concurrency threads, fresh dial+write+read per request.
        std_workers.reserve(static_cast<std::size_t>(args.concurrency));
        for (int t = 0; t < args.concurrency; ++t) {
            std_workers.emplace_back([&, t, per_thread_requests] {
                auto& lat = per_thread_lat[static_cast<std::size_t>(t)];
                lat.reserve(1024);
                std::vector<std::uint8_t> payload(static_cast<std::size_t>(args.payload_bytes),
                                                  static_cast<std::uint8_t>('a' + (t & 0x1F)));
                int sent_local = 0;
                while (true) {
                    if (per_thread_requests > 0 && sent_local >= per_thread_requests) break;
                    if (per_thread_requests < 0 && std::chrono::steady_clock::now() >= deadline) {
                        break;
                    }
                    auto t0 = std::chrono::steady_clock::now();
                    int c = ir::dial_tcp(args.host, target_port, 2000);
                    if (c < 0) {
                        total_err.fetch_add(1);
                        total_connect_failed.fetch_add(1);
                        ++sent_local;
                        continue;
                    }
                    total_connects.fetch_add(1);
                    if (ir::write_message(c, payload, 5000) != ir::IoStatus::kOk) {
                        total_err.fetch_add(1);
                        ir::safe_close(c);
                        ++sent_local;
                        continue;
                    }
                    std::vector<std::uint8_t> got;
                    auto rc = ir::read_message(c, got, 64 * 1024, 5000);
                    ir::safe_close(c);
                    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                                       std::chrono::steady_clock::now() - t0)
                                       .count();
                    if (rc == ir::IoStatus::kOk && got == payload) {
                        total_ok.fetch_add(1);
                        lat.push_back(static_cast<std::uint64_t>(elapsed));
                    } else {
                        total_err.fetch_add(1);
                    }
                    ++sent_local;
                }
            });
        }
        for (auto& w : std_workers) w.join();
    }
    auto run_end = std::chrono::steady_clock::now();

    if (acceptor) acceptor->stop();
    if (tpool) tpool->stop();
    if (bpool) bpool->shutdown();
    backend.shutdown();

    std::vector<std::uint64_t> all;
    for (auto& v : per_thread_lat) {
        all.insert(all.end(), v.begin(), v.end());
    }
    std::sort(all.begin(), all.end());
    auto pct = [&](double q) -> std::uint64_t {
        if (all.empty()) return 0;
        double n_minus_1 = static_cast<double>(all.size() - 1);
        std::size_t idx = static_cast<std::size_t>(q * n_minus_1);
        return all[idx];
    };
    auto total_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(run_end - run_start).count();
    double throughput = (total_ms > 0) ? (1000.0 * static_cast<double>(total_ok.load())) /
                                             static_cast<double>(total_ms)
                                       : 0.0;

    double connect_rate = (total_ms > 0) ? (1000.0 * static_cast<double>(total_connects.load())) /
                                               static_cast<double>(total_ms)
                                         : 0.0;

    std::printf(
        "ok=%llu err=%llu connects=%llu connect_failed=%llu ms=%lld "
        "throughput_rps=%.1f connect_rate_per_s=%.1f "
        "p50_us=%llu p95_us=%llu p99_us=%llu p999_us=%llu\n",
        (unsigned long long)total_ok.load(), (unsigned long long)total_err.load(),
        (unsigned long long)total_connects.load(), (unsigned long long)total_connect_failed.load(),
        (long long)total_ms, throughput, connect_rate, (unsigned long long)pct(0.50),
        (unsigned long long)pct(0.95), (unsigned long long)pct(0.99),
        (unsigned long long)pct(0.999));

    // Default the output path to bench/results/<timestamp>.json in clients mode unless
    // the caller picked a specific path.
    std::string out_path = args.out_path;
    if (use_clients_mode && out_path == "bench/bench-result.json") {
        char ts[64];
        time_t now_t = std::time(nullptr);
        struct tm gm;
        gmtime_r(&now_t, &gm);
        std::strftime(ts, sizeof(ts), "%Y%m%dT%H%M%SZ", &gm);
        out_path = std::string("bench/results/") + ts + ".json";
    }

    std::ofstream out(out_path);
    if (out) {
        out << "{\n";
        out << "  \"schema\": \"inference-router.bench.v2\",\n";
        out << "  \"mode\": \"" << (use_clients_mode ? "clients" : "legacy") << "\",\n";
        out << "  \"ok\": " << total_ok.load() << ",\n";
        out << "  \"err\": " << total_err.load() << ",\n";
        out << "  \"connects\": " << total_connects.load() << ",\n";
        out << "  \"connect_failed\": " << total_connect_failed.load() << ",\n";
        out << "  \"total_ms\": " << total_ms << ",\n";
        out << "  \"throughput_rps\": " << throughput << ",\n";
        out << "  \"connect_rate_per_s\": " << connect_rate << ",\n";
        out << "  \"p50_us\": " << pct(0.50) << ",\n";
        out << "  \"p95_us\": " << pct(0.95) << ",\n";
        out << "  \"p99_us\": " << pct(0.99) << ",\n";
        out << "  \"p999_us\": " << pct(0.999) << ",\n";
        out << "  \"concurrency\": " << args.concurrency << ",\n";
        out << "  \"clients\": " << args.clients << ",\n";
        out << "  \"reqs_per_client\": " << args.reqs_per_client << ",\n";
        out << "  \"router_threads\": " << args.router_threads << ",\n";
        out << "  \"router_pool\": " << args.router_pool << ",\n";
        out << "  \"payload_bytes\": " << args.payload_bytes << "\n";
        out << "}\n";
    } else {
        std::fprintf(stderr, "warning: could not open %s for write\n", out_path.c_str());
    }
    std::printf("wrote %s\n", out_path.c_str());
    return 0;
}
