#include <getopt.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "acceptor.h"
#include "backend_pool.h"
#include "backend_set.h"
#include "connection.h"
#include "handler.h"
#include "log.h"
#include "metrics.h"
#include "shutdown.h"
#include "thread_pool.h"
#include "tls.h"

namespace {

struct Args {
    std::string listen_host = "0.0.0.0";
    std::uint16_t listen_port = 8080;
    // Repeatable: each --backend HOST:PORT[:WEIGHT]. If no --backend is given,
    // defaults to a single 127.0.0.1:9090 backend with weight 1 (i.e. the v2
    // single-backend, round-robin-via-pool behaviour).
    std::vector<ir::BackendSet::BackendSpec> backends;
    std::size_t threads = 8;
    std::size_t pool_size = 16;
    int shutdown_grace_ms = 30'000;
    int log_level = 1;  // info

    // v4 TLS: terminate TLS on the acceptor side. Backend pool stays plaintext;
    // see ARCHITECTURE.md for why (production mTLS-via-mesh pattern).
    bool tls_enabled = false;
    std::string tls_cert_path;
    std::string tls_key_path;
    std::string tls_client_ca_path;  // empty: no mTLS; set: require client cert
};

void usage() {
    std::fprintf(stderr,
                 "inference-router\n"
                 "  --listen HOST:PORT             (default 0.0.0.0:8080)\n"
                 "  --backend HOST:PORT[:WEIGHT]   (repeatable; default 127.0.0.1:9090 weight=1)\n"
                 "                                   When more than one --backend is given,\n"
                 "                                   weighted-least-conn selection routes each\n"
                 "                                   request to the backend with the lowest\n"
                 "                                   in_flight/weight. Tie-break: lowest index.\n"
                 "  --threads N                    (default 8)\n"
                 "  --pool-size N                  (per-backend pool max; default 16)\n"
                 "  --shutdown-grace SECONDS       (default 30)\n"
                 "  --log-level [0..3]             (default 1=info)\n"
                 "  --tls                          enable TLS on the acceptor (requires "
                 "--tls-cert and --tls-key)\n"
                 "  --tls-cert PATH                PEM cert chain\n"
                 "  --tls-key PATH                 PEM private key\n"
                 "  --tls-client-ca PATH           PEM CA bundle; when set, mTLS is required\n"
                 "  --help\n");
}

bool parse_host_port(const char* s, std::string& host, std::uint16_t& port) {
    const char* colon = std::strrchr(s, ':');
    if (!colon || colon == s) return false;
    host.assign(s, static_cast<std::size_t>(colon - s));
    int p = std::atoi(colon + 1);
    if (p <= 0 || p > 65535) return false;
    port = static_cast<std::uint16_t>(p);
    return true;
}

// Parse --backend value as `HOST:PORT[:WEIGHT]`. WEIGHT is optional; defaults to 1.
// Returns false on any parse failure.
bool parse_backend_spec(const char* s, ir::BackendSet::BackendSpec& out) {
    // Find the LAST colon. If there are 2 colons, the suffix is WEIGHT and the
    // middle is PORT; if 1, the suffix is PORT.
    std::string in(s);
    auto last_colon = in.rfind(':');
    if (last_colon == std::string::npos || last_colon == 0) return false;
    auto first_colon = in.find(':');
    if (first_colon == last_colon) {
        // HOST:PORT, weight defaults to 1.
        out.host = in.substr(0, last_colon);
        int p = std::atoi(in.c_str() + last_colon + 1);
        if (p <= 0 || p > 65535) return false;
        out.port = static_cast<std::uint16_t>(p);
        out.weight = 1;
        return true;
    }
    // HOST:PORT:WEIGHT
    out.host = in.substr(0, first_colon);
    int p = std::atoi(in.c_str() + first_colon + 1);
    if (p <= 0 || p > 65535) return false;
    out.port = static_cast<std::uint16_t>(p);
    int w = std::atoi(in.c_str() + last_colon + 1);
    if (w < 0) return false;
    out.weight = static_cast<std::size_t>(w == 0 ? 1 : w);
    return true;
}

bool parse_args(int argc, char** argv, Args& out) {
    static struct option long_opts[] = {
        {"listen", required_argument, nullptr, 'l'},
        {"backend", required_argument, nullptr, 'b'},
        {"threads", required_argument, nullptr, 't'},
        {"pool-size", required_argument, nullptr, 'p'},
        {"shutdown-grace", required_argument, nullptr, 'g'},
        {"log-level", required_argument, nullptr, 'L'},
        {"tls", no_argument, nullptr, 'T'},
        {"tls-cert", required_argument, nullptr, 'C'},
        {"tls-key", required_argument, nullptr, 'K'},
        {"tls-client-ca", required_argument, nullptr, 'A'},
        {"help", no_argument, nullptr, 'h'},
        {nullptr, 0, nullptr, 0},
    };
    int opt;
    int idx = 0;
    while ((opt = getopt_long(argc, argv, "", long_opts, &idx)) != -1) {
        switch (opt) {
            case 'l':
                if (!parse_host_port(optarg, out.listen_host, out.listen_port)) {
                    std::fprintf(stderr, "invalid --listen value: %s\n", optarg);
                    return false;
                }
                break;
            case 'b': {
                ir::BackendSet::BackendSpec spec;
                if (!parse_backend_spec(optarg, spec)) {
                    std::fprintf(stderr,
                                 "invalid --backend value: %s (expected HOST:PORT[:WEIGHT])\n",
                                 optarg);
                    return false;
                }
                out.backends.push_back(spec);
                break;
            }
            case 't':
                out.threads = static_cast<std::size_t>(std::atoi(optarg));
                break;
            case 'p':
                out.pool_size = static_cast<std::size_t>(std::atoi(optarg));
                break;
            case 'g':
                out.shutdown_grace_ms = std::atoi(optarg) * 1000;
                break;
            case 'L':
                out.log_level = std::atoi(optarg);
                break;
            case 'T':
                out.tls_enabled = true;
                break;
            case 'C':
                out.tls_cert_path = optarg;
                break;
            case 'K':
                out.tls_key_path = optarg;
                break;
            case 'A':
                out.tls_client_ca_path = optarg;
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

}  // namespace

int main(int argc, char** argv) {
    Args args;
    if (!parse_args(argc, argv, args)) return 2;

    ir::set_log_level(static_cast<ir::LogLevel>(args.log_level));
    ir::Shutdown::instance().install_signal_handlers();

    ir::Metrics metrics;

    if (args.backends.empty()) {
        // Migration default: same as the v2 single-backend behaviour.
        args.backends.push_back({"127.0.0.1", 9090, 1});
    }

    ir::BackendPool::Options shared_opts;
    shared_opts.max_size = args.pool_size;
    shared_opts.min_idle = std::min<std::size_t>(2, args.pool_size);
    ir::BackendSet backend_set(args.backends, shared_opts, &metrics);

    ir::ThreadPool::Options topts;
    topts.worker_count = args.threads;
    topts.max_queue_depth = 4096;
    topts.reject_on_full = false;
    ir::ThreadPool pool(topts);

    ir::HandlerOptions hopts;

    // TLS: build the server context once, share it across worker threads. The
    // handshake itself runs on the worker thread in handle_one(), not here.
    ir::TlsContext tls_ctx;
    if (args.tls_enabled) {
        if (args.tls_cert_path.empty() || args.tls_key_path.empty()) {
            std::fprintf(stderr, "fatal: --tls requires --tls-cert and --tls-key\n");
            return 2;
        }
        ir::TlsContext::Options topts2;
        topts2.cert_path = args.tls_cert_path;
        topts2.key_path = args.tls_key_path;
        topts2.client_ca_path = args.tls_client_ca_path;
        topts2.require_client_cert = !args.tls_client_ca_path.empty();
        if (!tls_ctx.init_server(topts2)) {
            std::fprintf(stderr, "fatal: TLS context init failed\n");
            return 1;
        }
        hopts.server_tls = &tls_ctx;
    }

    ir::Acceptor::Options aopts;
    aopts.host = args.listen_host;
    aopts.port = args.listen_port;
    ir::Acceptor acceptor(aopts, &metrics, [&](int fd) {
        bool ok = pool.submit([fd, &backend_set, &metrics, hopts] {
            ir::handle_one(fd, backend_set, metrics, hopts);
        });
        if (!ok) {
            // Pool is shutting down. Listen socket has already been closed at this point,
            // so this branch is reachable only as a tiny race window. Closing the conn
            // surfaces a peer-close to the client, which is counted as an error response,
            // not a drop, by the load generator.
            ir::safe_close(fd);
            metrics.inc_errored();
        }
    });

    if (!acceptor.start()) {
        std::fprintf(stderr, "fatal: acceptor failed to start\n");
        return 1;
    }
    IR_LOG_INFO(
        "inference-router listening on %s:%u tls=%s backends=%zu threads=%zu pool/backend=%zu",
        args.listen_host.c_str(), acceptor.bound_port(),
        args.tls_enabled ? (args.tls_client_ca_path.empty() ? "on" : "mtls") : "off",
        args.backends.size(), args.threads, args.pool_size);
    for (std::size_t i = 0; i < args.backends.size(); ++i) {
        const auto& b = args.backends[i];
        IR_LOG_INFO("  backend[%zu] = %s:%u weight=%zu", i, b.host.c_str(), b.port, b.weight);
    }

    // Wait for SIGTERM/SIGINT.
    while (!ir::Shutdown::instance().requested()) {
        ir::Shutdown::instance().wait(std::chrono::milliseconds(1000));
    }
    IR_LOG_INFO("shutdown requested; entering drain phase");

    // Step 1: stop accepting new connections.
    acceptor.stop();

    // Step 2: wait for in-flight requests to finish, up to grace deadline.
    auto grace_end =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(args.shutdown_grace_ms);
    while (metrics.in_flight() > 0 && std::chrono::steady_clock::now() < grace_end) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    if (metrics.in_flight() > 0) {
        IR_LOG_WARN("grace deadline expired with %llu in-flight; counting as dropped",
                    static_cast<unsigned long long>(metrics.in_flight()));
        // The grace-deadline override is the ONLY documented path to a drop.
        std::uint64_t remaining = metrics.in_flight();
        for (std::uint64_t i = 0; i < remaining; ++i) {
            metrics.inc_dropped();
        }
    }

    // Step 3: stop the worker pool (no new tasks will be popped; queue should be empty).
    pool.stop();

    // Step 4: close all backend conns.
    backend_set.shutdown();

    IR_LOG_INFO("final: %s", metrics.snapshot_string().c_str());
    return 0;
}
