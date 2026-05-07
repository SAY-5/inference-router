// Throughput / latency micro-benchmark. Runs against an external router (default
// 127.0.0.1:8080) — assumes the router is already running, with a backend behind it.
//
// CI runs this against an in-process router/backend pair started inline.

#include <getopt.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
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
    int concurrency = 8;
    int requests = 0;    // if >0, total requests; else use duration
    int duration_s = 0;  // if >0, run for this long
    int payload_bytes = 64;
    bool spawn_inproc = true;  // start router+backend in-process for hermetic CI runs
    std::string out_path = "bench/bench-result.json";
};

void usage() {
    std::fprintf(stderr,
                 "load_bench --concurrency N (--requests N | --duration-s N)\n"
                 "  --host H --port P --payload-bytes N --out PATH --no-inproc\n");
}

bool parse_args(int argc, char** argv, Args& out) {
    static struct option longs[] = {
        {"host", required_argument, nullptr, 'h'},
        {"port", required_argument, nullptr, 'p'},
        {"concurrency", required_argument, nullptr, 'c'},
        {"requests", required_argument, nullptr, 'r'},
        {"duration-s", required_argument, nullptr, 'd'},
        {"payload-bytes", required_argument, nullptr, 'b'},
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
            case 'r':
                out.requests = std::atoi(optarg);
                break;
            case 'd':
                out.duration_s = std::atoi(optarg);
                break;
            case 'b':
                out.payload_bytes = std::atoi(optarg);
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
    if (out.requests == 0 && out.duration_s == 0) {
        out.duration_s = 5;
    }
    return true;
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

    std::uint16_t target_port = args.port;

    if (args.spawn_inproc) {
        if (!backend.start()) {
            std::fprintf(stderr, "backend start failed\n");
            return 1;
        }
        ir::BackendPool::Options bopts;
        bopts.host = "127.0.0.1";
        bopts.port = backend.port;
        bopts.max_size = 32;
        bopts.enable_health_check = false;
        bpool = std::make_unique<ir::BackendPool>(bopts, &metrics);

        ir::ThreadPool::Options topts;
        topts.worker_count = 8;
        topts.max_queue_depth = 4096;
        tpool = std::make_unique<ir::ThreadPool>(topts);

        ir::HandlerOptions hopts;
        ir::Acceptor::Options aopts;
        aopts.host = "127.0.0.1";
        aopts.port = 0;
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

    std::printf("bench: target=%s:%u concurrency=%d requests=%d duration_s=%d payload=%dB\n",
                args.host.c_str(), target_port, args.concurrency, args.requests, args.duration_s,
                args.payload_bytes);

    std::atomic<std::uint64_t> total_ok{0};
    std::atomic<std::uint64_t> total_err{0};
    std::vector<std::vector<std::uint64_t>> per_thread_lat(
        static_cast<std::size_t>(args.concurrency));

    auto run_start = std::chrono::steady_clock::now();
    auto deadline = run_start + std::chrono::seconds(args.duration_s);

    std::vector<std::thread> workers;
    workers.reserve(static_cast<std::size_t>(args.concurrency));

    int per_thread_requests = (args.requests > 0) ? args.requests / args.concurrency : -1;

    for (int t = 0; t < args.concurrency; ++t) {
        workers.emplace_back([&, t, per_thread_requests] {
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
                    ++sent_local;
                    continue;
                }
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
    for (auto& w : workers) w.join();
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

    std::printf(
        "ok=%llu err=%llu ms=%lld throughput_rps=%.1f "
        "p50_us=%llu p95_us=%llu p99_us=%llu p999_us=%llu\n",
        (unsigned long long)total_ok.load(), (unsigned long long)total_err.load(),
        (long long)total_ms, throughput, (unsigned long long)pct(0.50),
        (unsigned long long)pct(0.95), (unsigned long long)pct(0.99),
        (unsigned long long)pct(0.999));

    std::ofstream out(args.out_path);
    if (out) {
        out << "{\n";
        out << "  \"schema\": \"inference-router.bench.v1\",\n";
        out << "  \"ok\": " << total_ok.load() << ",\n";
        out << "  \"err\": " << total_err.load() << ",\n";
        out << "  \"total_ms\": " << total_ms << ",\n";
        out << "  \"throughput_rps\": " << throughput << ",\n";
        out << "  \"p50_us\": " << pct(0.50) << ",\n";
        out << "  \"p95_us\": " << pct(0.95) << ",\n";
        out << "  \"p99_us\": " << pct(0.99) << ",\n";
        out << "  \"p999_us\": " << pct(0.999) << ",\n";
        out << "  \"concurrency\": " << args.concurrency << ",\n";
        out << "  \"payload_bytes\": " << args.payload_bytes << "\n";
        out << "}\n";
    }
    return 0;
}
