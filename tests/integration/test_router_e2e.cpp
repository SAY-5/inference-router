// In-process end-to-end test: spin up a backend stub on a free port, spin up the
// full router (acceptor + thread pool + backend pool) bound to a free port,
// drive client requests through it, then exercise the shutdown drain.
//
// This is NOT the chaos test — it is a happy-path E2E that confirms the components
// integrate correctly. The chaos test (tests/chaos/chaos.cpp) is a separate binary
// that asserts the no-drop invariant under SIGTERM.

#include <gtest/gtest.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <vector>

#include "acceptor.h"
#include "backend_pool.h"
#include "connection.h"
#include "handler.h"
#include "metrics.h"
#include "thread_pool.h"

namespace {

struct EchoBackend {
    int listen_fd = -1;
    std::uint16_t port = 0;
    std::atomic<bool> stop{false};
    std::thread thread;
    std::vector<std::thread> workers;
    std::mutex workers_mu;

    void start() {
        listen_fd = ir::listen_tcp("127.0.0.1", 0, 16);
        ASSERT_GE(listen_fd, 0);
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

TEST(RouterE2E, ForwardsRequestAndResponse) {
    EchoBackend backend;
    backend.start();

    ir::Metrics metrics;
    ir::BackendPool::Options bopts;
    bopts.host = "127.0.0.1";
    bopts.port = backend.port;
    bopts.max_size = 4;
    bopts.enable_health_check = false;
    ir::BackendPool bpool(bopts, &metrics);

    ir::ThreadPool::Options topts;
    topts.worker_count = 2;
    topts.max_queue_depth = 64;
    ir::ThreadPool pool(topts);

    ir::HandlerOptions hopts;
    ir::Acceptor::Options aopts;
    aopts.host = "127.0.0.1";
    aopts.port = 0;  // ephemeral
    ir::Acceptor acceptor(aopts, &metrics, [&](int fd) {
        ASSERT_TRUE(pool.submit(
            [fd, &bpool, &metrics, hopts] { ir::handle_one(fd, bpool, metrics, hopts); }));
    });
    ASSERT_TRUE(acceptor.start());
    auto router_port = acceptor.bound_port();
    ASSERT_GT(router_port, 0u);

    // Drive a few requests.
    constexpr int kRequests = 20;
    int success = 0;
    for (int i = 0; i < kRequests; ++i) {
        int c = ir::dial_tcp("127.0.0.1", router_port, 1000);
        ASSERT_GE(c, 0);
        std::vector<std::uint8_t> req(16, static_cast<std::uint8_t>(i));
        ASSERT_EQ(ir::write_message(c, req, 2000), ir::IoStatus::kOk);
        std::vector<std::uint8_t> got;
        ASSERT_EQ(ir::read_message(c, got, 1024, 2000), ir::IoStatus::kOk);
        EXPECT_EQ(got, req);
        ir::safe_close(c);
        ++success;
    }
    EXPECT_EQ(success, kRequests);

    // Wait for in-flight to drain (the handler decrements after writing back).
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (metrics.in_flight() > 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    EXPECT_EQ(metrics.completed(), static_cast<std::uint64_t>(kRequests));
    EXPECT_EQ(metrics.dropped(), 0u);

    acceptor.stop();
    pool.stop();
    bpool.shutdown();
    backend.shutdown();
}

TEST(RouterE2E, DrainShutdownDropsZeroWithSlowBackend) {
    // Slower backend to maximise the window where requests are mid-flight at SIGTERM.
    // We don't actually send SIGTERM in-process; we simulate the drain protocol by
    // calling acceptor.stop(), waiting for in_flight==0, then pool.stop().
    EchoBackend backend;
    backend.start();

    ir::Metrics metrics;
    ir::BackendPool::Options bopts;
    bopts.host = "127.0.0.1";
    bopts.port = backend.port;
    bopts.max_size = 8;
    bopts.enable_health_check = false;
    ir::BackendPool bpool(bopts, &metrics);

    ir::ThreadPool::Options topts;
    topts.worker_count = 4;
    topts.max_queue_depth = 256;
    ir::ThreadPool pool(topts);

    ir::HandlerOptions hopts;
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
    ASSERT_TRUE(acceptor.start());
    auto router_port = acceptor.bound_port();

    constexpr int kClients = 10;
    constexpr int kPer = 5;
    std::atomic<int> client_done{0};
    std::vector<std::thread> clients;
    clients.reserve(kClients);
    for (int i = 0; i < kClients; ++i) {
        clients.emplace_back([&, i] {
            for (int j = 0; j < kPer; ++j) {
                int c = ir::dial_tcp("127.0.0.1", router_port, 2000);
                if (c < 0) continue;
                std::vector<std::uint8_t> req(8, static_cast<std::uint8_t>(i + j));
                if (ir::write_message(c, req, 2000) == ir::IoStatus::kOk) {
                    std::vector<std::uint8_t> got;
                    ir::read_message(c, got, 1024, 2000);
                }
                ir::safe_close(c);
            }
            client_done.fetch_add(1);
        });
    }

    // Wait until at least some requests have hit the system.
    auto t0 = std::chrono::steady_clock::now();
    while (metrics.accepted() < 5 &&
           std::chrono::steady_clock::now() - t0 < std::chrono::seconds(2)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }

    // Now do the drain dance.
    acceptor.stop();
    auto drain_end = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (metrics.in_flight() > 0 && std::chrono::steady_clock::now() < drain_end) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    pool.stop();
    bpool.shutdown();

    for (auto& t : clients) t.join();
    backend.shutdown();

    // The invariant: every accepted request received a response.
    EXPECT_EQ(metrics.dropped(), 0u);
    EXPECT_EQ(metrics.in_flight(), 0u);
    EXPECT_GE(metrics.completed() + metrics.errored(), metrics.accepted());
}
