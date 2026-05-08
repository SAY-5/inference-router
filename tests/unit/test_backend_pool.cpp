#include <gtest/gtest.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "backend_pool.h"
#include "connection.h"
#include "metrics.h"

namespace {

// Run a tiny in-process echo backend. Returns (port, stop_fn).
struct EchoBackend {
    int listen_fd = -1;
    std::uint16_t port = 0;
    std::atomic<bool> stop{false};
    std::thread thread;

    void start() {
        listen_fd = ir::listen_tcp("127.0.0.1", 0, 8);
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
                std::thread([c] {
                    // 60s timeouts so a slow scheduler under high contention won't
                    // make the echo worker quit mid-stream. Tests close conns at
                    // teardown via pool.shutdown(), which surfaces as kPeerClosed.
                    while (true) {
                        std::vector<std::uint8_t> msg;
                        auto rc = ir::read_message(c, msg, 64 * 1024, 60'000);
                        if (rc != ir::IoStatus::kOk) break;
                        if (ir::write_message(c, msg, 60'000) != ir::IoStatus::kOk) break;
                    }
                    ir::safe_close(c);
                }).detach();
            }
            ir::safe_close(listen_fd);
        });
    }
    void shutdown() {
        stop.store(true);
        if (thread.joinable()) thread.join();
    }
};

}  // namespace

TEST(BackendPool, BorrowReleaseRoundTrip) {
    EchoBackend backend;
    backend.start();

    ir::Metrics metrics;
    ir::BackendPool::Options opts;
    opts.host = "127.0.0.1";
    opts.port = backend.port;
    opts.max_size = 4;
    opts.min_idle = 0;
    opts.enable_health_check = false;
    opts.health_check_interval = std::chrono::milliseconds(60'000);
    ir::BackendPool pool(opts, &metrics);

    int fd = pool.borrow();
    ASSERT_GE(fd, 0);
    EXPECT_EQ(pool.in_use_count(), 1u);
    pool.release(fd, true);
    EXPECT_EQ(pool.in_use_count(), 0u);
    EXPECT_EQ(pool.idle_count(), 1u);

    int fd2 = pool.borrow();
    EXPECT_EQ(fd2, fd);  // reused
    pool.release(fd2, true);

    pool.shutdown();
    backend.shutdown();
}

TEST(BackendPool, FullPoolBlocksUntilRelease) {
    EchoBackend backend;
    backend.start();

    ir::Metrics metrics;
    ir::BackendPool::Options opts;
    opts.host = "127.0.0.1";
    opts.port = backend.port;
    opts.max_size = 1;
    opts.min_idle = 0;
    opts.enable_health_check = false;
    opts.borrow_timeout = std::chrono::milliseconds(2000);
    opts.health_check_interval = std::chrono::milliseconds(60'000);
    ir::BackendPool pool(opts, &metrics);

    int fd = pool.borrow();
    ASSERT_GE(fd, 0);

    std::atomic<int> second{-2};
    std::thread t([&] { second.store(pool.borrow()); });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(second.load(), -2);  // still blocked

    pool.release(fd, true);
    t.join();
    EXPECT_GE(second.load(), 0);
    pool.release(second.load(), true);

    pool.shutdown();
    backend.shutdown();
}

TEST(BackendPool, BorrowTimeoutFires) {
    EchoBackend backend;
    backend.start();

    ir::Metrics metrics;
    ir::BackendPool::Options opts;
    opts.host = "127.0.0.1";
    opts.port = backend.port;
    opts.max_size = 1;
    opts.min_idle = 0;
    opts.enable_health_check = false;
    opts.borrow_timeout = std::chrono::milliseconds(80);
    opts.health_check_interval = std::chrono::milliseconds(60'000);
    ir::BackendPool pool(opts, &metrics);

    int held = pool.borrow();
    ASSERT_GE(held, 0);
    int second = pool.borrow();
    EXPECT_EQ(second, -1);

    pool.release(held, true);
    pool.shutdown();
    backend.shutdown();
}

TEST(BackendPool, ReleaseWithErrorClosesAndDoesNotIdle) {
    EchoBackend backend;
    backend.start();

    ir::Metrics metrics;
    ir::BackendPool::Options opts;
    opts.host = "127.0.0.1";
    opts.port = backend.port;
    opts.max_size = 4;
    opts.min_idle = 0;
    opts.enable_health_check = false;
    opts.health_check_interval = std::chrono::milliseconds(60'000);
    ir::BackendPool pool(opts, &metrics);

    int fd = pool.borrow();
    ASSERT_GE(fd, 0);
    pool.release(fd, false);
    EXPECT_EQ(pool.idle_count(), 0u);

    pool.shutdown();
    backend.shutdown();
}

TEST(BackendPool, ConcurrentBorrowReleaseUnderContention) {
    EchoBackend backend;
    backend.start();

    ir::Metrics metrics;
    ir::BackendPool::Options opts;
    opts.host = "127.0.0.1";
    opts.port = backend.port;
    opts.max_size = 4;
    opts.min_idle = 0;
    opts.enable_health_check = false;
    opts.borrow_timeout = std::chrono::milliseconds(30'000);
    opts.health_check_interval = std::chrono::milliseconds(60'000);
    ir::BackendPool pool(opts, &metrics);

    constexpr int kThreads = 16;
    constexpr int kPer = 50;
    std::atomic<int> errors{0};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&] {
            for (int j = 0; j < kPer; ++j) {
                int fd = pool.borrow();
                if (fd < 0) {
                    errors.fetch_add(1);
                    continue;
                }
                std::vector<std::uint8_t> msg = {'p', 'i', 'n', 'g'};
                auto wr = ir::write_message(fd, msg, 5000);
                std::vector<std::uint8_t> got;
                auto rd = ir::read_message(fd, got, 64, 5000);
                bool ok = (wr == ir::IoStatus::kOk) && (rd == ir::IoStatus::kOk) && got == msg;
                pool.release(fd, ok);
                if (!ok) errors.fetch_add(1);
            }
        });
    }
    for (auto& t : threads) t.join();
    EXPECT_EQ(errors.load(), 0);

    pool.shutdown();
    backend.shutdown();
}

// High-contention TSan stress: 50 client threads × 100 ops each, racing on the
// connection-pool primitives. Pool is sized generously (16 slots) so the test
// exercises mu_/cv_/idle_/in_use_ under load without backend-throughput becoming
// the bottleneck. Under TSan, any data race in the pool surfaces here.
TEST(BackendPool, HighContention50x100UnderTSanStress) {
    EchoBackend backend;
    backend.start();

    ir::Metrics metrics;
    ir::BackendPool::Options opts;
    opts.host = "127.0.0.1";
    opts.port = backend.port;
    opts.max_size = 16;
    opts.min_idle = 0;
    opts.enable_health_check = false;
    opts.borrow_timeout = std::chrono::milliseconds(30'000);
    opts.health_check_interval = std::chrono::milliseconds(60'000);
    ir::BackendPool pool(opts, &metrics);

    constexpr int kThreads = 50;
    constexpr int kPer = 100;
    std::atomic<int> errors{0};
    std::atomic<int> ok{0};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&, i] {
            for (int j = 0; j < kPer; ++j) {
                int fd = pool.borrow();
                if (fd < 0) {
                    errors.fetch_add(1);
                    continue;
                }
                std::vector<std::uint8_t> msg = {'p', 'i', static_cast<std::uint8_t>(i & 0xFF),
                                                 static_cast<std::uint8_t>(j & 0xFF)};
                auto wr = ir::write_message(fd, msg, 5000);
                std::vector<std::uint8_t> got;
                auto rd = ir::read_message(fd, got, 64, 5000);
                bool round_trip_ok =
                    (wr == ir::IoStatus::kOk) && (rd == ir::IoStatus::kOk) && got == msg;
                pool.release(fd, round_trip_ok);
                if (round_trip_ok) {
                    ok.fetch_add(1);
                } else {
                    errors.fetch_add(1);
                }
            }
        });
    }
    for (auto& t : threads) t.join();
    EXPECT_EQ(errors.load(), 0);
    EXPECT_EQ(ok.load(), kThreads * kPer);

    pool.shutdown();
    backend.shutdown();
}
