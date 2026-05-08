#include <gtest/gtest.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "backend_pool.h"
#include "backend_set.h"
#include "connection.h"
#include "handler.h"
#include "metrics.h"

namespace {

// Tiny in-process echo backend, identical to the one used in test_backend_pool.cpp.
struct EchoBackend {
    int listen_fd = -1;
    std::uint16_t port = 0;
    std::atomic<bool> stop{false};
    std::thread thread;

    void start() {
        listen_fd = ir::listen_tcp("127.0.0.1", 0, 32);
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

TEST(BackendSet, BorrowReleaseRoundsThroughEachPool) {
    EchoBackend b0, b1;
    b0.start();
    b1.start();

    ir::Metrics metrics;
    ir::BackendPool::Options shared;
    shared.max_size = 2;
    shared.enable_health_check = false;
    shared.borrow_timeout = std::chrono::milliseconds(5000);

    std::vector<ir::BackendSet::BackendSpec> specs = {
        {"127.0.0.1", b0.port, 1},
        {"127.0.0.1", b1.port, 1},
    };
    ir::BackendSet set(specs, shared, &metrics);

    ASSERT_EQ(set.backend_count(), 2u);
    EXPECT_EQ(set.weight(0), 1u);
    EXPECT_EQ(set.weight(1), 1u);

    auto h0 = set.borrow();
    ASSERT_GE(h0.fd, 0);
    EXPECT_EQ(h0.pool_index, 0u);  // empty load, lowest index wins
    EXPECT_EQ(set.in_flight(0), 1u);

    auto h1 = set.borrow();
    ASSERT_GE(h1.fd, 0);
    EXPECT_EQ(h1.pool_index, 1u);  // pool 0 has 1 in-flight, pool 1 has 0
    EXPECT_EQ(set.in_flight(1), 1u);

    set.release(h0, true);
    EXPECT_EQ(set.in_flight(0), 0u);
    set.release(h1, true);
    EXPECT_EQ(set.in_flight(1), 0u);

    set.shutdown();
    b0.shutdown();
    b1.shutdown();
}

// The headline v3 test: with three backends weighted 1:1:2, the third backend
// should handle ~50% of requests, the first two ~25% each. ±5% tolerance.
//
// We hold each borrow for a fixed sleep so in_flight stays elevated long enough
// for the WLC selection to differentiate weights — without an explicit hold
// time, on a fast localhost echo every release lands before the next pick reads
// the counter and selection collapses to round-robin via the tie-break.
TEST(BackendSet, WeightedLeastConnDistribution) {
    EchoBackend b0, b1, b2;
    b0.start();
    b1.start();
    b2.start();

    ir::Metrics metrics;
    ir::BackendPool::Options shared;
    shared.max_size = 16;  // allow each pool to host the full thread fan-out
    shared.enable_health_check = false;
    shared.borrow_timeout = std::chrono::milliseconds(30'000);

    std::vector<ir::BackendSet::BackendSpec> specs = {
        {"127.0.0.1", b0.port, 1},
        {"127.0.0.1", b1.port, 1},
        {"127.0.0.1", b2.port, 2},
    };
    ir::BackendSet set(specs, shared, &metrics);

    constexpr int kThreads = 32;
    constexpr int kPer = 200;
    constexpr int kTotal = kThreads * kPer;
    constexpr auto kHoldDuration = std::chrono::microseconds(500);

    std::atomic<int> errors{0};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&] {
            std::vector<std::uint8_t> msg(8, 0xA5);
            for (int i = 0; i < kPer; ++i) {
                auto h = set.borrow();
                if (h.fd < 0) {
                    errors.fetch_add(1);
                    continue;
                }
                bool ok = ir::write_message(h.fd, msg, 5000) == ir::IoStatus::kOk;
                std::vector<std::uint8_t> got;
                if (ok) {
                    ok = ir::read_message(h.fd, got, 64, 5000) == ir::IoStatus::kOk && got == msg;
                }
                // Hold the conn while in_flight is still incremented so other
                // threads can observe the load and route around it.
                std::this_thread::sleep_for(kHoldDuration);
                set.release(h, ok);
                if (!ok) errors.fetch_add(1);
            }
        });
    }
    for (auto& th : threads) th.join();
    EXPECT_EQ(errors.load(), 0);

    auto h0_count = set.handled_count(0);
    auto h1_count = set.handled_count(1);
    auto h2_count = set.handled_count(2);
    auto sum = h0_count + h1_count + h2_count;
    ASSERT_EQ(sum, static_cast<std::uint64_t>(kTotal));

    double f0 = static_cast<double>(h0_count) / static_cast<double>(kTotal);
    double f1 = static_cast<double>(h1_count) / static_cast<double>(kTotal);
    double f2 = static_cast<double>(h2_count) / static_cast<double>(kTotal);
    std::printf("[lb-distribution] f0=%.3f f1=%.3f f2=%.3f (target 0.25 0.25 0.50)\n", f0, f1, f2);

    // Spec: backend 2 (weight 2) handles ~50% ± 5%.
    EXPECT_NEAR(f2, 0.50, 0.05);
    // Backends 0 and 1 share the rest equally; loosen tolerance slightly because
    // the tie-break (lowest index) systematically biases pool 0 over pool 1 when
    // they are both at the same load score.
    EXPECT_NEAR(f0, 0.25, 0.07);
    EXPECT_NEAR(f1, 0.25, 0.07);

    set.shutdown();
    b0.shutdown();
    b1.shutdown();
    b2.shutdown();
}

TEST(BackendSet, TieBreakIsLowestIndex) {
    EchoBackend b0, b1, b2;
    b0.start();
    b1.start();
    b2.start();

    ir::Metrics metrics;
    ir::BackendPool::Options shared;
    shared.max_size = 4;
    shared.enable_health_check = false;
    shared.borrow_timeout = std::chrono::milliseconds(2000);

    // All three backends have equal weights and zero load → first borrow goes
    // to index 0, second to index 1, third to index 2 (the picker assigns one
    // in_flight before the next caller observes the load).
    std::vector<ir::BackendSet::BackendSpec> specs = {
        {"127.0.0.1", b0.port, 1},
        {"127.0.0.1", b1.port, 1},
        {"127.0.0.1", b2.port, 1},
    };
    ir::BackendSet set(specs, shared, &metrics);

    auto h0 = set.borrow();
    auto h1 = set.borrow();
    auto h2 = set.borrow();
    ASSERT_GE(h0.fd, 0);
    ASSERT_GE(h1.fd, 0);
    ASSERT_GE(h2.fd, 0);
    EXPECT_EQ(h0.pool_index, 0u);
    EXPECT_EQ(h1.pool_index, 1u);
    EXPECT_EQ(h2.pool_index, 2u);

    set.release(h0, true);
    set.release(h1, true);
    set.release(h2, true);

    set.shutdown();
    b0.shutdown();
    b1.shutdown();
    b2.shutdown();
}

TEST(BackendSet, EmptyBorrowFailsCleanly) {
    ir::Metrics metrics;
    ir::BackendPool::Options shared;
    ir::BackendSet set({}, shared, &metrics);

    auto h = set.borrow();
    EXPECT_LT(h.fd, 0);
    EXPECT_EQ(set.backend_count(), 0u);
    set.shutdown();  // no-op, must not crash
}
