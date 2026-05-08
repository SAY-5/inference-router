#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "thread_pool.h"

TEST(ThreadPool, RunsSubmittedTasks) {
    ir::ThreadPool::Options opts;
    opts.worker_count = 4;
    opts.max_queue_depth = 16;
    ir::ThreadPool pool(opts);

    std::atomic<int> count{0};
    constexpr int kN = 100;
    for (int i = 0; i < kN; ++i) {
        ASSERT_TRUE(pool.submit([&] { count.fetch_add(1); }));
    }
    pool.stop();
    EXPECT_EQ(count.load(), kN);
}

TEST(ThreadPool, RejectsWhenQueueFullIfConfigured) {
    ir::ThreadPool::Options opts;
    opts.worker_count = 1;
    opts.max_queue_depth = 2;
    opts.reject_on_full = true;
    ir::ThreadPool pool(opts);

    std::atomic<int> running{0};
    std::atomic<bool> release{false};
    auto block = [&] {
        running.fetch_add(1);
        while (!release.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    };
    // Worker is blocked.
    ASSERT_TRUE(pool.submit(block));
    while (running.load() == 0) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    // Fill queue.
    ASSERT_TRUE(pool.submit([] {}));
    ASSERT_TRUE(pool.submit([] {}));
    // Now full.
    EXPECT_FALSE(pool.submit([] {}));

    release.store(true);
    pool.stop();
}

TEST(ThreadPool, BlocksOnFullByDefault) {
    ir::ThreadPool::Options opts;
    opts.worker_count = 1;
    opts.max_queue_depth = 1;
    opts.reject_on_full = false;
    ir::ThreadPool pool(opts);

    std::atomic<int> running{0};
    std::atomic<bool> release{false};
    auto block = [&] {
        running.fetch_add(1);
        while (!release.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    };
    ASSERT_TRUE(pool.submit(block));
    while (running.load() == 0) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    ASSERT_TRUE(pool.submit([] {}));  // queue depth 1, fills

    std::atomic<bool> third_done{false};
    std::thread t([&] {
        ASSERT_TRUE(pool.submit([] {}));
        third_done.store(true);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_FALSE(third_done.load());  // submit() should be blocked

    release.store(true);
    t.join();
    EXPECT_TRUE(third_done.load());
    pool.stop();
}

TEST(ThreadPool, HighContention50x100Submit) {
    // TSan stress: 50 producer threads each push 100 tasks into a small bounded queue
    // serviced by 8 workers. Any data race in queue_/not_empty_/not_full_/stop_ shows
    // up here when the test binary is built with -fsanitize=thread.
    ir::ThreadPool::Options opts;
    opts.worker_count = 8;
    opts.max_queue_depth = 64;  // forces blocking back-pressure
    ir::ThreadPool pool(opts);

    constexpr int kProducers = 50;
    constexpr int kPer = 100;
    std::atomic<int> ran{0};
    std::vector<std::thread> producers;
    producers.reserve(kProducers);
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&] {
            for (int i = 0; i < kPer; ++i) {
                ASSERT_TRUE(pool.submit([&] { ran.fetch_add(1, std::memory_order_relaxed); }));
            }
        });
    }
    for (auto& t : producers) t.join();
    pool.stop();
    EXPECT_EQ(ran.load(), kProducers * kPer);
}

TEST(ThreadPool, StopDrainsQueuedTasks) {
    ir::ThreadPool::Options opts;
    opts.worker_count = 2;
    opts.max_queue_depth = 64;
    ir::ThreadPool pool(opts);

    std::atomic<int> count{0};
    for (int i = 0; i < 64; ++i) {
        ASSERT_TRUE(pool.submit([&] {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            count.fetch_add(1);
        }));
    }
    pool.stop();
    EXPECT_EQ(count.load(), 64);
}
