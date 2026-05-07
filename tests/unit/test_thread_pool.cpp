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
