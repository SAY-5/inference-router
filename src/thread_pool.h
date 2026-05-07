#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace ir {

// A bounded MPMC thread pool. Tasks are std::function<void()> for simplicity.
//
// Design notes:
//   - We use a std::deque + std::mutex + std::condition_variable rather than a lock-free
//     queue. The hot-path here is request handling, which is dominated by syscall latency
//     (read/write/recv/send) and a backend round-trip — at that scale, mutex contention
//     on a single queue is below the noise floor. Lock-free MPMC queues exist (Vyukov,
//     moodycamel) but they trade a great deal of subtle correctness for a marginal win
//     we cannot measure in this workload. See docs/reactor.md.
//   - submit() blocks if the queue is full and `reject_on_full=false`; otherwise returns
//     false immediately. Default behaviour is "block on full" because dropping a queued
//     request would violate the no-drop invariant the project is built around.
class ThreadPool {
  public:
    using Task = std::function<void()>;

    struct Options {
        std::size_t worker_count = 4;
        std::size_t max_queue_depth = 1024;
        bool reject_on_full = false;
    };

    explicit ThreadPool(Options opts);
    ~ThreadPool();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    // Push a task. Returns false only if `reject_on_full` is set AND the queue is full,
    // or if stop() has been called.
    bool submit(Task task);

    // Stop accepting new work, drain the queue, join workers.
    void stop();

    // Stop accepting new work but DO NOT drain the queue. Already-popped tasks finish.
    // Used during shutdown when we want to know in_flight has hit zero.
    void stop_no_drain();

    std::size_t pending() const;
    std::size_t worker_count() const {
        return workers_.size();
    }
    bool stopped() const {
        return stop_.load(std::memory_order_acquire);
    }

  private:
    void worker_main();

    Options opts_;
    mutable std::mutex mu_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
    std::deque<Task> queue_;
    std::vector<std::thread> workers_;
    std::atomic<bool> stop_{false};
    std::atomic<bool> drain_on_stop_{true};
};

}  // namespace ir
