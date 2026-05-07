#include "thread_pool.h"

#include <algorithm>

namespace ir {

ThreadPool::ThreadPool(Options opts) : opts_(opts) {
    if (opts_.worker_count == 0) opts_.worker_count = 1;
    if (opts_.max_queue_depth == 0) opts_.max_queue_depth = 1;
    workers_.reserve(opts_.worker_count);
    for (std::size_t i = 0; i < opts_.worker_count; ++i) {
        workers_.emplace_back([this] { worker_main(); });
    }
}

ThreadPool::~ThreadPool() {
    stop();
}

bool ThreadPool::submit(Task task) {
    std::unique_lock<std::mutex> lock(mu_);
    if (stop_.load(std::memory_order_acquire)) return false;
    if (opts_.reject_on_full) {
        if (queue_.size() >= opts_.max_queue_depth) return false;
    } else {
        not_full_.wait(lock, [&] {
            return queue_.size() < opts_.max_queue_depth || stop_.load(std::memory_order_acquire);
        });
        if (stop_.load(std::memory_order_acquire)) return false;
    }
    queue_.emplace_back(std::move(task));
    not_empty_.notify_one();
    return true;
}

void ThreadPool::stop() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (stop_.load(std::memory_order_acquire)) return;
        stop_.store(true, std::memory_order_release);
        drain_on_stop_.store(true, std::memory_order_release);
    }
    not_empty_.notify_all();
    not_full_.notify_all();
    for (auto& t : workers_) {
        if (t.joinable()) t.join();
    }
}

void ThreadPool::stop_no_drain() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (stop_.load(std::memory_order_acquire)) return;
        stop_.store(true, std::memory_order_release);
        drain_on_stop_.store(false, std::memory_order_release);
        queue_.clear();
    }
    not_empty_.notify_all();
    not_full_.notify_all();
    for (auto& t : workers_) {
        if (t.joinable()) t.join();
    }
}

std::size_t ThreadPool::pending() const {
    std::lock_guard<std::mutex> lock(mu_);
    return queue_.size();
}

void ThreadPool::worker_main() {
    while (true) {
        Task task;
        {
            std::unique_lock<std::mutex> lock(mu_);
            not_empty_.wait(
                lock, [&] { return !queue_.empty() || stop_.load(std::memory_order_acquire); });
            if (queue_.empty()) {
                // stop_ is set
                if (!drain_on_stop_.load(std::memory_order_acquire)) return;
                return;
            }
            task = std::move(queue_.front());
            queue_.pop_front();
            not_full_.notify_one();
        }
        try {
            task();
        } catch (...) {
            // Tasks must not throw. If one does, swallow it — the alternative is
            // crashing the worker mid-shutdown.
        }
    }
}

}  // namespace ir
