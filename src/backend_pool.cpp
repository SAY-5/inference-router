#include "backend_pool.h"

#include <errno.h>

#include "connection.h"
#include "log.h"
#include "metrics.h"

namespace ir {

BackendPool::BackendPool(Options opts, Metrics* metrics)
    : opts_(std::move(opts)), metrics_(metrics) {
    if (opts_.max_size == 0) opts_.max_size = 1;
    if (opts_.min_idle > opts_.max_size) opts_.min_idle = opts_.max_size;
    reaper_ = std::thread([this] { reaper_main_(); });
}

BackendPool::~BackendPool() {
    shutdown();
    if (reaper_.joinable()) reaper_.join();
}

int BackendPool::borrow() {
    using clock = std::chrono::steady_clock;
    auto deadline = clock::now() + opts_.borrow_timeout;

    std::unique_lock<std::mutex> lock(mu_);
    while (true) {
        if (shutting_down_.load(std::memory_order_acquire)) {
            errno = ECONNABORTED;
            return -1;
        }
        if (!idle_.empty()) {
            Slot s = idle_.front();
            idle_.pop_front();
            ++in_use_;
            if (metrics_) metrics_->inc_pool_borrow();
            return s.fd;
        }
        if (in_use_ + idle_.size() < opts_.max_size) {
            // Drop the lock to dial — connecting can take long enough to matter.
            ++in_use_;
            lock.unlock();
            int fd = dial_one_();
            if (fd < 0) {
                lock.lock();
                --in_use_;
                cv_.notify_one();
                return -1;
            }
            if (metrics_) {
                metrics_->inc_pool_create();
                metrics_->inc_pool_borrow();
            }
            return fd;
        }
        // Pool is full. Wait for a release().
        if (cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
            errno = ETIMEDOUT;
            return -1;
        }
    }
}

void BackendPool::release(int fd, bool ok) {
    if (fd < 0) return;
    bool close_it = !ok || shutting_down_.load(std::memory_order_acquire);
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (in_use_ > 0) --in_use_;
        if (close_it) {
            // Will close below, after dropping the lock.
        } else {
            Slot s;
            s.fd = fd;
            s.last_used = std::chrono::steady_clock::now();
            s.created = s.last_used;  // approximate; recycler watches last_used + max_lifetime
            idle_.push_back(s);
        }
        cv_.notify_one();
    }
    if (close_it) safe_close(fd);
}

void BackendPool::shutdown() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (shutting_down_.load(std::memory_order_acquire)) return;
        shutting_down_.store(true, std::memory_order_release);
        // Close all idle conns now; in-use ones close on release().
        while (!idle_.empty()) {
            safe_close(idle_.front().fd);
            idle_.pop_front();
        }
    }
    cv_.notify_all();
    reaper_cv_.notify_all();
}

std::size_t BackendPool::idle_count() const {
    std::lock_guard<std::mutex> lock(mu_);
    return idle_.size();
}

std::size_t BackendPool::in_use_count() const {
    std::lock_guard<std::mutex> lock(mu_);
    return in_use_;
}

int BackendPool::dial_one_() {
    int fd = dial_tcp(opts_.host, opts_.port, static_cast<int>(opts_.connect_timeout.count()));
    if (fd < 0) {
        IR_LOG_WARN("backend dial failed host=%s port=%u errno=%d", opts_.host.c_str(), opts_.port,
                    errno);
    }
    return fd;
}

bool BackendPool::health_ping_(int fd) {
    auto rc = write_message(fd, nullptr, 0, 1000);
    if (rc != IoStatus::kOk) return false;
    std::vector<std::uint8_t> reply;
    rc = read_message(fd, reply, kDefaultMaxPayload, 1000);
    if (rc != IoStatus::kOk) return false;
    // Backend echoes the zero-length payload back.
    return reply.empty();
}

void BackendPool::reaper_main_() {
    using clock = std::chrono::steady_clock;
    while (true) {
        std::unique_lock<std::mutex> lock(mu_);
        auto wait = std::min(opts_.health_check_interval, opts_.idle_timeout);
        if (wait < std::chrono::milliseconds(100)) wait = std::chrono::milliseconds(100);
        reaper_cv_.wait_for(lock, wait, [&] { return shutting_down_.load(); });
        if (shutting_down_.load()) return;

        auto now = clock::now();
        std::deque<Slot> victims;
        for (auto it = idle_.begin(); it != idle_.end();) {
            bool kill = false;
            if (now - it->last_used > opts_.idle_timeout) kill = true;
            if (now - it->created > opts_.max_lifetime) kill = true;
            if (kill) {
                victims.push_back(*it);
                it = idle_.erase(it);
            } else {
                ++it;
            }
        }
        // Health check: ping each remaining idle conn. We do this OUTSIDE the lock
        // because it does I/O. Pop them, ping, push back if healthy.
        std::deque<Slot> to_check;
        if (opts_.enable_health_check) {
            std::swap(to_check, idle_);
        }
        lock.unlock();

        close_idle_(victims);

        if (!to_check.empty()) {
            std::deque<Slot> survivors;
            std::deque<Slot> dead;
            for (auto& s : to_check) {
                if (health_ping_(s.fd)) {
                    s.last_used = clock::now();
                    survivors.push_back(s);
                } else {
                    dead.push_back(s);
                }
            }
            close_idle_(dead);
            std::lock_guard<std::mutex> lk(mu_);
            for (auto& s : survivors) idle_.push_back(s);
        }
    }
}

void BackendPool::close_idle_(std::deque<Slot>& victims) {
    while (!victims.empty()) {
        safe_close(victims.front().fd);
        victims.pop_front();
    }
}

}  // namespace ir
