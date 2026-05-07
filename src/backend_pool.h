#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace ir {

class Metrics;

// A connection pool to a fixed (host, port). Thread-safe.
//
// Lifecycle of a connection:
//   create -> [idle] <-> [in-use] -> close (on idle_timeout, max_lifetime, health_check fail,
//                                            return-with-error, or shutdown).
// Health check: a goroutine-equivalent thread runs every `health_check_interval` and
//   sends a zero-length message to each idle conn; the backend stub echoes a zero-length
//   reply. Conns that fail are closed and removed from the idle set.
class BackendPool {
  public:
    struct Options {
        std::string host = "127.0.0.1";
        std::uint16_t port = 0;
        std::size_t max_size = 16;
        std::size_t min_idle = 2;
        std::chrono::milliseconds idle_timeout{60'000};
        std::chrono::milliseconds max_lifetime{5 * 60'000};
        std::chrono::milliseconds health_check_interval{10'000};
        std::chrono::milliseconds connect_timeout{2'000};
        std::chrono::milliseconds borrow_timeout{2'000};
        bool enable_health_check = true;
    };

    BackendPool(Options opts, Metrics* metrics);
    ~BackendPool();

    BackendPool(const BackendPool&) = delete;
    BackendPool& operator=(const BackendPool&) = delete;

    // Borrow an idle connection or create a new one if under max_size.
    // Returns -1 (errno set) on dial failure or borrow_timeout.
    int borrow();

    // Return the conn. If `ok=false` the conn is closed and forgotten.
    void release(int fd, bool ok);

    // Drain & close all idle conns. Future borrows will fail.
    // In-use conns are NOT touched — the caller must release() them; once released
    // (with any ok flag) they will be closed.
    void shutdown();

    std::size_t idle_count() const;
    std::size_t in_use_count() const;
    bool is_shutting_down() const {
        return shutting_down_.load(std::memory_order_acquire);
    }

  private:
    struct Slot {
        int fd = -1;
        std::chrono::steady_clock::time_point created;
        std::chrono::steady_clock::time_point last_used;
    };

    int dial_one_();
    bool health_ping_(int fd);
    void reaper_main_();
    void close_idle_(std::deque<Slot>& victims);

    Options opts_;
    Metrics* metrics_;

    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::deque<Slot> idle_;
    std::size_t in_use_ = 0;
    std::atomic<bool> shutting_down_{false};

    std::thread reaper_;
    std::condition_variable reaper_cv_;
};

}  // namespace ir
