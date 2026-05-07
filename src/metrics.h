#pragma once

#include <atomic>
#include <cstdint>
#include <string>

namespace ir {

// Process-wide counters. Atomic, lock-free on common platforms.
// `accepted` is the count of connections that the acceptor handed off to a worker.
// `completed` is the count of requests that received a response back to the client.
// `errored` is the count of requests where the worker reported an error to the client
//           (a response was still sent — these do NOT count as dropped).
// `dropped` is the count of requests the router gave up on without responding.
//           Under the documented shutdown protocol this MUST be zero unless the
//           grace deadline expired.
class Metrics {
  public:
    void inc_accepted() {
        accepted_.fetch_add(1, std::memory_order_relaxed);
    }
    void inc_completed() {
        completed_.fetch_add(1, std::memory_order_relaxed);
    }
    void inc_errored() {
        errored_.fetch_add(1, std::memory_order_relaxed);
    }
    void inc_dropped() {
        dropped_.fetch_add(1, std::memory_order_relaxed);
    }
    void inc_in_flight() {
        in_flight_.fetch_add(1, std::memory_order_relaxed);
    }
    void dec_in_flight() {
        in_flight_.fetch_sub(1, std::memory_order_relaxed);
    }
    void inc_pool_borrow() {
        pool_borrows_.fetch_add(1, std::memory_order_relaxed);
    }
    void inc_pool_create() {
        pool_creates_.fetch_add(1, std::memory_order_relaxed);
    }

    std::uint64_t accepted() const {
        return accepted_.load(std::memory_order_relaxed);
    }
    std::uint64_t completed() const {
        return completed_.load(std::memory_order_relaxed);
    }
    std::uint64_t errored() const {
        return errored_.load(std::memory_order_relaxed);
    }
    std::uint64_t dropped() const {
        return dropped_.load(std::memory_order_relaxed);
    }
    std::uint64_t in_flight() const {
        return in_flight_.load(std::memory_order_relaxed);
    }
    std::uint64_t pool_borrows() const {
        return pool_borrows_.load(std::memory_order_relaxed);
    }
    std::uint64_t pool_creates() const {
        return pool_creates_.load(std::memory_order_relaxed);
    }

    std::string snapshot_string() const;

  private:
    std::atomic<std::uint64_t> accepted_{0};
    std::atomic<std::uint64_t> completed_{0};
    std::atomic<std::uint64_t> errored_{0};
    std::atomic<std::uint64_t> dropped_{0};
    std::atomic<std::uint64_t> in_flight_{0};
    std::atomic<std::uint64_t> pool_borrows_{0};
    std::atomic<std::uint64_t> pool_creates_{0};
};

}  // namespace ir
