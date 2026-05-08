#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "backend_pool.h"

namespace ir {

class Metrics;

// A weighted, multi-backend frontend over BackendPool.
//
// Each backend has a configurable `weight` (default 1) and its own BackendPool.
// On borrow(), the set selects the backend with the smallest "effective load",
// defined as `in_flight / weight`. Ties are broken by lowest backend index.
//
// Migration from round-robin
// --------------------------
// The old single-backend router (one --backend HOST:PORT flag) is exactly the
// degenerate case where the set has one pool with weight 1: borrow() always
// returns it, in_flight/1 is the only number that matters. Existing CLIs and
// configs continue to work; the multi-backend mode is opt-in via repeated
// --backend HOST:PORT[:WEIGHT] arguments.
//
// Concurrency
// -----------
// Selection (read all `in_flight` counters → pick the smallest score → bump
// the chosen counter) runs under a small mutex. Without it, two concurrent
// borrowers can read identical snapshots and both pick the same "lowest" pool,
// destroying the weighted distribution. The mutex is held only across the
// pick-and-increment; the actual pool->borrow() call (which may block on
// dial/idle queue) runs unlocked. Release decrements its counter atomically;
// it does not need the mutex.
class BackendSet {
  public:
    struct BackendSpec {
        std::string host;
        std::uint16_t port;
        std::size_t weight = 1;  // 0 is treated as 1.
    };

    struct Handle {
        int fd = -1;
        std::size_t pool_index = 0;  // valid only when fd >= 0
    };

    BackendSet(const std::vector<BackendSpec>& specs, BackendPool::Options shared_opts,
               Metrics* metrics);
    ~BackendSet();

    BackendSet(const BackendSet&) = delete;
    BackendSet& operator=(const BackendSet&) = delete;

    // Borrow a connection from the lowest-effective-load backend. Returns Handle{-1, 0} on failure.
    Handle borrow();

    // Return a connection to the pool it came from. `h.fd < 0` is a no-op.
    void release(Handle h, bool ok);

    // Drain all backend pools. Future borrows fail.
    void shutdown();

    // Counts (mostly for tests).
    std::size_t backend_count() const {
        return pools_.size();
    }
    std::size_t weight(std::size_t i) const {
        return weights_.at(i);
    }
    std::uint64_t handled_count(std::size_t i) const {
        return handled_[i].load(std::memory_order_relaxed);
    }
    std::uint64_t in_flight(std::size_t i) const {
        return in_flight_[i].load(std::memory_order_relaxed);
    }

  private:
    std::size_t pick_lowest_load_locked_();  // caller must hold pick_mu_

    std::vector<std::unique_ptr<BackendPool>> pools_;
    std::vector<std::size_t> weights_;
    // `in_flight_` is the load that drives selection; `handled_` is the
    // observability counter for the LB-distribution test. Both are kept atomic
    // so release() can decrement without taking pick_mu_.
    std::vector<std::atomic<std::uint64_t>> in_flight_;
    std::vector<std::atomic<std::uint64_t>> handled_;
    // Serializes the pick-then-bump in borrow(); see class comment.
    std::mutex pick_mu_;
};

}  // namespace ir
