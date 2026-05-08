#include "backend_set.h"

#include <errno.h>

#include <cstdint>
#include <limits>

#include "log.h"
#include "metrics.h"

namespace ir {

BackendSet::BackendSet(const std::vector<BackendSpec>& specs, BackendPool::Options shared_opts,
                       Metrics* metrics)
    : in_flight_(specs.size()), handled_(specs.size()) {
    pools_.reserve(specs.size());
    weights_.reserve(specs.size());
    for (const auto& s : specs) {
        BackendPool::Options o = shared_opts;
        o.host = s.host;
        o.port = s.port;
        pools_.push_back(std::make_unique<BackendPool>(o, metrics));
        weights_.push_back(s.weight == 0 ? 1 : s.weight);
    }
}

BackendSet::~BackendSet() {
    shutdown();
}

std::size_t BackendSet::pick_lowest_load_locked_() {
    // Find the backend with the smallest effective load (in_flight / weight).
    // Compare pairs by cross-multiply to avoid float division:
    //   cand_in_flight / cand_weight < best_in_flight / best_weight
    //   iff cand_in_flight * best_weight < best_in_flight * cand_weight
    // Tie-break: lowest backend index wins (strict-less-than above preserves
    // the leftmost candidate on ties).
    std::size_t best = 0;
    std::uint64_t best_in_flight = in_flight_[0].load(std::memory_order_relaxed);
    std::size_t best_weight = weights_[0];
    for (std::size_t i = 1; i < pools_.size(); ++i) {
        std::uint64_t cand_in_flight = in_flight_[i].load(std::memory_order_relaxed);
        std::size_t cand_weight = weights_[i];
        if (cand_in_flight * static_cast<std::uint64_t>(best_weight) <
            best_in_flight * static_cast<std::uint64_t>(cand_weight)) {
            best = i;
            best_in_flight = cand_in_flight;
            best_weight = cand_weight;
        }
    }
    return best;
}

BackendSet::Handle BackendSet::borrow() {
    if (pools_.empty()) {
        errno = ENOENT;
        return Handle{-1, 0};
    }
    std::size_t idx;
    {
        // Pick AND bump in_flight under the mutex so two concurrent borrowers
        // can't read the same snapshot and both target the same backend. The
        // pool's actual borrow() (idle-queue/dial) runs unlocked.
        std::lock_guard<std::mutex> lk(pick_mu_);
        idx = pick_lowest_load_locked_();
        in_flight_[idx].fetch_add(1, std::memory_order_acq_rel);
    }
    int fd = pools_[idx]->borrow();
    if (fd < 0) {
        in_flight_[idx].fetch_sub(1, std::memory_order_acq_rel);
        IR_LOG_DEBUG("backend_set: borrow from pool[%zu] failed errno=%d", idx, errno);
        return Handle{-1, 0};
    }
    handled_[idx].fetch_add(1, std::memory_order_relaxed);
    return Handle{fd, idx};
}

void BackendSet::release(Handle h, bool ok) {
    if (h.fd < 0) return;
    if (h.pool_index >= pools_.size()) {
        // Defensive: if a stale handle ever reaches here, just leak the fd's
        // accounting rather than crash. Should be impossible from the in-tree
        // call sites.
        IR_LOG_WARN("backend_set: release with invalid pool_index=%zu", h.pool_index);
        return;
    }
    pools_[h.pool_index]->release(h.fd, ok);
    in_flight_[h.pool_index].fetch_sub(1, std::memory_order_acq_rel);
}

void BackendSet::shutdown() {
    for (auto& p : pools_) {
        if (p) p->shutdown();
    }
}

}  // namespace ir
