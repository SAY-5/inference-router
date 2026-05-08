#pragma once

#include <cstddef>

namespace ir {

class BackendPool;
class BackendSet;
class Metrics;

struct HandlerOptions {
    std::size_t max_payload = 16ULL * 1024 * 1024;
    int client_io_timeout_ms = 5'000;
    int backend_io_timeout_ms = 5'000;
};

// Handle one accepted client connection: read one request, forward to a backend conn,
// write the response, close the client conn. The backend conn is returned to the pool.
//
// The function takes ownership of `client_fd` and closes it before returning.
// Increments metrics on entry/exit.
//
// Two overloads:
//   * BackendPool& — single-backend (the original API). Existing call sites continue
//     to work unchanged.
//   * BackendSet& — multi-backend with weighted-least-conn selection. New code paths
//     should prefer this; the single-backend case is the degenerate (1 pool, weight 1)
//     instance of it.
void handle_one(int client_fd, BackendPool& backend, Metrics& metrics, const HandlerOptions& opts);
void handle_one(int client_fd, BackendSet& backend_set, Metrics& metrics,
                const HandlerOptions& opts);

}  // namespace ir
