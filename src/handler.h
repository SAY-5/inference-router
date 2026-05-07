#pragma once

#include <cstddef>

namespace ir {

class BackendPool;
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
void handle_one(int client_fd, BackendPool& backend, Metrics& metrics, const HandlerOptions& opts);

}  // namespace ir
