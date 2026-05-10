#pragma once

#include <cstddef>

namespace ir {

class BackendPool;
class BackendSet;
class Metrics;
class TlsContext;

struct HandlerOptions {
    std::size_t max_payload = 16ULL * 1024 * 1024;
    int client_io_timeout_ms = 5'000;
    int backend_io_timeout_ms = 5'000;

    // When non-null, every accepted client fd is wrapped via SSL_accept() before
    // any wire I/O happens. The TlsContext is owned by main(); HandlerOptions
    // just carries the pointer. Backend pool I/O remains plaintext (mTLS to
    // backends is the service-mesh layer's job; see ARCHITECTURE.md "v4 TLS").
    TlsContext* server_tls = nullptr;
    int tls_handshake_timeout_ms = 5'000;
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
