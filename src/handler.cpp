#include "handler.h"

#include <cstring>

#include "backend_pool.h"
#include "backend_set.h"
#include "connection.h"
#include "log.h"
#include "metrics.h"
#include "tls.h"

namespace ir {

namespace {

// Send a length-prefixed error reply to the client. Best-effort — if even this fails,
// the client gets a peer-close, which the load generator counts as an error response,
// not a drop.
void reply_error(Stream client, const char* msg, int timeout_ms) {
    write_message(client, reinterpret_cast<const std::uint8_t*>(msg), std::strlen(msg),
                  timeout_ms);
}

// Promote the accepted raw fd to a Stream. If TLS is configured, run the
// handshake here on the worker thread (NOT the acceptor thread — TLS handshakes
// are CPU-heavy enough to starve accept()). On handshake failure the fd is
// already closed by accept_handshake; we return a sentinel Stream with fd<0.
Stream wrap_client(int client_fd, const HandlerOptions& opts) {
    if (!opts.server_tls) return Stream::raw(client_fd);
    SSL* ssl = opts.server_tls->accept_handshake(client_fd, opts.tls_handshake_timeout_ms);
    if (!ssl) {
        IR_LOG_DEBUG("tls handshake failed");
        return Stream{-1, nullptr};
    }
    return Stream::tls(ssl);
}

}  // namespace

void handle_one(int client_fd, BackendPool& backend, Metrics& metrics, const HandlerOptions& opts) {
    metrics.inc_in_flight();

    Stream client = wrap_client(client_fd, opts);
    if (client.fd < 0 && !client.ssl) {
        // Handshake failure — fd is already closed inside accept_handshake.
        metrics.inc_errored();
        metrics.dec_in_flight();
        return;
    }

    std::vector<std::uint8_t> req;
    auto r = read_message(client, req, opts.max_payload, opts.client_io_timeout_ms);
    if (r != IoStatus::kOk) {
        IR_LOG_DEBUG("client read failed: %s", io_status_name(r));
        metrics.inc_errored();
        close_stream(client);
        metrics.dec_in_flight();
        return;
    }

    int backend_fd = backend.borrow();
    if (backend_fd < 0) {
        IR_LOG_WARN("backend borrow failed errno=%d", errno);
        reply_error(client, "ERR backend unavailable", opts.client_io_timeout_ms);
        metrics.inc_errored();
        close_stream(client);
        metrics.dec_in_flight();
        return;
    }

    bool conn_ok = true;
    auto wr = write_message(backend_fd, req.data(), req.size(), opts.backend_io_timeout_ms);
    if (wr != IoStatus::kOk) {
        IR_LOG_DEBUG("backend write failed: %s", io_status_name(wr));
        conn_ok = false;
        reply_error(client, "ERR backend write", opts.client_io_timeout_ms);
        backend.release(backend_fd, false);
        metrics.inc_errored();
        close_stream(client);
        metrics.dec_in_flight();
        return;
    }

    std::vector<std::uint8_t> resp;
    auto rr = read_message(backend_fd, resp, opts.max_payload, opts.backend_io_timeout_ms);
    if (rr != IoStatus::kOk) {
        IR_LOG_DEBUG("backend read failed: %s", io_status_name(rr));
        conn_ok = false;
        reply_error(client, "ERR backend read", opts.client_io_timeout_ms);
        backend.release(backend_fd, false);
        metrics.inc_errored();
        close_stream(client);
        metrics.dec_in_flight();
        return;
    }

    auto cw = write_message(client, resp.data(), resp.size(), opts.client_io_timeout_ms);
    backend.release(backend_fd, conn_ok);
    if (cw != IoStatus::kOk) {
        IR_LOG_DEBUG("client write failed: %s", io_status_name(cw));
        metrics.inc_errored();
        close_stream(client);
        metrics.dec_in_flight();
        return;
    }

    metrics.inc_completed();
    close_stream(client);
    metrics.dec_in_flight();
}

// BackendSet overload. Mirrors handle_one(BackendPool&) above but borrows from
// the weighted-least-conn set. Kept as a separate function (not delegating)
// because the borrow/release types differ (Handle vs raw fd) and the indirection
// would be more confusing than the duplication.
void handle_one(int client_fd, BackendSet& backend_set, Metrics& metrics,
                const HandlerOptions& opts) {
    metrics.inc_in_flight();

    Stream client = wrap_client(client_fd, opts);
    if (client.fd < 0 && !client.ssl) {
        metrics.inc_errored();
        metrics.dec_in_flight();
        return;
    }

    std::vector<std::uint8_t> req;
    auto r = read_message(client, req, opts.max_payload, opts.client_io_timeout_ms);
    if (r != IoStatus::kOk) {
        IR_LOG_DEBUG("client read failed: %s", io_status_name(r));
        metrics.inc_errored();
        close_stream(client);
        metrics.dec_in_flight();
        return;
    }

    auto handle = backend_set.borrow();
    if (handle.fd < 0) {
        IR_LOG_WARN("backend_set borrow failed errno=%d", errno);
        reply_error(client, "ERR backend unavailable", opts.client_io_timeout_ms);
        metrics.inc_errored();
        close_stream(client);
        metrics.dec_in_flight();
        return;
    }

    bool conn_ok = true;
    auto wr = write_message(handle.fd, req.data(), req.size(), opts.backend_io_timeout_ms);
    if (wr != IoStatus::kOk) {
        IR_LOG_DEBUG("backend write failed: %s", io_status_name(wr));
        conn_ok = false;
        reply_error(client, "ERR backend write", opts.client_io_timeout_ms);
        backend_set.release(handle, false);
        metrics.inc_errored();
        close_stream(client);
        metrics.dec_in_flight();
        return;
    }

    std::vector<std::uint8_t> resp;
    auto rr = read_message(handle.fd, resp, opts.max_payload, opts.backend_io_timeout_ms);
    if (rr != IoStatus::kOk) {
        IR_LOG_DEBUG("backend read failed: %s", io_status_name(rr));
        conn_ok = false;
        reply_error(client, "ERR backend read", opts.client_io_timeout_ms);
        backend_set.release(handle, false);
        metrics.inc_errored();
        close_stream(client);
        metrics.dec_in_flight();
        return;
    }

    auto cw = write_message(client, resp.data(), resp.size(), opts.client_io_timeout_ms);
    backend_set.release(handle, conn_ok);
    if (cw != IoStatus::kOk) {
        IR_LOG_DEBUG("client write failed: %s", io_status_name(cw));
        metrics.inc_errored();
        close_stream(client);
        metrics.dec_in_flight();
        return;
    }

    metrics.inc_completed();
    close_stream(client);
    metrics.dec_in_flight();
}

}  // namespace ir
