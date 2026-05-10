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
void reply_error_plain(int client_fd, const char* msg, int timeout_ms) {
    write_message(client_fd, reinterpret_cast<const std::uint8_t*>(msg), std::strlen(msg),
                  timeout_ms);
}
void reply_error_tls(SSL* ssl, const char* msg, int timeout_ms) {
    Stream s = Stream::tls(ssl);
    write_message(s, reinterpret_cast<const std::uint8_t*>(msg), std::strlen(msg), timeout_ms);
}

}  // namespace

void handle_one(int client_fd, BackendPool& backend, Metrics& metrics, const HandlerOptions& opts) {
    metrics.inc_in_flight();

    // ---- Plaintext fast path. Byte-for-byte identical to the v3 hot path; no Stream
    //      wrapping or TLS overhead. The 10k-client bench depends on this.
    if (!opts.server_tls) {
        std::vector<std::uint8_t> req;
        auto r = read_message(client_fd, req, opts.max_payload, opts.client_io_timeout_ms);
        if (r != IoStatus::kOk) {
            // Temporarily elevated to WARN to diagnose the post-v4 bench failure on CI.
            IR_LOG_WARN("client read failed: %s errno=%d", io_status_name(r), errno);
            metrics.inc_errored();
            safe_close(client_fd);
            metrics.dec_in_flight();
            return;
        }

        int backend_fd = backend.borrow();
        if (backend_fd < 0) {
            IR_LOG_WARN("backend borrow failed errno=%d", errno);
            reply_error_plain(client_fd, "ERR backend unavailable", opts.client_io_timeout_ms);
            metrics.inc_errored();
            safe_close(client_fd);
            metrics.dec_in_flight();
            return;
        }

        bool conn_ok = true;
        auto wr = write_message(backend_fd, req.data(), req.size(), opts.backend_io_timeout_ms);
        if (wr != IoStatus::kOk) {
            IR_LOG_WARN("backend write failed: %s", io_status_name(wr));
            conn_ok = false;
            reply_error_plain(client_fd, "ERR backend write", opts.client_io_timeout_ms);
            backend.release(backend_fd, false);
            metrics.inc_errored();
            safe_close(client_fd);
            metrics.dec_in_flight();
            return;
        }

        std::vector<std::uint8_t> resp;
        auto rr = read_message(backend_fd, resp, opts.max_payload, opts.backend_io_timeout_ms);
        if (rr != IoStatus::kOk) {
            IR_LOG_WARN("backend read failed: %s", io_status_name(rr));
            conn_ok = false;
            reply_error_plain(client_fd, "ERR backend read", opts.client_io_timeout_ms);
            backend.release(backend_fd, false);
            metrics.inc_errored();
            safe_close(client_fd);
            metrics.dec_in_flight();
            return;
        }

        auto cw = write_message(client_fd, resp.data(), resp.size(), opts.client_io_timeout_ms);
        backend.release(backend_fd, conn_ok);
        if (cw != IoStatus::kOk) {
            IR_LOG_WARN("client write failed: %s", io_status_name(cw));
            metrics.inc_errored();
            safe_close(client_fd);
            metrics.dec_in_flight();
            return;
        }

        metrics.inc_completed();
        safe_close(client_fd);
        metrics.dec_in_flight();
        return;
    }

    // ---- TLS path. Handshake runs on the worker thread (NOT the acceptor thread —
    //      handshakes are CPU-heavy enough to starve accept()). Failed handshake closes
    //      the fd inside accept_handshake and we just record errored.
    SSL* ssl = opts.server_tls->accept_handshake(client_fd, opts.tls_handshake_timeout_ms);
    if (!ssl) {
        IR_LOG_WARN("tls handshake failed");
        metrics.inc_errored();
        metrics.dec_in_flight();
        return;
    }
    Stream client = Stream::tls(ssl);

    std::vector<std::uint8_t> req;
    auto r = read_message(client, req, opts.max_payload, opts.client_io_timeout_ms);
    if (r != IoStatus::kOk) {
        IR_LOG_WARN("client read failed: %s", io_status_name(r));
        metrics.inc_errored();
        close_stream(client);
        metrics.dec_in_flight();
        return;
    }

    int backend_fd = backend.borrow();
    if (backend_fd < 0) {
        IR_LOG_WARN("backend borrow failed errno=%d", errno);
        reply_error_tls(ssl, "ERR backend unavailable", opts.client_io_timeout_ms);
        metrics.inc_errored();
        close_stream(client);
        metrics.dec_in_flight();
        return;
    }

    bool conn_ok = true;
    auto wr = write_message(backend_fd, req.data(), req.size(), opts.backend_io_timeout_ms);
    if (wr != IoStatus::kOk) {
        IR_LOG_WARN("backend write failed: %s", io_status_name(wr));
        conn_ok = false;
        reply_error_tls(ssl, "ERR backend write", opts.client_io_timeout_ms);
        backend.release(backend_fd, false);
        metrics.inc_errored();
        close_stream(client);
        metrics.dec_in_flight();
        return;
    }

    std::vector<std::uint8_t> resp;
    auto rr = read_message(backend_fd, resp, opts.max_payload, opts.backend_io_timeout_ms);
    if (rr != IoStatus::kOk) {
        IR_LOG_WARN("backend read failed: %s", io_status_name(rr));
        conn_ok = false;
        reply_error_tls(ssl, "ERR backend read", opts.client_io_timeout_ms);
        backend.release(backend_fd, false);
        metrics.inc_errored();
        close_stream(client);
        metrics.dec_in_flight();
        return;
    }

    auto cw = write_message(client, resp.data(), resp.size(), opts.client_io_timeout_ms);
    backend.release(backend_fd, conn_ok);
    if (cw != IoStatus::kOk) {
        IR_LOG_WARN("client write failed: %s", io_status_name(cw));
        metrics.inc_errored();
        close_stream(client);
        metrics.dec_in_flight();
        return;
    }

    metrics.inc_completed();
    close_stream(client);
    metrics.dec_in_flight();
}

// BackendSet overload. Same two-path structure as handle_one(BackendPool&) above;
// the plaintext branch matches v3 byte-for-byte so the bench's 10k-client path is
// not perturbed by v4.
void handle_one(int client_fd, BackendSet& backend_set, Metrics& metrics,
                const HandlerOptions& opts) {
    metrics.inc_in_flight();

    if (!opts.server_tls) {
        std::vector<std::uint8_t> req;
        auto r = read_message(client_fd, req, opts.max_payload, opts.client_io_timeout_ms);
        if (r != IoStatus::kOk) {
            IR_LOG_WARN("client read failed: %s", io_status_name(r));
            metrics.inc_errored();
            safe_close(client_fd);
            metrics.dec_in_flight();
            return;
        }

        auto handle = backend_set.borrow();
        if (handle.fd < 0) {
            IR_LOG_WARN("backend_set borrow failed errno=%d", errno);
            reply_error_plain(client_fd, "ERR backend unavailable", opts.client_io_timeout_ms);
            metrics.inc_errored();
            safe_close(client_fd);
            metrics.dec_in_flight();
            return;
        }

        bool conn_ok = true;
        auto wr = write_message(handle.fd, req.data(), req.size(), opts.backend_io_timeout_ms);
        if (wr != IoStatus::kOk) {
            IR_LOG_WARN("backend write failed: %s", io_status_name(wr));
            conn_ok = false;
            reply_error_plain(client_fd, "ERR backend write", opts.client_io_timeout_ms);
            backend_set.release(handle, false);
            metrics.inc_errored();
            safe_close(client_fd);
            metrics.dec_in_flight();
            return;
        }

        std::vector<std::uint8_t> resp;
        auto rr = read_message(handle.fd, resp, opts.max_payload, opts.backend_io_timeout_ms);
        if (rr != IoStatus::kOk) {
            IR_LOG_WARN("backend read failed: %s", io_status_name(rr));
            conn_ok = false;
            reply_error_plain(client_fd, "ERR backend read", opts.client_io_timeout_ms);
            backend_set.release(handle, false);
            metrics.inc_errored();
            safe_close(client_fd);
            metrics.dec_in_flight();
            return;
        }

        auto cw = write_message(client_fd, resp.data(), resp.size(), opts.client_io_timeout_ms);
        backend_set.release(handle, conn_ok);
        if (cw != IoStatus::kOk) {
            IR_LOG_WARN("client write failed: %s", io_status_name(cw));
            metrics.inc_errored();
            safe_close(client_fd);
            metrics.dec_in_flight();
            return;
        }

        metrics.inc_completed();
        safe_close(client_fd);
        metrics.dec_in_flight();
        return;
    }

    // TLS path.
    SSL* ssl = opts.server_tls->accept_handshake(client_fd, opts.tls_handshake_timeout_ms);
    if (!ssl) {
        IR_LOG_WARN("tls handshake failed");
        metrics.inc_errored();
        metrics.dec_in_flight();
        return;
    }
    Stream client = Stream::tls(ssl);

    std::vector<std::uint8_t> req;
    auto r = read_message(client, req, opts.max_payload, opts.client_io_timeout_ms);
    if (r != IoStatus::kOk) {
        IR_LOG_WARN("client read failed: %s", io_status_name(r));
        metrics.inc_errored();
        close_stream(client);
        metrics.dec_in_flight();
        return;
    }

    auto handle = backend_set.borrow();
    if (handle.fd < 0) {
        IR_LOG_WARN("backend_set borrow failed errno=%d", errno);
        reply_error_tls(ssl, "ERR backend unavailable", opts.client_io_timeout_ms);
        metrics.inc_errored();
        close_stream(client);
        metrics.dec_in_flight();
        return;
    }

    bool conn_ok = true;
    auto wr = write_message(handle.fd, req.data(), req.size(), opts.backend_io_timeout_ms);
    if (wr != IoStatus::kOk) {
        IR_LOG_WARN("backend write failed: %s", io_status_name(wr));
        conn_ok = false;
        reply_error_tls(ssl, "ERR backend write", opts.client_io_timeout_ms);
        backend_set.release(handle, false);
        metrics.inc_errored();
        close_stream(client);
        metrics.dec_in_flight();
        return;
    }

    std::vector<std::uint8_t> resp;
    auto rr = read_message(handle.fd, resp, opts.max_payload, opts.backend_io_timeout_ms);
    if (rr != IoStatus::kOk) {
        IR_LOG_WARN("backend read failed: %s", io_status_name(rr));
        conn_ok = false;
        reply_error_tls(ssl, "ERR backend read", opts.client_io_timeout_ms);
        backend_set.release(handle, false);
        metrics.inc_errored();
        close_stream(client);
        metrics.dec_in_flight();
        return;
    }

    auto cw = write_message(client, resp.data(), resp.size(), opts.client_io_timeout_ms);
    backend_set.release(handle, conn_ok);
    if (cw != IoStatus::kOk) {
        IR_LOG_WARN("client write failed: %s", io_status_name(cw));
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
