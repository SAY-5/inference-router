#include "handler.h"

#include <cstring>

#include "backend_pool.h"
#include "backend_set.h"
#include "connection.h"
#include "log.h"
#include "metrics.h"

namespace ir {

namespace {

// Send a length-prefixed error reply to the client. Best-effort — if even this fails,
// the client gets a peer-close, which the load generator counts as an error response,
// not a drop.
void reply_error(int client_fd, const char* msg, int timeout_ms) {
    write_message(client_fd, reinterpret_cast<const std::uint8_t*>(msg), std::strlen(msg),
                  timeout_ms);
}

}  // namespace

void handle_one(int client_fd, BackendPool& backend, Metrics& metrics, const HandlerOptions& opts) {
    metrics.inc_in_flight();
    bool responded = false;
    bool errored = false;

    std::vector<std::uint8_t> req;
    auto r = read_message(client_fd, req, opts.max_payload, opts.client_io_timeout_ms);
    if (r != IoStatus::kOk) {
        IR_LOG_DEBUG("client read failed: %s", io_status_name(r));
        // Client gave us nothing parseable — count as errored, the client knows.
        metrics.inc_errored();
        errored = true;
        responded = true;  // peer close is response-equivalent for our purposes
        safe_close(client_fd);
        metrics.dec_in_flight();
        return;
    }

    int backend_fd = backend.borrow();
    if (backend_fd < 0) {
        IR_LOG_WARN("backend borrow failed errno=%d", errno);
        reply_error(client_fd, "ERR backend unavailable", opts.client_io_timeout_ms);
        metrics.inc_errored();
        errored = true;
        responded = true;
        safe_close(client_fd);
        metrics.dec_in_flight();
        return;
    }

    bool conn_ok = true;
    auto wr = write_message(backend_fd, req.data(), req.size(), opts.backend_io_timeout_ms);
    if (wr != IoStatus::kOk) {
        IR_LOG_DEBUG("backend write failed: %s", io_status_name(wr));
        conn_ok = false;
        reply_error(client_fd, "ERR backend write", opts.client_io_timeout_ms);
        backend.release(backend_fd, false);
        metrics.inc_errored();
        safe_close(client_fd);
        metrics.dec_in_flight();
        return;
    }

    std::vector<std::uint8_t> resp;
    auto rr = read_message(backend_fd, resp, opts.max_payload, opts.backend_io_timeout_ms);
    if (rr != IoStatus::kOk) {
        IR_LOG_DEBUG("backend read failed: %s", io_status_name(rr));
        conn_ok = false;
        reply_error(client_fd, "ERR backend read", opts.client_io_timeout_ms);
        backend.release(backend_fd, false);
        metrics.inc_errored();
        safe_close(client_fd);
        metrics.dec_in_flight();
        return;
    }

    auto cw = write_message(client_fd, resp.data(), resp.size(), opts.client_io_timeout_ms);
    backend.release(backend_fd, conn_ok);
    if (cw != IoStatus::kOk) {
        IR_LOG_DEBUG("client write failed: %s", io_status_name(cw));
        metrics.inc_errored();
        safe_close(client_fd);
        metrics.dec_in_flight();
        return;
    }

    metrics.inc_completed();
    responded = true;
    (void)responded;
    (void)errored;
    safe_close(client_fd);
    metrics.dec_in_flight();
}

// BackendSet overload. Mirrors handle_one(BackendPool&) above but borrows from
// the weighted-least-conn set. Kept as a separate function (not delegating)
// because the borrow/release types differ (Handle vs raw fd) and the indirection
// would be more confusing than the duplication.
void handle_one(int client_fd, BackendSet& backend_set, Metrics& metrics,
                const HandlerOptions& opts) {
    metrics.inc_in_flight();

    std::vector<std::uint8_t> req;
    auto r = read_message(client_fd, req, opts.max_payload, opts.client_io_timeout_ms);
    if (r != IoStatus::kOk) {
        IR_LOG_DEBUG("client read failed: %s", io_status_name(r));
        metrics.inc_errored();
        safe_close(client_fd);
        metrics.dec_in_flight();
        return;
    }

    auto handle = backend_set.borrow();
    if (handle.fd < 0) {
        IR_LOG_WARN("backend_set borrow failed errno=%d", errno);
        reply_error(client_fd, "ERR backend unavailable", opts.client_io_timeout_ms);
        metrics.inc_errored();
        safe_close(client_fd);
        metrics.dec_in_flight();
        return;
    }

    bool conn_ok = true;
    auto wr = write_message(handle.fd, req.data(), req.size(), opts.backend_io_timeout_ms);
    if (wr != IoStatus::kOk) {
        IR_LOG_DEBUG("backend write failed: %s", io_status_name(wr));
        conn_ok = false;
        reply_error(client_fd, "ERR backend write", opts.client_io_timeout_ms);
        backend_set.release(handle, false);
        metrics.inc_errored();
        safe_close(client_fd);
        metrics.dec_in_flight();
        return;
    }

    std::vector<std::uint8_t> resp;
    auto rr = read_message(handle.fd, resp, opts.max_payload, opts.backend_io_timeout_ms);
    if (rr != IoStatus::kOk) {
        IR_LOG_DEBUG("backend read failed: %s", io_status_name(rr));
        conn_ok = false;
        reply_error(client_fd, "ERR backend read", opts.client_io_timeout_ms);
        backend_set.release(handle, false);
        metrics.inc_errored();
        safe_close(client_fd);
        metrics.dec_in_flight();
        return;
    }

    auto cw = write_message(client_fd, resp.data(), resp.size(), opts.client_io_timeout_ms);
    backend_set.release(handle, conn_ok);
    if (cw != IoStatus::kOk) {
        IR_LOG_DEBUG("client write failed: %s", io_status_name(cw));
        metrics.inc_errored();
        safe_close(client_fd);
        metrics.dec_in_flight();
        return;
    }

    metrics.inc_completed();
    safe_close(client_fd);
    metrics.dec_in_flight();
}

}  // namespace ir
