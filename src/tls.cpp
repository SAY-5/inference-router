#include "tls.h"

#include <errno.h>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <mutex>

#include "log.h"

namespace ir {

namespace {

std::once_flag g_init_once;

void log_openssl_error(const char* prefix) {
    unsigned long e;
    while ((e = ERR_get_error()) != 0) {
        char buf[256];
        ERR_error_string_n(e, buf, sizeof(buf));
        IR_LOG_ERROR("%s: %s", prefix, buf);
    }
}

// Drive an SSL operation (SSL_accept / SSL_read / SSL_write) that may return
// SSL_ERROR_WANT_READ / SSL_ERROR_WANT_WRITE. Pumps poll() until completion or
// deadline. `op` is invoked each iteration; it should call the SSL_* function
// and return its return code.
//
// Returns:
//   IoStatus::kOk          - op returned > 0 (and out_bytes is set, if non-null)
//   IoStatus::kPeerClosed  - clean shutdown
//   IoStatus::kTimeout     - deadline expired
//   IoStatus::kIoError     - hard error (errno is set; OpenSSL queue logged)
template <typename Op>
IoStatus pump_ssl(SSL* ssl, int fd, int timeout_ms, Op&& op, int* out_bytes = nullptr) {
    using clock = std::chrono::steady_clock;
    const bool has_deadline = timeout_ms >= 0;
    auto deadline = clock::now() + std::chrono::milliseconds(has_deadline ? timeout_ms : 0);
    while (true) {
        ERR_clear_error();
        int rc = op();
        if (rc > 0) {
            if (out_bytes) *out_bytes = rc;
            return IoStatus::kOk;
        }
        int err = SSL_get_error(ssl, rc);
        if (err == SSL_ERROR_ZERO_RETURN) {
            return IoStatus::kPeerClosed;
        }
        if (err != SSL_ERROR_WANT_READ && err != SSL_ERROR_WANT_WRITE) {
            // Hard failure. Stash any queue diagnostics for the operator.
            if (err == SSL_ERROR_SYSCALL && rc == 0) {
                return IoStatus::kPeerClosed;
            }
            log_openssl_error("ssl op failed");
            return IoStatus::kIoError;
        }
        short events = (err == SSL_ERROR_WANT_READ) ? POLLIN : POLLOUT;
        int remaining = -1;
        if (has_deadline) {
            auto now = clock::now();
            if (now >= deadline) return IoStatus::kTimeout;
            remaining = static_cast<int>(
                std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count());
        }
        struct pollfd pfd {};
        pfd.fd = fd;
        pfd.events = events;
        int pr = ::poll(&pfd, 1, remaining);
        if (pr < 0) {
            if (errno == EINTR) continue;
            return IoStatus::kIoError;
        }
        if (pr == 0 && has_deadline) return IoStatus::kTimeout;
        // POLLERR/POLLHUP: let the next SSL_* call surface the error.
    }
}

}  // namespace

void tls_global_init() {
    std::call_once(g_init_once, [] {
        // OpenSSL 1.1+ self-initialises on first use, but calling these is
        // cheap and makes the order explicit.
        SSL_load_error_strings();
        OpenSSL_add_ssl_algorithms();
    });
}

TlsContext::~TlsContext() {
    if (ctx_) {
        SSL_CTX_free(ctx_);
        ctx_ = nullptr;
    }
}

bool TlsContext::init_server(const Options& opts) {
    tls_global_init();
    if (opts.cert_path.empty() || opts.key_path.empty()) {
        IR_LOG_ERROR("tls: cert_path and key_path are required");
        return false;
    }
    ctx_ = SSL_CTX_new(TLS_server_method());
    if (!ctx_) {
        log_openssl_error("SSL_CTX_new");
        return false;
    }
    // TLS 1.2 floor. 1.3 is preferred when both sides support it.
    SSL_CTX_set_min_proto_version(ctx_, TLS1_2_VERSION);
    SSL_CTX_set_mode(ctx_, SSL_MODE_AUTO_RETRY | SSL_MODE_ENABLE_PARTIAL_WRITE);

    if (SSL_CTX_use_certificate_chain_file(ctx_, opts.cert_path.c_str()) != 1) {
        log_openssl_error("use_certificate_chain_file");
        return false;
    }
    if (SSL_CTX_use_PrivateKey_file(ctx_, opts.key_path.c_str(), SSL_FILETYPE_PEM) != 1) {
        log_openssl_error("use_PrivateKey_file");
        return false;
    }
    if (SSL_CTX_check_private_key(ctx_) != 1) {
        log_openssl_error("check_private_key");
        return false;
    }
    if (!opts.client_ca_path.empty()) {
        if (SSL_CTX_load_verify_locations(ctx_, opts.client_ca_path.c_str(), nullptr) != 1) {
            log_openssl_error("load_verify_locations");
            return false;
        }
        STACK_OF(X509_NAME)* ca_names = SSL_load_client_CA_file(opts.client_ca_path.c_str());
        if (ca_names) {
            SSL_CTX_set_client_CA_list(ctx_, ca_names);
        }
        int verify_mode = SSL_VERIFY_PEER;
        if (opts.require_client_cert) verify_mode |= SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
        SSL_CTX_set_verify(ctx_, verify_mode, nullptr);
    } else {
        SSL_CTX_set_verify(ctx_, SSL_VERIFY_NONE, nullptr);
    }
    return true;
}

SSL* TlsContext::accept_handshake(int fd, int timeout_ms) const {
    if (!ctx_ || fd < 0) {
        if (fd >= 0) safe_close(fd);
        return nullptr;
    }
    SSL* ssl = SSL_new(ctx_);
    if (!ssl) {
        log_openssl_error("SSL_new");
        safe_close(fd);
        return nullptr;
    }
    // The fd must be non-blocking for the pump_ssl helper to make progress
    // without indefinitely stalling a worker thread on a slow handshake.
    if (!set_nonblocking(fd, true)) {
        SSL_free(ssl);
        safe_close(fd);
        return nullptr;
    }
    if (SSL_set_fd(ssl, fd) != 1) {
        log_openssl_error("SSL_set_fd");
        SSL_free(ssl);
        safe_close(fd);
        return nullptr;
    }
    auto rc = pump_ssl(ssl, fd, timeout_ms, [&] { return SSL_accept(ssl); });
    if (rc != IoStatus::kOk) {
        SSL_free(ssl);
        safe_close(fd);
        return nullptr;
    }
    return ssl;
}

Stream Stream::tls(SSL* s) {
    Stream out;
    out.ssl = s;
    out.fd = -1;
    if (s) {
        out.fd = SSL_get_fd(s);
    }
    return out;
}

int Stream::underlying_fd() const {
    if (ssl) return SSL_get_fd(ssl);
    return fd;
}

void close_stream(Stream& s) {
    if (s.ssl) {
        // Best-effort send of close_notify. The peer is allowed to skip its
        // half of the bidirectional shutdown; we don't wait on it.
        SSL_shutdown(s.ssl);
        int fd = SSL_get_fd(s.ssl);
        SSL_free(s.ssl);
        s.ssl = nullptr;
        if (fd >= 0) safe_close(fd);
        s.fd = -1;
        return;
    }
    if (s.fd >= 0) {
        safe_close(s.fd);
        s.fd = -1;
    }
}

// ----- read/write delegating to plaintext or SSL based on Stream tag -----

namespace {

IoStatus tls_read_exact(SSL* ssl, std::uint8_t* buf, std::size_t need, int timeout_ms,
                        bool allow_eof_at_start) {
    int fd = SSL_get_fd(ssl);
    std::size_t got = 0;
    while (got < need) {
        int n = 0;
        auto rc = pump_ssl(
            ssl, fd, timeout_ms,
            [&] { return SSL_read(ssl, buf + got, static_cast<int>(need - got)); }, &n);
        if (rc != IoStatus::kOk) {
            if (rc == IoStatus::kPeerClosed && got == 0 && allow_eof_at_start) {
                return IoStatus::kPeerClosed;
            }
            return rc;
        }
        got += static_cast<std::size_t>(n);
    }
    return IoStatus::kOk;
}

IoStatus tls_write_exact(SSL* ssl, const std::uint8_t* buf, std::size_t need, int timeout_ms) {
    int fd = SSL_get_fd(ssl);
    std::size_t sent = 0;
    while (sent < need) {
        int n = 0;
        auto rc = pump_ssl(
            ssl, fd, timeout_ms,
            [&] { return SSL_write(ssl, buf + sent, static_cast<int>(need - sent)); }, &n);
        if (rc != IoStatus::kOk) return rc;
        sent += static_cast<std::size_t>(n);
    }
    return IoStatus::kOk;
}

}  // namespace

IoStatus read_message(Stream s, std::vector<std::uint8_t>& out, std::size_t max_payload,
                      int timeout_ms) {
    if (!s.ssl) {
        return read_message(s.fd, out, max_payload, timeout_ms);
    }
    std::uint8_t hdr[4];
    auto rc = tls_read_exact(s.ssl, hdr, sizeof(hdr), timeout_ms, /*allow_eof_at_start=*/true);
    if (rc != IoStatus::kOk) return rc;
    std::uint32_t net_len = 0;
    std::memcpy(&net_len, hdr, sizeof(net_len));
    // ntohl lives in arpa/inet.h, already pulled in via connection.cpp; replicate locally.
    std::uint32_t len = ((net_len & 0xFF000000u) >> 24) | ((net_len & 0x00FF0000u) >> 8) |
                        ((net_len & 0x0000FF00u) << 8) | ((net_len & 0x000000FFu) << 24);
    if (len > max_payload) return IoStatus::kProtocolError;
    out.resize(len);
    if (len == 0) return IoStatus::kOk;
    return tls_read_exact(s.ssl, out.data(), len, timeout_ms, /*allow_eof_at_start=*/false);
}

IoStatus write_message(Stream s, const std::uint8_t* data, std::size_t size, int timeout_ms) {
    if (!s.ssl) {
        return write_message(s.fd, data, size, timeout_ms);
    }
    if (size > 0xFFFFFFFFu) return IoStatus::kProtocolError;
    std::uint32_t len = static_cast<std::uint32_t>(size);
    std::uint32_t net_len = ((len & 0xFF000000u) >> 24) | ((len & 0x00FF0000u) >> 8) |
                            ((len & 0x0000FF00u) << 8) | ((len & 0x000000FFu) << 24);
    std::uint8_t hdr[4];
    std::memcpy(hdr, &net_len, sizeof(hdr));
    auto rc = tls_write_exact(s.ssl, hdr, sizeof(hdr), timeout_ms);
    if (rc != IoStatus::kOk) return rc;
    if (size == 0) return IoStatus::kOk;
    return tls_write_exact(s.ssl, data, size, timeout_ms);
}

}  // namespace ir
