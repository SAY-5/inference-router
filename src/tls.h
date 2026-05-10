#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "connection.h"

// Forward declarations to keep OpenSSL out of the public header. Callers that
// need the raw types include <openssl/ssl.h> themselves.
struct ssl_ctx_st;
struct ssl_st;
typedef struct ssl_ctx_st SSL_CTX;
typedef struct ssl_st SSL;

namespace ir {

// One-shot, process-lifetime initialiser for libcrypto/libssl error strings +
// algorithms. Safe to call repeatedly; idempotent.
void tls_global_init();

// Server-side TLS context. Loads cert/key pair; optionally a client CA bundle for
// mTLS. Thread-safe to share across worker threads (OpenSSL 1.1+ contract).
class TlsContext {
  public:
    struct Options {
        std::string cert_path;       // PEM, required
        std::string key_path;        // PEM, required
        std::string client_ca_path;  // PEM bundle; if non-empty, mTLS is enforced
        bool require_client_cert = false;
    };

    TlsContext() = default;
    ~TlsContext();

    TlsContext(const TlsContext&) = delete;
    TlsContext& operator=(const TlsContext&) = delete;

    // Load + configure. Returns false on any OpenSSL failure; error is written
    // to stderr via the project logger. Must be called before accept_handshake.
    bool init_server(const Options& opts);

    // Take ownership of an accepted fd and perform SSL_accept(). Returns a
    // valid SSL* on success; on failure, returns nullptr and the fd is closed.
    SSL* accept_handshake(int fd, int timeout_ms) const;

    SSL_CTX* raw() const {
        return ctx_;
    }

  private:
    SSL_CTX* ctx_ = nullptr;
};

// A Stream is either a plaintext fd or an SSL*. Exactly one is set. The struct
// is small (a pointer + an int) and is passed by value to keep the call sites
// readable.
struct Stream {
    int fd = -1;
    SSL* ssl = nullptr;  // when non-null, fd is owned by the SSL*

    static Stream raw(int fd_) {
        return Stream{fd_, nullptr};
    }
    static Stream tls(SSL* s);

    bool is_tls() const {
        return ssl != nullptr;
    }

    // Underlying fd, whether wrapped in SSL or not. Useful for poll().
    int underlying_fd() const;
};

// Close + free both the SSL* (if any) and the fd. Safe to call on a moved-from
// Stream. Mirrors safe_close() semantics for plaintext.
void close_stream(Stream& s);

// TLS-aware read/write. Same return codes as the plain-fd variants in
// connection.h. When `s.ssl == nullptr` these delegate to the plaintext path.
IoStatus read_message(Stream s, std::vector<std::uint8_t>& out, std::size_t max_payload,
                      int timeout_ms = -1);
IoStatus write_message(Stream s, const std::uint8_t* data, std::size_t size, int timeout_ms = -1);
inline IoStatus write_message(Stream s, const std::vector<std::uint8_t>& data,
                              int timeout_ms = -1) {
    return write_message(s, data.data(), data.size(), timeout_ms);
}

}  // namespace ir
