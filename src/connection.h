#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace ir {

// Wire protocol: 4-byte length prefix (network byte order, uint32) then payload bytes.
// Length zero is legal (used for the health check ping).
inline constexpr std::size_t kDefaultMaxPayload = 16ULL * 1024 * 1024;  // 16 MiB

enum class IoStatus {
    kOk,
    kPeerClosed,     // peer closed cleanly; no message to deliver
    kTimeout,        // I/O exceeded the deadline
    kProtocolError,  // length-prefix violated bounds (oversize, etc.)
    kIoError,        // unrecoverable read/write error (errno on the side)
};

const char* io_status_name(IoStatus status);

// Read one length-prefixed message off `fd` into `out` (resized to fit).
// Blocks; if `timeout_ms` >= 0 the read deadline is enforced via poll().
IoStatus read_message(int fd, std::vector<std::uint8_t>& out, std::size_t max_payload,
                      int timeout_ms = -1);

// Write one length-prefixed message to `fd`.
IoStatus write_message(int fd, const std::uint8_t* data, std::size_t size, int timeout_ms = -1);

inline IoStatus write_message(int fd, const std::vector<std::uint8_t>& data, int timeout_ms = -1) {
    return write_message(fd, data.data(), data.size(), timeout_ms);
}

// Connect to host:port (blocking). Returns the fd, or -1 on failure (errno set).
int dial_tcp(const std::string& host, std::uint16_t port, int connect_timeout_ms);

// Bind a TCP listener on host:port. Returns listening fd, or -1 (errno set).
// Sets SO_REUSEADDR + SO_REUSEPORT.
int listen_tcp(const std::string& host, std::uint16_t port, int backlog);

// Set non-blocking flag.
bool set_nonblocking(int fd, bool nonblocking);

// Best-effort close that ignores EINTR.
void safe_close(int fd);

}  // namespace ir
