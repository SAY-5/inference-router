#include "connection.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <cstring>

namespace ir {

const char* io_status_name(IoStatus status) {
    switch (status) {
        case IoStatus::kOk:
            return "ok";
        case IoStatus::kPeerClosed:
            return "peer_closed";
        case IoStatus::kTimeout:
            return "timeout";
        case IoStatus::kProtocolError:
            return "protocol_error";
        case IoStatus::kIoError:
            return "io_error";
    }
    return "?";
}

namespace {

// poll() helper that retries on EINTR. `events` is POLLIN or POLLOUT.
// Returns: 1 = ready, 0 = timeout, -1 = error.
int wait_for(int fd, short events, int timeout_ms) {
    if (timeout_ms < 0) {
        return 1;  // caller wants blocking I/O; fd should be in blocking mode
    }
    using clock = std::chrono::steady_clock;
    auto deadline = clock::now() + std::chrono::milliseconds(timeout_ms);
    while (true) {
        auto now = clock::now();
        int remaining = 0;
        if (now < deadline) {
            remaining = static_cast<int>(
                std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count());
        }
        struct pollfd pfd {};
        pfd.fd = fd;
        pfd.events = events;
        int rc = ::poll(&pfd, 1, remaining);
        if (rc < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (rc == 0) return 0;
        if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
            // Let the caller's read/write surface the actual condition.
            return 1;
        }
        return 1;
    }
}

IoStatus read_exact(int fd, std::uint8_t* buf, std::size_t need, int timeout_ms,
                    bool allow_eof_at_start) {
    std::size_t got = 0;
    while (got < need) {
        int wait_rc = wait_for(fd, POLLIN, timeout_ms);
        if (wait_rc == 0) return IoStatus::kTimeout;
        if (wait_rc < 0) return IoStatus::kIoError;

        ssize_t n = ::recv(fd, buf + got, need - got, 0);
        if (n > 0) {
            got += static_cast<std::size_t>(n);
            continue;
        }
        if (n == 0) {
            if (got == 0 && allow_eof_at_start) return IoStatus::kPeerClosed;
            return IoStatus::kPeerClosed;
        }
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            // Loop back through poll(); blocking sockets shouldn't see this often
            // but socket may have been put in nonblocking mode.
            continue;
        }
        return IoStatus::kIoError;
    }
    return IoStatus::kOk;
}

IoStatus write_exact(int fd, const std::uint8_t* buf, std::size_t need, int timeout_ms) {
    std::size_t sent = 0;
    while (sent < need) {
        int wait_rc = wait_for(fd, POLLOUT, timeout_ms);
        if (wait_rc == 0) return IoStatus::kTimeout;
        if (wait_rc < 0) return IoStatus::kIoError;

        // MSG_NOSIGNAL: receive EPIPE instead of SIGPIPE on closed peer.
        ssize_t n = ::send(fd, buf + sent, need - sent, MSG_NOSIGNAL);
        if (n >= 0) {
            sent += static_cast<std::size_t>(n);
            continue;
        }
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
        if (errno == EPIPE || errno == ECONNRESET) return IoStatus::kPeerClosed;
        return IoStatus::kIoError;
    }
    return IoStatus::kOk;
}

}  // namespace

IoStatus read_message(int fd, std::vector<std::uint8_t>& out, std::size_t max_payload,
                      int timeout_ms) {
    std::uint8_t hdr[4];
    auto rc = read_exact(fd, hdr, sizeof(hdr), timeout_ms, /*allow_eof_at_start=*/true);
    if (rc != IoStatus::kOk) return rc;

    std::uint32_t net_len = 0;
    std::memcpy(&net_len, hdr, sizeof(net_len));
    std::uint32_t len = ntohl(net_len);
    if (len > max_payload) {
        return IoStatus::kProtocolError;
    }
    out.resize(len);
    if (len == 0) return IoStatus::kOk;
    return read_exact(fd, out.data(), len, timeout_ms, /*allow_eof_at_start=*/false);
}

IoStatus parse_frame(const std::uint8_t* buf, std::size_t size, std::size_t max_payload,
                     std::vector<std::uint8_t>& out, std::size_t& consumed) {
    consumed = 0;
    if (buf == nullptr) return IoStatus::kIoError;
    if (size < 4) {
        // Same surface as a half-read header on a socket: not an error frame, but
        // not parseable either. The fuzzer treats this as kPeerClosed-equivalent.
        return IoStatus::kPeerClosed;
    }
    std::uint32_t net_len = 0;
    std::memcpy(&net_len, buf, sizeof(net_len));
    std::uint32_t len = ntohl(net_len);
    if (len > max_payload) {
        return IoStatus::kProtocolError;
    }
    if (size < static_cast<std::size_t>(4) + len) {
        return IoStatus::kPeerClosed;
    }
    out.assign(buf + 4, buf + 4 + len);
    consumed = static_cast<std::size_t>(4) + len;
    return IoStatus::kOk;
}

IoStatus write_message(int fd, const std::uint8_t* data, std::size_t size, int timeout_ms) {
    if (size > 0xFFFFFFFFu) return IoStatus::kProtocolError;
    std::uint32_t net_len = htonl(static_cast<std::uint32_t>(size));
    std::uint8_t hdr[4];
    std::memcpy(hdr, &net_len, sizeof(hdr));
    auto rc = write_exact(fd, hdr, sizeof(hdr), timeout_ms);
    if (rc != IoStatus::kOk) return rc;
    if (size == 0) return IoStatus::kOk;
    return write_exact(fd, data, size, timeout_ms);
}

int dial_tcp(const std::string& host, std::uint16_t port, int connect_timeout_ms) {
    struct addrinfo hints {};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    char port_buf[16];
    std::snprintf(port_buf, sizeof(port_buf), "%u", port);

    struct addrinfo* res = nullptr;
    int gai = ::getaddrinfo(host.c_str(), port_buf, &hints, &res);
    if (gai != 0) {
        errno = (gai == EAI_SYSTEM) ? errno : EINVAL;
        return -1;
    }

    int fd = -1;
    for (auto* ai = res; ai; ai = ai->ai_next) {
        fd = ::socket(ai->ai_family, ai->ai_socktype | SOCK_CLOEXEC, ai->ai_protocol);
        if (fd < 0) continue;

        // Non-blocking connect with a deadline.
        if (!set_nonblocking(fd, true)) {
            safe_close(fd);
            fd = -1;
            continue;
        }
        int rc = ::connect(fd, ai->ai_addr, ai->ai_addrlen);
        if (rc == 0) {
            set_nonblocking(fd, false);
            break;
        }
        if (errno != EINPROGRESS) {
            safe_close(fd);
            fd = -1;
            continue;
        }
        int wait_rc = wait_for(fd, POLLOUT, connect_timeout_ms);
        if (wait_rc <= 0) {
            safe_close(fd);
            fd = -1;
            continue;
        }
        int err = 0;
        socklen_t err_len = sizeof(err);
        if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &err_len) < 0 || err != 0) {
            errno = err ? err : ECONNREFUSED;
            safe_close(fd);
            fd = -1;
            continue;
        }
        set_nonblocking(fd, false);
        break;
    }
    ::freeaddrinfo(res);
    if (fd >= 0) {
        int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    }
    return fd;
}

int listen_tcp(const std::string& host, std::uint16_t port, int backlog) {
    struct addrinfo hints {};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    char port_buf[16];
    std::snprintf(port_buf, sizeof(port_buf), "%u", port);

    struct addrinfo* res = nullptr;
    int gai = ::getaddrinfo(host.empty() ? nullptr : host.c_str(), port_buf, &hints, &res);
    if (gai != 0) {
        errno = (gai == EAI_SYSTEM) ? errno : EINVAL;
        return -1;
    }
    int fd = -1;
    for (auto* ai = res; ai; ai = ai->ai_next) {
        fd = ::socket(ai->ai_family, ai->ai_socktype | SOCK_CLOEXEC, ai->ai_protocol);
        if (fd < 0) continue;
        int one = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
#ifdef SO_REUSEPORT
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
#endif
        if (::bind(fd, ai->ai_addr, ai->ai_addrlen) < 0) {
            safe_close(fd);
            fd = -1;
            continue;
        }
        if (::listen(fd, backlog) < 0) {
            safe_close(fd);
            fd = -1;
            continue;
        }
        break;
    }
    ::freeaddrinfo(res);
    return fd;
}

bool set_nonblocking(int fd, bool nonblocking) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) return false;
    if (nonblocking) {
        flags |= O_NONBLOCK;
    } else {
        flags &= ~O_NONBLOCK;
    }
    return ::fcntl(fd, F_SETFL, flags) == 0;
}

void safe_close(int fd) {
    if (fd < 0) return;
    while (::close(fd) < 0) {
        if (errno != EINTR) break;
    }
}

}  // namespace ir
