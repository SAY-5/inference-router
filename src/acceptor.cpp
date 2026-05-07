#include "acceptor.h"

#include <errno.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>

#include "connection.h"
#include "log.h"
#include "metrics.h"

namespace ir {

Acceptor::Acceptor(Options opts, Metrics* metrics, OnAccept on_accept)
    : opts_(std::move(opts)), metrics_(metrics), on_accept_(std::move(on_accept)) {}

Acceptor::~Acceptor() {
    stop();
}

bool Acceptor::start() {
    listen_fd_ = listen_tcp(opts_.host, opts_.port, opts_.backlog);
    if (listen_fd_ < 0) {
        IR_LOG_ERROR("listen_tcp failed host=%s port=%u errno=%d", opts_.host.c_str(), opts_.port,
                     errno);
        return false;
    }
    if (!set_nonblocking(listen_fd_, true)) {
        IR_LOG_ERROR("set_nonblocking on listen fd failed errno=%d", errno);
        return false;
    }

    wake_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (wake_fd_ < 0) {
        IR_LOG_ERROR("eventfd failed errno=%d", errno);
        return false;
    }

    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
        IR_LOG_ERROR("epoll_create1 failed errno=%d", errno);
        return false;
    }

    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = listen_fd_;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &ev) < 0) {
        IR_LOG_ERROR("epoll_ctl ADD listen failed errno=%d", errno);
        return false;
    }
    ev.data.fd = wake_fd_;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &ev) < 0) {
        IR_LOG_ERROR("epoll_ctl ADD wake failed errno=%d", errno);
        return false;
    }

    running_.store(true, std::memory_order_release);
    thread_ = std::thread([this] { loop_(); });
    return true;
}

void Acceptor::stop() {
    if (!running_.load(std::memory_order_acquire) && !thread_.joinable()) return;
    stop_.store(true, std::memory_order_release);
    if (wake_fd_ >= 0) {
        std::uint64_t one = 1;
        ssize_t w = ::write(wake_fd_, &one, sizeof(one));
        (void)w;
    }
    if (thread_.joinable()) thread_.join();
    if (epoll_fd_ >= 0) {
        safe_close(epoll_fd_);
        epoll_fd_ = -1;
    }
    if (listen_fd_ >= 0) {
        safe_close(listen_fd_);
        listen_fd_ = -1;
    }
    if (wake_fd_ >= 0) {
        safe_close(wake_fd_);
        wake_fd_ = -1;
    }
    running_.store(false, std::memory_order_release);
}

std::uint16_t Acceptor::bound_port() const {
    if (listen_fd_ < 0) return 0;
    struct sockaddr_storage ss {};
    socklen_t len = sizeof(ss);
    if (::getsockname(listen_fd_, reinterpret_cast<struct sockaddr*>(&ss), &len) < 0) {
        return 0;
    }
    if (ss.ss_family == AF_INET) {
        return ntohs(reinterpret_cast<struct sockaddr_in*>(&ss)->sin_port);
    }
    if (ss.ss_family == AF_INET6) {
        return ntohs(reinterpret_cast<struct sockaddr_in6*>(&ss)->sin6_port);
    }
    return 0;
}

void Acceptor::loop_() {
    constexpr int kMaxEvents = 16;
    epoll_event events[kMaxEvents];
    while (!stop_.load(std::memory_order_acquire)) {
        int n = ::epoll_wait(epoll_fd_, events, kMaxEvents, 1000);
        if (n < 0) {
            if (errno == EINTR) continue;
            IR_LOG_ERROR("epoll_wait failed errno=%d", errno);
            break;
        }
        for (int i = 0; i < n; ++i) {
            int fd = events[i].data.fd;
            if (fd == wake_fd_) {
                std::uint64_t v;
                ssize_t r = ::read(wake_fd_, &v, sizeof(v));
                (void)r;
                continue;
            }
            if (fd == listen_fd_) {
                while (true) {
                    int client = ::accept4(listen_fd_, nullptr, nullptr, SOCK_CLOEXEC);
                    if (client < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        if (errno == EINTR) continue;
                        IR_LOG_WARN("accept4 failed errno=%d", errno);
                        break;
                    }
                    if (metrics_) metrics_->inc_accepted();
                    on_accept_(client);
                }
            }
        }
    }
}

}  // namespace ir
