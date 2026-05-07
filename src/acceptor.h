#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <thread>

namespace ir {

class Metrics;

// Single-threaded epoll-based acceptor reactor.
//
// Why epoll for accept() but blocking I/O on workers?
//   - Acceptor must wake on (a) a new connection AND (b) a stop signal — the easiest
//     way to do both atomically is epoll on (listen_fd, eventfd-or-pipe).
//   - Workers do blocking I/O because handler logic is linear request->backend->response,
//     and a blocking style keeps the code small and easy to reason about. Per-connection
//     work is bounded by the I/O timeouts.
class Acceptor {
  public:
    using OnAccept = std::function<void(int client_fd)>;

    struct Options {
        std::string host = "0.0.0.0";
        std::uint16_t port = 0;
        int backlog = 256;
    };

    Acceptor(Options opts, Metrics* metrics, OnAccept on_accept);
    ~Acceptor();

    // Bind + listen + start the loop in a background thread.
    bool start();

    // Stop the loop: closes the listen socket and the eventfd, joins the thread.
    void stop();

    bool running() const {
        return running_.load(std::memory_order_acquire);
    }
    std::uint16_t bound_port() const;

  private:
    void loop_();

    Options opts_;
    Metrics* metrics_;
    OnAccept on_accept_;

    int listen_fd_ = -1;
    int wake_fd_ = -1;
    int epoll_fd_ = -1;

    std::thread thread_;
    std::atomic<bool> running_{false};
    std::atomic<bool> stop_{false};
};

}  // namespace ir
