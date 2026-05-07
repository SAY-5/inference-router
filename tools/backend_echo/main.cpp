// Tiny echo backend: accepts length-prefixed messages, echoes them back.
// Used for integration + chaos tests AND as a stand-in for a real model server in
// docker-compose. NOT a production component.

#include <errno.h>
#include <getopt.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>

#include "connection.h"
#include "log.h"
#include "shutdown.h"

namespace {

void serve_one(int fd) {
    while (true) {
        std::vector<std::uint8_t> msg;
        auto rc = ir::read_message(fd, msg, ir::kDefaultMaxPayload, 30'000);
        if (rc == ir::IoStatus::kPeerClosed) break;
        if (rc != ir::IoStatus::kOk) {
            IR_LOG_DEBUG("backend_echo: read %s", ir::io_status_name(rc));
            break;
        }
        auto wr = ir::write_message(fd, msg.data(), msg.size(), 5'000);
        if (wr != ir::IoStatus::kOk) {
            IR_LOG_DEBUG("backend_echo: write %s", ir::io_status_name(wr));
            break;
        }
    }
    ir::safe_close(fd);
}

}  // namespace

int main(int argc, char** argv) {
    std::string host = "0.0.0.0";
    int port = 9090;
    int log_level = 1;

    static struct option long_opts[] = {
        {"listen-host", required_argument, nullptr, 'h'},
        {"port", required_argument, nullptr, 'p'},
        {"log-level", required_argument, nullptr, 'L'},
        {nullptr, 0, nullptr, 0},
    };
    int opt, idx = 0;
    while ((opt = getopt_long(argc, argv, "", long_opts, &idx)) != -1) {
        switch (opt) {
            case 'h':
                host = optarg;
                break;
            case 'p':
                port = std::atoi(optarg);
                break;
            case 'L':
                log_level = std::atoi(optarg);
                break;
            default:
                std::fprintf(stderr, "usage: backend-echo --port N\n");
                return 2;
        }
    }
    ir::set_log_level(static_cast<ir::LogLevel>(log_level));
    ir::Shutdown::instance().install_signal_handlers();

    int listen_fd = ir::listen_tcp(host, static_cast<std::uint16_t>(port), 128);
    if (listen_fd < 0) {
        std::fprintf(stderr, "listen failed errno=%d\n", errno);
        return 1;
    }
    IR_LOG_INFO("backend-echo listening on %s:%d", host.c_str(), port);

    std::vector<std::thread> threads;

    while (!ir::Shutdown::instance().requested()) {
        struct pollfd pfd {};
        pfd.fd = listen_fd;
        pfd.events = POLLIN;
        int pr = ::poll(&pfd, 1, 200);
        if (pr <= 0) continue;

        int client = ::accept(listen_fd, nullptr, nullptr);
        if (client < 0) {
            if (errno == EINTR) continue;
            IR_LOG_WARN("accept failed errno=%d", errno);
            continue;
        }
        threads.emplace_back([client] { serve_one(client); });
    }

    ir::safe_close(listen_fd);
    for (auto& t : threads) {
        if (t.joinable()) t.join();
    }
    IR_LOG_INFO("backend-echo: bye");
    return 0;
}
