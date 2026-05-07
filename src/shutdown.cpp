#include "shutdown.h"

#include <signal.h>
#include <string.h>

namespace ir {

namespace {
extern "C" void signal_handler(int /*signo*/) {
    Shutdown::instance().request();
}
}  // namespace

Shutdown& Shutdown::instance() {
    static Shutdown s;
    return s;
}

void Shutdown::install_signal_handlers() {
    struct sigaction sa {};
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;  // do NOT set SA_RESTART — we want syscalls to return EINTR
    ::sigaction(SIGTERM, &sa, nullptr);
    ::sigaction(SIGINT, &sa, nullptr);

    // Ignore SIGPIPE process-wide; per-send sites use MSG_NOSIGNAL but other code paths
    // (e.g. test harness writes) might not.
    struct sigaction ign {};
    ign.sa_handler = SIG_IGN;
    sigemptyset(&ign.sa_mask);
    ::sigaction(SIGPIPE, &ign, nullptr);
}

void Shutdown::request() {
    requested_.store(true, std::memory_order_release);
    // condition_variable::notify_all is NOT async-signal-safe, but we only call it from
    // user code (request() called from main thread or test). The signal handler path
    // sets the atomic directly above.
    std::lock_guard<std::mutex> lock(mu_);
    cv_.notify_all();
}

bool Shutdown::wait(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait_for(lock, timeout, [&] { return requested_.load(std::memory_order_acquire); });
    return requested_.load(std::memory_order_acquire);
}

void Shutdown::reset_for_test() {
    std::lock_guard<std::mutex> lock(mu_);
    requested_.store(false, std::memory_order_release);
}

}  // namespace ir
