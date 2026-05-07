#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>

namespace ir {

// Process-wide shutdown coordinator. SIGTERM/SIGINT set the flag and wake any waiter.
// The signal handler does only an atomic store and a write() to a self-pipe — both are
// async-signal-safe.
class Shutdown {
  public:
    static Shutdown& instance();

    // Call once from main(). Installs handlers for SIGTERM and SIGINT.
    void install_signal_handlers();

    // Set the flag (programmatic shutdown).
    void request();

    bool requested() const {
        return requested_.load(std::memory_order_acquire);
    }

    // Block until requested or until `timeout` elapses. Returns true if requested.
    bool wait(std::chrono::milliseconds timeout);

    // Reset (test-only).
    void reset_for_test();

  private:
    Shutdown() = default;
    std::atomic<bool> requested_{false};
    std::mutex mu_;
    std::condition_variable cv_;
};

}  // namespace ir
