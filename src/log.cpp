#include "log.h"

#include <atomic>

namespace ir {

namespace {
std::atomic<LogLevel> g_level{LogLevel::kInfo};
}

void set_log_level(LogLevel level) {
    g_level.store(level, std::memory_order_relaxed);
}

LogLevel get_log_level() {
    return g_level.load(std::memory_order_relaxed);
}

namespace detail {

std::mutex& log_mutex() {
    static std::mutex m;
    return m;
}

const char* level_name(LogLevel level) {
    switch (level) {
        case LogLevel::kDebug:
            return "DEBUG";
        case LogLevel::kInfo:
            return "INFO";
        case LogLevel::kWarn:
            return "WARN";
        case LogLevel::kError:
            return "ERROR";
    }
    return "?";
}

}  // namespace detail
}  // namespace ir
