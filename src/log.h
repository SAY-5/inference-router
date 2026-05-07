#pragma once

#include <cstdio>
#include <mutex>
#include <string_view>

namespace ir {

enum class LogLevel { kDebug = 0, kInfo = 1, kWarn = 2, kError = 3 };

void set_log_level(LogLevel level);
LogLevel get_log_level();

namespace detail {
std::mutex& log_mutex();
const char* level_name(LogLevel level);
}  // namespace detail

template <typename... Args>
void log_at(LogLevel level, const char* fmt, Args... args) {
    if (level < get_log_level()) {
        return;
    }
    std::lock_guard<std::mutex> lock(detail::log_mutex());
    std::fprintf(stderr, "[%s] ", detail::level_name(level));
    // NOLINTNEXTLINE(cert-err33-c)
    std::fprintf(stderr, fmt, args...);
    std::fputc('\n', stderr);
}

#define IR_LOG_DEBUG(...) ::ir::log_at(::ir::LogLevel::kDebug, __VA_ARGS__)
#define IR_LOG_INFO(...) ::ir::log_at(::ir::LogLevel::kInfo, __VA_ARGS__)
#define IR_LOG_WARN(...) ::ir::log_at(::ir::LogLevel::kWarn, __VA_ARGS__)
#define IR_LOG_ERROR(...) ::ir::log_at(::ir::LogLevel::kError, __VA_ARGS__)

}  // namespace ir
