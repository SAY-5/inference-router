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
    // -Wformat-security wants the format string to be a literal; ours is a parameter
    // here, but that parameter is always a literal at the call site (the macros below
    // never pass a runtime-constructed string). Suppress the warning on this line only.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-security"
    if constexpr (sizeof...(Args) == 0) {
        std::fputs(fmt, stderr);
    } else {
        // NOLINTNEXTLINE(cert-err33-c)
        std::fprintf(stderr, fmt, args...);
    }
#pragma GCC diagnostic pop
    std::fputc('\n', stderr);
}

#define IR_LOG_DEBUG(...) ::ir::log_at(::ir::LogLevel::kDebug, __VA_ARGS__)
#define IR_LOG_INFO(...) ::ir::log_at(::ir::LogLevel::kInfo, __VA_ARGS__)
#define IR_LOG_WARN(...) ::ir::log_at(::ir::LogLevel::kWarn, __VA_ARGS__)
#define IR_LOG_ERROR(...) ::ir::log_at(::ir::LogLevel::kError, __VA_ARGS__)

}  // namespace ir
