#include "metrics.h"

#include <cstdio>

namespace ir {

std::string Metrics::snapshot_string() const {
    char buf[256];
    std::snprintf(
        buf, sizeof(buf),
        "accepted=%llu completed=%llu errored=%llu dropped=%llu "
        "in_flight=%llu pool_borrows=%llu pool_creates=%llu",
        static_cast<unsigned long long>(accepted()), static_cast<unsigned long long>(completed()),
        static_cast<unsigned long long>(errored()), static_cast<unsigned long long>(dropped()),
        static_cast<unsigned long long>(in_flight()),
        static_cast<unsigned long long>(pool_borrows()),
        static_cast<unsigned long long>(pool_creates()));
    return std::string(buf);
}

}  // namespace ir
