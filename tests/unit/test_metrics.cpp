#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "metrics.h"

TEST(Metrics, BasicCountersStartAtZero) {
    ir::Metrics m;
    EXPECT_EQ(m.accepted(), 0u);
    EXPECT_EQ(m.completed(), 0u);
    EXPECT_EQ(m.errored(), 0u);
    EXPECT_EQ(m.dropped(), 0u);
    EXPECT_EQ(m.in_flight(), 0u);
}

TEST(Metrics, IncDecAreThreadSafe) {
    ir::Metrics m;
    constexpr int kThreads = 8;
    constexpr int kPer = 10'000;
    std::vector<std::thread> threads;
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&] {
            for (int j = 0; j < kPer; ++j) {
                m.inc_accepted();
                m.inc_completed();
                m.inc_in_flight();
                m.dec_in_flight();
            }
        });
    }
    for (auto& t : threads) t.join();
    EXPECT_EQ(m.accepted(), static_cast<std::uint64_t>(kThreads * kPer));
    EXPECT_EQ(m.completed(), static_cast<std::uint64_t>(kThreads * kPer));
    EXPECT_EQ(m.in_flight(), 0u);
}

TEST(Metrics, SnapshotStringContainsAllFields) {
    ir::Metrics m;
    m.inc_accepted();
    m.inc_completed();
    m.inc_errored();
    auto s = m.snapshot_string();
    EXPECT_NE(s.find("accepted=1"), std::string::npos);
    EXPECT_NE(s.find("completed=1"), std::string::npos);
    EXPECT_NE(s.find("errored=1"), std::string::npos);
    EXPECT_NE(s.find("dropped=0"), std::string::npos);
}
