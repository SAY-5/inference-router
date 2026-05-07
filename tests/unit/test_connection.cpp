#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <thread>
#include <vector>

#include "connection.h"

namespace {

// Helper: create a connected socket pair (full-duplex, byte-stream, like a TCP pair).
std::pair<int, int> socket_pair() {
    int fds[2];
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
        return {-1, -1};
    }
    return {fds[0], fds[1]};
}

}  // namespace

TEST(Connection, RoundTripSmallMessage) {
    auto [a, b] = socket_pair();
    ASSERT_GE(a, 0);

    std::vector<std::uint8_t> payload = {'h', 'e', 'l', 'l', 'o'};
    auto wr = ir::write_message(a, payload, 1000);
    EXPECT_EQ(wr, ir::IoStatus::kOk);

    std::vector<std::uint8_t> got;
    auto rd = ir::read_message(b, got, 1024, 1000);
    EXPECT_EQ(rd, ir::IoStatus::kOk);
    EXPECT_EQ(got, payload);

    ir::safe_close(a);
    ir::safe_close(b);
}

TEST(Connection, ZeroLengthMessageOk) {
    auto [a, b] = socket_pair();
    ASSERT_GE(a, 0);

    auto wr = ir::write_message(a, nullptr, 0, 1000);
    EXPECT_EQ(wr, ir::IoStatus::kOk);

    std::vector<std::uint8_t> got;
    auto rd = ir::read_message(b, got, 1024, 1000);
    EXPECT_EQ(rd, ir::IoStatus::kOk);
    EXPECT_TRUE(got.empty());

    ir::safe_close(a);
    ir::safe_close(b);
}

TEST(Connection, OversizeRejected) {
    auto [a, b] = socket_pair();
    ASSERT_GE(a, 0);

    std::vector<std::uint8_t> payload(2048);
    ASSERT_EQ(ir::write_message(a, payload, 1000), ir::IoStatus::kOk);

    std::vector<std::uint8_t> got;
    auto rd = ir::read_message(b, got, /*max=*/1024, 1000);
    EXPECT_EQ(rd, ir::IoStatus::kProtocolError);

    ir::safe_close(a);
    ir::safe_close(b);
}

TEST(Connection, PeerClosedReturnedAtEofBoundary) {
    auto [a, b] = socket_pair();
    ASSERT_GE(a, 0);
    ir::safe_close(a);

    std::vector<std::uint8_t> got;
    auto rd = ir::read_message(b, got, 1024, 1000);
    EXPECT_EQ(rd, ir::IoStatus::kPeerClosed);

    ir::safe_close(b);
}

TEST(Connection, ReadTimeoutFiresWhenNoData) {
    auto [a, b] = socket_pair();
    ASSERT_GE(a, 0);
    std::vector<std::uint8_t> got;
    auto rd = ir::read_message(b, got, 1024, /*timeout_ms=*/50);
    EXPECT_EQ(rd, ir::IoStatus::kTimeout);
    ir::safe_close(a);
    ir::safe_close(b);
}

TEST(Connection, LargeMessageStreamedThroughChunks) {
    auto [a, b] = socket_pair();
    ASSERT_GE(a, 0);

    // 1 MiB payload — ensures multiple recv() calls in read_exact.
    std::vector<std::uint8_t> payload(1024 * 1024);
    for (size_t i = 0; i < payload.size(); ++i) {
        payload[i] = static_cast<std::uint8_t>(i & 0xFF);
    }

    std::thread writer([&] {
        auto wr = ir::write_message(a, payload, 5000);
        EXPECT_EQ(wr, ir::IoStatus::kOk);
        ir::safe_close(a);
    });

    std::vector<std::uint8_t> got;
    auto rd = ir::read_message(b, got, payload.size() + 1, 5000);
    EXPECT_EQ(rd, ir::IoStatus::kOk);
    EXPECT_EQ(got, payload);

    writer.join();
    ir::safe_close(b);
}

TEST(Connection, DialTcpLocalListener) {
    int server = ir::listen_tcp("127.0.0.1", 0, 8);
    ASSERT_GE(server, 0);
    // Get the bound port.
    struct sockaddr_in sa {};
    socklen_t len = sizeof(sa);
    ASSERT_EQ(::getsockname(server, reinterpret_cast<struct sockaddr*>(&sa), &len), 0);

    std::thread client([port = ntohs(sa.sin_port)] {
        int c = ir::dial_tcp("127.0.0.1", port, 1000);
        EXPECT_GE(c, 0);
        if (c >= 0) ir::safe_close(c);
    });

    int conn = ::accept(server, nullptr, nullptr);
    EXPECT_GE(conn, 0);
    if (conn >= 0) ir::safe_close(conn);
    client.join();
    ir::safe_close(server);
}
