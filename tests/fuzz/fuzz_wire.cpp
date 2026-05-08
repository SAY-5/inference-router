// libFuzzer harness for the wire-protocol frame parser.
//
// Invariants the harness asserts on every input:
//   1. parse_frame() never crashes / aborts / asserts on arbitrary bytes.
//   2. parse_frame() never reads past `size` (caught by AddressSanitizer).
//   3. parse_frame() is deterministic: the same input always yields the same
//      (status, payload, consumed) triple. (Re-parsed in the harness.)
//   4. Round-trip property: any payload P that fits the protocol can be encoded
//      with a 4-byte big-endian header and parse_frame() must return P.
//
// Build (locally):
//   clang++ -std=c++20 -O1 -g -fsanitize=fuzzer,address,undefined \
//           tests/fuzz/fuzz_wire.cpp src/connection.cpp \
//           -Isrc -o fuzz_wire
//
// CI runs the harness for a bounded time budget (-max_total_time) and asserts the
// run exits cleanly. Crashing inputs are uploaded as corpus artifacts.

#include <arpa/inet.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

#include "connection.h"

namespace {

// Cap derived from the router's default max payload (16 MiB) but the fuzzer treats
// `max_payload` as a parameter we can vary. We use a smaller cap (64 KiB) here so
// the round-trip property doesn't blow memory on every iteration.
constexpr std::size_t kFuzzMaxPayload = 64 * 1024;

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    // 1. Random-stream property: parser must not crash on arbitrary bytes.
    std::vector<std::uint8_t> out;
    std::size_t consumed = 0;
    auto status = ir::parse_frame(data, size, kFuzzMaxPayload, out, consumed);

    // 2. Determinism: re-parse and check identical result.
    std::vector<std::uint8_t> out2;
    std::size_t consumed2 = 0;
    auto status2 = ir::parse_frame(data, size, kFuzzMaxPayload, out2, consumed2);
    if (status != status2 || consumed != consumed2 || out != out2) {
        __builtin_trap();
    }

    // 3. On kOk the consumed count must match the declared length.
    if (status == ir::IoStatus::kOk) {
        if (size < 4) __builtin_trap();
        std::uint32_t net_len = 0;
        std::memcpy(&net_len, data, sizeof(net_len));
        std::uint32_t len = ntohl(net_len);
        if (consumed != static_cast<std::size_t>(4) + len) __builtin_trap();
        if (out.size() != len) __builtin_trap();
        if (consumed > size) __builtin_trap();
    } else {
        if (consumed != 0) __builtin_trap();
    }

    // 4. Round-trip property: encode the parsed payload, parse again, expect same bytes.
    if (status == ir::IoStatus::kOk) {
        std::vector<std::uint8_t> framed;
        framed.reserve(out.size() + 4);
        std::uint32_t net_len = htonl(static_cast<std::uint32_t>(out.size()));
        framed.resize(4);
        std::memcpy(framed.data(), &net_len, 4);
        framed.insert(framed.end(), out.begin(), out.end());

        std::vector<std::uint8_t> reparsed;
        std::size_t reconsumed = 0;
        auto rc =
            ir::parse_frame(framed.data(), framed.size(), kFuzzMaxPayload, reparsed, reconsumed);
        if (rc != ir::IoStatus::kOk) __builtin_trap();
        if (reparsed != out) __builtin_trap();
        if (reconsumed != framed.size()) __builtin_trap();
    }
    return 0;
}
