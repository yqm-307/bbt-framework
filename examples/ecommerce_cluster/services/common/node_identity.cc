#include "services/common/node_identity.hpp"

#include <chrono>
#include <cstdio>
#include <cstdint>

#include <unistd.h>

namespace ecommerce {

namespace {

// incarnation：同一 pid 重启后也必须换新身份，所以掺入单调时钟而不是只靠 pid。
std::string RandomHex() {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < sizeof(value); ++i)
        value = (value << 8) ^ static_cast<std::uint64_t>(::getpid() * (i + 3));
    const std::uint64_t mixed =
        value ^ static_cast<std::uint64_t>(std::chrono::steady_clock::now()
                                               .time_since_epoch()
                                               .count());
    char buffer[17];
    std::snprintf(buffer, sizeof(buffer), "%016llx",
                  static_cast<unsigned long long>(mixed));
    return std::string(buffer, 8);
}

} // namespace

NodeInfo& MutableNodeInfo() {
    static NodeInfo info;
    return info;
}

const NodeInfo& Node() { return MutableNodeInfo(); }

std::string MakeInstanceId(std::string_view service) {
    return std::string(service) + "-" + std::to_string(::getpid()) + "-" +
           RandomHex();
}

} // namespace ecommerce
