#include "services/common/hop_log.hpp"

#include <cstdio>
#include <mutex>

#include "services/common/node_identity.hpp"

namespace ecommerce {

namespace {

// 多个协程并发写同一个 stdout：整行加锁，避免两条日志的字段交错。
std::mutex& LogMutex() {
    static std::mutex mtx;
    return mtx;
}

// 空字段写成 '-'，让每行都能按空格分词且不出现空值。
std::string Token(std::string_view key, std::string_view value) {
    return " " + std::string(key) + "=" +
           (value.empty() ? std::string("-") : std::string(value));
}

} // namespace

void LogHop(const HopRecord& record) {
    const NodeInfo& node = Node();
    std::string line = "event=hop";
    line += Token("service", record.service);
    line += Token("method", record.method);
    line += Token("request_id", record.request_id);
    line += Token("downstream", record.downstream);
    line += Token("downstream_method", record.downstream_method);
    line += Token("pid", node.process_id);
    line += Token("instance", node.instance_id);
    line += Token("ok", record.ok ? "1" : "0");
    line += Token("code", std::to_string(record.error_code));

    std::lock_guard<std::mutex> lk(LogMutex());
    std::printf("%s\n", line.c_str());
    std::fflush(stdout);
}

std::string InboundRequestId(const bbt::framework::ServiceContext& context) {
    auto current = context.request();
    return current ? current.value()->request_id : std::string{};
}

} // namespace ecommerce
