#pragma once
// 节点身份：进程启动时写入一次，之后只读。
//
// 响应与逐跳日志都回带它，用来证明「这次请求实际落在哪个进程」，而不是由
// 调用方推断落点。写入点只有 main 的启动装配；handler 只读 Node()。

#include <string>
#include <string_view>

namespace ecommerce {

struct NodeInfo {
    std::string instance_id;   ///< <service>-<pid>-<incarnation>
    std::string process_id;    ///< 本进程 pid（十进制文本）
};

/// @brief 启动期写入身份的唯一入口；只在 main 装配阶段调用一次。
NodeInfo& MutableNodeInfo();

/// @brief 只读身份视图（handler 与日志使用）。
const NodeInfo& Node();

/// @brief 生成 <service>-<pid>-<incarnation> 形式的实例身份。
std::string MakeInstanceId(std::string_view service);

} // namespace ecommerce
