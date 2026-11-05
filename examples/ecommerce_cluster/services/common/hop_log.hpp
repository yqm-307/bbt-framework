#pragma once
// 逐跳日志：一次 handler 执行写一行 `event=hop`。
//
// 字段固定为 service/method/request_id/downstream/downstream_method/pid/
// instance/ok/code，值不含空格，驱动器与人工读同一份文本。request_id 来自
// 入站 envelope（context().request()），各跳独立。存储模式的 order_id
// 才是跨进程关联同一订单的稳定业务键。

#include <string>
#include <string_view>

#include <bbt/framework/ServiceContext.hpp>

namespace ecommerce {

struct HopRecord {
    std::string_view service;            ///< 本跳服务名
    std::string_view method;             ///< 本跳方法名
    std::string      request_id;         ///< 入站 request_id（空 = 无受管上下文）
    std::string_view downstream;         ///< 下游逻辑服务名；空 = 链路末端
    std::string_view downstream_method;  ///< 下游方法名；空 = 无下游调用
    bool             ok{true};           ///< 本跳（含下游调用）是否成功
    int              error_code{0};      ///< 失败时的 ErrorCode 数值
};

/// @brief 输出一行逐跳日志（整行加锁，避免并发协程字段交错）。
void LogHop(const HopRecord& record);

/// @brief 取入站 request_id；非受管调用返回空串，日志如实留空而不自造。
std::string InboundRequestId(const bbt::framework::ServiceContext& context);

} // namespace ecommerce
