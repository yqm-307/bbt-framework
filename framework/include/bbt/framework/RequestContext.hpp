#pragma once
// co-service-actor/v1 F1-a：请求上下文值类型（契约第 139 行）。
// 入站适配器从 infra::IncomingCallContext 接收已换算的 deadline、cancel
// 与已验证的 peer_principal，并从 RpcEnvelope 取得 request_id；本类型是
// 这些已验证字段在框架层的受管形态，字段不得由业务侧自行伪造。
//
// 绑定语义：随逻辑请求/协程绑定（登记/取出见 RequestScope）。禁止用普通
// thread_local 跨挂起缓存本类型或其字段——协程恢复可能换 worker，缓存
// 必须随协程走。Service 可并发处理多个请求，不得把当前请求存成 Service
// 可变成员。

#include <memory>
#include <optional>
#include <string>

#include <bbt/infra/ICoObject.hpp>

#include <bbt/framework/Result.hpp>

namespace bbt::framework {

struct RequestContext {
    std::string                        request_id;
    std::optional<std::string>         actor_key;   // Actor key（若有）
    // 调用预算上界（绝对时刻）：剩余预算 = deadline - now。
    // Deadline::max() 表示无有限预算；出站适配拒绝生成无有限预算的
    // infra deadline（契约第 141/143 行）。
    bbt::coroutine::Deadline           deadline =
        bbt::coroutine::Deadline::max();
    bbt::coroutine::CancellationToken  cancel;
    std::string                        peer_principal; // 权限上下文：已验证对端身份
    std::optional<std::string>         trace_id;       // 追踪关联字段
};

// 取出当前受管请求上下文；无 → err(InvalidContext)。
// 登记/退出由 internal::RequestScope 完成（入站分发建立）；业务侧只读。
result<std::shared_ptr<const RequestContext>> CurrentRequestContext();

} // namespace bbt::framework
