#pragma once
/**
 * @brief 当前入站请求的编号、期限和对端身份，由框架从传输层读取。
 * 不要用业务字段冒充对端身份。上下文跟随请求协程：协程恢复时可能换线程，
 * 同一服务也可能同时处理多个请求，因此不要把当前上下文存在线程局部变量或服务成员中。
 * 上下文不提供取消令牌；业务根据调用结果决定是否继续。
 */

#include <memory>
#include <optional>
#include <string>

#include <bbt/infra/ICoObject.hpp>
#include <bbt/coroutine/sync/WaitTypes.hpp>

#include <bbt/framework/Result.hpp>

namespace bbt::framework {

struct RequestContext {
    std::string request_id;
    /**
     * Actor key（若有）
     */
    std::optional<std::string> actor_key;
    /**
     * 调用预算上界（绝对时刻）：剩余预算 = deadline - now。
     * Deadline::max() 表示无有限预算；出站适配拒绝生成无有限预算的
     * infra deadline。
     */
    bbt::coroutine::Deadline deadline =
        bbt::coroutine::Deadline::max();
    /**
     * 权限上下文：已验证对端身份
     */
    std::string peer_principal;
    /**
     * 追踪关联字段
     */
    std::optional<std::string> trace_id;
};

/**
 * @brief 读取当前请求的只读上下文。
 * @return 当前协程或线程没有绑定请求时返回 InvalidContext。
 */
result<std::shared_ptr<const RequestContext>> CurrentRequestContext();

} // namespace bbt::framework
