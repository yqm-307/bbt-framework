#include <bbt/framework/internal/AppRuntimeState.hpp>

#include <chrono>
#include <utility>

#include <bbt/framework/internal/InboundDispatcher.hpp>
#include <bbt/framework/internal/RpcHttpBridge.hpp>

namespace bbt::framework {

result<bbt::infra::RpcEnvelope> AppRuntimeState::Send(
    const bbt::infra::RpcEnvelope& env,
    const bbt::infra::CallOptions& options) const {
    // 路由门：业务只给逻辑 service 名，目标必须命中显式静态路由。
    const auto it = routes.find(env.service);
    if (it == routes.end())
        return result<bbt::infra::RpcEnvelope>::err(MakeError(
            ErrorCode::NotFound,
            "find_route: no configured target for service '" +
                env.service + "'"));

    // 公开出站策略在路由授权之后、发送之前调用一次。策略只见只读事实
    // （已解析目标 + 不可改写 envelope + 已适配绝对期限），只能 Allow/Reject
    // 或收紧期限；无策略（nullptr）时逐字保持既有行为。
    bbt::infra::CallOptions opt = options;
    if (rpc_policy) {
        RpcPolicyDecision decision;
        auto d = rpc_policy->Preflight(
            RpcCallView{it->second, env, opt.deadline});
        if (!d)
            return result<bbt::infra::RpcEnvelope>::err(d.error());
        decision = d.value();

        if (decision.kind == RpcPolicyDecision::Kind::Reject)
            return result<bbt::infra::RpcEnvelope>::err(decision.reject_error);

        // 只收紧：框架取 min(原期限, 收紧值)，绝不放大策略结果。
        if (decision.tightened_deadline.has_value() &&
            *decision.tightened_deadline < opt.deadline)
            opt.deadline = *decision.tightened_deadline;

        // 收紧到已过期（或不晚于 now）→ 发起任何 I/O 之前按 TimedOut 拒绝，
        // 与 AdaptCallOptions 的过期门同语义。
        if (opt.deadline <= std::chrono::steady_clock::now())
            return result<bbt::infra::RpcEnvelope>::err(
                MakeError(ErrorCode::TimedOut,
                          "rpc policy tightened deadline already expired"));
    }

    if (default_egress)
        return default_egress->Send(it->second, env, opt);
    if (!seam_send)
        return result<bbt::infra::RpcEnvelope>::err(MakeError(
            ErrorCode::RuntimeUnavailable,
            "CoApp: rpc_send egress not configured"));
    return seam_send(it->second, env, opt);
}

void AppRuntimeState::Publish(
    std::shared_ptr<InboundDispatcher> d,
    std::map<std::string, std::shared_ptr<const RpcMethodTable>> t,
    std::map<std::string, std::shared_ptr<ICoService>> inst,
    std::shared_ptr<ActorRegistry> reg) {
    std::lock_guard<std::mutex> lock(mtx);
    tables   = std::move(t);
    instances = std::move(inst);
    registry = std::move(reg);
    dispatcher = std::move(d);   // 最后开放接纳
}

void AppRuntimeState::Release() noexcept {
    // 放弃指向业务对象的全部边：闭包/表项仍自持的副本不受本调用影响。
    std::lock_guard<std::mutex> lock(mtx);
    dispatcher.reset();
    tables.clear();
    instances.clear();
    registry.reset();
}

result<bbt::infra::RpcEnvelope> AppRuntimeState::Dispatch(
    const bbt::infra::IncomingCallContext& incoming,
    bbt::infra::RpcEnvelope request) {
    std::shared_ptr<InboundDispatcher> dispatcher;
    {
        std::lock_guard<std::mutex> lock(mtx);
        dispatcher = this->dispatcher;
    }
    if (!dispatcher)
        return result<bbt::infra::RpcEnvelope>::err(MakeError(
            ErrorCode::RuntimeUnavailable,
            "dispatch runtime not published or already released"));
    return dispatcher->Dispatch(incoming, std::move(request));
}

} // namespace bbt::framework
