/**
 * @brief 为服务发送 RPC，并核对回复是否属于本次请求。
 */

#include <bbt/framework/internal/ServiceBase.hpp>

#include <chrono>
#include <utility>

#include <bbt/framework/OrderedSession.hpp>
#include <bbt/framework/RequestContext.hpp>
#include <bbt/framework/internal/CallOptionsAdapter.hpp>

namespace bbt::framework {

bbt_co_async(result<std::vector<std::uint8_t>>) CoServiceBase::_CallSend(
    std::string_view service_name,
    std::string_view method_name,
    std::string_view request_schema,
    std::string_view response_schema,
    std::vector<std::uint8_t> payload,
    const RouteFields& custom,
    const CallOptions& options) {
    using Err = result<std::vector<std::uint8_t>>;

    if (!m_send)
        return Err::err(MakeError(ErrorCode::RuntimeUnavailable,
                                  "call: service is not hosted by CoApp"));
    auto ctx = CurrentRequestContext();
    if (!ctx)
        return Err::err(ctx.error());

    /**
     * 业务路由字段统一加 route. 前缀，不能覆盖框架的系统字段。
     */
    bbt::infra::rpc::Request env{std::string(service_name), std::string(method_name)};
    env.SetRequestSchema(std::string(request_schema))
        .SetResponseSchema(std::string(response_schema))
        .SetPayload(std::move(payload))
        .SetRequestId(options.ordered ? options.ordered->request_id() : _NextRequestId())
        .SetTraffic(bbt::infra::rpc::TrafficClass::Service)
        .SetReply(bbt::infra::rpc::ReplyMode::Required);
    bbt::infra::rpc::Metadata metadata;
    for (const auto& [k, v] : custom.values) {
        if (k.empty() || k.size() > 64 || v.size() > 256 ||
            metadata.size() >= 16)
            return Err::err(MakeError(ErrorCode::InvalidArgument,
                                      "call: custom route field out of bounds"));
        metadata.emplace_back("route." + k, v);
    }

    /**
     * 有序请求的 fw.* 字段取自票据和签发会话，不信任业务传入的路由字段。
     */
    std::shared_ptr<OrderedSession> ordered_session;
    if (options.ordered.has_value()) {
        if (options.ordered->state_)
            ordered_session = options.ordered->state_->session.lock();
        if (!ordered_session)
            return Err::err(MakeError(ErrorCode::InvalidArgument,
                                      "call: ordered stamp has no live session"));
        const auto& g = ordered_session->grant();
        metadata.emplace_back("fw.producer_id", g.producer_id);
        metadata.emplace_back("fw.producer_epoch", g.producer_epoch);
        metadata.emplace_back("fw.receiver_epoch", g.receiver_epoch);
        metadata.emplace_back(
            "fw.sequence", std::to_string(options.ordered->sequence()));
    }

    env.SetMetadata(std::move(metadata));

    const auto deadline = ResolveCallDeadline(
        ctx.value().get(), options.deadline, std::chrono::steady_clock::now());
    if (!deadline)
        return Err::err(deadline.error());

    // 票据在宿主查路由和执行策略前绑定，重试仍校验同一份请求内容。
    if (ordered_session) {
        OrderedTicket::BoundContent content;
        content.service = env.Service();
        content.method = env.Method();
        content.actor_key = ordered_session->grant().actor_key;
        for (const auto& [k, v] : env.Metadata()) {
            if (k.compare(0, 6, "route.") == 0) {
                content.custom += k;
                content.custom += '=';
                content.custom += v;
                content.custom += '\n';
            }
        }
        content.request_bytes.assign(env.Payload().begin(), env.Payload().end());
        auto bound = ordered_session->BindForSend(*options.ordered, content);
        if (!bound)
            return Err::err(std::move(bound.error()));
    }

    const bbt::infra::CallOptions send_options{deadline.value()};
    auto reply = bbt_co_await m_send(env, send_options);
    if (!reply)
        return Err::err(std::move(reply.error()));
    const auto& reply_env = reply.value();

    /**
     * 编号或类型不匹配的回复是协议错误，不能作为本次调用的结果。
     */
    if (reply_env.Service() != env.Service() ||
        reply_env.Method() != env.Method() ||
        reply_env.RequestSchema() != env.RequestSchema() ||
        reply_env.RequestId() != env.RequestId() ||
        reply_env.ResponseSchema() != env.ResponseSchema())
        return Err::err(MakeError(ErrorCode::ProtocolError,
                                  "call: reply envelope mismatch"));
    return Err::ok(reply_env.Payload());
}

/**
 * CoRpcReq 重载使用位置参数的 schema；protobuf 类型名由另一重载提供。
 */
bbt_co_async(result<CoRpcResp>) CoServiceBase::call(
    std::string_view service_name, std::string_view method_name,
    const CoRpcReq& request, const RouteFields& custom,
    const CallOptions& options) {
    auto reply = bbt_co_await _CallSend(
        service_name, method_name,
        std::string(kCoRpcPositionalSchema),
        std::string(kCoRpcPositionalSchema),
        request.payload(), custom, options);
    if (!reply)
        return result<CoRpcResp>::err(std::move(reply.error()));
    return result<CoRpcResp>::ok(
        CoRpcResp::FromPayload(std::move(reply.value())));
}

std::string CoServiceBase::_NextRequestId() const {
    /**
     * 进程内单调序号：request_id 的非票据来源（有序调用用票据
     * request_id）。对象 id 来自 Registry 身份句柄。
     */
    return "req-" + Handle().Id() + "-" +
           std::to_string(
               s_call_seq.fetch_add(1, std::memory_order_relaxed) + 1);
}

} // namespace bbt::framework
