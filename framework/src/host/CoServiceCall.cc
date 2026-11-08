// ICoService::call 的机器面半身。
//
// 公共头的 call<Reply> 模板只做编译期 codec 选择与请求编码；本文件
// 承接类型擦除后的发送路径：受管校验、隐式请求上下文、envelope 组装、
// 有序票据系统元数据（fw.* 只由本可信路径写入，不从业务 custom 读）、
// deadline 经 CallOptionsAdapter 适配、宿主注入点发送与回复
// 封包校验。成功返回回复 payload 字节，解码回公共头完成。

#include <bbt/framework/ICoService.hpp>

#include <optional>
#include <utility>

#include <bbt/framework/RequestContext.hpp>
#include <bbt/framework/internal/CallOptionsAdapter.hpp>

namespace bbt::framework {

result<std::vector<std::uint8_t>> ICoService::_CallSend(
    std::string_view service_name,
    std::string_view method_name,
    std::string_view request_schema,
    std::string_view response_schema,
    std::vector<std::uint8_t> payload,
    const RouteFields& custom,
    const CallOptions& options)
{
    using Err = result<std::vector<std::uint8_t>>;

    if (m_object_info.id == 0)
        return Err::err(MakeError(ErrorCode::RuntimeUnavailable,
            "call: service is not hosted by CoApp"));
    // 隐式上下文：从当前受管请求取得；非受管协程 → InvalidContext。
    auto ctx = CurrentRequestContext();
    if (!ctx)
        return Err::err(ctx.error());

    // 组 envelope：request_id 写顶层字段；custom 只进 route.*，
    // 不冒写系统字段（统一加 route. 前缀，接收端据此前提归类）。
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

    // 有序票据：签发会话回指在 stamp 内部状态里（业务不可见）；受限
    // 系统字段只由可信路径写入 fw.*——grant 四元组取签发会话本体，
    // 不从业务 custom 读。
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

    // deadline 经适配器换算（min 规则、过期 TimedOut，infra 只收期限）；
    // 发送动作经宿主注入点完成，回复回填 slot。
    std::optional<bbt::infra::rpc::Response> reply_env;
    CallEgressHooks hooks;
    if (m_send) {
        hooks.initiate_io =
            [this, &env, &reply_env](const bbt::infra::CallOptions& o)
                -> result<void> {
                auto r = m_send(env, o);
                if (!r)
                    return result<void>::err(std::move(r.error()));
                reply_env = std::move(r.value());
                return result<void>::ok();
            };
    }
    if (ordered_session) {
        hooks.consume_sequence =
            [ordered_session, &env](const OrderedStamp& stamp)
                -> result<void> {
                OrderedTicket::BoundContent c;
                c.service = env.Service();
                c.method = env.Method();
                c.actor_key = ordered_session->grant().actor_key;
                for (const auto& [k, v] : env.Metadata()) {
                    if (k.compare(0, 6, "route.") == 0) {
                        c.custom += k; c.custom += '=';
                        c.custom += v; c.custom += '\n';
                    }
                }
                c.request_bytes.assign(env.Payload().begin(),
                                       env.Payload().end());
                return ordered_session->BindForSend(stamp, c);
            };
    }
    auto adapted = AdaptCallOptions(ctx.value().get(), options, hooks);
    if (!adapted)
        return Err::err(std::move(adapted.error()));
    if (!reply_env.has_value())
        return Err::err(MakeError(ErrorCode::InternalError,
            "call: egress produced no reply envelope"));

    // 回复封包校验：回显 request_id、response_schema 与声明一致；
    // 不符属对端/链路协议错误，不当作业务结果。
    if (reply_env->Service() != env.Service() ||
        reply_env->Method() != env.Method() ||
        reply_env->RequestSchema() != env.RequestSchema() ||
        reply_env->RequestId() != env.RequestId() ||
        reply_env->ResponseSchema() != env.ResponseSchema())
        return Err::err(MakeError(ErrorCode::ProtocolError,
            "call: reply envelope mismatch"));
    return Err::ok(reply_env->Payload());
}

} // namespace bbt::framework
