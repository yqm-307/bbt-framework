// 正式 RPC body bridge 实现。
//
// 入站在受管协程内被 infra RpcServer 调用（经 InfraHttpHost::_InvokeRpc 记账）。
// 全部 RPC 元数据只来自 envelope；不读任何 x-bbt-* 头（wire profile 契约）。
//
// 职责边界（下沉后极薄）：
//  - local-min 预算、wire 编解码、五字段关联、remaining 回填、成功/错误信封
//    均由 infra HandleRpcRequest 承担，本文件不重复实现。
//  - framework 只做两件事：把 infra 已解析的 (ctx, env) 交给公开
//    dispatch_inbound；错误返回给 infra **之前**过 framework 错误边界校验
//    （ValidateErrorAtBoundary = infra 结构校验 + framework.actor 保留键语义）。
//    infra 无 framework.actor 保留键表，且 MakeRpcWireHttpResponse 在错误 details
//    结构违规时返回 err：此处降级是唯一防线，不能省略。

#include <bbt/framework/internal/RpcWireBridge.hpp>

#include <utility>

#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/internal/ErrorDomainRule.hpp>
#include <bbt/framework/internal/InfraHttpHost.hpp>

namespace bbt::framework {
namespace {

// 错误返回 infra 之前过框架边界：违规降级为通用 ProtocolError，不静默放行
// 非法 details，也不让 infra 编码期直接失败（回复变成 infra 编码错误）。
bbt::infra::Error SafeBoundaryError(const bbt::infra::Error& error)
{
    if (auto v = ValidateErrorAtBoundary(error); !v)
        return v.error();
    return error;
}

} // namespace

void InstallRpcWireBridge(InfraHttpHost& host, CoApp& app) {
    host.InstallRpcHandler([&app](bbt::infra::IncomingCallContext ctx,
                                  bbt::infra::RpcEnvelope env) -> bbt::infra::result<bbt::infra::RpcEnvelope> {
        // 关联字段（含 request_schema）由 InboundDispatcher 在源头回填，
        // 本桥只做转发，不再回显/补丁任何关联字段。
        auto reply = app.dispatch_inbound(ctx, std::move(env));
        if (!reply)
            return bbt::infra::result<bbt::infra::RpcEnvelope>::err(SafeBoundaryError(reply.error()));
        return reply;
    });
}

} // namespace bbt::framework
