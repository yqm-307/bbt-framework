// bbt-framework Issue #4（P0-A）：正式 RPC body bridge 实现。
//
// 入站在受管协程内被 infra HttpServer 调用（经 InfraHttpHost::_Handle 记账）。
// 全部 RPC 元数据只来自 envelope body；不读任何 x-bbt-* 头（wire profile 契约）。
//
// 处理逻辑内联在 InstallRpcWireBridge（CoApp 友元）的 handler lambda 内，与
// 既有 InstallRpcHttpBridge 同形态——经公开 dispatch_inbound 走真实分发链。

#include <bbt/framework/internal/RpcWireBridge.hpp>

#include <algorithm>
#include <chrono>
#include <string>
#include <utility>

#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/internal/ErrorDomainRule.hpp>
#include <bbt/framework/internal/InfraHttpHost.hpp>

namespace bbt::framework {
namespace {

using bbt::infra::Error;
using bbt::infra::HttpRequest;
using bbt::infra::HttpResponse;
using bbt::infra::IncomingCallContext;
using bbt::infra::rpc::RpcWireEnvelope;

// wire 层无法解析出 request_id 时的占位：envelope 要求 request_id 非空才可编码。
// 该分支只承载协议错误，不携带业务事实。
constexpr char kUnparsedRequestId[] = "unparsed";

// 错误信封：把 Error 装入 response 方向 envelope。错误离开本进程前过框架边界
// 校验（通用结构 + framework 域保留键），非法错误降级为通用 ProtocolError，
// 不静默放行非法 details。
bbt::infra::result<HttpResponse> MakeErrorResponse(
    const Error& error, const std::string& request_id,
    const std::string& service, const std::string& method,
    const std::string& request_schema, const std::string& response_schema) {
    const Error* ep = &error;
    Error downgraded;
    if (auto v = ValidateErrorAtBoundary(*ep); !v) {
        downgraded = v.error();
        ep = &downgraded;
    }
    RpcWireEnvelope out;
    out.profile_version     = bbt::infra::rpc::kRpcWireProfileVersion;
    out.request_id          = request_id.empty() ? kUnparsedRequestId : request_id;
    out.service             = service.empty() ? "unknown" : service;
    out.method              = method.empty() ? "unknown" : method;
    // 可解析请求在错误方向保留 schema，关联 id/schema 一并回显（不可解析时
    // 只作协议错误，schema 留空）。响应方向 wire 契约拒绝 0（至少 1ms）；
    // 错误路径不掌握已生效期限，此处只填合法编码哨兵 1ms，明确不宣称正剩余。
    out.request_schema      = request_schema;
    out.response_schema     = response_schema;
    out.remaining_budget_ms = 1;
    out.success             = false;
    out.error               = *ep;
    return bbt::infra::rpc::MakeRpcWireHttpResponse(out);
}

} // namespace

void InstallRpcWireBridge(InfraHttpHost& host, CoApp& app) {
    host.InstallHandler(
        [&app](IncomingCallContext ctx, HttpRequest req)
            -> bbt::infra::result<HttpResponse> {
            auto parsed = bbt::infra::rpc::ParseRpcWireHttpRequest(req);
            if (!parsed)
                return MakeErrorResponse(parsed.error(), "", "", "", "", "");
            const RpcWireEnvelope& w = parsed.value();

            // 接收端 local-min 预算：以入站 deadline（infra 传输硬看门）与本地按
            // remaining_budget_ms 换算出的期限取最小。预算在 handler 可见前收敛；
            // remaining_budget_ms==0 已在 wire 解码期拒绝（不进本分支）。
            IncomingCallContext effective;
            effective.peer_principal = ctx.peer_principal;  // 身份只来自 infra
                                                            // 通道，不被 metadata 升级
            const auto local_deadline = std::chrono::steady_clock::now() +
                std::chrono::milliseconds{w.remaining_budget_ms};
            effective.deadline = std::min(ctx.deadline, local_deadline);

            bbt::infra::RpcEnvelope env;
            env.service         = w.service;
            env.method          = w.method;
            env.request_id      = w.request_id;
            env.request_schema  = w.request_schema;
            env.response_schema = w.response_schema;
            env.payload         = w.payload;
            env.metadata        = w.metadata;

            auto reply = app.dispatch_inbound(effective, std::move(env));
            if (!reply)
                return MakeErrorResponse(reply.error(), w.request_id,
                                         w.service, w.method,
                                         w.request_schema, w.response_schema);

            // 生效剩余预算：以接收端 local-min 期限（effective.deadline，即
            // min(传输 hard deadline, now+budget)）的真实剩余回填，clamp 到
            // [1, kMaxRemainingBudgetMs]；不再原值回显请求声明的 budget——
            // 排队/处理已消耗预算，且超过本地 in-flight 上限的声明不得误导调用方。
            const auto now = std::chrono::steady_clock::now();
            auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                effective.deadline - now).count();
            if (remaining < 1)
                remaining = 1;
            if (remaining > static_cast<long long>(
                    bbt::infra::rpc::kMaxRemainingBudgetMs))
                remaining = static_cast<long long>(
                    bbt::infra::rpc::kMaxRemainingBudgetMs);

            RpcWireEnvelope out;
            out.profile_version     = bbt::infra::rpc::kRpcWireProfileVersion;
            out.request_id          = w.request_id;   // 响应必须回显 request_id
            out.service             = w.service;
            out.method              = w.method;
            out.remaining_budget_ms = static_cast<std::uint32_t>(remaining);
            out.request_schema      = w.request_schema;
            out.response_schema     = reply.value().response_schema;  // 声明 schema
            out.payload             = reply.value().payload;
            out.success             = true;
            return bbt::infra::rpc::MakeRpcWireHttpResponse(out);
        });
}

} // namespace bbt::framework
