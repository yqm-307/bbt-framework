#include <bbt/framework/internal/RpcHttpBridge.hpp>

#include <chrono>
#include <cctype>
#include <string_view>
#include <utility>

#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
#include <bbt/infra/rpc/HttpTransport.hpp>
#include <bbt/infra/http/Client.hpp>
#include <bbt/infra/net/Engine.hpp>
#endif
#if defined(BBT_FRAMEWORK_HAS_RPC_TCP)
#include <bbt/infra/rpc/TcpTransport.hpp>
#endif

#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/internal/ErrorDomainRule.hpp>
#include <bbt/framework/internal/InfraHttpHost.hpp>

namespace bbt::framework::http_bridge {

namespace {

constexpr std::string_view kMetaPrefix = "x-bbt-meta-";

std::optional<std::string> HeaderOf(
    const std::vector<std::pair<std::string, std::string>>& hs,
    std::string_view name) {
    for (const auto& [k, v] : hs)
        if (k == name) return v;
    return std::nullopt;
}

} // namespace

bbt::infra::HttpRequest ToHttpRequest(
    const bbt::infra::RpcEnvelope& env, std::string url) {
    bbt::infra::HttpRequest r;
    r.method = "POST";
    r.url    = std::move(url);
    r.headers = {
        {"x-bbt-service",         env.service},
        {"x-bbt-method",          env.method},
        {"x-bbt-request-id",      env.request_id},
        {"x-bbt-request-schema",  env.request_schema},
        {"x-bbt-response-schema", env.response_schema},
    };
    for (const auto& [k, v] : env.metadata)
        r.headers.emplace_back(std::string(kMetaPrefix) + k, v);
    r.body.assign(env.payload.begin(), env.payload.end());
    return r;
}

result<bbt::infra::RpcEnvelope> EnvelopeFromRequest(
    const bbt::infra::HttpRequest& req) {
    bbt::infra::RpcEnvelope env;
    auto need = [&](const char* n, std::string& dst) -> bool {
        auto v = HeaderOf(req.headers, n);
        if (!v) return false;
        dst = std::move(*v);
        return true;
    };
    if (!need("x-bbt-service", env.service) ||
        !need("x-bbt-method", env.method) ||
        !need("x-bbt-request-id", env.request_id) ||
        !need("x-bbt-request-schema", env.request_schema) ||
        !need("x-bbt-response-schema", env.response_schema))
        return result<bbt::infra::RpcEnvelope>::err(MakeError(
            ErrorCode::InvalidArgument,
            "http bridge: envelope headers incomplete"));
    for (const auto& [k, v] : req.headers)
        if (k.compare(0, kMetaPrefix.size(), kMetaPrefix) == 0)
            env.metadata.emplace_back(
                k.substr(kMetaPrefix.size()), v);
    env.payload.assign(req.body.begin(), req.body.end());
    return result<bbt::infra::RpcEnvelope>::ok(std::move(env));
}

bbt::infra::HttpResponse ToHttpResponse(
    const result<bbt::infra::RpcEnvelope>& r) {
    bbt::infra::HttpResponse res;
    if (r) {
        res.status  = 200;
        res.headers = {
            {"x-bbt-request-id",      r.value().request_id},
            {"x-bbt-response-schema", r.value().response_schema},
        };
        res.body.assign(r.value().payload.begin(), r.value().payload.end());
        return res;
    }
    // infra #39：回复出站是框架可信边界——携带 details 的错误在离开本进程前
    // 过一遍完整校验（通用结构 + framework 域保留键），非法错误不序列化上
    // wire；校验失败降级为通用 ProtocolError，不静默放行。
    const Error* ep = &r.error();
    Error downgraded;
    if (auto v = ValidateErrorAtBoundary(*ep); !v) {
        downgraded = v.error();
        ep = &downgraded;
    }
    const Error& e = *ep;
    res.status = 400;
    res.headers = {
        {"x-bbt-err-code",        std::to_string(static_cast<int>(e.code))},
        {"x-bbt-err-domain",      e.domain},
        {"x-bbt-err-domain-code", e.domain_code},
        {"x-bbt-err-message",     e.message},
    };
    return res;
}

result<bbt::infra::RpcEnvelope> EnvelopeFromResponse(
    const bbt::infra::HttpResponse& res) {
    if (res.status == 200) {
        bbt::infra::RpcEnvelope env;
        env.request_id =
            HeaderOf(res.headers, "x-bbt-request-id").value_or("");
        env.response_schema =
            HeaderOf(res.headers, "x-bbt-response-schema").value_or("");
        env.payload.assign(res.body.begin(), res.body.end());
        return result<bbt::infra::RpcEnvelope>::ok(std::move(env));
    }
    Error e;
    if (auto c = HeaderOf(res.headers, "x-bbt-err-code"))
        e.code = static_cast<ErrorCode>(std::stoi(*c));
    else
        e.code = ErrorCode::ProtocolError;
    e.domain      = HeaderOf(res.headers, "x-bbt-err-domain")
                        .value_or(std::string(bbt::infra::kErrorDomainInfra));
    e.domain_code = HeaderOf(res.headers, "x-bbt-err-domain-code")
                        .value_or("");
    e.message     = HeaderOf(res.headers, "x-bbt-err-message")
                        .value_or("http bridge: no error message");
    return result<bbt::infra::RpcEnvelope>::err(std::move(e));
}

} // namespace bbt::framework::http_bridge

namespace bbt::framework {

void InstallRpcHttpBridge(InfraHttpHost& host, CoApp& app) {
    host.InstallHandler(
        [&app](bbt::infra::IncomingCallContext ctx,
               bbt::infra::HttpRequest req)
            -> result<bbt::infra::HttpResponse> {
            auto env = http_bridge::EnvelopeFromRequest(req);
            if (!env)
                return result<bbt::infra::HttpResponse>::ok(
                    http_bridge::ToHttpResponse(
                        result<bbt::infra::RpcEnvelope>::err(
                            std::move(env.error()))));
            return result<bbt::infra::HttpResponse>::ok(
                http_bridge::ToHttpResponse(
                    app.dispatch_inbound(ctx, std::move(env.value()))));
        });
}

HttpEgress::HttpEgress(std::weak_ptr<InfraHttpHost> host,
                       RpcEgressProfile profile)
    : m_host(std::move(host)), m_profile(profile) {}

#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
result<bbt::infra::RpcEnvelope> HttpEgress::_SendRpcProfile(const bbt::infra::RpcAddress& addr,
                                                            const bbt::infra::RpcEnvelope& env,
                                                            const bbt::infra::CallOptions& opt)
{
    // 三仓 §5 迁移后的正式出站：infra 公开 rpc::Client + adapter seam。
    // wire 编解码/remaining_budget_ms 换算/五字段关联校验全部由 infra
    // Client::Call 承担；HTTP/TCP transport adapter 按 profile 选择：
    //   ProtoWireV1 → HttpRpcTransport（底座 http::Client，POST /rpc 固定 profile）
    //   TcpV1       → TcpRpcTransport（底座 client-only Engine，u32be 帧）
    // 两者共用同一份懒建 rpc::Client 缓存；底座是宿主 egress_engine()
    // （client-only Engine，Start 时已 Run）。LegacyHeaders 不经本路径。
    std::shared_ptr<bbt::infra::rpc::Client> client;
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        client = m_rpc_client;
    }
    // 请求侧 body 限额在发起任何 I/O 之前校验（与旧
    // NetworkRuntime::CreateHttpClient → ValidateRequest 的同一语义）：
    // wire body 为 envelope 编码大小，超限 → InvalidArgument，远端零触达。
    // host 已释放且 client 已缓存时按宿主生前限额无法读取——交由 adapter/
    // Engine 状态如实结算（旧链路同此窗口）。
    if (auto host = m_host.lock()) {
        if (env.payload.size() > host->network_limits().max_body_bytes)
            return result<bbt::infra::RpcEnvelope>::err(MakeError(
                ErrorCode::InvalidArgument,
                "rpc egress: request payload exceeds host max_body_bytes"));
    }
    if (!client) {
        auto host = m_host.lock();
        std::shared_ptr<bbt::infra::net::Engine> engine;
        if (host)
            engine = host->egress_engine();
        if (!engine)
            return result<bbt::infra::RpcEnvelope>::err(MakeError(
                ErrorCode::RuntimeUnavailable,
                "rpc egress: client-only engine unavailable"));

        bbt::infra::result<std::shared_ptr<bbt::infra::rpc::IRpcTransport>>
            transport{bbt::infra::result<std::shared_ptr<bbt::infra::rpc::IRpcTransport>>::err(
                MakeError(ErrorCode::InvalidArgument, "unset"))};
        const auto& limits = host->network_limits();
        if (m_profile == RpcEgressProfile::TcpV1) {
#if defined(BBT_FRAMEWORK_HAS_RPC_TCP)
            // 帧上限按宿主 NetworkLimits.max_body_bytes 收敛（与入站同一限额
            // owner 语义）；连接/inflight 由 adapter 自带默认（64），宿主
            // 限额不放大 adapter 上限——取 min 保留旧链路的较小者生效语义。
            bbt::infra::rpc::TcpRpcOptions tcp_options;
            tcp_options.max_inflight = std::min<std::size_t>(
                tcp_options.max_inflight, limits.max_inflight);
            tcp_options.max_frame_bytes = std::min<std::size_t>(
                tcp_options.max_frame_bytes, limits.max_body_bytes);
            auto made = bbt::infra::rpc::TcpRpcTransport::Create(
                engine, tcp_options);
            if (!made)
                return result<bbt::infra::RpcEnvelope>::err(std::move(made.error()));
            transport = bbt::infra::result<std::shared_ptr<bbt::infra::rpc::IRpcTransport>>::ok(
                std::move(made).value());
#else
            return result<bbt::infra::RpcEnvelope>::err(MakeError(
                ErrorCode::RuntimeUnavailable,
                "rpc egress: TcpV1 profile requires bbt::infra_rpc_tcp "
                "(BBT_INFRA_RPC_TCP not configured)"));
#endif
        } else {
            // http::Client 选项按宿主 NetworkLimits 收敛：连接池上限取
            // max_connections 与默认池上限的较小者（不放大），排队上界取
            // max_inflight；单响应体上限 = max_body_bytes（请求侧 body 校验
            // 在下方显式执行，与旧 CreateHttpClient 链路同语义）。
            bbt::infra::http::ClientOptions client_options;
            client_options.limits.max_connections = std::min<std::size_t>(
                client_options.limits.max_connections,
                limits.max_connections);
            client_options.limits.max_queue = std::min<std::size_t>(
                client_options.limits.max_queue, limits.max_inflight);
            client_options.max_body_bytes = limits.max_body_bytes;
            auto http_client = bbt::infra::http::Client::Create(
                engine, client_options);
            if (!http_client)
                return result<bbt::infra::RpcEnvelope>::err(
                    std::move(http_client.error()));
            auto made = bbt::infra::rpc::HttpRpcTransport::Create(
                std::move(http_client).value());
            if (!made)
                return result<bbt::infra::RpcEnvelope>::err(std::move(made.error()));
            transport = bbt::infra::result<std::shared_ptr<bbt::infra::rpc::IRpcTransport>>::ok(
                std::move(made).value());
        }

        auto created = bbt::infra::rpc::Client::Create(
            std::move(transport).value());
        if (!created)
            return result<bbt::infra::RpcEnvelope>::err(std::move(created.error()));
        std::lock_guard<std::mutex> lk(m_mtx);
        if (!m_rpc_client)
            m_rpc_client = std::move(created.value());
        client = m_rpc_client;
    }

    // transport/排队/取消/OutcomeUnknown（含 request_phase 事实）原样透传；
    // rpc::Client::Call 是 bbt_co_async——本 Send 在受管协程内调用。
    return bbt_co_await client->Call(
        addr, bbt::infra::RpcEnvelope(env), opt);
}
#else
result<bbt::infra::RpcEnvelope> HttpEgress::_SendRpcProfile(const bbt::infra::RpcAddress&,
                                                            const bbt::infra::RpcEnvelope&,
                                                            const bbt::infra::CallOptions&)
{
    return result<bbt::infra::RpcEnvelope>::err(MakeError(
        ErrorCode::RuntimeUnavailable,
        "rpc egress: ProtoWireV1/TcpV1 profile requires bbt::infra_rpc_net "
        "(BBT_INFRA_RPC_NET + protobuf prefix not configured)"));
}
#endif

result<bbt::infra::RpcEnvelope> HttpEgress::Send(
    const bbt::infra::RpcAddress& addr,
    const bbt::infra::RpcEnvelope& env,
    const bbt::infra::CallOptions& opt) {
    // transport 是出站 adapter 路由标识：LegacyHeaders 仅 http；新栈
    // profile 由 infra rpc::Client/adapter 校验 transport∈{http,tcp} 与
    // endpoint 形态，本层不重复 wire 语义检查。
    if (addr.transport != "http" && addr.transport != "tcp")
        return result<bbt::infra::RpcEnvelope>::err(MakeError(
            ErrorCode::InvalidArgument,
            "rpc egress: unsupported transport '" + addr.transport + "'"));
    if (addr.endpoint.empty())
        return result<bbt::infra::RpcEnvelope>::err(MakeError(
            ErrorCode::InvalidArgument,
            "rpc egress: empty endpoint"));

    // 新栈 profile（ProtoWireV1/TcpV1）：统一走 infra rpc::Client + adapter
    // seam；transport/编解码/关联全部归 infra。
    if (m_profile != RpcEgressProfile::LegacyHeaders)
        return _SendRpcProfile(addr, env, opt);

    // LegacyHeaders 保持 x-bbt-* 编解码逐字不变；仅承载于 HTTP。
    if (addr.transport != "http")
        return result<bbt::infra::RpcEnvelope>::err(MakeError(
            ErrorCode::InvalidArgument,
            "rpc egress: LegacyHeaders profile only supports http transport"));

    std::shared_ptr<bbt::infra::HttpClient> client;
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        client = m_client;
    }
    if (!client) {
        auto host = m_host.lock();
        std::shared_ptr<bbt::infra::NetworkRuntime> rt;
        if (host)
            rt = host->network_runtime();
        if (!rt)
            return result<bbt::infra::RpcEnvelope>::err(MakeError(
                ErrorCode::RuntimeUnavailable,
                "http egress: network runtime unavailable"));
        auto created = rt->CreateHttpClient();
        if (!created)
            return result<bbt::infra::RpcEnvelope>::err(
                std::move(created.error()));
        std::lock_guard<std::mutex> lk(m_mtx);
        if (!m_client)
            m_client = std::move(created.value());
        client = m_client;
    }

    auto res = client->Request(
        http_bridge::ToHttpRequest(
            env, addr.transport + "://" + addr.endpoint + "/rpc"),
        opt);
    if (!res)
        return result<bbt::infra::RpcEnvelope>::err(
            std::move(res.error()));
    return http_bridge::EnvelopeFromResponse(res.value());
}

} // namespace bbt::framework
