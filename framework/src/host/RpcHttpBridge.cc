#include <bbt/framework/internal/RpcHttpBridge.hpp>

#include <chrono>
#include <cctype>
#include <string_view>
#include <utility>

#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
#include <bbt/infra/rpc/RpcWire.hpp>
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

result<bbt::infra::RpcEnvelope> HttpEgress::_SendWireProfile(const bbt::infra::RpcAddress& addr,
                                                             const bbt::infra::RpcEnvelope& env,
                                                             const bbt::infra::CallOptions& opt)
{
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
    // 正式 ProtoWireV1 出站：承载通道是同一 NetworkRuntime::CreateHttpClient
    // （与 LegacyHeaders 共用懒建缓存）——runtime 限额 owner 统一持有
    // max_connections/max_inflight，ValidateRequest 与响应 parser 都按宿主
    // NetworkLimits 做请求/响应字节校验；不另建 http::Client/Engine/pool，
    // 也不引入丢失共享限额的第二 owner（F3/F2）。wire 编码/关联/预算换算
    // 全部经 infra 公开 wire 面（ToWireEnvelope/EncodeRpcWireEnvelope/
    // DecodeRpcWireEnvelope/FromWireEnvelope），POST /rpc 固定 profile；
    // 本地 deadline→remaining_budget_ms 的换算与五字段关联校验沿用旧
    // CreateRpcClient→CreateHttpClient 链路的同一语义（上游把客户端封装
    // 下沉为 rpc::Client，本仓按既有 wire 语义经公开函数直接承载）。

    // endpoint 边界：裸 authority（host[:port]），同旧链路 IsRpcAuthority。
    auto is_authority = [](std::string_view ep) noexcept {
        if (ep.empty() || ep.find("://") != std::string_view::npos)
            return false;
        for (const char ch : ep) {
            const auto c = static_cast<unsigned char>(ch);
            if (c <= 0x20 || c == 0x7f || ch == '/' || ch == '?' ||
                ch == '#' || ch == '@')
                return false;
        }
        return true;
    };
    if (!is_authority(addr.endpoint))
        return result<bbt::infra::RpcEnvelope>::err(MakeError(
            ErrorCode::InvalidArgument,
            "http egress: rpc endpoint must be an authority without scheme or path"));

    // 本地期限 → wire remaining_budget_ms：已过期 → TimedOut（不做任何 I/O）；
    // 合法值 clamp 到 [1, kMaxRemainingBudgetMs]（同旧链路 RemainingBudgetMs）。
    const auto now = std::chrono::steady_clock::now();
    if (opt.deadline <= now)
        return result<bbt::infra::RpcEnvelope>::err(MakeError(
            ErrorCode::TimedOut, "rpc deadline already expired"));
    auto remaining =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            opt.deadline - now).count();
    if (remaining < 1)
        remaining = 1;
    if (remaining > static_cast<long long>(bbt::infra::rpc::kMaxRemainingBudgetMs))
        remaining = static_cast<long long>(bbt::infra::rpc::kMaxRemainingBudgetMs);
    const auto budget = static_cast<std::uint32_t>(remaining);

    auto wire = bbt::infra::rpc::ToWireEnvelope(env, budget);
    if (!wire)
        return result<bbt::infra::RpcEnvelope>::err(std::move(wire.error()));
    auto body = bbt::infra::rpc::EncodeRpcWireEnvelope(
        wire.value(), /*is_response=*/false);
    if (!body)
        return result<bbt::infra::RpcEnvelope>::err(std::move(body.error()));

    // 懒建并缓存共享 HttpClient（一次；runtime 限额 owner）。缓存锁不跨
    // Request：Request 是协程阻塞调用，持锁会串行化所有出站并可能与取消
    // 路径互锁。
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

    bbt::infra::HttpRequest req;
    req.method  = "POST";
    req.url     = "http://" + addr.endpoint + bbt::infra::rpc::kRpcWireHttpPath;
    req.headers = {{std::string(bbt::infra::rpc::kRpcWireContentTypeKey),
                    std::string(bbt::infra::rpc::kRpcWireContentType)}};
    req.body.assign(reinterpret_cast<const char*>(body.value().data()),
                    body.value().size());

    // 传输/排队/取消/未知结果错误原样透传（含 request_phase 事实）。
    auto res = client->Request(std::move(req), opt);
    if (!res)
        return result<bbt::infra::RpcEnvelope>::err(std::move(res.error()));

    const auto& resp = res.value();
    if (resp.status != 200) {
        auto error = MakeError(ErrorCode::Unavailable,
            "rpc endpoint returned non-success HTTP status");
        error.backend_category = "http";
        error.backend_code = static_cast<int>(resp.status);
        return result<bbt::infra::RpcEnvelope>::err(std::move(error));
    }
    // 回复形态（与 infra ParseRpcWireHttpResponse 同契约）：Content-Type
    // 恰为 application/x-protobuf + 非空 body；违规 InvalidArgument/ProtocolError。
    const auto has_content_type = [&resp]() {
        const auto eq_ci = [](const std::string& a, const std::string& b) {
            if (a.size() != b.size()) return false;
            for (std::size_t i = 0; i < a.size(); ++i)
                if (std::tolower(static_cast<unsigned char>(a[i])) !=
                    std::tolower(static_cast<unsigned char>(b[i])))
                    return false;
            return true;
        };
        for (const auto& [k, v] : resp.headers)
            if (eq_ci(k, bbt::infra::rpc::kRpcWireContentTypeKey) &&
                v == bbt::infra::rpc::kRpcWireContentType)
                return true;
        return false;
    };
    if (!has_content_type())
        return result<bbt::infra::RpcEnvelope>::err(MakeError(
            ErrorCode::InvalidArgument,
            "rpc wire response missing Content-Type"));
    if (resp.body.empty())
        return result<bbt::infra::RpcEnvelope>::err(MakeError(
            ErrorCode::ProtocolError, "rpc wire response empty body"));

    auto decoded = bbt::infra::rpc::DecodeRpcWireEnvelope(
        resp.body, /*is_response=*/true);
    if (!decoded)
        return result<bbt::infra::RpcEnvelope>::err(
            std::move(decoded.error()));
    const auto& reply = decoded.value();
    if (reply.request_id != env.request_id ||
        reply.service != env.service ||
        reply.method != env.method ||
        reply.request_schema != env.request_schema ||
        reply.response_schema != env.response_schema)
        return result<bbt::infra::RpcEnvelope>::err(MakeError(
            ErrorCode::ProtocolError,
            "rpc reply envelope association mismatch"));
    if (!reply.success)
        return result<bbt::infra::RpcEnvelope>::err(reply.error);
    return bbt::infra::rpc::FromWireEnvelope(reply);
#else
    (void)addr;
    (void)env;
    (void)opt;
    return result<bbt::infra::RpcEnvelope>::err(MakeError(
        ErrorCode::RuntimeUnavailable,
        "http egress: ProtoWireV1 profile requires bbt::infra_rpc_net "
        "(BBT_INFRA_RPC_NET + protobuf prefix not configured)"));
#endif
}

result<bbt::infra::RpcEnvelope> HttpEgress::Send(
    const bbt::infra::RpcAddress& addr,
    const bbt::infra::RpcEnvelope& env,
    const bbt::infra::CallOptions& opt) {
    if (addr.transport != "http")
        return result<bbt::infra::RpcEnvelope>::err(MakeError(
            ErrorCode::InvalidArgument,
            "http egress: unsupported transport '" + addr.transport + "'"));
    if (addr.endpoint.empty())
        return result<bbt::infra::RpcEnvelope>::err(MakeError(
            ErrorCode::InvalidArgument,
            "http egress: empty endpoint"));

    // ProtoWireV1 走正式 body wire（同一共享 HttpClient + infra 公开 RpcWire
    // 编解码）；LegacyHeaders 保持 x-bbt-* 编解码逐字不变。两者共用同一份
    // 懒建 HttpClient 缓存（同一 runtime 限额 owner）。
    if (m_profile == RpcEgressProfile::ProtoWireV1)
        return _SendWireProfile(addr, env, opt);

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
