#include <bbt/framework/internal/RpcHttpBridge.hpp>

#include <chrono>
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

result<bbt::infra::RpcEnvelope> HttpEgress::_SendWireProfile(
    const bbt::infra::RpcAddress& addr,
    const bbt::infra::RpcEnvelope& env,
    const bbt::infra::CallOptions& opt,
    const std::shared_ptr<bbt::infra::HttpClient>& client) {
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
    namespace rpcw = bbt::infra::rpc;
    // 发起 I/O 前再次检查本进程既定生效期限：已过期 → 不做任何 I/O。
    // （AdaptCallOptions 已在出站口拦截一次；此处是 egress 自身最后一道，
    // 防止 caller 传入已过期 deadline 时误发。）
    const auto now = std::chrono::steady_clock::now();
    if (opt.deadline <= now)
        return result<bbt::infra::RpcEnvelope>::err(MakeError(
            ErrorCode::TimedOut,
            "http egress: budget expired before wire io"));
    long long remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
        opt.deadline - now).count();
    if (remaining < 1)
        remaining = 1;
    if (remaining > static_cast<long long>(rpcw::kMaxRemainingBudgetMs))
        remaining = static_cast<long long>(rpcw::kMaxRemainingBudgetMs);

    auto wire = rpcw::ToWireEnvelope(env, static_cast<std::uint32_t>(remaining));
    if (!wire)
        return result<bbt::infra::RpcEnvelope>::err(std::move(wire.error()));
    auto req = rpcw::MakeRpcWireHttpRequest(
        wire.value(), addr.transport + "://" + addr.endpoint);
    if (!req)
        return result<bbt::infra::RpcEnvelope>::err(std::move(req.error()));

    auto res = client->Request(req.value(), opt);
    if (!res)
        return result<bbt::infra::RpcEnvelope>::err(std::move(res.error()));

    auto parsed = rpcw::ParseRpcWireHttpResponse(res.value());
    if (!parsed)
        return result<bbt::infra::RpcEnvelope>::err(std::move(parsed.error()));
    const rpcw::RpcWireEnvelope& w = parsed.value();

    // 关联字段校验：request_id 必须回显；错误分支（success=false）经既有
    // Error 映射保留 code/domain/details，不被当成成功（含 OutcomeUnknown）。
    if (w.request_id != env.request_id)
        return result<bbt::infra::RpcEnvelope>::err(MakeError(
            ErrorCode::ProtocolError,
            "wire egress: reply request_id mismatch"));
    if (!w.success)
        return result<bbt::infra::RpcEnvelope>::err(w.error);
    if (w.response_schema != env.response_schema)
        return result<bbt::infra::RpcEnvelope>::err(MakeError(
            ErrorCode::ProtocolError,
            "wire egress: reply response_schema mismatch"));
    return rpcw::FromWireEnvelope(w);
#else
    (void)addr; (void)env; (void)opt; (void)client;
    return result<bbt::infra::RpcEnvelope>::err(MakeError(
        ErrorCode::RuntimeUnavailable,
        "http egress: ProtoWireV1 profile requires bbt::infra_rpc "
        "(protobuf prefix not configured)"));
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

    if (m_profile == RpcEgressProfile::ProtoWireV1)
        return _SendWireProfile(addr, env, opt, client);

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
