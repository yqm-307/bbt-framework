#include <bbt/framework/internal/RpcHttpBridge.hpp>

#include <utility>

#include <bbt/framework/CoApp.hpp>
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
    res.status = 400;
    const auto& e = r.error();
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

HttpEgress::HttpEgress(std::weak_ptr<InfraHttpHost> host)
    : m_host(std::move(host)) {}

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
