#include <bbt/framework/internal/RpcBridge.hpp>

#include <utility>
#include <algorithm>
#include <cctype>

#include <bbt/framework/internal/AppRuntimeState.hpp>
#include <bbt/framework/internal/ErrorDomainRule.hpp>
#include <bbt/framework/internal/InfraHttpHost.hpp>

namespace bbt::framework {
void InstallRpcAdmission(InfraHttpHost& host, std::shared_ptr<AppRuntimeState> state) {
    host.InstallRpcHandler([state = std::move(state)](const bbt::infra::rpc::Request& request, const std::function<void()>& on_dispatched) {
        auto reply = state->Dispatch(request, on_dispatched);
        if (!reply) {
            if (auto valid = ValidateErrorAtBoundary(reply.error()); !valid)
                return result<bbt::infra::rpc::Response>::err(std::move(valid.error()));
        }
        return reply;
    });
}

void InstallHttpAdmission(InfraHttpHost& host, std::shared_ptr<AppRuntimeState> state,
                          std::vector<HttpServiceRoute> routes) {
    host.InstallHandler([state = std::move(state), routes = std::move(routes)](
                            bbt::infra::IncomingCallContext ctx,
                            bbt::infra::http::IncomingRequest http) {
        using HttpResult = result<bbt::infra::http::Response>;
        bbt::infra::http::Response out;
        const auto fail = [&out](int status, Error error) {
            out.status_code = status;
            out.headers = {{"Content-Type", "text/plain"},
                           {"X-Bbt-Error-Code", std::to_string(static_cast<int>(error.code))}};
            out.body = std::move(error.message);
            return HttpResult::ok(std::move(out));
        };
        auto route = std::find_if(routes.begin(), routes.end(), [&http](const auto& r) {
            return r.path == http.target;
        });
        if (route == routes.end())
            return fail(404, MakeError(ErrorCode::NotFound, "HTTP route not found"));
        if (http.method != "POST")
            return fail(405, MakeError(ErrorCode::InvalidArgument, "HTTP route requires POST"));
        std::string request_id;
        std::string content_type;
        bool saw_content_type = false;
        for (const auto& [key, value] : http.headers) {
            std::string lower = key;
            std::transform(lower.begin(), lower.end(), lower.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            if (lower == "x-request-id") {
                if (!request_id.empty() || value.empty() || value.size() > 128 ||
                    value.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.") != std::string::npos)
                    return fail(400, MakeError(ErrorCode::InvalidArgument, "invalid HTTP request id"));
                request_id = value;
            } else if (lower == "content-type") {
                if (saw_content_type)
                    return fail(415, MakeError(ErrorCode::InvalidArgument, "duplicate HTTP content type"));
                saw_content_type = true;
                content_type = value;
            }
        }
        if (request_id.empty())
            return fail(400, MakeError(ErrorCode::InvalidArgument, "HTTP request id required"));
        if (content_type != "application/x-protobuf")
            return fail(415, MakeError(ErrorCode::InvalidArgument,
                                       "HTTP content type must be application/x-protobuf"));
        bbt::infra::rpc::Request request(route->service, route->method);
        request.SetRequestId(request_id)
            .SetRequestSchema(route->request_schema)
            .SetResponseSchema(route->response_schema)
            .SetPayload(std::vector<std::uint8_t>(http.body.begin(), http.body.end()));
        // HTTP 上下文直接来自 parser，不把自报头变成主体、期限或系统元数据。
        auto reply = state->Dispatch(request, {}, &ctx);
        if (!reply) {
            auto error = reply.error();
            if (auto valid = ValidateErrorAtBoundary(error); !valid)
                error = valid.error();
            const int status = error.code == ErrorCode::InvalidArgument ? 400
                               : error.code == ErrorCode::NotFound      ? 404
                               : error.code == ErrorCode::TimedOut      ? 504
                                                                        : 503;
            return fail(status, std::move(error));
        }
        out.status_code = 200;
        out.headers = {{"Content-Type", "application/x-protobuf"}, {"X-Request-Id", request_id}};
        const auto& payload = reply.value().Payload();
        out.body.assign(payload.begin(), payload.end());
        return HttpResult::ok(std::move(out));
    });
}

RpcEgress::RpcEgress(std::weak_ptr<InfraHttpHost> host) : m_host(std::move(host)) {}

result<bbt::infra::rpc::Response> RpcEgress::Call(std::string_view target,
                                                  const bbt::infra::rpc::Request& request, const bbt::infra::CallOptions& options) {
    auto host = m_host.lock();
    auto client = host ? host->rpc_client() : nullptr;
    if (!client)
        return result<bbt::infra::rpc::Response>::err(MakeError(
            ErrorCode::RuntimeUnavailable, "rpc egress: host client unavailable"));
    if (request.Payload().size() > host->network_limits().max_body_bytes)
        return result<bbt::infra::rpc::Response>::err(MakeError(
            ErrorCode::InvalidArgument, "rpc egress: request body exceeds host limit"));
    auto reply = bbt_co_await client->Call(target, request, options);
    if (!reply) {
        if (auto valid = ValidateErrorAtBoundary(reply.error()); !valid)
            return result<bbt::infra::rpc::Response>::err(std::move(valid.error()));
    }
    return reply;
}

result<void> RpcEgress::Send(std::string_view target, const bbt::infra::rpc::Request& request,
                             const bbt::infra::rpc::SendOptions& options) {
    auto host = m_host.lock();
    auto client = host ? host->rpc_client() : nullptr;
    if (!client)
        return result<void>::err(MakeError(
            ErrorCode::RuntimeUnavailable, "rpc egress: host client unavailable"));
    if (request.Payload().size() > host->network_limits().max_body_bytes)
        return result<void>::err(MakeError(
            ErrorCode::InvalidArgument, "rpc egress: request body exceeds host limit"));
    return bbt_co_await client->Send(target, request, options);
}
} // namespace bbt::framework
