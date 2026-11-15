#include <bbt/framework/internal/RpcBridge.hpp>

#include <utility>

#include <bbt/framework/CoApp.hpp>
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

void InstallRpcAdmission(InfraHttpHost& host, CoApp& app) {
    InstallRpcAdmission(host, app.m_state);
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
