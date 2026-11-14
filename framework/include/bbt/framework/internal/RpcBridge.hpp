#pragma once
// V2 接线：运行时状态只处理业务 Request/Response，wire 与会话由 infra 持有。

#include <memory>
#include <string_view>

#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/rpc/Options.hpp>
#include <bbt/infra/rpc/Request.hpp>
#include <bbt/infra/rpc/Response.hpp>
#include <bbt/framework/Result.hpp>
#include <bbt/framework/Object.hpp>

namespace bbt::framework {
class InfraHttpHost;
struct AppRuntimeState;

/** @brief 安装框架业务入口；宿主负责有界 admission 转移与回复寿命。 */
void InstallRpcAdmission(InfraHttpHost& host, std::shared_ptr<AppRuntimeState> state);

/** @brief 将显式 HTTP 路由接入同一受管分发器。 */
void InstallHttpAdmission(InfraHttpHost& host, std::shared_ptr<AppRuntimeState> state,
                          std::vector<HttpServiceRoute> routes);

/** @brief 借用宿主唯一会话 owner，不另建传输或关闭共享资源。 */
class RpcEgress {
public:
    explicit RpcEgress(std::weak_ptr<InfraHttpHost> host);
    result<bbt::infra::rpc::Response> Call(std::string_view target,
                                           const bbt::infra::rpc::Request& request, const bbt::infra::CallOptions& options);
    result<void> Send(std::string_view target, const bbt::infra::rpc::Request& request,
                      const bbt::infra::rpc::SendOptions& options);

private:
    std::weak_ptr<InfraHttpHost> m_host;
};
} // namespace bbt::framework
