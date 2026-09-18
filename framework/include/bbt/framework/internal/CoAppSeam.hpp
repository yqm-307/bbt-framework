#pragma once
// service-actor/v2：CoApp 测试缝（机器面，internal/）。
//
// 业务路径只用 CoApp(CoAppOptions)——宿主与出站由框架默认装配
// （InfraHttpHost + RpcHttpBridge + HttpEgress）。本头暴露的注入点
// 给框架自身测试用：替换网络宿主（INetworkHost 桩）或出站发送
// （逻辑 endpoint → 对端真实地址的解析），业务侧不出现这些参数。
//
// 测试缝 CoApp 只能经 MakeCoAppForTest 构造——CoApp(CoAppOptions,
// CoAppSeam) 是私有构造，普通 public 业务 API 拿不到测试缝构造。

#include <functional>
#include <memory>

#include <bbt/infra/NetworkTypes.hpp>

#include <bbt/framework/Result.hpp>

namespace bbt::framework {

class CoApp;
class INetworkHost;
struct CoAppOptions;

// 应用级出站发送注入点（测试缝）：经路由门解析出的目标地址 +
// 完整 envelope + 已适配的 infra::CallOptions → 回复 envelope。
// 业务路径由框架默认 HttpEgress 供给，不暴露本类型。
using RpcSendAppFn = std::function<result<bbt::infra::RpcEnvelope>(
    const bbt::infra::RpcAddress&,
    const bbt::infra::RpcEnvelope&,
    const bbt::infra::CallOptions&)>;

// CoApp 测试缝：显式注入网络宿主与出站发送。host 为空 → run 校验
// 失败（与业务路径「宿主由框架装配」同口径：空宿主不是合法配置）。
// rpc_send 为空 → 受管出站如实 RuntimeUnavailable。
struct CoAppSeam {
    std::shared_ptr<INetworkHost> host;
    RpcSendAppFn                rpc_send;
};

// 测试侧唯一构造口：把测试缝交给 CoApp。普通 public 构造
// CoApp(CoAppOptions) 在 CoApp.hpp；本工厂仅框架测试经本头获得。
std::unique_ptr<CoApp> MakeCoAppForTest(CoAppOptions options,
                                        CoAppSeam seam);

} // namespace bbt::framework
