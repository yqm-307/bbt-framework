#pragma once
/**
 * 应用运行时状态（机器面，internal/）：run 期持有的全部受管对象。
 *
 * 寿命契约：CoApp 只是外壳。入站 handler 闭包与出站回调各自复制本对象的
 * shared_ptr（见 InstallRpcAdmission 与 Send），不借用
 * CoApp 成员、不捕获 CoApp this——CoApp 先析构也不影响在途请求，在途请求
 * 自带其需要的全部对象。
 *
 * 关闭序列（on_release → Release）切断本对象指向业务对象的全部边：
 * dispatcher 一并放弃，不再接纳新分发。在途分发
 * 已取走本地 dispatcher 强引用继续执行，分发表项各自强持有
 * table/registry/instance/egress，不依赖本对象继续存活。由此 CoApp 正常
 * 收束后本对象可被完全回收——业务对象内的出站回调只指向出站依赖
 * （routes/policy/egress/seam_send 所在的本对象字段），不外环回业务对象。
 *
 * 锁序：Publish/Release/find_service/registry 访问在 mtx 下完成；CoApp 侧
 * 调用方先持 m_mtx 再取本对象 mtx（单向 m_mtx → mtx）。Dispatch 只在 mtx 内
 * 取 dispatcher 本地强引用，锁外执行分发，不与 m_mtx 交互。
 */

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>

#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/rpc/Request.hpp>
#include <bbt/infra/rpc/Response.hpp>

#include <bbt/coroutine/syntax/Async.hpp>

#include <bbt/framework/DynamicRouteTable.hpp>
#include <bbt/framework/Result.hpp>
#include <bbt/framework/RpcPolicy.hpp>
#include <bbt/framework/internal/CoAppSeam.hpp>
#include <bbt/framework/internal/MethodTable.hpp>

namespace bbt::framework {

class RpcEgress;
class InboundDispatcher;

struct AppRuntimeState {
    /**
     * 装配期写入（构造/Publish，先于接纳），此后只读：出站依赖。
     */
    std::map<std::string, std::string> routes;
    /**
     * 出站回调强持有同一动态表，不借用 CoApp；表内同步保护后续快照更新。
     */
    std::shared_ptr<DynamicRouteTable> dynamic_routes;
    std::shared_ptr<RpcPolicy> rpc_policy;
    std::shared_ptr<RpcEgress> default_egress;
    RpcSendAppFn seam_send;

    /**
     * run 期受管对象（mtx 保护）。Publish 填入；Release 全部放弃。
     * 服务对象本体由 Registry 强持有；分发链路只在 Publish 与 Release
     * 之间经 dispatcher 表项访问它们（排空先于 Release，见 CoApp 收口序）。
     */
    std::mutex mtx;
    std::shared_ptr<InboundDispatcher> dispatcher;

    /**
     * 查询与真实出站共用路由门：动态已配置服务不回退静态地址。
     */
    result<std::string> FindRoute(std::string_view service) const;

    /**
     * 出站发送（机器面）：路由门 → 公开策略 Preflight → 默认 RpcEgress 或
     * 测试缝发送。回调复制本对象的 shared_ptr，不捕获 CoApp this。
     * 真实出站可挂起当前协程，消费端经 bbt_co_await 解包。
     */
    bbt_co_async(result<bbt::infra::rpc::Response>) Send(
        const bbt::infra::rpc::Request& env,
        const bbt::infra::CallOptions& options) const;

    /**
     * 绑定完成：填入分发表并开放接纳（Publish 与 Release 互斥配对）。
     */
    void Publish(std::shared_ptr<InboundDispatcher> d);

    /**
     * 收束：拒新分发并放弃全部受管对象引用；在途分发靠本地强引用与
     * 分发表项自持收尾。
     */
    void Release() noexcept;

    /**
     * 入站分发入口：取 dispatcher 本地强引用后锁外分发；未安装/已收束 →
     * RuntimeUnavailable，明确失败而非悬垂访问。
     */
    result<bbt::infra::rpc::Response> Dispatch(
        const bbt::infra::rpc::Request& request,
        const std::function<void()>& on_dispatched = {});

    /**
     * 运行期移除单个服务分发表项（卸载路径；在途执行由对象收口经
     * 执行租约等待）。未发布/已收束为空操作。
     */
    void RemoveDispatch(const std::string& service) noexcept;
};

} // namespace bbt::framework
