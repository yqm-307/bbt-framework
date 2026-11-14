#pragma once
// Consul 发现组件（应用侧组装，v2::CoComponent）。
//
// 职责：
//   - 借用宿主共享 net::Engine（不 Run、不 RequestClose、不关闭——共享
//     Engine 归应用根，唯一一次 Run 由入口 host hook 执行）；
//   - 自持专属 http::Client + bbt::infra::consul::Client（infra 已验收的
//     adapter，不重写协议；专属客户端的关闭责任归本组件 OnStop）；
//   - 对每个 watch 的逻辑服务跑一个受管协程查询循环，把权威快照整体发布到
//     framework::DynamicRouteTable（出站数据面只读内存）；
//   - 可选自注册/注销：业务进程把自身监听端点登记为某个逻辑服务名。
//
// 不承担：业务路由策略、重试业务请求、TTL/业务健康判定（TCP check 只证明端口
// 可达）、把发现结果写进框架核心。
//
// 生命周期（v2::CoComponent hook）：
//   - OnInstall 只做参数校验与 Idle 装配（http/consul client），不 Run、不建
//     线程、不拨号；
//   - OnStart 登记 watcher 并等全部首快照；任一失败即返回错误，不留半启动
//     状态（watcher 已停、首快照未就绪）；
//   - OnStop 消费调用方同一 CloseOptions：停止 watcher 与视图发布、封口
//     consul（同一模式/期限），不触碰共享 Engine；
//   - watcher 协程只持有共享状态，不借用本对象地址，组件销毁后仍可安全退出。
//
// 边界：
//   - 注册中心只接受 loopback 数值地址（infra adapter 的 fail-closed 校验）；
//   - 查询失败不刷新新鲜度，也不清空 last-known-good；超过路由表陈旧边界后
//     由 Resolve 显式失败（RuntimeUnavailable），绝不无限使用陈旧地址；
//   - 关闭后不再发布新视图；关闭顺序固定为「停 watcher → 封口 consul →
//     等 watcher 退出」。共享 Engine 的收口不在本组件职责内。

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include <bbt/coroutine/sync/WaitTypes.hpp>
#include <bbt/coroutine/syntax/Async.hpp>
#include <bbt/infra/CloseOptions.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/consul/Client.hpp>
#include <bbt/infra/http/Client.hpp>
#include <bbt/infra/net/Engine.hpp>

#include <bbt/framework/Component.hpp>
#include <bbt/framework/DynamicRouteTable.hpp>
#include <bbt/framework/Result.hpp>

namespace ecommerce {

class ConsulDiscoveryComponent final : public bbt::framework::v2::CoComponent {
public:
    struct Options {
        /// 宿主拥有并推进的共享 Engine；不得为空。组件不 Run/不关闭它。
        std::shared_ptr<bbt::infra::net::Engine> engine;
        /// 快照发布目标；不得为空（出站路由表由装配方持有）。
        std::shared_ptr<bbt::framework::DynamicRouteTable> table;
        std::string agent_url; // http://127.0.0.1:<port>
        std::chrono::milliseconds request_timeout{3000};
        std::chrono::milliseconds query_wait{2000};
        std::vector<std::string> watch_services; // 逻辑服务名
    };

    ConsulDiscoveryComponent(bbt::framework::detail::ConstructKey key,
                             Options options);

    ConsulDiscoveryComponent(const ConsulDiscoveryComponent&) = delete;
    ConsulDiscoveryComponent& operator=(const ConsulDiscoveryComponent&) = delete;

    /// 自注册（协程上下文）：成功后记住 instance id 供 DeregisterSelf 使用。
    bbt_co_async(bbt::infra::result<void>) RegisterSelf(
        std::string service, std::string instance_id, std::string address,
        std::uint16_t port);

    /// 等全部首快照（业务在 CoStart 完成后调用；引擎 Running 后 watcher
    /// 才发首个查询）。任一 watch 的首快照失败如实透出，未完成的停在
    /// 「已登记 watcher、尚未就绪」——组件仍 Running、可显式收口。
    bbt_co_async(bbt::infra::result<void>) WaitFirstSnapshots(
        bbt::coroutine::Deadline deadline);

    /// 注销本进程的登记（协程上下文，幂等）。
    bbt_co_async(bbt::infra::result<void>) DeregisterSelf();

    /// 仍在运行的 watcher 协程数（观测口）。
    std::size_t watchers_active() const noexcept;

    /// 观测口：某服务最近一次成功发布的 revision（0 表示尚无成功查询）。
    std::uint64_t Revision(std::string_view service) const;
    /// 观测口：最近一次查询/注册失败原因（成功后被清空）。
    std::string LastError() const;

    /// 当前已装配的组件本体（非拥有；OnInstall 时登记、对象销毁时清空）。
    /// 只供装配进程在组件存活窗口（CoStart 后、CoStop 前）调用
    /// Register/DeregisterSelf——v2 面没有取回受管对象本体的公共口，
    /// 本组件是应用侧类型，自证存活窗口即可，不进入产品面。
    static ConsulDiscoveryComponent* Current() noexcept;

protected:
    ~ConsulDiscoveryComponent() noexcept override;

    /// 参数校验 + Idle 装配专属 http/consul client（不拨号、不建线程）。
    bbt::framework::result<void> OnInstall() override;
    /// 登记 watcher（不等待）；首快照等待由业务在 CoStart 后显式调用
    /// WaitFirstSnapshots。
    bbt::framework::result<void> OnStart() override;
    /// 停 watcher → 视图标记不可用 → 同一 CloseOptions 封口 consul。
    bbt::framework::result<void> OnStop(
        const bbt::infra::CloseOptions& options) override;
    /// 组件销毁路径：清空 Current 槽位。
    void OnUninstall() override;

private:
    struct State;
    struct Watch;

    enum class WaitKind {
        FirstSnapshots,
        WatchersDrained
    };

    static bbt_co_async(void) _WatchLoop(
        std::shared_ptr<State> state, std::size_t watch_index);
    static bbt_co_async(bbt::infra::result<void>) _WaitFor(
        const std::shared_ptr<State>& state, WaitKind kind,
        bbt::coroutine::Deadline deadline);
    static void _SetLastError(const std::shared_ptr<State>& state,
                              std::string message);

    Options m_options;
    std::shared_ptr<State> m_state;
    std::shared_ptr<bbt::infra::http::Client> m_http;
    std::atomic<bool> m_sealed{false};

    static std::atomic<ConsulDiscoveryComponent*> s_current;

    friend class bbt::framework::detail::Registry;
};

} // namespace ecommerce
