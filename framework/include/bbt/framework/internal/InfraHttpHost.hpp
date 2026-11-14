#pragma once
// Framework 网络宿主：同一 Engine 承担入站接纳与出站会话。
//
// Create 在 Engine Idle 阶段装配 Client/Server/handoff（不联网、不建线程）；
// Start 在宿主控制协程内经 Engine::CoWaitRunning 等入口 native host hook 在
// 主线程执行的唯一一次 Run 发布 Ready，之后才启动 Server 的接纳消费；
// StopAccepting 先关闭业务门；WaitHandlersDone 只用 CoWaiter 等业务与回复写
// 终态归零；Close 发布首因、封口 server/client。
//
// Engine 归属有两种形态：
//  - 借用（生产）：构造期由应用根传入共享 `net::Engine`；本宿主只借用，Close
//    不关闭它，WaitPhysicalReleased 只释放本宿主自己的模块 owner（Run 与物理
//    收口由应用入口的宿主闸门在主任务结束后 RequestClose 兑现），因此 CoStop
//    不等待 Engine；
//  - 自有（测试缝/过渡）：不传入 Engine 时由 Create 自建；Close 对其发非阻塞
//    RequestClose，WaitPhysicalReleased 观察 Run 已返回且 Phase 为 Stopped。
//
// 本类不持有 native runner/thread/future/join：Engine 的 native 执行只由应用
// 入口 host hook 调用一次；析构只发非阻塞关闭请求并释放自身 owner 引用，不 join、
// 不跑 Engine 析构兜底。
//
// HandlerState 自持；业务对象寿命由运行时状态 shared_ptr（AppRuntimeState）
// 保证——handler 闭包复制该指针，CoApp 先析构也不影响在途请求。

#include <cstddef>
#include <cstdint>
#include <deque>
#include <set>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <system_error>
#include <utility>

#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/coroutine/sync/WaitTypes.hpp>
#include <bbt/coroutine/syntax/Async.hpp>

#include <bbt/infra/CloseOptions.hpp>
#include <bbt/infra/http/Server.hpp>
#include <bbt/infra/http/Client.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/rpc/CoRpcServer.hpp>
#include <bbt/infra/rpc/CoRpcClient.hpp>

#include <bbt/framework/internal/HostLifecycle.hpp>

namespace bbt::framework {

class InfraHttpHost final : public INetworkHost {
public:
    /// @brief 自有 Engine 形态（测试缝/过渡宿主）：Create 时自建 Engine。
    InfraHttpHost(bbt::infra::NetworkLimits limits,
                  bbt::infra::ListenAddress listen,
                  std::string local_identity);
    /// @brief 借用共享 Engine 形态（生产）：Engine 归应用根，本宿主不关闭它。
    InfraHttpHost(std::shared_ptr<bbt::infra::net::Engine> engine,
                  bbt::infra::NetworkLimits limits,
                  bbt::infra::ListenAddress listen,
                  std::string local_identity);
    // 非阻塞析构：封业务门、发布关闭原因、对 server/client 与自有 Engine 发
    // 非阻塞关闭请求，并释放本对象的 owner 引用（不 join、不阻塞）。
    ~InfraHttpHost() noexcept override;

    void InstallHandler(bbt::infra::http::ServerHandler handler);
    // 业务入口必须在任何可挂起操作之前调用 on_dispatched（同步校验/入邮箱后）。
    using RpcDispatch = std::function<result<bbt::infra::rpc::Response>(
        const bbt::infra::rpc::Request&, const std::function<void()>&)>;
    void InstallRpcHandler(RpcDispatch handler);
    /// @brief 是否已安装 HTTP 入站 handler（seam 装配判据）。
    bool http_handler_installed() const;
    /// @brief 是否已安装 RPC 入站 handler（seam 装配判据）。
    bool rpc_handler_installed() const;

    std::shared_ptr<bbt::infra::rpc::CoRpcServer> rpc_server() const;
    std::shared_ptr<bbt::infra::rpc::CoRpcClient> rpc_client() const;

    result<void> Create() override;
    bbt_co_async(result<void>) Start(
        const bbt::coroutine::WaitOptions& wait) override;
    void StopAccepting() noexcept override;
    bbt_co_async(result<void>) WaitHandlersDone(
        const bbt::coroutine::WaitOptions& wait) override;
    bbt_co_async(result<void>) Close(
        const bbt::infra::CloseOptions& options) override;
    bbt_co_async(result<void>) WaitPhysicalReleased(
        const bbt::coroutine::WaitOptions& wait) override;
    std::optional<bbt::infra::Error> CloseReason() const override;

    std::optional<bbt::infra::ListenAddress> bound_address() const;
    std::string bound_endpoint() const;
    std::shared_ptr<bbt::infra::http::Server> http_server() const;
    // 本宿主当前持有的 Engine 引用（借用形态下与 App 共享）；释放后为空。
    std::shared_ptr<bbt::infra::net::Engine> egress_engine() const;
    std::shared_ptr<bbt::infra::http::Client> http_client() const;
    // Run 的输入：由持有 Engine 的应用入口 native host hook（测试为等价
    // 原生编排）消费；本宿主自身不调用 Run、不建 runner 线程。
    bbt::infra::net::EngineConfig engine_config() const;
    const bbt::infra::NetworkLimits& network_limits() const noexcept { return m_limits; }

private:
    struct HandlerState {
        std::mutex mtx;
        // 排空等待位：一次只容纳一个 WaitHandlersDone 等待者；归还计数变化时
        // 由持锁侧取指针、锁外 Notify（跨协程/线程唤醒）。
        std::shared_ptr<bbt::coroutine::sync::CoWaiter> drained;
        std::size_t inflight{0};
        bool open{true};
        // 接纳顺序由票据 + 派发轮次承担，不用 OS 锁跨业务挂起：
        //  - next_ticket：下一个接纳票据；
        //  - dispatch_ticket：下一个允许进入同步派发的票据（FIFO）；
        //  - cancelled：登记失败的票据，推进轮次时跳过；
        //  - turn：轮次变化唤醒（与 drained 相同的 CoWaiter 用法）。
        std::uint64_t next_ticket{0};
        std::uint64_t dispatch_ticket{0};
        std::set<std::uint64_t> cancelled;
        std::shared_ptr<bbt::coroutine::sync::CoWaiter> turn;
        std::optional<bbt::infra::Error> close_reason; // Close 发布；首个固定
        bbt::infra::http::ServerHandler http;
        RpcDispatch rpc;
        std::size_t max_inflight{0};
    };
    static result<bbt::infra::http::Response> _InvokeHttp(
        const std::shared_ptr<HandlerState>&, bbt::infra::IncomingCallContext,
        bbt::infra::http::IncomingRequest);
    static result<void> _AdmitRpc(const std::shared_ptr<HandlerState>&,
                                  bbt::infra::rpc::AdmissionItem&&);
    static void _RunRpc(const std::shared_ptr<HandlerState>&,
                        std::uint64_t ticket,
                        std::shared_ptr<bbt::infra::rpc::AdmissionItem> item);
    // 票据轮次推进：只在持有 state->mtx 时改 dispatch_ticket，锁外 Notify。
    static void _AdvanceDispatchLocked(HandlerState& state,
                                       std::uint64_t ticket);
    static void _NotifyDrained(const std::shared_ptr<HandlerState>&) noexcept;

    // 逻辑封口与首因发布（幂等）；Close 与析构共用。
    void _Seal(bbt::infra::Error reason) noexcept;
    result<void> _CheckCoContext(const char* entry) const;

    const bbt::infra::NetworkLimits m_limits;
    const bbt::infra::ListenAddress m_listen;
    const std::string m_local_identity;
    const std::shared_ptr<HandlerState> m_state;

    mutable std::mutex m_mtx;
    bool m_created{false};
    bool m_bound_ready{false};
    bbt::infra::ListenAddress m_bound{};
    std::optional<bbt::infra::Error> m_close_reason;
    std::shared_ptr<bbt::infra::http::Server> m_server;
    std::shared_ptr<bbt::infra::rpc::CoRpcServer> m_rpc_server;
    std::shared_ptr<bbt::infra::rpc::CoRpcClient> m_rpc_client;
    std::shared_ptr<bbt::infra::http::Client> m_client;
    std::shared_ptr<bbt::infra::net::Engine> m_engine;
    bbt::infra::net::EngineConfig m_engine_config;
    // Start 只尝试一次；Run 由入口 host hook（测试为等价原生编排）执行。
    bool m_engine_started{false};
    // 借用形态：Engine 归应用根，Close/WaitPhysicalReleased 不关闭/不等待它。
    const bool m_engine_borrowed{false};
};

} // namespace bbt::framework
