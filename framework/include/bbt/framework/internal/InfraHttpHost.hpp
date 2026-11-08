#pragma once
// Framework 网络宿主：同一 Engine 承担入站接纳与出站会话。
// Create 在 Idle 装配；Start 在 native Run 线程监听后启动 server。
// StopAccepting 先关闭业务门；WaitHandlersDone 等业务与回复写终态，再以
// CoRpcServer::Shutdown 同步封口 RPC 入站（infra 会话层已收敛为统一
// Close/Shutdown，物理回收由 Close 的 Engine 收口证明）。Close 立即发布
// 原因、封 server/client、RequestClose 并 join 自有 Engine 线程（不 detach）；
// 它不等 handler、也不再自带 5s 等待。
// HandlerState 自持；业务对象寿命由运行时状态 shared_ptr（AppRuntimeState）
// 保证——handler 闭包复制该指针，CoApp 先析构也不影响在途请求。

#include <chrono>
#include <cstddef>
#include <condition_variable>
#include <future>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <bbt/infra/http/Server.hpp>
#include <bbt/infra/http/Client.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/rpc/CoRpcServer.hpp>
#include <bbt/infra/rpc/CoRpcClient.hpp>
#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/internal/HostLifecycle.hpp>

namespace bbt::framework {
class InfraHttpHost final : public INetworkHost {
public:
    InfraHttpHost(bbt::infra::NetworkLimits limits, bbt::infra::ListenAddress listen,
                  std::string local_identity);
    // 析构同步点：Close 不 join 仍被业务占用的驱动线程（见 _JoinEngine 的
    // 短同步策略），这里必须 join 才能安全释放，既不 detach 也不 terminate。
    ~InfraHttpHost() noexcept override;
    void InstallHandler(bbt::infra::http::ServerHandler handler);
    // 业务入口必须在任何可挂起操作之前调用 on_dispatched（同步校验/入邮箱后）。
    using RpcDispatch = std::function<result<bbt::infra::rpc::Response>(const bbt::infra::rpc::Request&, const std::function<void()>&)>;
    void InstallRpcHandler(RpcDispatch handler);
    std::shared_ptr<bbt::infra::rpc::CoRpcServer> rpc_server() const;
    std::shared_ptr<bbt::infra::rpc::CoRpcClient> rpc_client() const;
    result<void> Create() override;
    result<void> Start() override;
    void StopAccepting() noexcept override;
    result<void> WaitHandlersDone(bbt::coroutine::Deadline deadline) override;
    // 立即关闭：不等 handler、不自带等待预算，重复/并发调用只执行一次；
    // 析构再调为空操作（不会因仍 joinable 的 std::thread terminate）。
    void Close() noexcept override;
    void Close(bbt::infra::Error reason) noexcept override;
    std::optional<bbt::infra::Error> CloseReason() const noexcept override;
    std::optional<bbt::infra::ListenAddress> bound_address() const;
    std::string bound_endpoint() const;
    std::shared_ptr<bbt::infra::http::Server> http_server() const;
    std::shared_ptr<bbt::infra::net::Engine> egress_engine() const;
    std::shared_ptr<bbt::infra::http::Client> http_client() const;
    const bbt::infra::NetworkLimits& network_limits() const noexcept { return m_limits; }
    std::optional<bbt::infra::Error> close_error() const;

private:
    struct HandlerState {
        std::mutex mtx;
        // 只串行同步派发与 Actor 入队；业务进入可挂起阶段前解锁。
        std::mutex dispatch_mtx;
        std::condition_variable cv;
        std::size_t inflight{0};
        bool open{true};
        std::optional<bbt::infra::Error> close_reason;  // Close 发布；首个固定
        bbt::infra::http::ServerHandler http;
        RpcDispatch rpc;
        std::deque<bbt::infra::rpc::AdmissionItem> queue;
        std::size_t max_inflight{0};
    };
    static result<bbt::infra::http::Response> _InvokeHttp(
        const std::shared_ptr<HandlerState>&, bbt::infra::IncomingCallContext,
        bbt::infra::http::IncomingRequest);
    static result<void> _AdmitRpc(const std::shared_ptr<HandlerState>&,
                                  bbt::infra::rpc::AdmissionItem&&);
    static void _RunRpc(const std::shared_ptr<HandlerState>&);
    void _Close(bbt::infra::Error reason) noexcept;
    bool _JoinEngine(const std::shared_ptr<bbt::infra::net::Engine>&,
                     std::thread&, std::shared_future<std::error_code>&);
    const bbt::infra::NetworkLimits m_limits;
    const bbt::infra::ListenAddress m_listen;
    const std::string m_local_identity;
    const std::shared_ptr<HandlerState> m_state;
    std::once_flag m_close_once;
    mutable std::mutex m_mtx;
    bool m_created{false};
    bool m_bound_ready{false};
    bbt::infra::ListenAddress m_bound{};
    std::optional<bbt::infra::Error> m_close_reason;
    std::optional<bbt::infra::Error> m_close_error;
    std::shared_ptr<bbt::infra::http::Server> m_server;
    std::shared_ptr<bbt::infra::rpc::CoRpcServer> m_rpc_server;
    std::shared_ptr<bbt::infra::rpc::CoRpcClient> m_rpc_client;
    std::shared_ptr<bbt::infra::net::Engine> m_engine;
    std::thread m_engine_thread;
    std::shared_future<std::error_code> m_engine_result;
    std::shared_ptr<bbt::infra::http::Client> m_client;
};
} // namespace bbt::framework
