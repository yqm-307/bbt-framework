#pragma once
// Framework 网络宿主：独占入站接纳 Engine、共享 client-only 出站 Engine。
// Create 在 Idle 装配；Start 在 native Run 线程监听后启动 server。
// StopAccepting 先关闭业务门；WaitHandlersDone 等业务与回复写终态，再
// Shutdown 封口 + 等已接纳 handler 排空（Shutdown 续等按 infra 新契约转
// Drain），Close/WaitStopped 证明 callback 与连接物理收口。Close 立即发布
// 原因、封 server/client、RequestClose 并 join 自有 Engine 线程（不
// detach）；它不等 handler、也不再自带 5s 等待。
// HandlerState 自持；业务对象寿命由运行时状态 shared_ptr（AppRuntimeState）
// 保证——handler 闭包复制该指针，CoApp 先析构也不影响在途请求。

#include <chrono>
#include <cstddef>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <bbt/infra/http/Server.hpp>
#include <bbt/infra/net/Engine.hpp>
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
#include <bbt/infra/rpc/Server.hpp>
#include <bbt/infra/rpc/Client.hpp>
#endif
#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/internal/HostLifecycle.hpp>

namespace bbt::framework {
class InfraHttpHost final : public INetworkHost {
public:
    InfraHttpHost(bbt::infra::NetworkLimits limits, bbt::infra::ListenAddress listen);
    // 析构同步点：Close 不 join 仍被业务占用的驱动线程（见 _JoinEngine 的
    // 短同步策略），这里必须 join 才能安全释放，既不 detach 也不 terminate。
    ~InfraHttpHost() noexcept override;
    void InstallHandler(bbt::infra::http::ServerHandler handler);
    void InstallRpcHandler(bbt::infra::RpcHandler handler);
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
    void SetRpcInboundTransport(RpcInboundBridge bridge);
    std::shared_ptr<bbt::infra::rpc::Server> rpc_server() const;
    std::shared_ptr<bbt::infra::rpc::Client> rpc_http_client() const;
#endif
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
    result<bbt::infra::RpcEnvelope> InvokeRpcForTest(
        bbt::infra::IncomingCallContext ctx, bbt::infra::RpcEnvelope request) {
        return _InvokeRpc(m_state, std::move(ctx), std::move(request));
    }

private:
    struct HandlerState {
        std::mutex mtx;
        std::condition_variable cv;
        std::size_t inflight{0};
        bool open{true};
        std::optional<bbt::infra::Error> close_reason;  // Close 发布；首个固定
        bbt::infra::http::ServerHandler http;
        bbt::infra::RpcHandler rpc;
    };
    static result<bbt::infra::http::Response> _InvokeHttp(
        const std::shared_ptr<HandlerState>&, bbt::infra::IncomingCallContext,
        bbt::infra::http::IncomingRequest);
    static result<bbt::infra::RpcEnvelope> _InvokeRpc(
        const std::shared_ptr<HandlerState>&, bbt::infra::IncomingCallContext,
        bbt::infra::RpcEnvelope);
    void _Close(bbt::infra::Error reason) noexcept;
    bool _JoinEngine(const std::shared_ptr<bbt::infra::net::Engine>&,
                     std::thread&, std::shared_future<std::error_code>&);
    const bbt::infra::NetworkLimits m_limits;
    const bbt::infra::ListenAddress m_listen;
    const std::shared_ptr<HandlerState> m_state;
    std::once_flag m_close_once;
    mutable std::mutex m_mtx;
    bool m_created{false};
    bool m_bound_ready{false};
    bbt::infra::ListenAddress m_bound{};
    std::optional<bbt::infra::Error> m_close_reason;
    std::optional<bbt::infra::Error> m_close_error;
    std::shared_ptr<bbt::infra::http::Server> m_server;
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
    std::shared_ptr<bbt::infra::rpc::Server> m_rpc_server;
    std::shared_ptr<bbt::infra::rpc::Client> m_rpc_http_client;
    RpcInboundBridge m_inbound_bridge{RpcInboundBridge::ProtoWireV1};
#endif
    std::shared_ptr<bbt::infra::net::Engine> m_engine;
    std::thread m_engine_thread;
    std::shared_future<std::error_code> m_engine_result;
    std::shared_ptr<bbt::infra::net::Engine> m_egress_engine;
    std::shared_ptr<bbt::infra::http::Client> m_client;
    std::thread m_egress_thread;
    std::shared_future<std::error_code> m_egress_result;
};
} // namespace bbt::framework
