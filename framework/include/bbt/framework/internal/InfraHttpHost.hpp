#pragma once
// co-service-actor/v1 F1-b2：INetworkHost 的真实实现——infra
// NetworkRuntime + HttpServer/RpcServer（已验收的真实 loopback 传输）。
//
// 两种入站模式（Start 前二选一安装）：
//  - InstallHandler(HttpHandler)：LegacyHeaders 迁移期桥。Start 经
//    NetworkRuntime::ListenHttp 监听；x-bbt-* header 承载 envelope。
//  - InstallRpcHandler(RpcHandler)：ProtoWireV1 正式桥。Start 经
//    NetworkRuntime::ListenRpc 监听；wire 编解码、接收端 local-min 预算、
//    成功/错误信封回显、五字段关联与 remaining 回填全部由 infra
//    HandleRpcRequest 承担，framework 只提供 dispatch 适配。
//  二者共用本类的在途 handler 记账与关闭序列；均未安装时 Start 跳过监听
//  （纯出站宿主）。
//
// 口径差异（契约 F1/F3 与 infra N0）：
//  - infra 关闭是同步契约（ICoCloseable::Close 返回即物理释放）；本类
//    Close 在控制线程直接调用 NetworkRuntime::Close，与 infra 同一语义，
//    不做协程等待桥接。
//  - handler 在 Close 前完成的出站回复按 infra 的正常发送语义处理；Close
//    开始后仍在途且尚未交给传输层的回复不承诺送达原客户端，测试应断言
//    handler/owner 侧收口证据而不是伪造 flush 保证。
//  - WaitHandlersDone 是控制线程语义：infra 不暴露「已接纳 handler 排空」
//    入口，本类对已安装的 handler 做在途记账，由自身
//    std::condition_variable 在给定期限内等待排空，不走协程等待。
//
// 寿命边界（如实记录，不在本类内宣称已解决业务入口寿命）：
//  - handler 闭包按 shared_ptr 捕获 HandlerState，不捕获宿主裸 this：宿主
//    析构后仍在途的 infra handler 协程只访问该状态（handler 本体 + 计数 +
//    拒绝门），不再触达已析构的宿主成员。这是对「handler 协程寿命不受宿主
//    控制线程管辖」的**直接**缓解。
//  - 拒绝门在 StopAccepting（不再是 Close）于 m_state->mtx 内置关，且先于本类
//    调用的 infra StopAccepting 生效：infra 在 StopAccepting 之前已接纳、但协程
//    尚未进入本类入站包装的请求，一旦进入即在门口被拒（RuntimeUnavailable），
//    不再触达业务；WaitHandlersDone 只需等待「已过门」者。Close 仍置关（幂等，
//    兼容不经 StopAccepting 的直接 Close），但不追溯已过门者——其寿命由
//    WaitHandlersDone 与 CoApp 生命周期负责。门只是拒绝「之后进入」，不承诺
//    外部直接 host.Close() 即保证 app 安全。
//  - 该状态**不**拥有业务入口（CoApp）；闭包内的 app 寿命仍由 CoApp 生命周期
//    「先排空（WaitHandlersDone，耗尽预算后无界续等）再 Close」保证，不由本
//    shared_ptr 保证。
//
// 生命周期：Create（NetworkRuntime::Create）→ Start（runtime Start +
// 按需 ListenHttp/ListenRpc）→ StopAccepting → WaitHandlersDone → Close（同步）。

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include <bbt/infra/HttpServer.hpp>
#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/RpcServer.hpp>

#include <bbt/framework/internal/HostLifecycle.hpp>
#include <bbt/framework/Result.hpp>

namespace bbt::framework {

class InfraHttpHost final : public INetworkHost {
public:
    // limits 透传 NetworkRuntime::Create；listen 为监听地址
    // （port==0 时内核分配，Start 后经 bound_address() 可读）。
    InfraHttpHost(bbt::infra::NetworkLimits limits,
                  bbt::infra::ListenAddress listen);

    // 安装入站 HttpHandler（LegacyHeaders；Start 前调用）。幂等覆盖；未安装
    // 任何 handler 时 Start 跳过监听（该宿主只做出站）。handler 在受管协程内
    // 被调用，其整个执行期计入在途 handler 记账。
    void InstallHandler(bbt::infra::HttpHandler handler);

    // 安装入站 RpcHandler（ProtoWireV1；Start 前调用）。Start 经
    // NetworkRuntime::ListenRpc 监听，wire/local-min/五字段关联由 infra
    // 承担。与 InstallHandler 二选一（同一次 Start 只安装一条入站桥）。
    void InstallRpcHandler(bbt::infra::RpcHandler handler);

    // ---- INetworkHost（全部在宿主控制线程调用）----
    result<void> Create() override;
    result<void> Start() override;
    void         StopAccepting() noexcept override;
    result<void> WaitHandlersDone(
        bbt::coroutine::Deadline deadline) override;
    void         Close() noexcept override;

    // Start 成功后的实际绑定地址；未监听返回 std::nullopt。
    std::optional<bbt::infra::ListenAddress> bound_address() const;
    // "host:port" 形式的接入点（静态路由 endpoint 形态）；未监听返回空串。
    std::string bound_endpoint() const;

    // 桥接断言/观测用：被托管的 infra 对象本体（可能为空）。
    // http_server() 只在 LegacyHeaders 模式非空（ProtoWireV1 模式下 infra 的
    // 底层 HttpServer 由 ListenRpc 内部持有，不经本类暴露）；观测 RPC 模式请用
    // rpc_server()。
    std::shared_ptr<bbt::infra::HttpServer>     http_server() const;
    std::shared_ptr<bbt::infra::NetworkRuntime> network_runtime() const;
    // ProtoWireV1 模式下托管的 RpcServer（观测用，可能为空）。
    std::shared_ptr<bbt::infra::RpcServer> rpc_server() const;

    // 测试缝（internal，仅框架回归使用）：在受控时刻重放一次「infra 已接纳、
    // 但协程尚未进入入站包装」的调用——走与 infra 完全相同的生产包装
    // （_InvokeRpc，含拒绝门与在途记账），不重实现门语义。用于判别
    // StopAccepting 是否闭门（迟到进入者应被拒 RuntimeUnavailable、不触达业务、
    // 不漏记在途）与 Close 幂等/直接 Close 兼容。业务路径不调用。
    // 只有 RPC 这一路：受审的 C1 窗口位于 RPC 包装路径的关闭序列；HTTP 侧无对应
    // 缝（infra 无 http 派发注入点），故**不提供** InvokeHttpForTest。
    result<bbt::infra::RpcEnvelope> InvokeRpcForTest(bbt::infra::IncomingCallContext ctx,
                                                     bbt::infra::RpcEnvelope request)
    {
        return _InvokeRpc(m_state, std::move(ctx), std::move(request));
    }

private:
    // 入站 handler 与其在途记账的自持状态。闭包按 shared_ptr 捕获它（见文件头
    // 寿命边界）：计数/拒绝门/handler 本体都在这里，宿主析构不影响在途协程访问
    // 它们。open 由 StopAccepting 在 mtx 内置关（先于 infra StopAccepting），
    // Close 亦置关（幂等，兼容直接 Close）；handler 在 mtx 内做「门检查 + 计数」
    // ——要么先于置关通过，要么被拒，不产生半进入状态。
    struct HandlerState {
        std::mutex mtx;
        std::condition_variable cv;
        std::size_t inflight = 0;
        bool open = true;             // false：拒绝新进入的 handler
        bbt::infra::HttpHandler http; // LegacyHeaders 模式
        bbt::infra::RpcHandler rpc;   // ProtoWireV1 模式
    };

    // 入站 handler 调用包装（无 this）：拒绝门 + 在途记账 + 转发。静态成员，
    // 只经 shared_ptr<HandlerState> 访问状态，绝不解引用宿主裸 this。
    static result<bbt::infra::HttpResponse> _InvokeHttp(const std::shared_ptr<HandlerState>& st,
                                                        bbt::infra::IncomingCallContext ctx,
                                                        bbt::infra::HttpRequest request);
    static result<bbt::infra::RpcEnvelope> _InvokeRpc(const std::shared_ptr<HandlerState>& st,
                                                      bbt::infra::IncomingCallContext ctx,
                                                      bbt::infra::RpcEnvelope request);

    const bbt::infra::NetworkLimits m_limits;
    const bbt::infra::ListenAddress m_listen;

    // 构造期建立、恒非空：handler 本体与在途记账自持状态。
    std::shared_ptr<HandlerState> m_state;

    // m_runtime/m_server/m_rpc_server/m_bound 经 m_mtx 发布：Create/Start/Close
    // 在宿主控制线程写，bound_*/http_server/rpc_server/network_runtime/
    // StopAccepting 允许从任意线程读——m_server/m_rpc_server 与 m_bound 必须在
    // 同一临界区成对可见，否则轮询方能读到半成品就绪状态。
    std::shared_ptr<bbt::infra::NetworkRuntime> m_runtime;
    std::shared_ptr<bbt::infra::HttpServer> m_server;    // LegacyHeaders
    std::shared_ptr<bbt::infra::RpcServer> m_rpc_server; // ProtoWireV1
    bbt::infra::ListenAddress m_bound{};

    mutable std::mutex m_mtx;
};

} // namespace bbt::framework
