#pragma once
// INetworkHost 的真实实现（机器面，internal/）：infra
// NetworkRuntime + HttpServer（LegacyHeaders 旧栈）或 net::Engine +
// rpc::Server + inbound transport adapter（ProtoWireV1=HTTP、TcpV1=TCP；
// 三仓 §5/§6 迁移后的新栈）。
//
// 三种入站模式（Start 前按 InstallHandler/InstallRpcHandler + 桥选择二选一）：
//  - InstallHandler(HttpHandler)：LegacyHeaders 迁移期桥。Start 经
//    NetworkRuntime::ListenHttp 监听；x-bbt-* header 承载 envelope。
//    旧栈上游仍在（BBT_INFRA_LEGACY_STACK 默认 ON），路径不变。
//  - InstallRpcHandler(RpcHandler) + RpcInboundBridge::ProtoWireV1：
//    HttpRpcInboundTransport + rpc::Server，宿主专用 Engine::Run 线程。
//  - InstallRpcHandler(RpcHandler) + RpcInboundBridge::TcpV1：
//    TcpRpcInboundTransport + rpc::Server，独立的 TCP server Engine::Run
//    线程（accept-handoff 每 Engine 独占，HTTP/TCP 不可共享）。
// wire 编解码、接收端 local-min 预算、五字段关联与 remaining 回填全部由
// infra rpc::Server 承担，framework 只提供 dispatch 适配。未安装 handler
// 时 Start 跳过监听（纯出站宿主）。
//
// 口径差异（framework 侧契约与 infra 网络栈之间）：
//  - infra 关闭是同步收口：rpc::Server 序列
//    StopAccepting → Drain → Close → WaitStopped 均由宿主控制线程驱动；
//    Engine::RequestClose + Run 线程 join 返回即网络资源/回调/driver 已收口。
//    旧栈 NetworkRuntime::Close 语义不变。
//  - Engine::Run 占用一个宿主专用 native 线程（不是 control 线程本身，
//    也不是协程/P 线程：Run 在协程上下文或已登记 Processer 上会被拒）。
//    执行底座（coroutine Scheduler + Engine）由本类装配并收口。Run 返回的
//    error_code 经 promise/future 跨线程发布（唯一同步口，Start 的相位
//    等待据此即刻观察失败，不复制共享变量）。
//    出站：ProtoWireV1/TcpV1 出站经 infra 公开 rpc::Client + adapter
//    seam——HttpEgress 懒建的 rpc::Client 底座由本类 egress_engine()
//    提供（client-only Engine，Start 时按需创建并 Run 在独立线程）；
//    LegacyHeaders 出站仍走旧栈 NetworkRuntime::CreateHttpClient。
//  - handler 在 Close 前完成的出站回复按 infra 的正常发送语义处理；Close
//    开始后仍在途且尚未交给传输层的回复不承诺送达原客户端，测试应断言
//    handler/owner 侧收口证据而不是伪造 flush 保证。
//  - WaitHandlersDone 是控制线程语义：先等本类已记账的 framework 在途
//    handler 归零（测试缝 InvokeRpcForTest 的迟到进入者不在 infra 账本内，
//    只能由本记账覆盖），再对 rpc::Server 调 Drain（等已授权 handler 返回
//    且回复写终态），随后 server->Close()+WaitStopped（listener 停、全部
//    callback 销毁、连接 Released）——Drain==0 只证明 inflight==0，
//    tasks==0 只能由 WaitStopped 证实（TaskLease 在 callback 捕获最后销毁
//    时递减）。Drain/WaitStopped 仅 native 宿主可调用——本类按契约在控制
//    线程调用，不挂协程。失败（超时/非超时）如实返回给 HostLifecycle：
//    预算耗尽经 Deadline::max 续等，续等仍失败 → fail-closed 保留 owner。
//  - Close 只在取得 WaitStopped 成功凭据（m_stopped）后停 Engine
//    （RequestClose+join+Run 结果+Stopped 相位实际检查）并释放
//    server/engine owner；未取得凭据（裸 Close 兜底失败或续等仍失败）→
//    保留 owner，不伪报已物理关闭（fail-closed）。
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
// 生命周期：Create（NetworkRuntime::Create；RPC 模式另建 Engine 与
// rpc::Server）→ Start（runtime Start + 按需 ListenHttp，或 Engine::Run 线程
// + rpc::Server::Start）→ StopAccepting → WaitHandlersDone（inflight 归零 +
// Drain + server Close/WaitStopped）→ Close（Engine RequestClose + join +
// Run 结果/Stopped 相位核验后释放 owner；legacy 走 NetworkRuntime::Close；
// 均同步）。未证实收口保留 owner（fail-closed）。

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <system_error>
#include <thread>

#include <bbt/infra/HttpServer.hpp>
#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/NetworkTypes.hpp>

#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/rpc/Server.hpp>
#endif

#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/internal/HostLifecycle.hpp>
#include <bbt/framework/Result.hpp>

namespace bbt::framework {

class InfraHttpHost final : public INetworkHost {
public:
    // limits 透传 NetworkRuntime::Create / rpc::Server 的 http::ServerOptions；
    // listen 为监听地址（port==0 时内核分配，Start 后经 bound_address() 可读）。
    InfraHttpHost(bbt::infra::NetworkLimits limits,
                  bbt::infra::ListenAddress listen);
    // 析构兜底按 Close() 收（幂等）：Engine Run 线程是 joinable std::thread，
    // 不经 Close 析构会以 std::thread::~thread terminate。handler 闭包自持
    // 状态（m_state）不随宿主析构失效（见文件头寿命边界）。
    ~InfraHttpHost() noexcept override { Close(); }

    // 安装入站 HttpHandler（LegacyHeaders；Start 前调用）。幂等覆盖；未安装
    // 任何 handler 时 Start 跳过监听（该宿主只做出站）。handler 在受管协程内
    // 被调用，其整个执行期计入在途 handler 记账。
    void InstallHandler(bbt::infra::HttpHandler handler);

    // 安装入站 RpcHandler（ProtoWireV1/TcpV1；Start 前调用）。Start 经
    // 对应 server Engine + inbound transport + rpc::Server 监听，
    // wire/local-min/五字段关联由 infra 承担。与 InstallHandler 二选一
    // （同一次 Start 只安装一条入站桥）。
    void InstallRpcHandler(bbt::infra::RpcHandler handler);

#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
    // 入站桥选择（Start 前调用，默认 ProtoWireV1）：ProtoWireV1 =
    // HttpRpcInboundTransport，TcpV1 = TcpRpcInboundTransport；两者都只换
    // transport adapter，rpc::Server/wire profile/关闭序列不变。
    void SetRpcInboundTransport(bbt::framework::RpcInboundBridge bridge);
#endif

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
    // http_server() 只在 LegacyHeaders 模式非空（ProtoWireV1 模式下底层
    // http::Server 由 rpc::Server 内部持有，不经本类暴露）；观测 RPC 模式请用
    // rpc_server()。
    std::shared_ptr<bbt::infra::HttpServer>     http_server() const;
    std::shared_ptr<bbt::infra::NetworkRuntime> network_runtime() const;
    // 宿主装配限额（构造期常量）：出站 adapter 的 body/连接预算据此收敛。
    const bbt::infra::NetworkLimits& network_limits() const noexcept {
        return m_limits;
    }
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
    // ProtoWireV1 模式下托管的 rpc::Server（观测用，可能为空）。
    std::shared_ptr<bbt::infra::rpc::Server> rpc_server() const;
    // Close 收口期 rpc::Server::WaitStopped 的失败事实（观测用，可能为空）。
    // WaitStopped 超时仍保留任务责任——Engine RequestClose+join 是物理凭据；
    // 该错误只用于如实观测/诊断，不改变幂等同步收口契约。
    std::optional<bbt::infra::Error> close_error() const;

    // 出站执行底座：client-only Engine（Start 时按需创建并 Run；HttpEgress
    // 经此装配 HttpRpcTransport/TcpRpcTransport 的 rpc::Client 底座）。
    // LegacyHeaders 出站不走本 Engine。未创建返回 nullptr。
    std::shared_ptr<bbt::infra::net::Engine> egress_engine() const;
#endif

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
    bbt::infra::ListenAddress m_bound{};
    // bound_*/就绪判据：仅在 Start 成功完成监听装配后置位（rpc::Server 本体
    // 在 Create 期已就位，不能用它冒充「已监听」）。
    bool m_bound_ready{false};

    mutable std::mutex m_mtx;

#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
    // ProtoWireV1：server Engine + rpc::Server + 其专属 Run 线程。
    // Run 线程只在本类 Start 与 Close 控制下生灭；Engine::Run 的阻塞循环
    // 必须跑在宿主专用 native 线程。Run 结果经 m_engine_result
    // （promise/future）发布——Start 相位等待与失败观察的唯一同步口。
    std::shared_ptr<bbt::infra::net::Engine>  m_engine;
    std::shared_ptr<bbt::infra::rpc::Server>  m_rpc_server;
    std::thread                             m_engine_thread;
    std::future<std::error_code>            m_engine_result;

    // Close 期 WaitStopped 的失败事实（观测用；Engine join 仍为物理凭据）。
    std::optional<bbt::infra::Error>          m_close_error;
    // RPC 收口凭据：WaitHandlersDone（或 Close 兜底路径）取得
    // rpc::Server::WaitStopped 成功后置位；Close 只在此凭据成立后停 Engine
    // 并释放 server/engine owner，否则 fail-closed 保留。
    bool                                      m_stopped{false};
    // Engine 收口（RequestClose+join+get）只尝试一次：幂等重入不再重复。
    bool                                      m_engine_closed{false};

    // 入站桥选择：Start 前经 SetRpcInboundTransport 设定（默认 ProtoWireV1）。
    RpcInboundBridge                          m_inbound_bridge{
                                                  RpcInboundBridge::ProtoWireV1};

    // client-only 出站 Engine（按需）：Start 时创建并 Run 在独立线程；
    // Close 在取得 server 收口凭据后 RequestClose+join。HttpEgress 的
    // rpc::Client 底座（HttpRpcTransport/TcpRpcTransport）借用它。
    std::shared_ptr<bbt::infra::net::Engine>  m_egress_engine;
    std::thread                             m_egress_thread;
    std::future<std::error_code>            m_egress_result;
    bool                                    m_egress_engine_closed{false};
#endif
};

} // namespace bbt::framework
