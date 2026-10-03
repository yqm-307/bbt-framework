#pragma once
// co-service-actor/v1 F1-b2：INetworkHost 的真实实现——infra
// NetworkRuntime + HttpServer（已验收的真实 HTTP loopback 传输）。
//
// 口径差异（契约 F1/F3 与 infra N0）：
//  - infra 关闭是同步契约（ICoCloseable::Close 返回即物理释放，无
//    RequestClose/WaitClosed 等待）；本类 Close 在控制线程直接调用
//    NetworkRuntime::Close，与 infra 同一语义，不做协程等待桥接。
//  - handler 在 Close 前完成的出站回复按 infra 的正常发送语义处理；Close
//    开始后仍在途且尚未交给传输层的回复不承诺送达原客户端，测试应断言
//    handler/owner 侧收口证据而不是伪造 flush 保证。
//  - WaitHandlersDone 是控制线程语义：infra 不暴露「已接纳 handler 排空」
//    入口，本类对已安装的 HttpHandler 做在途记账，由自身
//    std::condition_variable 在给定期限内等待排空，不走协程等待。
//
// 生命周期：Create（NetworkRuntime::Create）→ Start（runtime Start +
// 按需 ListenHttp）→ StopAccepting → WaitHandlersDone → Close（同步）。
// 关闭序列的每一步在宿主控制线程调用；未安装 handler 时 Start 跳过监听
// （纯出站宿主），其余步骤语义不变。

#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include <bbt/infra/HttpServer.hpp>
#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/NetworkTypes.hpp>

#include <bbt/framework/internal/HostLifecycle.hpp>
#include <bbt/framework/Result.hpp>

namespace bbt::framework {

class InfraHttpHost final : public INetworkHost {
public:
    // limits 透传 NetworkRuntime::Create；listen 为监听地址
    // （port==0 时内核分配，Start 后经 bound_address() 可读）。
    InfraHttpHost(bbt::infra::NetworkLimits limits,
                  bbt::infra::ListenAddress listen);

    // 安装入站 HttpHandler（Start 前调用）。幂等覆盖；未安装时 Start
    // 跳过监听（该宿主只做出站）。handler 在受管协程内被调用，其整个
    // 执行期计入在途 handler 记账。
    void InstallHandler(bbt::infra::HttpHandler handler);

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
    std::shared_ptr<bbt::infra::HttpServer>     http_server() const;
    std::shared_ptr<bbt::infra::NetworkRuntime> network_runtime() const;

private:
    // 已安装 handler 的包装：在途记账 + 转发。
    result<bbt::infra::HttpResponse> _Handle(
        bbt::infra::IncomingCallContext ctx,
        bbt::infra::HttpRequest request);

    const bbt::infra::NetworkLimits m_limits;
    const bbt::infra::ListenAddress m_listen;

    bbt::infra::HttpHandler                 m_handler;
    // m_runtime/m_server/m_bound 经 m_mtx 发布：Create/Start/Close
    // 在宿主控制线程写，bound_*/http_server/network_runtime/StopAccepting
    // 允许从任意线程读——m_server 与 m_bound 必须在
    // 同一临界区成对可见，否则轮询方能读到半成品就绪状态。
    std::shared_ptr<bbt::infra::NetworkRuntime> m_runtime;
    std::shared_ptr<bbt::infra::HttpServer>     m_server;
    bbt::infra::ListenAddress                 m_bound{};

    // 在途 handler 记账（控制线程 WaitHandlersDone 的等待对象）；
    // 同时保护上述 infra 对象的发布与读取（const 接口内也取锁 → mutable）。
    mutable std::mutex                      m_mtx;
    std::condition_variable                 m_cv;
    std::size_t                             m_inflight = 0;
};

} // namespace bbt::framework
