// bbt-framework Issue #5（EX-T2）：单进程双 Service fixture。
//
// 形态（R8/S9）：**一个** 公开 `CoApp` 经 `add_service<T>` 注册两个公开
// `CoService<T>`，其中一个服务的 handler 经受保护公开调用路径
// `this->call<Req,Resp>(service, method, req, ...)` 调用另一个服务；两个服务
// 的入站/出站都走 #4 的正式 ProtoWireV1 body profile。
//
//   caller 服务（getvalue::GetValueCallerService，命名 "…GetValueCallerService"）
//     └─ this->call("bbt.example.v1.GetValueService", "GetValue", …)
//   callee 服务（getvalue::GetValueService，命名 "bbt.example.v1.GetValueService"）
//
// 单进程内的「跨 Service」调用不是进程内捷径：框架出站实现只有一条路径——
// `find_route` 静态路由门 + 真实 HTTP 出站。故本 fixture 让唯一的静态路由
// 指向**本进程自己的 loopback 端点**，调用经真实 socket 回到同一宿主的入站
// 分发器。这证明公开 App/Service 注册、调用、错误与生命周期消费面，不证明
// 跨进程、真实后端或身份可信。
//
// 业务 schema/服务实现/typed codec 全部复用 #4 的 fixtures
// （getvalue_service.hpp / getvalue_caller.hpp + getvalue.proto），#5 不另造
// schema、协议、错误信封或第二套 codec。
//
// 不使用 internal/、Binder、CoAppSeam、MakeCoAppForTest、
// DispatchInboundForTest、INetworkHost 桩或任何发送注入；不创建额外 io_thread
// （只调用公开 CoApp::run / request_shutdown）。
//
// argv: two_service fixture <port_file> <meta_file> <journal_file>
// 就绪：绑定成功后写 port_file（动态 loopback 端口）与 meta_file；
// 关闭：stdin EOF → request_shutdown() → run() 返回后写 shutdown 记录并退出。
//
// 所有结构化结果标注 auth_state=unauthenticated_loopback。

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/infra/ICoCloseable.hpp>

#include <bbt/framework/CoApp.hpp>

#include "getvalue_caller.hpp"   // #4 caller 服务（公开面）
#include "getvalue_service.hpp"  // #4 callee 服务 + ProtoCodec<GetValueRequest/Response>

namespace fw  = bbt::framework;
namespace inf = bbt::infra;

namespace {

constexpr const char* kAuthState = "unauthenticated_loopback";

std::mutex    g_journal_mtx;
std::ofstream g_journal;

void Journal(const std::string& line) {
    std::lock_guard<std::mutex> lk(g_journal_mtx);
    g_journal << line << '\n';
    g_journal.flush();
}

// callee handler 被真正调用时回调（#4 公开观测钩子）。request_id 是**出站**
// 调用生成的 id（框架不把上游 request_id 透传给下游），故它只用于计数与
// 交叉对照，不能与驱动器 request_id 直接比对。
void AppendCalleeHandlerEntered(const std::string& request_id,
                                const std::string& peer_principal) {
    Journal(std::string("event=callee_handler_entered req=") + request_id +
            " peer_principal=" + peer_principal +
            " auth_state=" + kAuthState);
}

// 生命周期观测资源（公开 `add_resource<ICoCloseable>` 面）：只用于断言关闭
// 序列在 handler 排空后**同步** Close 一次。不伪造任何后端。
class CloseJournal final : public inf::ICoCloseable {
public:
    void Close() noexcept override {
        bool first = false;
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            first  = !m_closed;
            m_closed = true;
        }
        if (first)
            Journal(std::string("event=resource_close auth_state=") + kAuthState);
    }
    bool IsClosed() const noexcept override {
        std::lock_guard<std::mutex> lk(m_mtx);
        return m_closed;
    }

private:
    mutable std::mutex m_mtx;
    bool               m_closed = false;
};

// 动态 loopback 端口：内核分配后立即释放，供 `listen` 与指向自身的静态路由
// 共用。释放到 infra bind 之间的窗口极小；就绪后严格核对 bound_endpoint 的
// 端口等于所选端口，不符即失败，不静默继续（竞态落败时报错而非假通过）。
std::uint16_t PickLoopbackPort() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return 0;
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = 0;
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return 0;
    }
    socklen_t len = sizeof(addr);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
        ::close(fd);
        return 0;
    }
    const std::uint16_t port = ntohs(addr.sin_port);
    ::close(fd);
    return port;
}

fw::ServiceOptions ConcurrentOpts() {
    return fw::ServiceOptions{fw::ExecutionPolicy::Concurrent,
                              /*max_inflight*/ 64,
                              /*mailbox_capacity*/ 0, /*max_actors*/ 0,
                              /*ordered_ingress*/ false,
                              /*max_ordered_streams*/ 0,
                              /*max_cached_results*/ 0,
                              /*max_cached_result_bytes*/ 0};
}

} // namespace

int main(int argc, char* argv[]) {
    if (argc < 5 || std::strcmp(argv[1], "fixture") != 0)
        return 90;   // EX_USAGE
    const std::string port_file    = argv[2];
    const std::string meta_file    = argv[3];
    const std::string journal_path = argv[4];

    g_journal.open(journal_path, std::ios::out | std::ios::trunc);
    if (!g_journal) {
        std::fprintf(stderr, "fixture: cannot open journal %s\n",
                     journal_path.c_str());
        return 6;
    }

    // 单进程内 handler 既要跑入站 wire 解码/分发，又要发起同进程 loopback 出站
    // （父 handler 与 callee handler 并发）。默认 12KB 协程栈不足以承载
    // protobuf 业务 codec（见 #4 README 实测记录）；经 coroutine 公开配置宏
    // （须在调度器 Start 前设置；process-global）放大到 512KB 并给足静态线程，
    // 使自环出站与入站接纳互不阻塞。
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size        = 512 * 1024;

    getvalue::SetServedHook(&AppendCalleeHandlerEntered);

    const std::uint16_t port = PickLoopbackPort();
    if (port == 0) {
        std::fprintf(stderr, "fixture: cannot pick a free loopback port\n");
        return 6;
    }
    const std::string endpoint = "127.0.0.1:" + std::to_string(port);

    fw::CoAppOptions options;
    options.network_limits = inf::NetworkLimits{
        /*max_connections*/ 64, /*max_inflight*/ 64,
        /*max_header_bytes*/ 16 * 1024, /*max_body_bytes*/ 64 * 1024,
        /*incoming_timeout*/ std::chrono::milliseconds{30000}};
    options.listen = inf::ListenAddress{"127.0.0.1", port};
    options.shutdown_step_budget = std::chrono::milliseconds{3000};
    options.inbound_bridge       = fw::RpcInboundBridge::ProtoWireV1;
    // 唯一静态路由：把 callee 服务名指向本进程自己的 loopback 端点。未配置的
    // 目标一律 NotFound，不存在自动发现或进程内直连捷径。
    options.static_routes = {fw::StaticRoute{
        std::string(getvalue::GetValueCallerService::kCalleeService),
        inf::RpcAddress{"http", endpoint}}};

    fw::CoApp app{options};
    if (auto r = app.add_service<getvalue::GetValueService>(ConcurrentOpts());
        !r) {
        std::fprintf(stderr, "fixture: add_service(callee) failed: %s\n",
                     r.error().message.c_str());
        return 4;
    }
    if (auto r =
            app.add_service<getvalue::GetValueCallerService>(ConcurrentOpts());
        !r) {
        std::fprintf(stderr, "fixture: add_service(caller) failed: %s\n",
                     r.error().message.c_str());
        return 4;
    }
    auto owner = std::make_shared<CloseJournal>();
    if (auto r = app.add_resource<CloseJournal>(owner); !r) {
        std::fprintf(stderr, "fixture: add_resource failed: %s\n",
                     r.error().message.c_str());
        return 4;
    }

    int rc = -1;
    std::thread runner([&] { rc = app.run(); });

    std::string bound;
    for (int i = 0; i < 400; ++i) {
        bound = app.bound_endpoint();
        if (!bound.empty()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds{25});
    }
    if (bound != endpoint) {
        app.request_shutdown();
        if (runner.joinable()) runner.join();
        std::fprintf(stderr,
                     "fixture: bind mismatch: picked=%s bound='%s' rc=%d\n",
                     endpoint.c_str(), bound.c_str(), rc);
        return 5;
    }

    {
        std::ofstream f(port_file, std::ios::out | std::ios::trunc);
        f << port;
    }
    {
        std::ofstream f(meta_file, std::ios::out | std::ios::trunc);
        f << "process=single\n";
        f << "caller_service="
          << getvalue::GetValueCallerService::kServiceName << "\n";
        f << "callee_service="
          << getvalue::GetValueCallerService::kCalleeService << "\n";
        f << "method=GetValue\n";
        f << "request_schema="
          << bbt::example::v1::GetValueRequest::descriptor()->full_name()
          << "\n";
        f << "response_schema="
          << bbt::example::v1::GetValueResponse::descriptor()->full_name()
          << "\n";
        f << "static_route=" << getvalue::GetValueCallerService::kCalleeService
          << "->http://" << endpoint << "\n";
        f << "inbound_bridge=proto_wire_v1\n";
        f << "auth_state=" << kAuthState << "\n";
    }
    std::fprintf(stderr, "two_service fixture up endpoint=%s\n",
                 endpoint.c_str());

    // 有界等待控制输入：stdin EOF（驱动器关闭管道）→ 请求关闭 → run 收束。
    char buf[8];
    while (::read(STDIN_FILENO, buf, sizeof(buf)) > 0) {
    }
    app.request_shutdown();
    if (runner.joinable()) runner.join();

    const int state = static_cast<int>(app.shutdown_state());
    Journal(std::string("event=shutdown rc=") + std::to_string(rc) +
            " state=" + std::to_string(state) + " owner_closed=" +
            (owner->IsClosed() ? "1" : "0") +
            " auth_state=" + kAuthState);
    std::fprintf(stderr, "two_service fixture rc=%d state=%d owner_closed=%d\n",
                 rc, state, owner->IsClosed() ? 1 : 0);
    return rc;
}
