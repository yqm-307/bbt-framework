// bbt-framework Service→Service fixture（两个互异 identity 的真实 CoApp）。
//
// V2 会话拒绝 self-connection：同一 identity 的出站不得拨回自身。框架又约束
// 进程内同一时刻至多一个活跃 CoApp。因此本 fixture 由驱动器启动两个进程：
//
//   caller 进程（local_identity=two-service-caller）
//     CoApp 宿主 getvalue::GetValueCallerService；其 handler 经公开
//     ICoService::call<Req,Resp> 按显式静态路由向 callee 进程发起真实 V2 Call
//     callee 进程（local_identity=two-service-callee）
//     CoApp 宿主 getvalue::GetValueService；入站经同一 V2 会话分发
//
// 两个进程身份互异且都与驱动器客户端互异，不存在自环；调用只走「静态路由
// 门 + 真实 TCP 会话」这一条公开路径。
//
// 业务 schema/服务实现/typed codec 全部复用 getvalue 切片
// （getvalue_service.hpp / getvalue_caller.hpp + getvalue.proto），不另造
// schema、协议、错误信封或第二套 codec。
//
// 不使用 internal/、Binder、CoAppSeam、MakeCoAppForTest、
// DispatchInboundForTest、INetworkHost 桩或任何发送注入；不创建额外 io_thread
// （只调用公开 CoApp::run / request_shutdown）。
//
// argv:
//   two_service callee <port_file> <meta_file> <journal_file>
//   two_service caller <port_file> <callee_ep> <meta_file> <journal_file>
// 就绪：绑定成功后写 port_file（动态 loopback 端口）与 meta_file；
// 关闭：stdin EOF → request_shutdown() → run() 返回后写 shutdown 记录并退出。
//
// 两个进程共同 append 同一 journal（O_APPEND 整行写入），驱动器据此核对
// handler 计数与「handler 排空后资源同步 Close」的真实先后。
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

#include <unistd.h>

#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/infra/ICoCloseable.hpp>

#include <bbt/framework/CoApp.hpp>

#include "getvalue_caller.hpp"  // caller 服务（公开面）
#include "getvalue_service.hpp" // callee 服务 + ProtoCodec<GetValueRequest/Response>

namespace fw = bbt::framework;
namespace inf = bbt::infra;

namespace {

constexpr const char* kAuthState = "unauthenticated_loopback";
constexpr const char* kCalleeIdentity = "two-service-callee";
constexpr const char* kCallerIdentity = "two-service-caller";

std::mutex g_journal_mtx;
std::ofstream g_journal;

// 整行一次写入 + flush：两个进程都以 O_APPEND 打开同一文件，行不会互相穿插，
// 驱动器读到的顺序即真实发生顺序。
void Journal(const std::string& line) {
    std::lock_guard<std::mutex> lk(g_journal_mtx);
    const std::string buf = line + '\n';
    g_journal.write(buf.data(), static_cast<std::streamsize>(buf.size()));
    g_journal.flush();
}

// callee handler 被真正调用时回调（公开观测钩子）。request_id 是**出站**调用
// 生成的 id（框架不把上游 request_id 透传给下游），故它只用于计数与交叉对照，
// 不能与驱动器 request_id 直接比对。
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
            first = !m_closed;
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
    bool m_closed = false;
};

fw::ServiceOptions ConcurrentOpts() {
    return fw::ServiceOptions{fw::ExecutionPolicy::Concurrent,
                              /*max_inflight*/ 64,
                              /*mailbox_capacity*/ 0, /*max_actors*/ 0,
                              /*ordered_ingress*/ false,
                              /*max_ordered_streams*/ 0,
                              /*max_cached_results*/ 0,
                              /*max_cached_result_bytes*/ 0};
}

fw::CoAppOptions BaseOptions(std::string identity) {
    fw::CoAppOptions options;
    options.network_limits = inf::NetworkLimits{
        /*max_connections*/ 64, /*max_inflight*/ 64,
        /*max_header_bytes*/ 16 * 1024, /*max_body_bytes*/ 64 * 1024,
        /*incoming_timeout*/ std::chrono::milliseconds{30000}};
    // 内核分配动态 loopback 端口；就绪后由 bound_endpoint 回读。
    options.listen = inf::ListenAddress{"127.0.0.1", 0};
    options.shutdown_step_budget = std::chrono::milliseconds{3000};
    // V2 会话身份必填；caller/callee/驱动器三方互异。
    options.local_identity = std::move(identity);
    return options;
}

std::string BoundEndpointOrEmpty(fw::CoApp& app, int timeout_ms) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds{timeout_ms};
    while (std::chrono::steady_clock::now() < deadline) {
        std::string bound = app.bound_endpoint();
        if (!bound.empty())
            return bound;
        std::this_thread::sleep_for(std::chrono::milliseconds{25});
    }
    return {};
}

void WriteTextFile(const std::string& path, const std::string& text,
                   const char* what) {
    std::ofstream f(path, std::ios::out | std::ios::trunc);
    f << text;
    if (!f)
        std::fprintf(stderr, "two_service: cannot write %s file %s\n", what,
                     path.c_str());
}

std::uint16_t PortOf(const std::string& endpoint) {
    const auto colon = endpoint.rfind(':');
    if (colon == std::string::npos)
        return 0;
    return static_cast<std::uint16_t>(std::stoi(endpoint.substr(colon + 1)));
}

std::string SchemaMeta(const std::string& local_identity) {
    return "method=GetValue\nrequest_schema=" +
           bbt::example::v1::GetValueRequest::descriptor()->full_name() +
           "\nresponse_schema=" +
           bbt::example::v1::GetValueResponse::descriptor()->full_name() +
           "\nlocal_identity=" + local_identity +
           "\nauth_state=" + kAuthState + "\n";
}

// 等待 stdin EOF 后优雅关闭。
void ServeUntilEof(fw::CoApp& app) {
    char buf[8];
    while (::read(STDIN_FILENO, buf, sizeof(buf)) > 0) {
    }
    app.request_shutdown();
}

int RunCallee(const std::string& port_file, const std::string& meta_file) {
    getvalue::SetServedHook(&AppendCalleeHandlerEntered);

    fw::CoApp app{BaseOptions(kCalleeIdentity)};
    if (auto r = app.add_service<getvalue::GetValueService>(ConcurrentOpts()); !r) {
        std::fprintf(stderr, "callee: add_service failed: %s\n",
                     r.error().message.c_str());
        return 4;
    }

    int rc = -1;
    std::thread runner([&] { rc = app.run(); });
    const std::string bound = BoundEndpointOrEmpty(app, 10000);
    if (bound.empty()) {
        app.request_shutdown();
        if (runner.joinable())
            runner.join();
        std::fprintf(stderr, "callee: bind did not become ready rc=%d\n", rc);
        return 5;
    }
    WriteTextFile(port_file, std::to_string(PortOf(bound)), "port");
    WriteTextFile(meta_file,
                  "process=callee\ncaller_service=" +
                      std::string(getvalue::GetValueCallerService::kServiceName) +
                      "\ncallee_service=" +
                      std::string(getvalue::GetValueCallerService::kCalleeService) +
                      "\n" + SchemaMeta(kCalleeIdentity),
                  "meta");
    std::fprintf(stderr, "two_service callee up endpoint=%s\n", bound.c_str());

    ServeUntilEof(app);
    if (runner.joinable())
        runner.join();
    Journal(std::string("event=callee_shutdown rc=") + std::to_string(rc) +
            " auth_state=" + kAuthState);
    return rc;
}

int RunCaller(const std::string& port_file, const std::string& callee_ep,
              const std::string& meta_file) {
    fw::CoAppOptions options = BaseOptions(kCallerIdentity);
    // 唯一静态路由：callee 服务名 → callee 进程的显式 endpoint。未配置的目标
    // 一律 NotFound，不存在自动发现或进程内直连捷径。
    options.static_routes = {fw::StaticRoute{
        std::string(getvalue::GetValueCallerService::kCalleeService), callee_ep}};

    fw::CoApp app{std::move(options)};
    if (auto r = app.add_service<getvalue::GetValueCallerService>(ConcurrentOpts());
        !r) {
        std::fprintf(stderr, "caller: add_service failed: %s\n",
                     r.error().message.c_str());
        return 4;
    }
    auto owner = std::make_shared<CloseJournal>();
    if (auto r = app.add_resource<CloseJournal>(owner); !r) {
        std::fprintf(stderr, "caller: add_resource failed: %s\n",
                     r.error().message.c_str());
        return 4;
    }

    int rc = -1;
    std::thread runner([&] { rc = app.run(); });
    const std::string bound = BoundEndpointOrEmpty(app, 10000);
    if (bound.empty()) {
        app.request_shutdown();
        if (runner.joinable())
            runner.join();
        std::fprintf(stderr, "caller: bind did not become ready rc=%d\n", rc);
        return 5;
    }
    WriteTextFile(port_file, std::to_string(PortOf(bound)), "port");
    WriteTextFile(meta_file,
                  "process=caller\ncaller_service=" +
                      std::string(getvalue::GetValueCallerService::kServiceName) +
                      "\ncallee_service=" +
                      std::string(getvalue::GetValueCallerService::kCalleeService) +
                      "\nstatic_route=" +
                      std::string(getvalue::GetValueCallerService::kCalleeService) +
                      "->" + callee_ep + "\n" + SchemaMeta(kCallerIdentity),
                  "meta");
    std::fprintf(stderr, "two_service caller up endpoint=%s callee=%s\n",
                 bound.c_str(), callee_ep.c_str());

    ServeUntilEof(app);
    if (runner.joinable())
        runner.join();

    const int state = static_cast<int>(app.shutdown_state());
    Journal(std::string("event=shutdown rc=") + std::to_string(rc) +
            " state=" + std::to_string(state) + " owner_closed=" +
            (owner->IsClosed() ? "1" : "0") + " auth_state=" + kAuthState);
    std::fprintf(stderr, "two_service caller rc=%d state=%d owner_closed=%d\n",
                 rc, state, owner->IsClosed() ? 1 : 0);
    return rc;
}

} // namespace

int main(int argc, char* argv[]) {
    if (argc < 5)
        return 90; // EX_USAGE
    const std::string mode = argv[1];
    const std::string port_file = argv[2];
    const std::string journal_path = argv[argc - 1];

    g_journal.open(journal_path, std::ios::out | std::ios::app);
    if (!g_journal) {
        std::fprintf(stderr, "two_service: cannot open journal %s\n",
                     journal_path.c_str());
        return 6;
    }

    // handler 内做 protobuf 编解码 + 入站分发 + 真实出站/入站并发。默认
    // 12KB 协程栈不足以承载 protobuf 业务 codec（见 getvalue 切片 README
    // 实测记录）；经 coroutine 公开配置宏（须在调度器 Start 前设置；
    // process-global）放大到 512KB 并给足静态线程。
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 512 * 1024;

    if (mode == "callee")
        return RunCallee(port_file, argv[3]);

    if (mode == "caller" && argc >= 6) {
        const std::string callee_ep = argv[3];
        const std::string meta_file = argv[4];
        return RunCaller(port_file, callee_ep, meta_file);
    }

    return 90;
}
