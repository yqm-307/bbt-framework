// bbt-framework Service→Service fixture（两个互异 identity 的真实 v2::CoApp）。
//
// V2 会话拒绝 self-connection：同一 identity 的出站不得拨回自身。因此本
// fixture 由驱动器启动两个进程：
//
//   caller 进程（local_identity=two-service-caller）
//     v2::CoApp 宿主 getvalue::GetValueCallerService；其 handler 经公开
//     CoServiceBase::call<Req,Resp> 按显式静态路由向 callee 进程发起真实
//     V2 Call
//   callee 进程（local_identity=two-service-callee）
//     v2::CoApp 宿主 getvalue::GetValueService；入站经同一 V2 会话分发
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
// （只用公开 v2::CoApp::CoStart / CoWaitShutdownRequest / CoStop）。
//
// 主内容在业务主协程里：装配 → CoStart → 写就绪文件 → 等关闭触发 → CoStop。
// 入口是 runtime 的生成式 `bbtco_main`：不写 native main、不启动/停止
// scheduler、不建 runner 线程、不 join；共享 Engine 的唯一一次
// Engine::Run 由 bbtco_main_host_hook 执行。
//
// 参数经环境变量传入（生成式入口不暴露 argv）：
//   BBT_EXAMPLE_MODE=callee|caller
//   callee: BBT_EXAMPLE_PORT_FILE / BBT_EXAMPLE_META_FILE / BBT_EXAMPLE_JOURNAL_FILE
//   caller: BBT_EXAMPLE_PORT_FILE / BBT_EXAMPLE_CALLEE_EP /
//           BBT_EXAMPLE_META_FILE / BBT_EXAMPLE_JOURNAL_FILE
// 就绪：CoStart 返回后写 port_file（动态 loopback 端口）与 meta_file；
// 关闭：stdin EOF → RequestShutdown() → CoStop 返回后写 shutdown 记录并退出。
//
// 两个进程共同 append 同一 journal（O_APPEND 整行写入），驱动器据此核对
// handler 计数与「handler 排空后组件 OnStop」的真实先后。
//
// 所有结构化结果标注 auth_state=unauthenticated_loopback。

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>

#include <poll.h>
#include <unistd.h>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/infra/CloseOptions.hpp>

#include <bbt/framework/Component.hpp>
#include <bbt/framework/HostGate.hpp>
#include <bbt/framework/V2.hpp>

#include "getvalue_caller.hpp"  // caller 服务（公开面）
#include "getvalue_service.hpp" // callee 服务 + ProtoCodec<GetValueRequest/Response>

namespace fw = bbt::framework;
namespace v2 = bbt::framework::v2;
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

// 生命周期观测组件（公开 v2::CoComponent 面）：只用于断言关闭序列在 handler
// 排空后**同步** OnStop 一次。不伪造任何后端。
class CloseJournalComponent final : public v2::CoComponent {
public:
    struct Options {};
    static std::atomic<bool> stopped;

    explicit CloseJournalComponent(fw::detail::ConstructKey k, Options)
        : v2::CoComponent(k) {}

protected:
    ~CloseJournalComponent() noexcept override = default;

    fw::result<void> OnStop(const inf::CloseOptions&) override {
        if (!stopped.exchange(true))
            Journal(std::string("event=resource_close auth_state=") +
                    kAuthState);
        return fw::result<void>::ok();
    }
    friend class fw::detail::Registry;
};
std::atomic<bool> CloseJournalComponent::stopped{false};

fw::ServiceOptions ConcurrentOpts() {
    return fw::ServiceOptions{}.SetMaxInflight(64);
}

fw::AppOptions BaseOptions(std::string identity) {
    fw::AppNetworkOptions network;
    network.SetLimits(inf::NetworkLimits{
                          /*max_connections*/ 64, /*max_inflight*/ 64,
                          /*max_header_bytes*/ 16 * 1024,
                          /*max_body_bytes*/ 64 * 1024,
                          /*incoming_timeout*/
                          std::chrono::milliseconds{30000}})
        // 内核分配动态 loopback 端口；就绪后由 BoundEndpoint 回读。
        .SetListenAddress(inf::ListenAddress{"127.0.0.1", 0})
        // V2 会话身份必填；caller/callee/驱动器三方互异。
        .SetLocalAppId(std::move(identity));
    return fw::AppOptions{}
        .SetLimits(64, 64, 8, 64)
        .SetShutdownBudget(std::chrono::milliseconds{3000})
        .SetLifecycleBudget(std::chrono::milliseconds{3000})
        .SetNetworkOptions(std::move(network));
}

std::string Env(const char* name) {
    const char* value = std::getenv(name);
    return value == nullptr ? std::string{} : std::string{value};
}

// 生命周期结果 → 进程退出码，沿用既有验收口径：
//   0 按期优雅关闭 / 1 配置校验拒绝（未启动任何组件）/ 2 启动中途失败回退 /
//   3 关闭曾超预算（StopIncomplete）/ 4 关闭收尾存在失败项。
int CoRc(const fw::result<void>& r) {
    if (r)
        return 0;
    switch (r.error().code) {
    case fw::ErrorCode::InvalidArgument:
        return 1;
    case fw::ErrorCode::TimedOut:
        return 3;
    default:
        return 4;
    }
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

// 在当前协程内非阻塞地观察 stdin EOF（驱动器关管道即关闭触发）。
// 不建 native 观察线程：示例的生命周期只有业务主协程一条驱动线。
void WaitStdinEof() {
    for (;;) {
        pollfd pfd{STDIN_FILENO, POLLIN, 0};
        const int ready = ::poll(&pfd, 1, 0);
        if (ready > 0) {
            char buf[64];
            const ssize_t n = ::read(STDIN_FILENO, buf, sizeof(buf));
            if (n <= 0)
                return;
            continue;
        }
        if (ready < 0 && errno != EINTR)
            return;
        bbtco_sleep(20);
    }
}

// 关闭触发只有一个：驱动器关 stdin。请求槽是持久位，先置位再等待不丢通知。
void ServeUntilEof(v2::CoApp& app) {
    WaitStdinEof();
    app.RequestShutdown();
}

int RunCallee(const std::string& port_file, const std::string& meta_file) {
    getvalue::SetServedHook(&AppendCalleeHandlerEntered);

    auto app = v2::CoApp::Create(BaseOptions(kCalleeIdentity),
                                 "two-service-callee");
    if (auto r = bbt_co_await
            app->CoInstallService<getvalue::GetValueService>(
                ConcurrentOpts());
        !r) {
        std::fprintf(stderr, "callee: CoInstallService failed: %s\n",
                     r.error().message.c_str());
        return 4;
    }

    auto started = bbt_co_await app->CoStart();
    if (!started) {
        std::fprintf(stderr, "callee: start failed: %s\n",
                     started.error().message.c_str());
        return CoRc(started);
    }
    const std::string bound = app->BoundEndpoint();
    if (bound.empty()) {
        std::fprintf(stderr, "callee: bind did not become ready\n");
        auto closed = bbt_co_await app->CoStop();
        (void)closed;
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

    ServeUntilEof(*app);
    auto stopped = bbt_co_await app->CoStop();
    const int rc = CoRc(stopped);
    Journal(std::string("event=callee_shutdown rc=") + std::to_string(rc) +
            " auth_state=" + kAuthState);
    return rc;
}

int RunCaller(const std::string& port_file, const std::string& callee_ep,
              const std::string& meta_file) {
    auto options = BaseOptions(kCallerIdentity);
    // 唯一静态路由：callee 服务名 → callee 进程的显式 endpoint。未配置的目标
    // 一律 NotFound，不存在自动发现或进程内直连捷径。
    options.AddRoute(
        std::string(getvalue::GetValueCallerService::kCalleeService),
        callee_ep);

    auto app = v2::CoApp::Create(std::move(options), "two-service-caller");
    if (auto r = bbt_co_await
            app->CoInstallService<getvalue::GetValueCallerService>(
                ConcurrentOpts());
        !r) {
        std::fprintf(stderr, "caller: CoInstallService failed: %s\n",
                     r.error().message.c_str());
        return 4;
    }
    if (auto r = bbt_co_await
            app->CoInstallComponent<CloseJournalComponent>(
                "journal", CloseJournalComponent::Options{});
        !r) {
        std::fprintf(stderr, "caller: CoInstallComponent failed: %s\n",
                     r.error().message.c_str());
        return 4;
    }
    CloseJournalComponent::stopped.store(false);

    auto started = bbt_co_await app->CoStart();
    if (!started) {
        std::fprintf(stderr, "caller: start failed: %s\n",
                     started.error().message.c_str());
        return CoRc(started);
    }
    const std::string bound = app->BoundEndpoint();
    if (bound.empty()) {
        std::fprintf(stderr, "caller: bind did not become ready\n");
        auto closed = bbt_co_await app->CoStop();
        (void)closed;
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

    ServeUntilEof(*app);
    auto stopped = bbt_co_await app->CoStop();
    const int rc = CoRc(stopped);

    const int state = static_cast<int>(app->State());
    Journal(std::string("event=shutdown rc=") + std::to_string(rc) +
            " state=" + std::to_string(state) +
            " owner_closed=" +
            (CloseJournalComponent::stopped.load() ? "1" : "0") +
            " auth_state=" + kAuthState);
    std::fprintf(stderr, "two_service caller rc=%d state=%d owner_closed=%d\n",
                 rc, state,
                 CloseJournalComponent::stopped.load() ? 1 : 0);
    return rc;
}

} // namespace

// handler 内做 protobuf 编解码 + 入站分发 + 真实出站/入站并发。默认 12KB
// 协程栈不足以承载 protobuf 业务 codec；经 coroutine 公开配置宏（只能在
// Start 前的 setup_runtime 阶段写；process-global）放大到 512KB 并给足静态线程。
void setup_runtime() {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 512 * 1024;
}

int bbtco_main() {
    const std::string mode = Env("BBT_EXAMPLE_MODE");
    const std::string port_file = Env("BBT_EXAMPLE_PORT_FILE");
    const std::string journal_path = Env("BBT_EXAMPLE_JOURNAL_FILE");
    if (port_file.empty() || journal_path.empty())
        return 90; // EX_USAGE

    g_journal.open(journal_path, std::ios::out | std::ios::app);
    if (!g_journal) {
        std::fprintf(stderr, "two_service: cannot open journal %s\n",
                     journal_path.c_str());
        return 6;
    }

    if (mode == "callee")
        return RunCallee(port_file, Env("BBT_EXAMPLE_META_FILE"));

    if (mode == "caller") {
        const std::string callee_ep = Env("BBT_EXAMPLE_CALLEE_EP");
        if (callee_ep.empty())
            return 90;
        return RunCaller(port_file, callee_ep, Env("BBT_EXAMPLE_META_FILE"));
    }

    return 90;
}

// 入口 native host hook：共享 Engine 的 Engine::Run 在主线程执行一次。
int bbtco_main_host_hook(bbt::coroutine::HostHookContext& ctx) {
    return bbt::framework::HostGate::RunHostGate(ctx);
}

BBTCO_MAIN_WITH_SETUP_AND_HOST_HOOK();
