/**
 * bbt-framework Service→Service fixture（两个互异 identity 的真实 CoApp 进程）。
 *
 * V2 会话拒绝 self-connection：同一 identity 的出站不得拨回自身；单个 CoApp 只持
 * 一个 local_identity。因此本 fixture 由驱动器启动两个进程：
 *
 * caller 进程（local_identity=two-service-caller）
 *   CoApp 宿主 getvalue::GetValueCallerService；其 handler 经受保护的
 *   CoServiceBase::call<Req,Resp> 按显式静态路由向 callee 进程发起真实 V2 Call。
 * callee 进程（local_identity=two-service-callee）
 *   CoApp 宿主 getvalue::GetValueService；入站经同一 V2 会话分发。
 *
 * 两个进程身份互异且都与驱动器客户端互异，不存在自环；调用只走「静态路由
 * 门 + 真实 TCP 会话」这一条公开路径。
 *
 * 业务 schema/服务实现/typed codec 全部复用 getvalue 切片
 * （getvalue_service.hpp / getvalue_caller.hpp + getvalue.proto），不另造
 * schema、协议、错误信封或第二套 codec。
 *
 * 装配口径与 getvalue 两进程一致：g_scheduler->Start() 后把
 * Create → CoInstall* → CoStart → BoundEndpoint → CoWaitShutdownRequest → CoStop
 * 投递进受管协程；不使用 internal/、Binder、CoAppSeam、MakeCoAppForTest、
 * DispatchInboundForTest、INetworkHost 桩或任何发送注入；不创建额外 io_thread
 * （stdin EOF → RequestShutdown 仅置位并唤醒 waiter，主协程亲自 CoStop 收口）。
 *
 * argv:
 *   two_service callee <port_file> <meta_file> <journal_file>
 *   two_service caller <port_file> <callee_ep> <meta_file> <journal_file>
 * 就绪：CoStart 后经 BoundEndpoint 回读动态 loopback 端口写 port_file 与 meta_file；
 * 关闭：stdin EOF → RequestShutdown → CoStop 收口后写 shutdown 记录并退出。
 *
 * 两个进程共同 append 同一 journal（O_APPEND 整行写入），驱动器据此核对
 * handler 计数与「handler 排空后组件 OnStop 同步收口」的真实先后。
 *
 * 所有结构化结果标注 auth_state=unauthenticated_loopback。
 */

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <future>
#include <memory>
#include <mutex>
#include <string>

#include <unistd.h>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>

#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/Component.hpp>

/**
 * caller 服务（公开面）
 */
#include "getvalue_caller.hpp"
/**
 * callee 服务 + ProtoCodec<GetValueRequest/Response>
 */
#include "getvalue_service.hpp"

namespace fw = bbt::framework;
namespace inf = bbt::infra;

namespace {

constexpr const char* kAuthState = "unauthenticated_loopback";
constexpr const char* kCalleeIdentity = "two-service-callee";
constexpr const char* kCallerIdentity = "two-service-caller";

std::mutex g_journal_mtx;
std::ofstream g_journal;

/**
 * 整行一次写入 + flush：两个进程都以 O_APPEND 打开同一文件，行不会互相穿插，
 * 驱动器读到的顺序即真实发生顺序。
 */
void Journal(const std::string& line) {
    std::lock_guard<std::mutex> lk(g_journal_mtx);
    const std::string buf = line + '\n';
    g_journal.write(buf.data(), static_cast<std::streamsize>(buf.size()));
    g_journal.flush();
}

/**
 * callee handler 被真正调用时回调（公开观测钩子）。request_id 是**出站**调用
 * 生成的 id（框架不把上游 request_id 透传给下游），故它只用于计数与交叉对照，
 * 不能与驱动器 request_id 直接比对。
 */
void AppendCalleeHandlerEntered(const std::string& request_id,
                                const std::string& peer_principal) {
    Journal(std::string("event=callee_handler_entered req=") + request_id +
            " peer_principal=" + peer_principal +
            " auth_state=" + kAuthState);
}

/**
 * 生命周期观测组件（公开 `CoInstallComponent` 面）：旧 `add_resource<ICoCloseable>`
 * 的对应物——OnStop 在 _StopImpl 中被调用，其时网络宿主已 StopAccepting 且入站
 * handler 已排空，故可用于断言「关闭序列在 handler 排空后**同步**收口一次」。
 * 不伪造任何后端。
 */
class CloseJournal final : public fw::CoComponent {
public:
    struct Options {};

    explicit CloseJournal(fw::detail::ConstructKey key, Options)
        : fw::CoComponent(key) {}

    /**
     * OnStop 完成计数：宿主线程在退出码交付后读它写 owner_closed。
     */
    inline static std::atomic<int> s_stop_calls{0};

protected:
    ~CloseJournal() noexcept override = default;

private:
    friend class bbt::framework::detail::Registry;

protected:
    fw::result<void> OnStop() override {
        const int n = s_stop_calls.fetch_add(1) + 1;
        if (n == 1)
            Journal(std::string("event=resource_close auth_state=") + kAuthState);
        return fw::result<void>::ok();
    }
};

const char* AppStateName(fw::AppState s) {
    switch (s) {
    case fw::AppState::Created:
        return "Created";
    case fw::AppState::Starting:
        return "Starting";
    case fw::AppState::Running:
        return "Running";
    case fw::AppState::Paused:
        return "Paused";
    case fw::AppState::Stopping:
        return "Stopping";
    case fw::AppState::StopIncomplete:
        return "StopIncomplete";
    case fw::AppState::Stopped:
        return "Stopped";
    }
    return "Unknown";
}

fw::ServiceOptions ServiceOpts() {
    return fw::ServiceOptions{}
        .SetExecution(fw::ExecutionPolicy::Concurrent)
        .SetMaxInflight(64);
}

fw::AppOptions BaseOptions(std::string identity) {
    fw::AppLimits limits;
    limits.max_objects = 64;
    limits.max_bindings = 64;
    limits.max_observers = 64;
    limits.observer_queue_capacity = 64;
    return fw::AppOptions{}
        .SetAppLimits(limits)
        .SetNetworkLimits(inf::NetworkLimits{
            /*max_connections*/ 64, /*max_inflight*/ 64,
            /*max_header_bytes*/ 16 * 1024, /*max_body_bytes*/ 64 * 1024,
            /*incoming_timeout*/ std::chrono::milliseconds{30000}})
        /**
         * 内核分配动态 loopback 端口；就绪后由 BoundEndpoint 回读。
         */
        .Listen("127.0.0.1:0")
        .SetShutdownBudget(std::chrono::milliseconds{3000})
        .SetLifecycleBudget(std::chrono::milliseconds{3000})
        /**
         * V2 会话身份必填；caller/callee/驱动器三方互异。
         */
        .SetLocalIdentity(std::move(identity));
}

fw::LifecycleOptions DeadlineSec(int sec) {
    return fw::LifecycleOptions{}.SetDeadline(
        std::chrono::steady_clock::now() + std::chrono::seconds{sec});
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

/**
 * 装配结果跨协程/线程交接：与 getvalue 两进程同一口径。
 * endpoint 经 promise 交给宿主线程写端口文件；App 指针发布后宿主线程只做
 * RequestShutdown（幂等 noexcept，任意线程可调）；退出码经 promise 回传。
 * rc 映射：0=按期优雅收口且 State()==Stopped，2=安装/启动失败，
 * 4=Create 失败或收口未竟，5=宿主侧绑定超时。
 */
struct HostSlots {
    std::promise<std::string> bound_ep;
    std::promise<int> rc_slot;
    std::shared_ptr<fw::CoApp> app_handle;
    std::mutex app_mtx;
};

/**
 * 宿主侧通用等待段：等 endpoint → 超时则触发关闭请求按 5 退出（协程侧已如实
 * 写入启动相位退出码，promise 单次写入，宿主不二次写）；成功写 port/meta →
 * 阻塞读 stdin EOF → RequestShutdown。返回 false 表示绑定超时。
 */
bool WaitBoundAndServe(HostSlots& slots, const std::string& port_file,
                       const std::string& meta_file,
                       const std::string& meta_text, const char* who) {
    auto bound = slots.bound_ep.get_future();
    if (bound.wait_for(std::chrono::seconds{10}) !=
        std::future_status::ready) {
        {
            std::lock_guard<std::mutex> lk(slots.app_mtx);
            if (slots.app_handle)
                slots.app_handle->RequestShutdown();
        }
        std::fprintf(stderr, "two_service %s did not bind\n", who);
        return false;
    }
    const std::string endpoint = bound.get();
    WriteTextFile(port_file, std::to_string(PortOf(endpoint)), "port");
    WriteTextFile(meta_file, meta_text, "meta");
    std::fprintf(stderr, "two_service %s up endpoint=%s\n", who,
                 endpoint.c_str());

    char buf[8];
    while (::read(STDIN_FILENO, buf, sizeof(buf)) > 0) {
    }
    /**
     * stdin EOF：native 线程发关闭请求（仅置位并唤醒唯一 waiter；
     * 主协程亲自 CoStop 收口）。
     */
    {
        std::lock_guard<std::mutex> lk(slots.app_mtx);
        if (slots.app_handle)
            slots.app_handle->RequestShutdown();
    }
    return true;
}

int RunCallee(const std::string& port_file, const std::string& meta_file) {
    getvalue::SetServedHook(&AppendCalleeHandlerEntered);

    HostSlots slots;
    bool submitted = false;
    g_scheduler->RegistCoroutineTask(
        [options = BaseOptions(kCalleeIdentity), &slots]() mutable {
            auto app = fw::CoApp::Create(options, "two-service-callee");
            if (!app) {
                std::fprintf(stderr, "callee: CoApp::Create failed\n");
                slots.rc_slot.set_value(4);
                return;
            }
            {
                std::lock_guard<std::mutex> lk(slots.app_mtx);
                slots.app_handle = app;
            }
            auto install = bbt_co_await app->CoInstallService<
                getvalue::GetValueService>(ServiceOpts());
            if (!install) {
                std::fprintf(stderr, "callee: CoInstallService failed: %s\n",
                             install.error().message.c_str());
                slots.rc_slot.set_value(2);
                return;
            }
            auto started = bbt_co_await app->CoStart(DeadlineSec(5));
            if (!started) {
                std::fprintf(stderr, "callee: CoStart failed: %s\n",
                             started.error().message.c_str());
                slots.rc_slot.set_value(2);
                return;
            }
            slots.bound_ep.set_value(app->BoundEndpoint());
            const auto waited = bbt_co_await app->CoWaitShutdownRequest();
            (void)waited;
            auto stopped = bbt_co_await app->CoStop(DeadlineSec(3));
            slots.rc_slot.set_value(
                stopped && app->State() == fw::AppState::Stopped ? 0 : 4);
        },
        submitted);
    if (!submitted) {
        std::fprintf(stderr, "callee: coroutine task rejected\n");
        return 4;
    }

    const std::string meta_text =
        "process=callee\ncaller_service=" +
        std::string(getvalue::GetValueCallerService::kServiceName) +
        "\ncallee_service=" +
        std::string(getvalue::GetValueCallerService::kCalleeService) + "\n" +
        SchemaMeta(kCalleeIdentity);
    if (!WaitBoundAndServe(slots, port_file, meta_file, meta_text, "callee"))
        return 5;

    const int rc = slots.rc_slot.get_future().get();
    Journal(std::string("event=callee_shutdown rc=") + std::to_string(rc) +
            " auth_state=" + kAuthState);
    return rc;
}

int RunCaller(const std::string& port_file, const std::string& callee_ep,
              const std::string& meta_file) {
    fw::AppOptions options = BaseOptions(kCallerIdentity);
    /**
     * 唯一静态路由：callee 服务名 → callee 进程的显式 endpoint。未配置的目标
     * 一律 NotFound，不存在自动发现或进程内直连捷径。
     */
    options.AddRoute(
        std::string(getvalue::GetValueCallerService::kCalleeService),
        callee_ep);

    HostSlots slots;
    bool submitted = false;
    g_scheduler->RegistCoroutineTask(
        [options = std::move(options), &slots]() mutable {
            auto app = fw::CoApp::Create(options, "two-service-caller");
            if (!app) {
                std::fprintf(stderr, "caller: CoApp::Create failed\n");
                slots.rc_slot.set_value(4);
                return;
            }
            {
                std::lock_guard<std::mutex> lk(slots.app_mtx);
                slots.app_handle = app;
            }
            auto install = bbt_co_await app->CoInstallService<
                getvalue::GetValueCallerService>(ServiceOpts());
            if (!install) {
                std::fprintf(stderr, "caller: CoInstallService failed: %s\n",
                             install.error().message.c_str());
                slots.rc_slot.set_value(2);
                return;
            }
            /**
             * 关闭观测组件：旧 add_resource<CloseJournal> 的对应物；App scope、
             * 不被服务绑定，只在 CoStop 收口相位同步 OnStop 一次。
             */
            auto comp = bbt_co_await app->CoInstallComponent<CloseJournal>(
                "close-journal", CloseJournal::Options{});
            if (!comp) {
                std::fprintf(stderr, "caller: CoInstallComponent failed: %s\n",
                             comp.error().message.c_str());
                slots.rc_slot.set_value(2);
                return;
            }
            auto started = bbt_co_await app->CoStart(DeadlineSec(5));
            if (!started) {
                std::fprintf(stderr, "caller: CoStart failed: %s\n",
                             started.error().message.c_str());
                slots.rc_slot.set_value(2);
                return;
            }
            slots.bound_ep.set_value(app->BoundEndpoint());
            const auto waited = bbt_co_await app->CoWaitShutdownRequest();
            (void)waited;
            auto stopped = bbt_co_await app->CoStop(DeadlineSec(3));
            slots.rc_slot.set_value(
                stopped && app->State() == fw::AppState::Stopped ? 0 : 4);
        },
        submitted);
    if (!submitted) {
        std::fprintf(stderr, "caller: coroutine task rejected\n");
        return 4;
    }

    const std::string meta_text =
        "process=caller\ncaller_service=" +
        std::string(getvalue::GetValueCallerService::kServiceName) +
        "\ncallee_service=" +
        std::string(getvalue::GetValueCallerService::kCalleeService) +
        "\nstatic_route=" +
        std::string(getvalue::GetValueCallerService::kCalleeService) + "->" +
        callee_ep + "\n" + SchemaMeta(kCallerIdentity);
    if (!WaitBoundAndServe(slots, port_file, meta_file, meta_text, "caller"))
        return 5;

    const int rc = slots.rc_slot.get_future().get();
    /**
     * State() 原子读、任意线程可调；owner_closed 读组件 OnStop 计数
     * （happens-after：rc_slot 在 CoStop 返回后才写入）。
     */
    std::shared_ptr<fw::CoApp> app;
    {
        std::lock_guard<std::mutex> lk(slots.app_mtx);
        app = slots.app_handle;
    }
    const char* state = app ? AppStateName(app->State()) : "Unknown";
    const int owner_closed = CloseJournal::s_stop_calls.load() > 0 ? 1 : 0;
    Journal(std::string("event=shutdown rc=") + std::to_string(rc) +
            " state=" + state + " owner_closed=" +
            std::to_string(owner_closed) + " auth_state=" + kAuthState);
    std::fprintf(stderr, "two_service caller rc=%d state=%s owner_closed=%d\n",
                 rc, state, owner_closed);
    return rc;
}

} // namespace

int main(int argc, char* argv[]) {
    if (argc < 5)
        /**
         * EX_USAGE
         */
        return 90;
    const std::string mode = argv[1];
    const std::string port_file = argv[2];
    const std::string journal_path = argv[argc - 1];

    g_journal.open(journal_path, std::ios::out | std::ios::app);
    if (!g_journal) {
        std::fprintf(stderr, "two_service: cannot open journal %s\n",
                     journal_path.c_str());
        return 6;
    }

    /**
     * handler 内做 protobuf 编解码 + 入站分发 + 真实出站/入站并发。默认
     * 12KB 协程栈不足以承载 protobuf 业务 codec（见 getvalue 切片 README
     * 实测记录）；经 coroutine 公开配置宏（须在调度器 Start 前设置；
     * process-global）放大到 512KB 并给足静态线程。
     */
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 512 * 1024;

    /**
     * 普通 main + 协程入口：CoApp::Create 与全部 Co* 入口要求受管协程上下文，
     * 先启动 runtime，再把宿主流程投递进受管协程。
     */
    g_scheduler->Start();

    if (mode == "callee")
        return RunCallee(port_file, argv[3]);

    if (mode == "caller" && argc >= 6) {
        const std::string callee_ep = argv[3];
        const std::string meta_file = argv[4];
        return RunCaller(port_file, callee_ep, meta_file);
    }

    return 90;
}
