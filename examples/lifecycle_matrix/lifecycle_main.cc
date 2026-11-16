/**
 * bbt-framework 容量 / 并发上下文 / 关闭生命周期验收 fixture。
 *
 * 形态：**一个自有进程**（真实公开 bbt::framework 唯一 API：CoApp::Create +
 * 真实 loopback V2 会话宿主）注册一个公开 CoService<>（
 * lifecycle::LifecycleGateService，复用 getvalue 切片 schema/codec）与一个
 * 公开 CoComponent（CloseJournal，关闭序列观测）。外部 Python 驱动器复用
 * 唯一 V2 codec（tests/rpc_v2_wire.py）走真实 SessionFrame 会话发起 Call。
 *
 * 覆盖（真实公开入口；不使用 internal/、Binder、CoAppSeam、MakeCoAppForTest、
 * DispatchInboundForTest、INetworkHost 桩或任何发送注入；不额外创建 io_thread）：
 * R3 并发挂起 / 上下文跨 worker 不串 / 迟到完成不悬空；
 * R4 服务容量（max_inflight）耗尽 → Overloaded（不无界排队）/ 字节上限；
 * R5 拒新 → 排空 → 组件 OnStop 同步收口一次；StopIncomplete 保留强持有、
 * 迟到完成不悬空；外部 supervisor 硬杀只留「未完成」证据、不称优雅。
 *
 * 验收控制面（**仅验收用**，不是生产注入）：stdin 逐行命令
 * `shutdown` → RequestShutdown()；`open` → 放行验收闩；`quit`/EOF → 收尾。
 * 断言全部基于 journal 事件与进程退出码，不用固定 sleep 猜时序。
 *
 * 退出码：0=按期优雅收口（State()==Stopped 且无 pending）；2=安装/启动失败；
 * 3=StopIncomplete 后在途 handler 迟到完成且逐对象收口完成；4=收口未竟；
 * 5=绑定不符/绑定超时；6=journal 打开失败；90=用法错误。
 *
 * argv: fixture <port_file> <meta_file> <journal_file> <max_inflight> <step_budget_ms>
 * 全部结构化结果标注 auth_state=unauthenticated_loopback。
 */

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>

#include <bbt/framework/CoApp.hpp>

/**
 * 验收服务 + 验收闩（复用 getvalue 切片 codec）
 */
#include "lifecycle_service.hpp"

namespace fw  = bbt::framework;
namespace inf = bbt::infra;

namespace {

using lifecycle::kAuthState;

std::mutex    g_journal_mtx;
std::ofstream g_journal;

void Journal(const std::string& line) {
    std::lock_guard<std::mutex> lk(g_journal_mtx);
    g_journal << line << '\n';
    g_journal.flush();
}

/**
 * 关闭序列观测组件（公开 CoInstallComponent 面；examples/two_service 的
 * CloseJournal 同款）：OnStop 在 CoStop 收口相位执行，此时网络宿主已
 * StopAccepting 且入站 handler 已排空——journal `event=resource_close` 因此
 * 可断言「handler 排空后同步收口一次」。StopIncomplete 路径下该组件不被
 * CoStop 触达（owner 保留），迟到完成后的逐对象收口才会执行 OnStop。
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
            Journal(std::string("event=resource_close auth_state=") +
                    kAuthState);
        return fw::result<void>::ok();
    }
};

/**
 * 动态 loopback 端口：内核分配后立即释放，供 listen 使用。就绪后严格核对
 * bound_endpoint 端口等于所选端口，不符即失败（竞态落败报错，不假通过）。
 */
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

std::string Trim(const std::string& s) {
    const auto b = s.find_first_not_of(" \t\r\n");
    if (b == std::string::npos) return {};
    const auto e = s.find_last_not_of(" \t\r\n");
    return s.substr(b, e - b + 1);
}

/**
 * 关闭收尾判定（公开 API 语义）：
 * - State()==Stopped 且无 pending → 0（按期优雅收口）；
 * - State()==StopIncomplete：CoStop 的有限期限内入站 handler 未被证实排空，
 *   网络宿主已同步 Close、业务对象与 owner 保留。本进程随后**等在途
 *   handler 真实终态**（有限轮询 PendingCleanup，不猜时序），再逐对象
 *   CoUninstall 完成收口；全部完成 → 3（迟到完成），仍有未竟 → 4。
 * 在受管协程内调用（CoUninstall* 与 bbtco_sleep 都要求协程上下文）。
 */
int SettleIncompleteStop(const std::shared_ptr<fw::CoApp>& app) {
    if (app->State() == fw::AppState::Stopped &&
        app->PendingCleanup().empty())
        return 0;
    if (app->State() != fw::AppState::StopIncomplete) return 4;

    const auto settle_deadline = std::chrono::steady_clock::now() +
        std::chrono::milliseconds{15000};
    while (!app->PendingCleanup().empty()) {
        if (std::chrono::steady_clock::now() >= settle_deadline) return 4;
        bbtco_sleep(10);
    }
    auto un_svc = bbt_co_await app->CoUninstallService<
        lifecycle::LifecycleGateService>(
        fw::LifecycleOptions{}.SetDeadline(
            std::chrono::steady_clock::now() + std::chrono::milliseconds{5000}));
    if (!un_svc) {
        std::fprintf(stderr, "fixture: CoUninstallService failed: %s\n",
                     un_svc.error().message.c_str());
        return 4;
    }
    auto un_comp = bbt_co_await app->CoUninstallComponent<CloseJournal>(
        "close-journal", fw::ComponentOptions{},
        fw::LifecycleOptions{}.SetDeadline(
            std::chrono::steady_clock::now() + std::chrono::milliseconds{5000}));
    if (!un_comp) {
        std::fprintf(stderr, "fixture: CoUninstallComponent failed: %s\n",
                     un_comp.error().message.c_str());
        return 4;
    }
    return app->PendingCleanup().empty() ? 3 : 4;
}

} // namespace

int main(int argc, char* argv[]) {
    if (argc < 7 || std::strcmp(argv[1], "fixture") != 0)
        /**
         * EX_USAGE
         */
        return 90;
    const std::string port_file     = argv[2];
    const std::string meta_file     = argv[3];
    const std::string journal_path  = argv[4];
    const std::size_t max_inflight  = std::strtoul(argv[5], nullptr, 10);
    const long        step_budget   = std::strtol(argv[6], nullptr, 10);

    g_journal.open(journal_path, std::ios::out | std::ios::trunc);
    if (!g_journal) {
        std::fprintf(stderr, "fixture: cannot open journal %s\n",
                     journal_path.c_str());
        return 6;
    }
    lifecycle::SetJournal(&Journal);

    /**
     * handler 内做 protobuf 编解码 + 入站分发，默认 12KB 协程栈不足；经
     * coroutine 公开配置宏（须在调度器 Start 前设置；process-global）放大到
     * 512KB 并给足静态线程（4），使并发挂起与入站接纳互不阻塞。本进程在
     * g_scheduler->Start() 前设置（同 getvalue 切片口径）。
     */
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size        = 512 * 1024;

    const std::uint16_t port = PickLoopbackPort();
    if (port == 0) {
        std::fprintf(stderr, "fixture: cannot pick a free loopback port\n");
        return 6;
    }
    const std::string endpoint = "127.0.0.1:" + std::to_string(port);

    auto options = fw::AppOptions{}
        .SetLimits(64, 64, 64, 64)
        .SetNetworkLimits(inf::NetworkLimits{
            /*max_connections*/ 64, /*max_inflight*/ 64,
            /*max_header_bytes*/ 16 * 1024,
            /**
             * R4「字节上限」维度（功能检查非压测）
             */
            /*max_body_bytes*/ 4096,
            /*incoming_timeout*/ std::chrono::milliseconds{30000}})
        /**
         * 内核已分配动态端口，listen 到它；CoStart 后经 BoundEndpoint 回读。
         */
        .Listen("127.0.0.1:" + std::to_string(port))
        /**
         * V2 会话身份必填。
         */
        .SetLocalIdentity("lifecycle-fixture")
        .SetShutdownBudget(std::chrono::milliseconds{
            step_budget > 0 ? step_budget : 1})
        .SetLifecycleBudget(std::chrono::milliseconds{3000});

    /**
     * 装配结果跨协程/线程交接（与 getvalue/two_service 同口径）：endpoint 经
     * promise 交给宿主线程写端口文件；App 指针发布后宿主线程只做
     * RequestShutdown（幂等 noexcept）；退出码经 promise 回传（单次写入）。
     */
    std::promise<std::string>  bound_ep;
    std::promise<int>          rc_slot;
    std::shared_ptr<fw::CoApp> app_handle;
    std::mutex                 app_mtx;

    g_scheduler->Start();

    bool submitted = false;
    g_scheduler->RegistCoroutineTask(
        [options = std::move(options), max_inflight, step_budget, &bound_ep,
         &rc_slot, &app_handle, &app_mtx]() mutable {
            auto app = fw::CoApp::Create(options, "lifecycle-fixture");
            if (!app) {
                std::fprintf(stderr, "fixture: CoApp::Create failed\n");
                rc_slot.set_value(4);
                return;
            }
            {
                std::lock_guard<std::mutex> lk(app_mtx);
                app_handle = app;
            }
            auto install_svc = bbt_co_await app->CoInstallService<
                lifecycle::LifecycleGateService>(
                fw::ServiceOptions{}
                    .SetExecution(fw::ExecutionPolicy::Concurrent)
                    .SetMaxInflight(max_inflight));
            if (!install_svc) {
                std::fprintf(stderr,
                             "fixture: CoInstallService failed: %s\n",
                             install_svc.error().message.c_str());
                rc_slot.set_value(2);
                return;
            }
            /**
             * 关闭序列观测组件：App scope，不被服务绑定，只在 CoStop 收口
             * 相位（或在 StopIncomplete 后的逐对象收口）同步 OnStop 一次。
             */
            auto install_comp = bbt_co_await app->CoInstallComponent<
                CloseJournal>("close-journal", CloseJournal::Options{});
            if (!install_comp) {
                std::fprintf(stderr,
                             "fixture: CoInstallComponent failed: %s\n",
                             install_comp.error().message.c_str());
                rc_slot.set_value(2);
                return;
            }
            auto started = bbt_co_await app->CoStart(
                fw::LifecycleOptions{}.SetDeadline(
                    std::chrono::steady_clock::now() +
                    std::chrono::seconds{5}));
            if (!started) {
                std::fprintf(stderr, "fixture: CoStart failed: %s\n",
                             started.error().message.c_str());
                rc_slot.set_value(2);
                return;
            }
            bound_ep.set_value(app->BoundEndpoint());
            const auto waited = bbt_co_await app->CoWaitShutdownRequest();
            (void)waited;
            /**
             * 关闭相位：step_budget 即 CoStop 的有限期限（旧
             * shutdown_step_budget 的对应物）。期限耗尽时 CoStop 如实置
             * StopIncomplete 保留 owner，由 SettleIncompleteStop 等在途
             * handler 终态后逐对象收口，不削弱关闭/排空断言。
             */
            auto stopped = bbt_co_await app->CoStop(
                fw::LifecycleOptions{}.SetDeadline(
                    std::chrono::steady_clock::now() +
                    std::chrono::milliseconds{step_budget}));
            rc_slot.set_value(SettleIncompleteStop(app));
        },
        submitted);
    if (!submitted) {
        std::fprintf(stderr, "fixture: coroutine task rejected\n");
        return 4;
    }

    /**
     * 轮询绑定地址（动态端口；CoStart 完成后由协程侧发布）。
     */
    auto bound_fut = bound_ep.get_future();
    if (bound_fut.wait_for(std::chrono::seconds{10}) !=
        std::future_status::ready) {
        {
            std::lock_guard<std::mutex> lk(app_mtx);
            if (app_handle) app_handle->RequestShutdown();
        }
        std::fprintf(stderr, "fixture: bind timeout\n");
        return 5;
    }
    const std::string bound = bound_fut.get();
    if (bound != endpoint) {
        {
            std::lock_guard<std::mutex> lk(app_mtx);
            if (app_handle) app_handle->RequestShutdown();
        }
        std::fprintf(stderr,
                     "fixture: bind mismatch: picked=%s bound='%s'\n",
                     endpoint.c_str(), bound.c_str());
        return 5;
    }

    {
        std::ofstream f(port_file, std::ios::out | std::ios::trunc);
        f << port;
    }
    {
        std::ofstream f(meta_file, std::ios::out | std::ios::trunc);
        f << "process=lifecycle\n";
        f << "service=" << lifecycle::LifecycleGateService::kServiceName
          << "\n";
        f << "method_hold=Hold\nmethod_fast=Fast\n";
        f << "request_schema="
          << bbt::example::v1::GetValueRequest::descriptor()->full_name()
          << "\n";
        f << "response_schema="
          << bbt::example::v1::GetValueResponse::descriptor()->full_name()
          << "\n";
        f << "local_identity=lifecycle-fixture\n";
        f << "max_inflight=" << max_inflight << "\n";
        f << "step_budget_ms=" << step_budget << "\n";
        f << "auth_state=" << kAuthState << "\n";
    }
    Journal(std::string("event=ready endpoint=") + endpoint +
            " max_inflight=" + std::to_string(max_inflight) +
            " step_budget_ms=" + std::to_string(step_budget) +
            " auth_state=" + kAuthState);
    std::fprintf(stderr, "lifecycle fixture up endpoint=%s max_inflight=%zu\n",
                 endpoint.c_str(), max_inflight);

    /**
     * 关闭相位观测线程：轮询 AppState 并在**变化时**记录（观察即成事件，
     * 驱动器据 journal 事件推进，不靠固定 sleep 猜时序）。退出码交付后
     * （run_done）且状态已达终态即退出。state 取值 = AppState 枚举序：
     * 4=Stopping、5=StopIncomplete、6=Stopped。
     */
    std::atomic<bool> run_done{false};
    std::thread observer([&] {
        int last = -1;
        while (true) {
            const int now = static_cast<int>(app_handle->State());
            if (now != last) {
                last = now;
                Journal("event=shutdown_state state=" + std::to_string(now) +
                        " pending=" +
                        std::to_string(app_handle->PendingCleanup().size()) +
                        " auth_state=" + kAuthState);
            }
            if (run_done.load(std::memory_order_acquire) &&
                (now == static_cast<int>(fw::AppState::Stopped) ||
                 now == static_cast<int>(fw::AppState::StopIncomplete)))
                break;
            std::this_thread::sleep_for(std::chrono::milliseconds{10});
        }
    });

    /**
     * 验收控制面：stdin 逐行命令（见文件头）。
     */
    std::string line;
    while (std::getline(std::cin, line)) {
        const std::string cmd = Trim(line);
        if (cmd == "quit") {
            break;
        } else if (cmd == "shutdown") {
            Journal(std::string("event=shutdown_requested auth_state=") +
                    kAuthState);
            std::lock_guard<std::mutex> lk(app_mtx);
            if (app_handle) app_handle->RequestShutdown();
        } else if (cmd == "open") {
            Journal(std::string("event=gate_opened auth_state=") + kAuthState);
            lifecycle::Gate().Open();
        }
        /**
         * 未知命令忽略（不改变状态）。
         */
    }
    /**
     * quit/EOF 收尾：RequestShutdown 幂等；若已先收到 shutdown 命令则无效果。
     */
    {
        std::lock_guard<std::mutex> lk(app_mtx);
        if (app_handle) app_handle->RequestShutdown();
    }

    const int rc = rc_slot.get_future().get();
    run_done.store(true, std::memory_order_release);
    if (observer.joinable()) observer.join();

    const int state = static_cast<int>(app_handle->State());
    Journal(std::string("event=shutdown rc=") + std::to_string(rc) +
            " state=" + std::to_string(state) + " owner_closed=" +
            (CloseJournal::s_stop_calls.load() > 0 ? "1" : "0") + " pending=" +
            std::to_string(app_handle->PendingCleanup().size()) + " failures=" +
            std::to_string(app_handle->LifecycleFailures().size()) +
            " auth_state=" + kAuthState);
    std::fprintf(stderr,
                 "lifecycle fixture rc=%d state=%d owner_closed=%d\n", rc,
                 state, CloseJournal::s_stop_calls.load() > 0 ? 1 : 0);
    return rc;
}
