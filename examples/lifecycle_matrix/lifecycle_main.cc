// bbt-framework 容量 / 并发上下文 / 关闭生命周期验收 fixture。
//
// 形态：**一个自有进程**（真实公开 `v2::CoApp` + 真实 loopback V2 会话宿主）注册一个
// 公开 `v2::CoService`（lifecycle::LifecycleGateService，复用 getvalue 切片
// schema/codec）。外部 Python 驱动器复用唯一 V2 codec（`tests/rpc_v2_wire.py`）
// 走真实 `SessionFrame` 会话（Hello/HelloAck + u32be）发起 Call。
//
// 覆盖（真实公开入口；不使用 internal/、Binder、CoAppSeam、MakeCoAppForTest、
// DispatchInboundForTest、INetworkHost 桩或任何发送注入；不额外创建 io_thread）：
//   R3 并发挂起 / 上下文跨 worker 不串 / 迟到完成不悬空；
//   R4 服务容量（max_inflight）耗尽 → Overloaded（不无界排队）/ 字节上限；
//   R5 正常拒新→drain→同步 Close→release；StopIncomplete 保留强持有、
//      迟到完成；外部 supervisor 硬杀只留「未完成」证据、不称优雅。
//
// 主内容全部在业务主协程里：装配 → CoStart → 写就绪文件 → 命令循环 → CoStop。
// 入口是 runtime 的生成式 `bbtco_main`：进程不写 native main、不启动/停止
// scheduler、不建 runner 线程、不 join，也没有 native 观察线程——关闭状态迁移
// 由同一个 runtime 里的第二个协程观察；共享 Engine 的唯一一次 Engine::Run
// 由 bbtco_main_host_hook 执行。
//
// 参数（生成式入口不暴露 argv，参数经环境变量传入）：
//   BBT_EXAMPLE_MODE=fixture
//   BBT_EXAMPLE_PORT_FILE / BBT_EXAMPLE_META_FILE / BBT_EXAMPLE_JOURNAL_FILE /
//   BBT_EXAMPLE_MAX_INFLIGHT / BBT_EXAMPLE_STEP_BUDGET_MS
// 验收控制面（**仅验收用**，不是生产注入）：stdin 逐行命令
//   `shutdown` → RequestShutdown() + CoStop；`open` → 放行验收闩；
//   `quit`/EOF → 收尾。断言全部基于 journal 事件与进程退出码，不用固定 sleep
//   猜时序。全部结构化结果标注 auth_state=unauthenticated_loopback。

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/infra/CloseOptions.hpp>

#include <bbt/framework/Component.hpp>
#include <bbt/framework/HostGate.hpp>
#include <bbt/framework/V2.hpp>

#include "lifecycle_service.hpp"   // 验收服务 + 验收闩（复用 getvalue 切片 codec）

namespace fw  = bbt::framework;
namespace v2  = bbt::framework::v2;
namespace inf = bbt::infra;

namespace {

using lifecycle::kAuthState;

std::mutex    g_journal_mtx;
std::ofstream g_journal;
std::atomic_bool g_stop_observer{false};

void Journal(const std::string& line) {
    std::lock_guard<std::mutex> lk(g_journal_mtx);
    g_journal << line << '\n';
    g_journal.flush();
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

// 生命周期观测组件（公开 v2::CoComponent 面）：断言关闭序列在 handler 排空
// 后**同步** OnStop 一次；StopIncomplete 期间不得被 OnStop。不伪造任何后端。
class CloseJournalComponent final : public v2::CoComponent {
public:
    struct Options {};
    static std::atomic_bool stopped;

    explicit CloseJournalComponent(fw::detail::ConstructKey k, Options)
        : v2::CoComponent(k) {}

protected:
    ~CloseJournalComponent() noexcept override = default;

    fw::result<void> OnStop(const inf::CloseOptions&) override {
        stopped.store(true);
        Journal(std::string("event=resource_close auth_state=") + kAuthState);
        return fw::result<void>::ok();
    }
    friend class fw::detail::Registry;
};
std::atomic_bool CloseJournalComponent::stopped{false};

// 动态 loopback 端口：内核分配后立即释放，供 listen 使用。就绪后严格核对
// BoundEndpoint 端口等于所选端口，不符即失败（竞态落败报错，不假通过）。
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

fw::ServiceOptions ConcurrentOpts(std::size_t max_inflight) {
    return fw::ServiceOptions{}.SetMaxInflight(max_inflight);
}

std::string Trim(const std::string& s) {
    const auto b = s.find_first_not_of(" \t\r\n");
    if (b == std::string::npos) return {};
    const auto e = s.find_last_not_of(" \t\r\n");
    return s.substr(b, e - b + 1);
}

enum class StdinEvent {
    kIdle,
    kLine,
    kEof
};

// 在当前协程内非阻塞读一行：没有整行时返回 kIdle 且保留已读字节，绝不让
// worker 阻塞在 read 上（否则要额外的 native 线程与 join，示例不允许）。
StdinEvent PollCommandLine(std::string& buffer, std::string* line) {
    for (;;) {
        const auto pos = buffer.find('\n');
        if (pos != std::string::npos) {
            *line = Trim(buffer.substr(0, pos));
            buffer.erase(0, pos + 1);
            return StdinEvent::kLine;
        }
        pollfd pfd{STDIN_FILENO, POLLIN, 0};
        const int ready = ::poll(&pfd, 1, 0);
        if (ready == 0)
            return StdinEvent::kIdle;
        if (ready < 0) {
            if (errno == EINTR)
                continue;
            return StdinEvent::kEof;
        }
        char buf[256];
        const ssize_t n = ::read(STDIN_FILENO, buf, sizeof(buf));
        if (n <= 0) {
            // 残留的半行按最后一行交付，下一次调用再报 EOF。
            if (!buffer.empty()) {
                *line = Trim(buffer);
                buffer.clear();
                return StdinEvent::kLine;
            }
            return StdinEvent::kEof;
        }
        buffer.append(buf, static_cast<std::size_t>(n));
    }
}

// 关闭状态迁移观察：只在**变化时**写 journal（观察即成事件，驱动器据此推进，
// 不靠固定 sleep 猜时序）。用独立协程而非 native 观察线程——同一 runtime 内
// 并发观察，进程退出前不需要 join。
void ObserveShutdownStates(v2::CoApp& app) {
    int last = -1;
    for (;;) {
        const int now = static_cast<int>(app.State());
        if (now != last) {
            last = now;
            Journal(std::string("event=shutdown_state state=") +
                    std::to_string(now) +
                    " pending=" + std::to_string(app.PendingCleanup().size()) +
                    " auth_state=" + kAuthState);
        }
        if (g_stop_observer.load(std::memory_order_acquire))
            return;
        bbtco_sleep(10);
    }
}

} // namespace

// handler 内做 protobuf 编解码 + 入站分发，默认 12KB 协程栈不足；经公开配置宏
// 在 Start 前放大到 512KB 并给足静态线程（4），使并发挂起与入站接纳互不阻塞。
void setup_runtime() {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 512 * 1024;
}

int bbtco_main() {
    if (Env("BBT_EXAMPLE_MODE") != "fixture")
        return 90;   // EX_USAGE
    const std::string port_file    = Env("BBT_EXAMPLE_PORT_FILE");
    const std::string meta_file    = Env("BBT_EXAMPLE_META_FILE");
    const std::string journal_path = Env("BBT_EXAMPLE_JOURNAL_FILE");
    const std::size_t max_inflight =
        std::strtoul(Env("BBT_EXAMPLE_MAX_INFLIGHT").c_str(), nullptr, 10);
    const long step_budget =
        std::strtol(Env("BBT_EXAMPLE_STEP_BUDGET_MS").c_str(), nullptr, 10);
    if (port_file.empty() || meta_file.empty() || journal_path.empty() ||
        max_inflight == 0 || step_budget <= 0)
        return 90;

    g_journal.open(journal_path, std::ios::out | std::ios::trunc);
    if (!g_journal) {
        std::fprintf(stderr, "fixture: cannot open journal %s\n",
                     journal_path.c_str());
        return 6;
    }
    lifecycle::SetJournal(&Journal);

    const std::uint16_t port = PickLoopbackPort();
    if (port == 0) {
        std::fprintf(stderr, "fixture: cannot pick a free loopback port\n");
        return 6;
    }
    const std::string endpoint = "127.0.0.1:" + std::to_string(port);

    fw::AppNetworkOptions network;
    network.SetLimits(inf::NetworkLimits{
                          /*max_connections*/ 64, /*max_inflight*/ 64,
                          /*max_header_bytes*/ 16 * 1024,
                          /*max_body_bytes*/ 4096,
                          /*incoming_timeout*/
                          std::chrono::milliseconds{30000}})
        .SetListenAddress(inf::ListenAddress{"127.0.0.1", port})
        // V2 会话身份必填。
        .SetLocalAppId("lifecycle-fixture");

    auto app = v2::CoApp::Create(
        fw::AppOptions{}
            .SetLimits(64, 64, 8, 64)
            .SetShutdownBudget(std::chrono::milliseconds{step_budget})
            .SetLifecycleBudget(std::chrono::milliseconds{step_budget})
            .SetNetworkOptions(std::move(network)),
        "lifecycle-fixture");
    if (auto r = bbt_co_await
            app->CoInstallService<lifecycle::LifecycleGateService>(
                ConcurrentOpts(max_inflight));
        !r) {
        std::fprintf(stderr, "fixture: CoInstallService failed: %s\n",
                     r.error().message.c_str());
        return 4;
    }
    CloseJournalComponent::stopped.store(false);
    CloseJournalComponent::Options journal_opts;
    if (auto r = bbt_co_await
            app->CoInstallComponent<CloseJournalComponent>(
                "journal", std::move(journal_opts));
        !r) {
        std::fprintf(stderr, "fixture: CoInstallComponent failed: %s\n",
                     r.error().message.c_str());
        return 4;
    }

    auto started = bbt_co_await app->CoStart();
    if (!started) {
        std::fprintf(stderr, "fixture: start failed: %s\n",
                     started.error().message.c_str());
        return CoRc(started);
    }

    const std::string bound = app->BoundEndpoint();
    if (bound != endpoint) {
        std::fprintf(stderr,
                     "fixture: bind mismatch: picked=%s bound='%s'\n",
                     endpoint.c_str(), bound.c_str());
        lifecycle::Gate().Open();
        auto closed = bbt_co_await app->CoStop();
        (void)closed;
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

    auto app_ptr = app.get();
    bbtco[app_ptr]() {
        ObserveShutdownStates(*app_ptr);
    };

    int rc = 0;
    // 一次收口尝试：预算耗尽时如实记 StopIncomplete 与未完成项，进程继续
    // 存活，之后可再次 CoStop 推进（幂等）。
    auto stop_attempt = [&]() -> bool {
        auto stopped = bbt_co_await app->CoStop();
        const int code = CoRc(stopped);
        if (code != 0 && rc == 0)
            rc = code;
        if (code != 0) {
            Journal(std::string("event=shutdown_state state=") +
                    std::to_string(static_cast<int>(app->State())) +
                    " pending=" +
                    std::to_string(app->PendingCleanup().size()) +
                    " auth_state=" + kAuthState);
        }
        return code == 0;
    };

    std::string buffer;
    for (;;) {
        std::string line;
        const auto event = PollCommandLine(buffer, &line);
        if (event == StdinEvent::kIdle) {
            bbtco_sleep(10);
            continue;
        }
        if (event == StdinEvent::kEof)
            break;
        if (line.empty())
            continue;
        if (line == "quit")
            break;
        if (line == "shutdown") {
            Journal(std::string("event=shutdown_requested auth_state=") +
                    kAuthState);
            app->RequestShutdown();
            (void)stop_attempt();
        } else if (line == "open") {
            Journal(std::string("event=gate_opened auth_state=") + kAuthState);
            lifecycle::Gate().Open();
        }
        // 未知命令忽略（不改变状态）。
    }

    // 仍在 StopIncomplete（首次超预算）时再推进一次，以观察真实终态；
    // 已 Stopped 的重复调用幂等。
    if (app->State() != fw::AppState::Stopped)
        (void)stop_attempt();

    g_stop_observer.store(true, std::memory_order_release);
    bbtco_sleep(30); // 让状态观察协程写出最后一次迁移后退出

    const int state = static_cast<int>(app->State());
    Journal(std::string("event=shutdown rc=") + std::to_string(rc) +
            " state=" + std::to_string(state) + " owner_closed=" +
            (CloseJournalComponent::stopped.load() ? "1" : "0") +
            " pending=" +
            std::to_string(app->PendingCleanup().size()) + " failures=" +
            std::to_string(app->LifecycleFailures().size()) +
            " auth_state=" + kAuthState);
    std::fprintf(stderr,
                 "lifecycle fixture rc=%d state=%d pending=%zu\n", rc,
                 state, app->PendingCleanup().size());
    return rc;
}

// 入口 native host hook：共享 Engine 的 Engine::Run 在主线程执行一次。
int bbtco_main_host_hook(bbt::coroutine::HostHookContext& ctx) {
    return bbt::framework::HostGate::RunHostGate(ctx);
}

BBTCO_MAIN_WITH_SETUP_AND_HOST_HOOK();
