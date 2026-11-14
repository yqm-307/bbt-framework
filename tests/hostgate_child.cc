// 真实入口验收子进程（Test_framework_inbound_http T17 的 helper）。
//
// 走 runtime 生成式入口：`bbtco_main` 在主协程里装配 v2::CoApp
// （AppNetworkOptions → 内部 InstallRpcAdmission + 共享 Engine）→ CoStart →
// 写 port 文件 → 等 stdin EOF → CoStop；`bbtco_main_host_hook` 经
// `HostGate::RunHostGate` 对同一共享 Engine 恰执行一次 `Engine::Run`。
// 父进程断言 port 文件出现（真实监听就绪）与退出码 0——Run 恰好一次由
// HostGate 的单槽位/OnMainTaskDone 收口保证。
//
// 参数经环境变量传入：BBT_EXAMPLE_MODE=hostgate BBT_EXAMPLE_PORT_FILE=<path>。

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <string>

#include <poll.h>
#include <unistd.h>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/infra/NetworkTypes.hpp>

#include <bbt/framework/HostGate.hpp>
#include <bbt/framework/V2.hpp>

namespace fw = bbt::framework;
namespace v2 = bbt::framework::v2;
namespace inf = bbt::infra;
using Clock = std::chrono::steady_clock;

namespace {

std::string Env(const char* name) {
    const char* value = std::getenv(name);
    return value == nullptr ? std::string{} : std::string{value};
}

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

} // namespace

void setup_runtime() {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 3;
    g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
}

int bbtco_main() {
    if (Env("BBT_EXAMPLE_MODE") != "hostgate")
        return 90;
    const std::string port_file = Env("BBT_EXAMPLE_PORT_FILE");
    if (port_file.empty())
        return 90;

    fw::AppNetworkOptions network;
    network.SetLimits(inf::NetworkLimits{
                          16, 16, 16 * 1024, 64 * 1024,
                          std::chrono::milliseconds{30000}})
        .SetListenAddress(inf::ListenAddress{"127.0.0.1", 0})
        .SetLocalAppId("hostgate-child");

    auto app = v2::CoApp::Create(
        fw::AppOptions{}
            .SetLimits(32, 32, 8, 32)
            .SetShutdownBudget(std::chrono::milliseconds{3000})
            .SetLifecycleBudget(std::chrono::milliseconds{5000})
            .SetNetworkOptions(std::move(network)),
        "hostgate-child");

    auto started = bbt_co_await app->CoStart();
    if (!started) {
        std::fprintf(stderr, "hostgate child: CoStart failed: %s\n",
                     started.error().message.c_str());
        return 3;
    }
    const std::string endpoint = app->BoundEndpoint();
    if (endpoint.empty()) {
        std::fprintf(stderr, "hostgate child: did not bind\n");
        auto closed = bbt_co_await app->CoStop();
        (void)closed;
        return 5;
    }
    {
        std::ofstream f(port_file, std::ios::trunc);
        f << endpoint.substr(endpoint.rfind(':') + 1);
    }

    WaitStdinEof();
    app->RequestShutdown();
    auto stopped = bbt_co_await app->CoStop();
    if (!stopped) {
        std::fprintf(stderr, "hostgate child: CoStop failed: %s\n",
                     stopped.error().message.c_str());
        return 4;
    }
    return 0;
}

// 入口 native host hook：共享 Engine 的 Engine::Run 在主线程执行一次。
int bbtco_main_host_hook(bbt::coroutine::HostHookContext& ctx) {
    return bbt::framework::HostGate::RunHostGate(ctx);
}

BBTCO_MAIN_WITH_SETUP_AND_HOST_HOOK();
