// bbt-framework C++ framework 服务端（V2 会话入站）。
//
// 装配路径与业务一致：公开 v2::CoApp（local_identity 显式、V2 入站经
// InstallRpcAdmission）+ 公开 v2::CoService，不使用测试缝、不提供 Binder、
// 不伪造结果。主内容全部在业务主协程里：装配 → CoStart → 写
// 端口/元数据 → 等关闭触发 → CoStop。入口是 runtime 的生成式
// `bbtco_main`：进程不写 native main、不启动/停止 scheduler、不建 runner
// 线程，也不等待 future/join。共享 Engine 的唯一一次 `Engine::Run` 由
// `bbtco_main_host_hook` 经 `HostGate::RunHostGate` 在主线程执行。
//
// 参数经环境变量传入（生成式入口不暴露 argv）：
//   BBT_EXAMPLE_MODE=server
//   BBT_EXAMPLE_PORT_FILE / BBT_EXAMPLE_META_FILE / BBT_EXAMPLE_JOURNAL_FILE
//
// 关闭触发：stdin EOF（驱动器关闭管道）→ RequestShutdown() → CoStop。
//
// 版本/来源：链接 deps.lock 固定的 bbtools-infra 之 bbt::infra_rpc_net
// （V2 SessionFrame profile 2 + protobuf 3.21.12）；bbt::framework 以 V2 为
// 必需依赖，不再有条件编译开关。

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <mutex>
#include <string>

#include <poll.h>
#include <unistd.h>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/example/v1/get_value.pb.h>

#include <bbt/framework/HostGate.hpp>
#include <bbt/framework/V2.hpp>

#include "getvalue_service.hpp"

namespace {

std::mutex  g_journal_mtx;
std::string g_journal_path;

std::string Env(const char* name) {
    const char* value = std::getenv(name);
    return value == nullptr ? std::string{} : std::string{value};
}

// handler 被真正调用时追加一行（只记已进业务处理的请求；协议错误/预算过期在
// 到达 handler 之前被拒，因此不出现）。auth_state 是显式的未认证 loopback 标记，
// 不表示身份已认证。
void AppendHandlerEntry(const std::string& request_id,
                        const std::string& peer_principal) {
    std::lock_guard<std::mutex> lk(g_journal_mtx);
    std::ofstream out(g_journal_path, std::ios::app);
    out << "req=" << request_id
        << " event=handler_entered"
        << " peer_principal=" << peer_principal
        << " auth_state=unauthenticated_loopback\n";
}

// 在当前协程内非阻塞地观察 stdin：读到 EOF 即返回。
// 不用阻塞 read + 额外线程：示例的生命周期只有业务主协程一条驱动线，阻塞读
// 会占住 worker 并逼出 native 线程与 join。poll 的 0 超时只是「现在有数据吗」，
// 两次检查之间用 bbtco_sleep 把 worker 让给入站 handler。
void WaitStdinEof() {
    for (;;) {
        pollfd pfd{STDIN_FILENO, POLLIN, 0};
        const int ready = ::poll(&pfd, 1, 0);
        if (ready > 0) {
            char buf[64];
            const ssize_t n = ::read(STDIN_FILENO, buf, sizeof(buf));
            if (n <= 0)
                return; // EOF 或读错误都按「不再有输入」收口
            continue;
        }
        if (ready < 0 && errno != EINTR)
            return;
        bbtco_sleep(20);
    }
}

// 生命周期结果 → 进程退出码，沿用既有验收口径：
//   0 按期优雅关闭 / 1 配置校验拒绝（未启动任何组件）/ 2 启动中途失败回退 /
//   3 关闭曾超预算（ShutdownIncomplete）/ 4 关闭收尾存在失败项。
int CoRc(const bbt::framework::result<void>& r) {
    if (r)
        return 0;
    switch (r.error().code) {
    case bbt::framework::ErrorCode::InvalidArgument:
        return 1;
    case bbt::framework::ErrorCode::TimedOut:
        return 3;
    default:
        return 4;
    }
}

} // namespace

// 入站 handler 在传输层协程内运行：本示例在 handler 内做 protobuf 业务
// payload 编解码（解析请求 + 编码响应），叠加框架分发与 wire 桥的栈占用，
// 超过 coroutine 默认 12KB 栈。经 coroutine **公开配置入口**
// `g_bbt_coroutine_config`（= GlobalConfig 访问宏；见 coroutine agent-docs
// 「配置」）在 runtime Start 前把协程栈调到 512KB。
//   - 该配置非线程安全，只能在 setup_runtime 阶段（Start 之前）写；
//   - 是 process-global：影响本进程全部协程，不是 per-service/per-request；
//   - 这是运行期资源配置，不是业务/协议语义。
// 512KB 只覆盖本示例负载规模；框架默认 12KB 栈对任意 protobuf 业务不保证安全
// （栈上编解码开销随 payload/嵌套增长）。
void setup_runtime() {
    // Two server drivers + one client-only driver + one application worker.
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 512 * 1024;
}

int bbtco_main() {
    if (Env("BBT_EXAMPLE_MODE") != "server")
        return 90;
    const std::string port_file = Env("BBT_EXAMPLE_PORT_FILE");
    const std::string meta_file = Env("BBT_EXAMPLE_META_FILE");
    g_journal_path = Env("BBT_EXAMPLE_JOURNAL_FILE");
    if (port_file.empty() || meta_file.empty() || g_journal_path.empty())
        return 90;

    getvalue::SetServedHook(&AppendHandlerEntry);

    bbt::framework::AppNetworkOptions network;
    network.SetLimits(bbt::infra::NetworkLimits{
                          64, 64, 16 * 1024, 64 * 1024,
                          std::chrono::milliseconds{30000}})
        .SetListenAddress(bbt::infra::ListenAddress{"127.0.0.1", 0})
        // V2 会话身份必填；进程内每个会话 owner 使用明确身份。
        .SetLocalAppId("getvalue-server");

    bbt::framework::AppOptions options;
    options.SetLimits(64, 64, 8, 64)
        .SetShutdownBudget(std::chrono::milliseconds{3000})
        .SetLifecycleBudget(std::chrono::milliseconds{3000})
        .SetNetworkOptions(std::move(network));

    auto app = bbt::framework::v2::CoApp::Create(std::move(options),
                                               "getvalue-server");
    if (auto r = bbt_co_await
            app->CoInstallService<getvalue::GetValueService>(
                bbt::framework::ServiceOptions{}.SetMaxInflight(64));
        !r) {
        std::fprintf(stderr, "CoInstallService failed: %s\n",
                     r.error().message.c_str());
        return 4;
    }

    // CoStart 完成资源/业务绑定与网络 Start 并等到 Ready：返回后
    // BoundEndpoint() 已是真实监听地址，不需要轮询。
    auto started = bbt_co_await app->CoStart();
    if (!started) {
        std::fprintf(stderr, "server start failed: %s\n",
                     started.error().message.c_str());
        return CoRc(started);
    }

    const std::string endpoint = app->BoundEndpoint();
    if (endpoint.empty()) {
        std::fprintf(stderr, "server did not bind\n");
        auto closed = bbt_co_await app->CoStop();
        (void)closed;
        return 5;
    }
    const auto colon = endpoint.rfind(':');
    const std::string port = endpoint.substr(colon + 1);

    {
        std::ofstream f(port_file, std::ios::trunc);
        f << port;
    }
    {
        // schema 从生成物 descriptor 推导：.proto 是唯一真源，此处不手写 schema。
        std::ofstream f(meta_file, std::ios::trunc);
        f << "service=" << getvalue::GetValueService::kServiceName << "\n";
        f << "method=GetValue\n";
        f << "request_schema="
          << bbt::example::v1::GetValueRequest::descriptor()->full_name() << "\n";
        f << "response_schema="
          << bbt::example::v1::GetValueResponse::descriptor()->full_name() << "\n";
        f << "local_identity=getvalue-server\n";
        f << "auth_state=unauthenticated_loopback\n";
    }
    std::fprintf(stderr, "getvalue server up endpoint=%s\n", endpoint.c_str());

    // 关闭触发只有一个：驱动器关 stdin。请求槽是持久位，先置位再等待不丢通知。
    WaitStdinEof();
    app->RequestShutdown();
    auto requested = bbt_co_await app->CoWaitShutdownRequest();
    if (!requested) {
        std::fprintf(stderr, "server wait shutdown request failed: %s\n",
                     requested.error().message.c_str());
    }

    auto stopped = bbt_co_await app->CoStop();
    const int rc = CoRc(stopped);
    if (rc != 0)
        std::fprintf(stderr, "server stop: %s\n",
                     stopped.error().message.c_str());
    std::fprintf(stderr, "getvalue server rc=%d\n", rc);
    return rc;
}

// 入口 native host hook：共享 Engine 的 Engine::Run 在主线程执行一次。
int bbtco_main_host_hook(bbt::coroutine::HostHookContext& ctx) {
    return bbt::framework::HostGate::RunHostGate(ctx);
}

BBTCO_MAIN_WITH_SETUP_AND_HOST_HOOK();
