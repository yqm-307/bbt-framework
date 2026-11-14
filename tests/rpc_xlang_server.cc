// V2 AppSession wire profile 的跨语言互通 C++ 服务端。
//
// 消费 bbt::infra_rpc_net（唯一 V2 公共面）与 bbt::framework 的真实宿主
// InfraHttpHost：同一 net::Engine 上跑 CoRpcClient owner 与 CoRpcServer，
// 入站经 host.InstallRpcHandler 的业务入口接纳，回复经 ReplyCompletion。
// 非 C++ 客户端（tests/rpc_xlang_client.py，共用 tests/rpc_v2_wire.py）
// 走真实 TCP loopback + SessionFrame（u32be 分帧 + Hello/HelloAck）。
//
// 入口是 runtime 的生成式 `bbtco_main`：Start/关闭都是宿主 Co 面（`Start` /
// `WaitHandlersDone` / `Close` / `WaitPhysicalReleased`），进程不写 native main、
// 不启动/停止 scheduler、不建 runner 线程；自有 Engine 的唯一一次
// `Engine::Run` 由 `bbtco_main_host_hook` 经 `HostGate::RunHostGate` 在主线程
// 执行（Idle 装配完成后经 `HostGate::Publish` 交给入口 hook）。参数经环境变量传入
// （生成式入口不暴露 argv）：
//   BBT_EXAMPLE_MODE=server  BBT_EXAMPLE_PORT_FILE=<path>
// 启动后把实际监听端口写入 port_file，stdin EOF 后按序关闭。
//
// 场景由请求的 service/method 决定：
//   svc.echo/Echo       —— 回显 payload/request_id/metadata（正常路径）
//   svc.echo/Fail       —— 业务错误（InvalidArgument + framework.test 域）
//   svc.echo/FailDomain —— 带 domain/domain_code/backend_* 的错误
// 其他 method 返回 RemoteError（未知路由）。
//
// 协议层拒绝（profile_version 不兼容、字段矩阵违规、帧前缀为 0）由 infra
// 的 SessionFrameDecoder/SessionCodec 在入站解码处拒绝，本服务不吞掉：
// 字段级违规由 infra 发一次 SESSION_ERROR，帧级违规退役该 link。

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <memory>
#include <string>

#include <poll.h>
#include <unistd.h>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>

#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/rpc/Request.hpp>
#include <bbt/infra/rpc/Response.hpp>
#include <bbt/framework/HostGate.hpp>
#include <bbt/framework/internal/InfraHttpHost.hpp>

using namespace bbt::infra;

namespace {

constexpr std::size_t kTestMaxBody = 64 * 1024;
// 服务端会话身份；客户端（tests/rpc_xlang_client.py）使用互异身份。
constexpr const char* kServerIdentity = "cpp-xlang-server";

result<rpc::Response> XlangRpcHandler(const rpc::Request& request,
                                      const std::function<void()>& on_dispatched) {
    // 同步业务：先释放同步派发门，再做业务/回复构造。
    on_dispatched();

    const auto& service = request.Service();
    const auto& method = request.Method();

    if (service == "svc.echo" && method == "Echo") {
        auto reply = rpc::Response::FromRequest(request);
        reply.SetPayload(request.Payload());
        reply.SetMetadata(request.Metadata());
        return result<rpc::Response>::ok(std::move(reply));
    }
    if (service == "svc.echo" && method == "Fail") {
        auto error = MakeError(ErrorCode::InvalidArgument,
                               "business rejected by svc.echo/Fail");
        error.domain = "framework.test";
        error.details = {{"reason", "explicit-fail"},
                         {"req", request.RequestId().substr(0, 32)}};
        return result<rpc::Response>::err(std::move(error));
    }
    if (service == "svc.echo" && method == "FailDomain") {
        auto error = MakeError(ErrorCode::Unavailable, "backend says no");
        error.domain = "framework.test.backend";
        error.domain_code = "E_BACKEND_DOWN";
        error.backend_category = "tcp";
        error.backend_code = 111; // ECONNREFUSED
        error.details = {{"backend", "127.0.0.1:9999"}};
        return result<rpc::Response>::err(std::move(error));
    }
    return result<rpc::Response>::err(MakeError(
        ErrorCode::RemoteError,
        "unknown route " + service + "/" + method));
}

std::string Env(const char* name) {
    const char* value = std::getenv(name);
    return value == nullptr ? std::string{} : std::string{value};
}

// 在当前协程内非阻塞观察 stdin EOF（驱动器关管道即关闭触发）。
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

bbt::coroutine::WaitOptions Soon() {
    bbt::coroutine::WaitOptions wait;
    wait.deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    return wait;
}

} // namespace

void setup_runtime() {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
}

int bbtco_main() {
    if (Env("BBT_EXAMPLE_MODE") != "server")
        return 90;
    const std::string port_file = Env("BBT_EXAMPLE_PORT_FILE");
    if (port_file.empty())
        return 90;

    NetworkLimits limits{};
    limits.max_connections = 16;
    limits.max_inflight = 16;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes = kTestMaxBody;
    limits.incoming_timeout = std::chrono::milliseconds{5000};

    auto host = std::make_shared<bbt::framework::InfraHttpHost>(
        limits, ListenAddress{"127.0.0.1", 0}, kServerIdentity);
    host->InstallRpcHandler(XlangRpcHandler);
    if (auto created = host->Create(); !created)
        return 3;

    // Idle 装配完成：把自有 Engine + 其 Run 输入发布给入口 host hook；
    // hook 在主线程执行一次 Run，本协程经 CoWaitRunning 等 Ready。
    bbt::framework::HostGate::Publication publication;
    publication.engine = host->egress_engine();
    publication.config = host->engine_config();
    bbt::framework::HostGate::Publish(std::move(publication));

    auto started = bbt_co_await host->Start(Soon());
    if (!started) {
        auto closed = bbt_co_await host->Close(CloseOptions::Graceful(
            std::chrono::steady_clock::now() + std::chrono::seconds{5}));
        (void)closed;
        return 3;
    }

    {
        std::ofstream f(port_file, std::ios::trunc);
        f << host->bound_address()->port;
    }

    WaitStdinEof();

    // 收口固定顺序：拒新 → 等 handler 排空 → 逻辑关闭 → 观察物理释放。
    host->StopAccepting();
    auto drained = bbt_co_await host->WaitHandlersDone(Soon());
    if (!drained) {
        std::fprintf(stderr, "xlang server: handlers did not drain: %s\n",
                     drained.error().message.c_str());
    }
    auto closed = bbt_co_await host->Close(
        CloseOptions::Graceful(std::chrono::steady_clock::now() +
                               std::chrono::seconds{5}));
    if (!closed) {
        std::fprintf(stderr, "xlang server: close failed: %s\n",
                     closed.error().message.c_str());
        return 4;
    }
    auto released = bbt_co_await host->WaitPhysicalReleased(Soon());
    if (!released) {
        std::fprintf(stderr, "xlang server: physical release failed: %s\n",
                     released.error().message.c_str());
        return 4;
    }
    // 自有 Engine 的 Run 由入口 host hook 在主任务结束后 RequestClose 收口。
    return 0;
}

// 入口 native host hook：自有 Engine 的 Engine::Run 在主线程执行一次。
int bbtco_main_host_hook(bbt::coroutine::HostHookContext& ctx) {
    return bbt::framework::HostGate::RunHostGate(ctx);
}

BBTCO_MAIN_WITH_SETUP_AND_HOST_HOOK();
