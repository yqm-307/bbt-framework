// V2 AppSession wire profile 的跨语言互通 C++ 服务端。
//
// 消费 bbt::infra_rpc_net（唯一 V2 公共面）与 bbt::framework 的真实宿主
// InfraHttpHost：同一 net::Engine 上跑 CoRpcClient owner 与 CoRpcServer，
// 入站经 host.InstallRpcHandler 的业务入口接纳，回复经 ReplyCompletion。
// 非 C++ 客户端（tests/rpc_xlang_client.py，共用 tests/rpc_v2_wire.py）
// 走真实 TCP loopback + SessionFrame（u32be 分帧 + Hello/HelloAck）。
//
// argv 分流：
//   <bin> server <port_file>
// 启动后把实际监听端口写入 port_file，阻塞等待 stdin EOF 后按序关闭。
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

#include <chrono>
#include <cstdio>
#include <functional>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <string>
#include <thread>

#include <unistd.h>

#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>

#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/rpc/Request.hpp>
#include <bbt/infra/rpc/Response.hpp>
#include <bbt/framework/internal/InfraHttpHost.hpp>

using namespace bbt::infra;

namespace {

constexpr std::size_t kTestMaxBody = 64 * 1024;
// 服务端会话身份；客户端（tests/rpc_xlang_client.py）使用互异身份。
constexpr const char* kServerIdentity = "cpp-xlang-server";

std::shared_ptr<bbt::framework::InfraHttpHost> g_host;

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

void RuntimeSetup() {
    auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    cfg->m_cfg_static_thread_num = 4;
    if (!g_scheduler->IsInitialized())
        g_scheduler->Start(bbt::coroutine::SCHE_START_OPT_SCHE_THREAD);

    NetworkLimits limits{};
    limits.max_connections = 16;
    limits.max_inflight = 16;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes = kTestMaxBody;
    limits.incoming_timeout = std::chrono::milliseconds{5000};

    g_host = std::make_shared<bbt::framework::InfraHttpHost>(
        limits, ListenAddress{"127.0.0.1", 0}, kServerIdentity);
    g_host->InstallRpcHandler(XlangRpcHandler);
    if (!g_host->Create() || !g_host->Start()) {
        g_host->Close();
        g_host.reset();
    }
}

void RuntimeTeardown() {
    if (g_host) {
        g_host->StopAccepting();
        if (!g_host->WaitHandlersDone(std::chrono::steady_clock::now() +
                                      std::chrono::seconds{5}))
            std::abort();
        g_host->Close();
        if (g_host->close_error())
            std::abort();
        g_host.reset();
    }
}

} // namespace

int main(int argc, char* argv[]) {
    if (argc < 3 || std::strcmp(argv[1], "server") != 0)
        return 90;
    RuntimeSetup();
    if (!g_host)
        return 3;
    {
        std::ofstream f(argv[2], std::ios::trunc);
        f << g_host->bound_address()->port;
    }
    char buf[8];
    while (::read(STDIN_FILENO, buf, sizeof(buf)) > 0) {}
    RuntimeTeardown();
    return 0;
}
