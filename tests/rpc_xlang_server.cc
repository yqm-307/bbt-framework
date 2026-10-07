// framework #4：G5 RpcEnvelope wire profile 的跨语言互通 C++ 服务端。
//
// 消费 bbt::infra_rpc（Issue #8 的正式 wire profile）+ bbt::infra_http，
// 提供多场景 RPC 端点供非 C++ 客户端（tests/rpc_xlang_client.py）经
// 真实 HTTP loopback 调用。argv 分流：
//   <bin> server <port_file>
// 启动后把实际监听端口写入 port_file，阻塞等待 stdin EOF 后按序关闭。
//
// 场景由请求的 service/method 决定：
//   svc.echo/Echo    —— 回显 payload/request_id/metadata（正常路径）
//   svc.echo/Fail    —— 返回业务错误 envelope（InvalidArgument + details）
//   svc.echo/FailDomain —— 返回带 domain/domain_code/backend_* 的错误
// 其他 method 返回 RemoteError（未知路由），用于验证错误兼容路径。
// envelope 层错误（profile_version 不支持、metadata 违规、空标识符等）
// 由 ParseRpcWireHttpRequest/DecodeRpcWireEnvelope 在 adapter 内拒绝，
// 本服务把该 Error 原样装入 error envelope 返回，不让协议错误丢失。

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <string>
#include <thread>

#include <unistd.h>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/LocalThread.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/detail/Hook.hpp>
#include <bbt/coroutine/io/IoExecutor.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>

#include <bbt/infra/HttpServer.hpp>
#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/rpc/RpcWire.hpp>

using namespace bbt::infra;
using namespace bbt::infra::rpc;

namespace {

constexpr std::size_t kTestMaxBody = 64 * 1024;

std::shared_ptr<NetworkRuntime> g_runtime;
std::shared_ptr<HttpServer>     g_server;

// 把 wire 层错误包成 error envelope 返回（HTTP 200 + protobuf body）。
// request_id 不可知时保留空字符串以外的尽力回填：协议要求 request_id
// 非空，无法解析入站 envelope 时用占位值保持 out envelope 自身可编码。
// remaining_budget_ms=0 在 G5 冻结契约中是非法值（编码端显式拒绝），
// 错误回填路径取 1 保证 envelope 可编码；正常路径回显入站预算。
result<HttpResponse> ErrorEnvelopeResponse(const Error& e,
                                           const std::string& request_id) {
    RpcWireEnvelope err_env;
    err_env.request_id          = request_id.empty() ? "unparsed" : request_id;
    err_env.service             = "unknown";
    err_env.method              = "unknown";
    err_env.remaining_budget_ms = 1;
    err_env.success             = false;
    err_env.error               = e;
    return MakeRpcWireHttpResponse(err_env);
}

result<HttpResponse> XlangRpcHandler(IncomingCallContext ctx, HttpRequest req) {
    (void)ctx;
    auto parsed = ParseRpcWireHttpRequest(req);
    if (!parsed)
        return ErrorEnvelopeResponse(parsed.error(), "");
    const auto& in = parsed.value();

    RpcWireEnvelope out;
    out.profile_version     = kRpcWireProfileVersion;
    out.request_id          = in.request_id;
    out.service             = in.service;
    out.method              = in.method;
    // 响应方向回显入站预算；G5 契约拒绝 0（发送端至少 1ms），入站已经过
    // decode 复检保证 ≥1，直接透传即可保持响应自身可编码。
    out.remaining_budget_ms = in.remaining_budget_ms;
    out.request_schema      = in.request_schema;
    out.response_schema     = in.response_schema;
    out.metadata            = in.metadata;

    if (in.service == "svc.echo" && in.method == "Echo") {
        out.payload = in.payload;
        out.success = true;
    } else if (in.service == "svc.echo" && in.method == "Fail") {
        out.success      = false;
        out.error        = MakeError(ErrorCode::InvalidArgument,
                                     "business rejected by svc.echo/Fail");
        out.error.domain = "framework.test";
        out.error.details = {{"reason", "explicit-fail"},
                             {"req", in.request_id.substr(0, 32)}};
    } else if (in.service == "svc.echo" && in.method == "FailDomain") {
        out.success              = false;
        out.error                = MakeError(ErrorCode::Unavailable,
                                             "backend says no");
        out.error.domain         = "framework.test.backend";
        out.error.domain_code    = "E_BACKEND_DOWN";
        out.error.backend_category = "tcp";
        out.error.backend_code   = 111; // ECONNREFUSED
        out.error.details        = {{"backend", "127.0.0.1:9999"}};
    } else {
        out.success = false;
        out.error   = MakeError(ErrorCode::RemoteError,
                                "unknown route " + in.service + "/" + in.method);
    }

    auto http = MakeRpcWireHttpResponse(out);
    if (!http)
        return result<HttpResponse>::err(http.error());
    return result<HttpResponse>::ok(std::move(http.value()));
}

void RuntimeSetup() {
    auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    cfg->m_cfg_static_thread_num = 2;
    if (!g_scheduler->IsInitialized())
        g_scheduler->Start(bbt::coroutine::SCHE_START_OPT_SCHE_THREAD);

    NetworkLimits limits{};
    limits.max_connections  = 16;
    limits.max_inflight     = 16;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes   = kTestMaxBody;
    limits.incoming_timeout = std::chrono::milliseconds{5000};

    auto rt = NetworkRuntime::Create(limits);
    if (rt) {
        g_runtime = std::move(rt).value();
        g_runtime->Start();
    }
}

void RuntimeTeardown() {
    // infra 关闭是同步契约：Close() 返回即物理释放；runtime 是进程寿命
    // 单例，无 Stop/restart。
    if (g_server)  { g_server->Close(); g_server.reset(); }
    if (g_runtime) { g_runtime->Close(); g_runtime.reset(); }
}

} // namespace

int main(int argc, char* argv[]) {
    if (argc < 3 || std::strcmp(argv[1], "server") != 0)
        return 90;
    RuntimeSetup();
    if (!g_runtime) return 3;
    auto srv = g_runtime->ListenHttp({"127.0.0.1", 0}, XlangRpcHandler);
    if (!srv) { RuntimeTeardown(); return 5; }
    g_server = std::move(srv).value();
    {
        std::ofstream f(argv[2], std::ios::trunc);
        f << g_server->LocalAddress().port;
    }
    char buf[8];
    while (::read(STDIN_FILENO, buf, sizeof(buf)) > 0) {}
    RuntimeTeardown();
    return 0;
}
