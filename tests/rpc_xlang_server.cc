// RpcEnvelope wire profile 的跨语言互通 C++ 服务端。
//
// 消费 bbt::infra_rpc_net（正式 wire profile，新 bbt::infra::http
// 栈绑定）与 bbt::infra_http_net，
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
//
// 入站直接使用新 HTTP 值类型与宿主的有序关闭序列。

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

#include <bbt/infra/http/Server.hpp>
#include <bbt/framework/internal/InfraHttpHost.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/http/Server.hpp>
#include <bbt/infra/rpc/HttpTransport.hpp>
#include <bbt/infra/rpc/RpcWire.hpp>

using namespace bbt::infra;
using namespace bbt::infra::rpc;

namespace {

constexpr std::size_t kTestMaxBody = 64 * 1024;

std::shared_ptr<bbt::framework::InfraHttpHost> g_host;

// 把 wire 层错误包成 error envelope 返回（HTTP 200 + protobuf body）。
// request_id 不可知时保留空字符串以外的尽力回填：协议要求 request_id
// 非空，无法解析入站 envelope 时用占位值保持 out envelope 自身可编码。
// remaining_budget_ms=0 在 G5 冻结契约中是非法值（编码端显式拒绝），
// 错误回填路径取 1 保证 envelope 可编码；正常路径回显入站预算。
//
result<http::Response> ErrorEnvelopeResponse(const Error& e,
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

result<http::Response> XlangRpcHandler(IncomingCallContext ctx, http::IncomingRequest req) {
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
        return result<http::Response>::err(http.error());
    return http;
}

void RuntimeSetup() {
    auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    cfg->m_cfg_static_thread_num = 4;
    if (!g_scheduler->IsInitialized())
        g_scheduler->Start(bbt::coroutine::SCHE_START_OPT_SCHE_THREAD);

    NetworkLimits limits{};
    limits.max_connections  = 16;
    limits.max_inflight     = 16;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes   = kTestMaxBody;
    limits.incoming_timeout = std::chrono::milliseconds{5000};

    g_host = std::make_shared<bbt::framework::InfraHttpHost>(limits, ListenAddress{"127.0.0.1", 0});
    g_host->InstallHandler(XlangRpcHandler);
    if (!g_host->Create() || !g_host->Start()) {
        g_host->Close();
        g_host.reset();
    }
}

void RuntimeTeardown() {
    if (g_host) {
        g_host->StopAccepting();
        if (!g_host->WaitHandlersDone(std::chrono::steady_clock::now() + std::chrono::seconds{5}))
            std::abort();
        g_host->Close();
        if (g_host->close_error()) std::abort();
        g_host.reset();
    }
}

} // namespace

int main(int argc, char* argv[]) {
    if (argc < 3 || std::strcmp(argv[1], "server") != 0)
        return 90;
    RuntimeSetup();
    if (!g_host) return 3;
    {
        std::ofstream f(argv[2], std::ios::trunc);
        f << g_host->bound_address()->port;
    }
    char buf[8];
    while (::read(STDIN_FILENO, buf, sizeof(buf)) > 0) {}
    RuntimeTeardown();
    return 0;
}
