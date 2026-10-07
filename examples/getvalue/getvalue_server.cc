// bbt-framework Issue #4（P0-A）：C++ framework 服务端（正式 body wire bridge）。
//
// 装配路径与业务一致：公开 CoApp（inbound_bridge=ProtoWireV1）+ 公开 CoService，
// 不使用测试缝、不提供 Binder、不伪造结果。监听 loopback 动态端口，启动后写
// 端口/元数据文件，阻塞等待 stdin EOF 后按序优雅关闭。
//
// argv: getvalue_server server <port_file> <meta_file> <journal_file>
//
// 版本/来源：链接 bbtools-infra locked 162bb5fd… 的 bbt::infra_rpc（protobuf
// 3.21.12），bbt::framework 经 BBT_FRAMEWORK_HAS_RPC_WIRE 启用正式桥。

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>

#include <unistd.h>

#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>

#include <bbt/example/v1/get_value.pb.h>

#include <bbt/framework/CoApp.hpp>

#include "getvalue_service.hpp"

namespace {

std::mutex  g_journal_mtx;
std::string g_journal_path;

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

} // namespace

int main(int argc, char* argv[]) {
    if (argc < 5 || std::strcmp(argv[1], "server") != 0)
        return 90;
    const std::string port_file    = argv[2];
    const std::string meta_file    = argv[3];
    g_journal_path                 = argv[4];

    // 入站 handler 在传输层协程内运行：本示例在 handler 内做 protobuf 业务
    // payload 编解码（解析请求 + 编码响应），叠加框架分发与 wire 桥的栈占用，
    // 超过 coroutine 默认 12KB 栈。示例 server 经 coroutine **公开配置入口**
    // `g_bbt_coroutine_config`（= GlobalConfig 访问宏；头
    // bbt/coroutine/detail/GlobalConfig.hpp，见 coroutine agent-docs
    // api-reference.md §12「配置」）显式把协程栈调到 512KB。
    //   - 该配置非线程安全，必须在调度器 Start 前（即 app.run() 前）设置；
    //   - 是 process-global：影响本进程全部协程，不是 per-service/per-request；
    //   - 这是运行期资源配置，不是业务/协议语义。
    // 见 README「已知限制」：512KB 只覆盖本示例负载规模；框架默认 12KB 栈对
    // 任意 protobuf 业务不保证安全（栈上编解码开销随 payload/嵌套增长）。
    g_bbt_coroutine_config->m_cfg_stack_size = 512 * 1024;

    getvalue::SetServedHook(&AppendHandlerEntry);

    bbt::framework::CoAppOptions options;
    options.network_limits = bbt::infra::NetworkLimits{
        64, 64, 16 * 1024, 64 * 1024, std::chrono::milliseconds{30000}};
    options.listen = bbt::infra::ListenAddress{"127.0.0.1", 0};
    options.shutdown_step_budget = std::chrono::milliseconds{3000};
    options.inbound_bridge = bbt::framework::RpcInboundBridge::ProtoWireV1;

    bbt::framework::CoApp app(options);
    if (auto r = app.add_service<getvalue::GetValueService>(
            bbt::framework::ServiceOptions{
                bbt::framework::ExecutionPolicy::Concurrent,
                64, 0, 0, false, 0, 0, 0});
        !r) {
        std::fprintf(stderr, "add_service failed: %s\n", r.error().message.c_str());
        return 4;
    }

    int rc = -1;
    std::thread runner([&] { rc = app.run(); });

    // 轮询绑定地址（动态端口）。
    std::string endpoint;
    for (int i = 0; i < 400; ++i) {
        endpoint = app.bound_endpoint();
        if (!endpoint.empty())
            break;
        std::this_thread::sleep_for(std::chrono::milliseconds{25});
    }
    if (endpoint.empty()) {
        app.request_shutdown();
        if (runner.joinable()) runner.join();
        std::fprintf(stderr, "server did not bind (rc=%d)\n", rc);
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
        f << "inbound_bridge=proto_wire_v1\n";
        f << "auth_state=unauthenticated_loopback\n";
    }
    std::fprintf(stderr, "getvalue server up endpoint=%s\n", endpoint.c_str());

    char buf[8];
    while (::read(STDIN_FILENO, buf, sizeof(buf)) > 0) {
    }
    app.request_shutdown();
    if (runner.joinable())
        runner.join();
    std::fprintf(stderr, "getvalue server rc=%d\n", rc);
    return rc;
}
