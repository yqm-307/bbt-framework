/**
 * bbt-framework C++ framework 服务端（V2 会话入站）。
 *
 * 装配路径与业务一致：公开 CoApp（local_identity 显式、V2 入站经
 * InstallRpcAdmission）+ 公开 CoService，不使用测试缝、不提供 Binder、
 * 不伪造结果。监听 loopback 动态端口，启动后写
 * 端口/元数据文件，阻塞等待 stdin EOF 后按序优雅关闭。
 *
 * argv: getvalue_server server <port_file> <meta_file> <journal_file>
 *
 * 版本/来源：链接 deps.lock 固定的 bbtools-infra 之 bbt::infra_rpc_net
 * （V2 SessionFrame profile 2 + protobuf 3.21.12）；bbt::framework 以 V2 为
 * 必需依赖，不再有条件编译开关。
 */

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <future>
#include <mutex>
#include <string>
#include <thread>

#include <unistd.h>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>

#include <bbt/example/v1/get_value.pb.h>

#include <bbt/framework/CoApp.hpp>

#include "getvalue_service.hpp"

namespace {

std::mutex g_journal_mtx;
std::string g_journal_path;

/**
 * handler 被真正调用时追加一行（只记已进业务处理的请求；协议错误/预算过期在
 * 到达 handler 之前被拒，因此不出现）。auth_state 是显式的未认证 loopback 标记，
 * 不表示身份已认证。
 */
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
    const std::string port_file = argv[2];
    const std::string meta_file = argv[3];
    g_journal_path = argv[4];

    /**
     * 入站 handler 在传输层协程内运行：本示例在 handler 内做 protobuf 业务
     * payload 编解码（解析请求 + 编码响应），叠加框架分发与 wire 桥的栈占用，
     * 超过 coroutine 默认 12KB 栈。示例 server 经 coroutine **公开配置入口**
     * `g_bbt_coroutine_config`（= GlobalConfig 访问宏；头
     * bbt/coroutine/detail/GlobalConfig.hpp，见 coroutine agent-docs「配置」）显式把协程栈调到 512KB。
     * - 该配置非线程安全，必须在调度器 Start 前设置；
     * - 是 process-global：影响本进程全部协程，不是 per-service/per-request；
     * - 这是运行期资源配置，不是业务/协议语义。
     * 见 README「已知限制」：512KB 只覆盖本示例负载规模；框架默认 12KB 栈对
     * 任意 protobuf 业务不保证安全（栈上编解码开销随 payload/嵌套增长）。
     * Two server drivers + one client-only driver + one application worker.
     */
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 512 * 1024;

    getvalue::SetServedHook(&AppendHandlerEntry);

    /**
     * 普通 main + 协程入口：先启动 runtime，再把宿主流程投递进受管协程
     * （装配/启动/等待关闭/收口全部在 Co 域；RequestShutdown 由 native
     * stdin-EOF 路径发出，仅触发请求，不执行收口）。
     */
    g_scheduler->Start();

    bbt::framework::AppLimits limits;
    limits.max_objects = 64;
    limits.max_bindings = 64;
    limits.max_observers = 64;
    limits.observer_queue_capacity = 64;
    auto options = bbt::framework::AppOptions{}
                       .SetAppLimits(limits)
                       .SetNetworkLimits(bbt::infra::NetworkLimits{
                           64, 64, 16 * 1024, 64 * 1024, std::chrono::milliseconds{30000}})
                       .Listen("127.0.0.1:0")
                       .SetShutdownBudget(std::chrono::milliseconds{3000})
                       .SetLifecycleBudget(std::chrono::milliseconds{3000})
                       /**
         * V2 会话身份必填；进程内每个会话 owner 使用明确身份。
         */
                       .SetLocalIdentity("getvalue-server");

    /**
     * 装配结果跨协程/线程交接：endpoint 经 promise 交给宿主线程写端口
     * 文件；App 指针发布后宿主线程只做 RequestShutdown（幂等 noexcept）；
     * 退出码经 promise 回传（0=按期优雅收口，2=启动失败，4=收口未竟）。
     */
    std::promise<std::string> bound_ep;
    std::promise<int> rc_slot;
    std::shared_ptr<bbt::framework::CoApp> app_handle;
    std::mutex app_mtx;

    bool submitted = false;
    g_scheduler->RegistCoroutineTask(
        [options = std::move(options), &bound_ep, &rc_slot,
         &app_handle, &app_mtx]() mutable {
            auto app = bbt::framework::CoApp::Create(options,
                                                     "getvalue-server");
            if (!app) {
                std::fprintf(stderr, "server: CoApp::Create failed\n");
                rc_slot.set_value(4);
                return;
            }
            {
                std::lock_guard<std::mutex> lk(app_mtx);
                app_handle = app;
            }
            auto install = bbt_co_await app->CoInstallService<
                getvalue::GetValueService>(
                bbt::framework::ServiceOptions{}
                    .SetExecution(bbt::framework::ExecutionPolicy::Concurrent)
                    .SetMaxInflight(64));
            if (!install) {
                std::fprintf(stderr, "server: CoInstallService failed: %s\n",
                             install.error().message.c_str());
                rc_slot.set_value(2);
                return;
            }
            auto started = bbt_co_await app->CoStart(
                bbt::framework::LifecycleOptions{}.SetDeadline(
                    std::chrono::steady_clock::now() +
                    std::chrono::seconds{5}));
            if (!started) {
                std::fprintf(stderr, "server: CoStart failed: %s\n",
                             started.error().message.c_str());
                rc_slot.set_value(2);
                return;
            }
            bound_ep.set_value(app->BoundEndpoint());
            const auto waited = bbt_co_await app->CoWaitShutdownRequest();
            (void)waited;
            auto stopped = bbt_co_await app->CoStop(
                bbt::framework::LifecycleOptions{}.SetDeadline(
                    std::chrono::steady_clock::now() +
                    std::chrono::seconds{3}));
            rc_slot.set_value(
                stopped && app->State() ==
                               bbt::framework::AppState::Stopped
                    ? 0
                    : 4);
        },
        submitted);
    if (!submitted) {
        std::fprintf(stderr, "server: coroutine task rejected\n");
        return 4;
    }

    /**
     * 轮询绑定地址（动态端口；CoStart 完成后由协程侧发布）。
     */
    auto bound = bound_ep.get_future();
    if (bound.wait_for(std::chrono::seconds{10}) != std::future_status::ready) {
        /**
         * 启动相位已由协程侧如实写入 rc_slot（promise 单次写入）；宿主侧只
         * 触发关闭请求后按启动失败退出，不二次写入。
         */
        {
            std::lock_guard<std::mutex> lk(app_mtx);
            if (app_handle)
                app_handle->RequestShutdown();
        }
        std::fprintf(stderr, "server did not bind\n");
        return 5;
    }
    const std::string endpoint = bound.get();
    const auto colon = endpoint.rfind(':');
    const std::string port = endpoint.substr(colon + 1);

    {
        std::ofstream f(port_file, std::ios::trunc);
        f << port;
    }
    {
        /**
         * schema 从生成物 descriptor 推导：.proto 是唯一真源，此处不手写 schema。
         */
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

    char buf[8];
    while (::read(STDIN_FILENO, buf, sizeof(buf)) > 0) {
    }
    /**
     * stdin EOF：native 线程发关闭请求（仅置位并唤醒唯一 waiter；
     * 主协程亲自 CoStop 收口）。
     */
    {
        std::lock_guard<std::mutex> lk(app_mtx);
        if (app_handle)
            app_handle->RequestShutdown();
    }
    const int rc = rc_slot.get_future().get();
    std::fprintf(stderr, "getvalue server rc=%d\n", rc);
    return rc;
}
