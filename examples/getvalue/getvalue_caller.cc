/**
 * bbt-framework Service→Service 正式出站 caller 进程。
 *
 * 与 getvalue_server 同装配口径：普通 main + 协程入口（g_scheduler->Start 后
 * 把装配/启动/等待关闭/收口投递进受管协程），公开 CoApp::Create（local_identity
 * 显式、V2 入站经 InstallRpcAdmission）+ 公开 CoService，不使用测试缝。宿主
 * GetValueCallerService；其 handler 内经 CoServiceBase::call<Req,Resp> 向另一个
 * framework CoApp（callee 进程）发起真实 V2 Call 出站。监听 loopback 动态端口，
 * 写端口/元数据文件，阻塞等待 stdin EOF 后按序优雅关闭。
 *
 * argv:
 * getvalue_caller caller <port_file> <callee_ep> <blackhole_ep> <meta_file>
 * callee_ep / blackhole_ep 形如 127.0.0.1:<port>（静态路由 endpoint 形态）。
 * getvalue_caller badroute <kind> <result_file>
 * 以公开入口验证三类非法静态路由在启动任何组件前被拒绝（CoStart 校验拒，
 * 对应旧 run() kExitRejected 语义）。
 * kind ∈ {empty-service, empty-endpoint, duplicate}；
 * 写出机器可读观测（rc/lifecycle_failures/bound_endpoint/组件启动计数）。
 */

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>

#include <bbt/example/v1/get_value.pb.h>

#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/Component.hpp>

#include "getvalue_caller.hpp"

namespace {

/**
 * 「启动任何组件前拒绝」的公开面额外证据：本组件的 OnStart 只在 CoStart
 * 通过启动前校验、进入组件启动相位时被调用。被拒绝的配置下计数必须保持
 * 0——证明拒绝发生在组件/网络/服务启动之前（对应旧 run() 的延迟资源工厂
 * 在 on_scheduler_started 相位被调用一次的观测口径）。
 */
std::atomic<int> g_probe_component_starts{0};

class ProbeComponent final : public bbt::framework::CoComponent {
public:
    struct Options {};

    explicit ProbeComponent(bbt::framework::detail::ConstructKey key, Options)
        : bbt::framework::CoComponent(key) {}

protected:
    ~ProbeComponent() noexcept override = default;

private:
    friend class bbt::framework::detail::Registry;

protected:
    bbt::framework::result<void> OnStart() override {
        g_probe_component_starts.fetch_add(1);
        return bbt::framework::result<void>::ok();
    }
};

/**
 * caller 与 badroute 共用的基础装配（除静态路由外完全一致）。
 */
bbt::framework::AppOptions BaseOptions() {
    bbt::framework::AppLimits limits;
    limits.max_objects = 64;
    limits.max_bindings = 64;
    limits.max_observers = 64;
    limits.observer_queue_capacity = 64;
    return bbt::framework::AppOptions{}
        .SetAppLimits(limits)
        .SetNetworkLimits(bbt::infra::NetworkLimits{
            64, 64, 16 * 1024, 64 * 1024, std::chrono::milliseconds{30000}})
        .Listen("127.0.0.1:0")
        .SetShutdownBudget(std::chrono::milliseconds{3000})
        .SetLifecycleBudget(std::chrono::milliseconds{3000})
        /**
         * V2 会话身份必填；caller 与 callee/badroute 进程使用互异身份。
         */
        .SetLocalIdentity("getvalue-caller");
}

bool IsKnownBadRouteKind(const std::string& kind) {
    return kind == "empty-service" || kind == "empty-endpoint" ||
           kind == "duplicate";
}

/**
 * 三类非法静态路由（其余字段保持与正常 caller 相同的合法口径），供公开
 * CoApp 装配后在 CoStart 的启动前校验被拒：
 * empty-service  空 service_name
 * empty-endpoint 空 endpoint（只校验 endpoint 非空，地址语法归 infra）
 * duplicate      两条相同 service_name
 */
void AddBadRoutes(bbt::framework::AppOptions* options,
                  const std::string& kind) {
    if (kind == "empty-service") {
        options->AddRoute("", "127.0.0.1:1");
    } else if (kind == "empty-endpoint") {
        options->AddRoute("svc.route", "");
    } else if (kind == "duplicate") {
        options->AddRoute("svc.route", "127.0.0.1:1");
        options->AddRoute("svc.route", "127.0.0.1:2");
    }
}

std::string JsonEscape(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (const char c : s) {
        switch (c) {
        case '"':
            out += "\\\"";
            break;
        case '\\':
            out += "\\\\";
            break;
        case '\n':
            out += "\\n";
            break;
        case '\r':
            out += "\\r";
            break;
        case '\t':
            out += "\\t";
            break;
        default:
            out += c;
            break;
        }
    }
    return out;
}

bbt::framework::ServiceOptions CallerServiceOptions() {
    return bbt::framework::ServiceOptions{}
        .SetExecution(bbt::framework::ExecutionPolicy::Concurrent)
        .SetMaxInflight(64);
}

/**
 * 公开入口：装配非法静态路由 → Create → CoInstall* → CoStart（启动前校验）。
 * 断言依据是 CoStart 的返回与公开观测口（LifecycleFailures/BoundEndpoint/
 * 组件启动计数），本进程只如实写观测，不打印硬编码期望值。
 */
int RunBadRoute(const std::string& kind, const std::string& result_file) {
    bbt::framework::AppOptions options = BaseOptions();
    AddBadRoutes(&options, kind);

    /**
     * CoApp::Create 与全部 Co* 入口要求受管协程上下文：先启动 runtime，
     * 装配/启动/收口全部投递进受管协程执行。
     */
    g_scheduler->Start();

    std::promise<int> rc_slot;
    std::promise<std::vector<bbt::framework::LifecycleFailure>> failure_slot;
    std::promise<std::string> endpoint_slot;
    std::shared_ptr<bbt::framework::CoApp> app_handle;
    std::mutex app_mtx;

    bool submitted = false;
    g_scheduler->RegistCoroutineTask(
        [options = std::move(options), &rc_slot, &failure_slot,
         &endpoint_slot, &app_handle, &app_mtx]() mutable {
            auto app = bbt::framework::CoApp::Create(options,
                                                     "getvalue-caller");
            if (!app) {
                rc_slot.set_value(4);
                failure_slot.set_value({});
                endpoint_slot.set_value("");
                return;
            }
            {
                std::lock_guard<std::mutex> lk(app_mtx);
                app_handle = app;
            }
            auto comp = bbt_co_await app->CoInstallComponent<ProbeComponent>(
                "probe", ProbeComponent::Options{});
            if (!comp) {
                std::fprintf(stderr, "badroute install component failed: %s\n",
                             comp.error().message.c_str());
                rc_slot.set_value(4);
                failure_slot.set_value({});
                endpoint_slot.set_value("");
                return;
            }
            auto svc = bbt_co_await app->CoInstallService<
                getvalue::GetValueCallerService>(CallerServiceOptions());
            if (!svc) {
                std::fprintf(stderr, "badroute install service failed: %s\n",
                             svc.error().message.c_str());
                rc_slot.set_value(4);
                failure_slot.set_value({});
                endpoint_slot.set_value("");
                return;
            }

            /**
             * 坏配置 → CoStart 在任何组件启动前拒绝并回退到 Stopped；
             * rc 映射沿用旧 run() 语义：1 = kExitRejected。
             */
            auto started = bbt_co_await app->CoStart(
                bbt::framework::LifecycleOptions{}.SetDeadline(
                    std::chrono::steady_clock::now() +
                    std::chrono::seconds{5}));
            failure_slot.set_value(app->LifecycleFailures());
            endpoint_slot.set_value(app->BoundEndpoint());
            rc_slot.set_value(started ? 0 : 1);
        },
        submitted);
    if (!submitted) {
        std::fprintf(stderr, "badroute: coroutine task rejected\n");
        return 4;
    }

    const int rc = rc_slot.get_future().get();
    const std::vector<bbt::framework::LifecycleFailure> failures =
        failure_slot.get_future().get();
    const std::string endpoint = endpoint_slot.get_future().get();

    std::ofstream f(result_file, std::ios::trunc);
    f << "{\n"
      << "  \"mode\": \"badroute\",\n"
      << "  \"kind\": \"" << JsonEscape(kind) << "\",\n"
      << "  \"rc\": " << rc << ",\n"
      << "  \"bound_endpoint\": \"" << JsonEscape(endpoint) << "\",\n"
      << "  \"resource_factory_calls\": "
      << g_probe_component_starts.load() << ",\n"
      << "  \"lifecycle_failures\": [";
    for (std::size_t i = 0; i < failures.size(); ++i) {
        if (i != 0)
            f << ", ";
        f << "\"" << JsonEscape(failures[i].message) << "\"";
    }
    f << "],\n"
      << "  \"auth_state\": \"unauthenticated_loopback\"\n"
      << "}\n";
    if (!f) {
        std::fprintf(stderr, "badroute result write failed: %s\n",
                     result_file.c_str());
        return 4;
    }
    std::fprintf(stderr,
                 "badroute kind=%s rc=%d failures=%zu endpoint='%s' probe=%d\n",
                 kind.c_str(), rc, failures.size(), endpoint.c_str(),
                 g_probe_component_starts.load());
    return rc;
}

} // namespace

int main(int argc, char* argv[]) {
    if (argc < 2)
        return 90;
    const std::string mode = argv[1];

    /**
     * 非法静态路由的公开入口验收模式。
     */
    if (mode == "badroute") {
        if (argc < 4 || !IsKnownBadRouteKind(argv[2]))
            return 90;
        return RunBadRoute(argv[2], argv[3]);
    }

    if (mode != "caller" || argc < 6)
        return 90;
    const std::string port_file = argv[2];
    const std::string callee_ep = argv[3];
    const std::string black_ep = argv[4];
    const std::string meta_file = argv[5];

    /**
     * caller handler 内做 typed 出站（protobuf 编解码 + 框架分发），显式把
     * 协程栈调大（公开配置宏，须在调度器 Start 前设置；process-global）。
     * Two server drivers + one client-only driver + one application worker.
     */
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 512 * 1024;

    bbt::framework::AppOptions options = BaseOptions();
    options.AddRoute(std::string(getvalue::GetValueCallerService::kCalleeService),
                     callee_ep);
    options.AddRoute(
        std::string(getvalue::GetValueCallerService::kBlackholeService),
        black_ep);

    /**
     * 普通 main + 协程入口：与 getvalue_server 同一装配口径。装配结果跨
     * 协程/线程交接：endpoint 经 promise 交给宿主线程写端口文件；App 指针
     * 发布后宿主线程只做 RequestShutdown（幂等 noexcept）；退出码经
     * promise 回传（0=按期优雅收口，2=启动失败，4=收口未竟）。
     */
    g_scheduler->Start();

    std::promise<std::string> bound_ep;
    std::promise<int> rc_slot;
    std::shared_ptr<bbt::framework::CoApp> app_handle;
    std::mutex app_mtx;

    bool submitted = false;
    g_scheduler->RegistCoroutineTask(
        [options = std::move(options), &bound_ep, &rc_slot,
         &app_handle, &app_mtx]() mutable {
            auto app = bbt::framework::CoApp::Create(options,
                                                     "getvalue-caller");
            if (!app) {
                rc_slot.set_value(4);
                return;
            }
            {
                std::lock_guard<std::mutex> lk(app_mtx);
                app_handle = app;
            }
            auto install = bbt_co_await app->CoInstallService<
                getvalue::GetValueCallerService>(CallerServiceOptions());
            if (!install) {
                rc_slot.set_value(2);
                return;
            }
            auto started = bbt_co_await app->CoStart(
                bbt::framework::LifecycleOptions{}.SetDeadline(
                    std::chrono::steady_clock::now() +
                    std::chrono::seconds{5}));
            if (!started) {
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
        std::fprintf(stderr, "caller: coroutine task rejected\n");
        return 4;
    }

    /**
     * 轮询绑定地址（动态端口；CoStart 完成后由协程侧发布）。
     */
    auto bound = bound_ep.get_future();
    if (bound.wait_for(std::chrono::seconds{10}) != std::future_status::ready) {
        {
            std::lock_guard<std::mutex> lk(app_mtx);
            if (app_handle)
                app_handle->RequestShutdown();
        }
        std::fprintf(stderr, "caller did not bind\n");
        return 5;
    }
    const std::string endpoint = bound.get();
    const auto colon = endpoint.rfind(':');
    {
        std::ofstream f(port_file, std::ios::trunc);
        f << endpoint.substr(colon + 1);
    }
    {
        /**
         * schema 从生成物 descriptor 推导：.proto 是唯一真源，此处不手写 schema。
         */
        std::ofstream f(meta_file, std::ios::trunc);
        f << "service=" << getvalue::GetValueCallerService::kServiceName << "\n";
        f << "callee_service="
          << getvalue::GetValueCallerService::kCalleeService << "\n";
        f << "request_schema="
          << bbt::example::v1::GetValueRequest::descriptor()->full_name() << "\n";
        f << "response_schema="
          << bbt::example::v1::GetValueResponse::descriptor()->full_name() << "\n";
        f << "local_identity=getvalue-caller\n";
        f << "auth_state=unauthenticated_loopback\n";
    }
    std::fprintf(stderr, "getvalue caller up endpoint=%s callee=%s blackhole=%s\n",
                 endpoint.c_str(), callee_ep.c_str(), black_ep.c_str());

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
    std::fprintf(stderr, "getvalue caller rc=%d\n", rc);
    return rc;
}
