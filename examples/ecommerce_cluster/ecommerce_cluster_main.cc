// 电商集群示例入口：按环境变量装配 gateway/order/catalog/settlement 角色。
//
// 入口是 runtime 的生成式 `bbtco_main`：进程不写 native main、不启动/停止
// scheduler、不建 runner 线程、不等待 future。注册/注销与关闭都在业务主协程里
// 直接 await；共享 Engine 的唯一一次 Engine::Run 由 bbtco_main_host_hook
// 执行。参数（生成式入口不暴露 argv）：
//   BBT_EXAMPLE_MODE=gateway|order|catalog|settlement
//   BBT_EXAMPLE_CONSUL=<http://127.0.0.1:PORT>
//   BBT_EXAMPLE_INSTANCE_ID / BBT_EXAMPLE_STORAGE=1 /
//   BBT_EXAMPLE_REDIS / BBT_EXAMPLE_MONGO / BBT_EXAMPLE_DATABASE /
//   BBT_EXAMPLE_KEY_PREFIX
//
// 四个角色是独立的真实进程（各自 v2::CoApp、各自监听动态 loopback 端口）：
//   gateway  对外入口：只装配 GatewayService；动态发现 OrderService；
//   order    装配 OrderService；动态发现 CatalogService；把自己注册为 OrderService；
//   catalog  装配 CatalogService；把自己注册为 CatalogService。
//   settlement 装配独立结算服务；仅存储模式启用并拥有账单集合。
//
// 业务只给逻辑服务名；AppOptions::SetDynamicRoutes 固定选择发现表，
// Consul provider 后续发布快照。gateway 的公开 HTTP 路由复用同一分发器。
//
// 生命周期：监听绑定后先自注册（若该角色需要），再输出 `event=ready`；stdin EOF
// 触发「注销 → 优雅关闭」。注册中心地址只经 --consul 传入，业务代码不感知后端。

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include <poll.h>
#include <unistd.h>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>

#include <bbt/framework/HostGate.hpp>
#include <bbt/framework/V2.hpp>

#ifdef EC_STORAGE_AVAILABLE
#include <bbt/framework/MongoDBComponent.hpp>
#include <bbt/framework/RedisComponent.hpp>
#include "services/catalog/cache_view.hpp"
#endif

#include "discovery.hpp"
#include "services/catalog/catalog_service.hpp"
#include "services/common/node_identity.hpp"
#include "services/gateway/gateway_service.hpp"
#include "services/order/order_service.hpp"
#include "services/settlement/settlement_service.hpp"
#include "services/common/storage.hpp"

using namespace bbt::framework;

namespace {

namespace v2 = bbt::framework::v2;

using ecommerce::ConsulDiscoveryComponent;
using ecommerce::Node;

constexpr const char* kUsage =
    "usage: ecommerce_cluster <gateway|order|catalog|settlement> --consul <http://127.0.0.1:PORT>\n"
    "  [--instance-id <id>] [--storage --redis host:port --mongo URI --database name --key-prefix prefix]\n";

struct Args {
    std::string mode;
    std::string consul;
    std::string instance_id;
    bool storage{false};
    std::string redis;
    std::string mongo;
    std::string database;
    std::string key_prefix;
};

// 生成式入口（bbtco_main）不暴露 argv：角色与依赖地址经环境变量传入。
std::string Env(const char* name) {
    const char* value = std::getenv(name);
    return value == nullptr ? std::string{} : std::string{value};
}

bool ParseArgs(Args* out) {
    out->mode = Env("BBT_EXAMPLE_MODE");
    out->consul = Env("BBT_EXAMPLE_CONSUL");
    out->instance_id = Env("BBT_EXAMPLE_INSTANCE_ID");
    out->redis       = Env("BBT_EXAMPLE_REDIS");
    out->mongo       = Env("BBT_EXAMPLE_MONGO");
    out->database    = Env("BBT_EXAMPLE_DATABASE");
    out->key_prefix  = Env("BBT_EXAMPLE_KEY_PREFIX");
    out->storage     = !Env("BBT_EXAMPLE_STORAGE").empty();
    if (out->consul.empty()) return false;
    return out->mode == "gateway" || out->mode == "order" ||
           out->mode == "catalog" ||
           (out->mode == "settlement" && out->storage);
}

void Journal(const std::string& line) {
    std::printf("%s\n", line.c_str());
    std::fflush(stdout);
}

ServiceOptions ConcurrentOptions() {
    return ServiceOptions{}.SetMaxInflight(64);
}

} // namespace

int bbtco_main() {
    Args args;
    if (!ParseArgs(&args)) {
        std::fputs(kUsage, stderr);
        return 90;   // EX_USAGE
    }

    if (args.storage) {
#ifndef EC_STORAGE_AVAILABLE
        std::fputs("storage dependencies unavailable\n", stderr);
        return 65;
#else
        if (args.database.empty() || args.key_prefix.empty() || args.mongo.empty() || args.redis.empty()) {
            std::fputs("storage requires --redis host:port --mongo URI --database name --key-prefix prefix\n", stderr);
            return 65;
        }
#endif
    }
    ecommerce::Storage().enabled = args.storage;
    ecommerce::Storage().key_prefix = args.key_prefix;
    const std::string pid = std::to_string(::getpid());
    const std::string service =
        args.mode == "gateway"
            ? std::string(ecommerce::kGatewayService)
        : args.mode == "order"      ? std::string(ecommerce::kOrderService)
        : args.mode == "settlement" ? std::string(ecommerce::kSettlementService)
                                    : std::string(ecommerce::kCatalogService);
    ecommerce::MutableNodeInfo().instance_id =
        args.instance_id.empty()
            ? ecommerce::MakeInstanceId(service)
            : args.instance_id;
    ecommerce::MutableNodeInfo().process_id = pid;

    // gateway 发现 order；order 发现 catalog；存储模式两者还发现 settlement。
    // catalog 和 settlement 为链路末端，仅登记自己。
    std::vector<std::string> watches;
    std::string register_as;
    if (args.mode == "gateway") {
        watches.emplace_back(ecommerce::kOrderService);
        if (args.storage)
            watches.emplace_back(ecommerce::kSettlementService);
    } else if (args.mode == "order") {
        watches.emplace_back(ecommerce::kCatalogService);
        if (args.storage)
            watches.emplace_back(ecommerce::kSettlementService);
        register_as = std::string(ecommerce::kOrderService);
    } else {
        register_as = args.mode == "settlement" ? std::string(ecommerce::kSettlementService)
                                                : std::string(ecommerce::kCatalogService);
    }

    // handler 内做 protobuf 编解码 + 出站调用，默认 12KB 栈不够；这是运行期
    // 资源配置，必须在 Scheduler Start 前设置（process-global，非 per-service）。
    g_bbt_coroutine_config->m_cfg_stack_size = 512 * 1024;

    auto table = std::make_shared<DynamicRouteTable>();

    AppNetworkOptions network;
    network.SetLimits(bbt::infra::NetworkLimits{
                          64, 64, 16 * 1024, 64 * 1024,
                          std::chrono::milliseconds{30000}})
        .SetListenAddress(bbt::infra::ListenAddress{"127.0.0.1", 0})
        // V2 会话身份必填；每个进程角色使用自己的身份。
        .SetLocalAppId(ecommerce::Node().instance_id);

    auto options = AppOptions{}
                       .SetLimits(64, 64, 8, 64)
                       .SetShutdownBudget(std::chrono::milliseconds{8000})
                       .SetLifecycleBudget(std::chrono::milliseconds{8000})
                       .SetDynamicRoutes(table)
                       .SetNetworkOptions(std::move(network));
    if (args.mode == "gateway") {
        const std::string prefix = "bbt.example.ecommerce.v1.";
        for (const auto& method : {"Checkout", "CreateOrder", "GetOrder", "GetBill"}) {
            const std::string name = method;
            options.AddHttpRoute(HttpServiceRoute{
                "/" + name, std::string(ecommerce::kGatewayService), name,
                prefix + (name == "Checkout" ? "CheckoutRequest" : name == "CreateOrder" ? "OrderRequest"
                                                                                         : "LookupRequest"),
                prefix + (name == "Checkout" ? "CheckoutResponse" : "Record")});
        }
    }
    auto app = v2::CoApp::Create(std::move(options), std::string("ecommerce-") + args.mode);

    // 发现组件经 v2 组件装配：OnInstall 只做 Idle 装配（专属 http/consul
    // client，借用共享 Engine，不 Run/不建线程），OnStart 登记 watcher；
    // CoStart 成功后由业务等全部首快照，OnStop 收 watcher 并同一 CloseOptions
    // 封口 consul。
    ConsulDiscoveryComponent::Options discovery_options;
    discovery_options.engine         = app->engine();
    discovery_options.table          = table;
    discovery_options.agent_url      = args.consul;
    discovery_options.watch_services = watches;
    discovery_options.request_timeout = std::chrono::milliseconds{3000};
    discovery_options.query_wait      = std::chrono::milliseconds{2000};
    if (auto r = bbt_co_await
            app->CoInstallComponent<ConsulDiscoveryComponent>(
                "consul-discovery", std::move(discovery_options));
        !r) {
        std::fprintf(stderr, "install consul-discovery failed: %s\n",
                     r.error().message.c_str());
        return 4;
    }

#ifdef EC_STORAGE_AVAILABLE
    if (args.storage && args.mode == "catalog") {
        const auto colon = args.redis.rfind(':');
        if (colon == std::string::npos)
            return 65;
        unsigned long port = 0;
        std::size_t consumed = 0;
        const auto port_text = args.redis.substr(colon + 1);
        try {
            port = std::stoul(port_text, &consumed);
        } catch (const std::exception&) { return 65; }
        if (consumed != port_text.size())
            return 65;
        if (port == 0 || port > 65535)
            return 65;
        std::error_code address_error;
        auto address = bbt::infra::net::Address::Ip(
            args.redis.substr(0, colon),
            static_cast<std::uint16_t>(port), address_error);
        if (address_error || !address.Valid())
            return 65;
        v2::RedisComponent::Options redis_options;
        redis_options.engine = app->engine();
        redis_options.address = address;
        redis_options.pool.connections = 2;
        redis_options.connect_timeout = std::chrono::milliseconds{3000};
        auto aux_options = redis_options;
        if (auto r = bbt_co_await app->CoInstallComponent<v2::RedisComponent>(
                "cache", std::move(redis_options));
            !r)
            return 65;
        if (auto r = bbt_co_await app->CoInstallComponent<v2::RedisComponent>(
                "aux-cache", std::move(aux_options));
            !r)
            return 65;
        if (!(bbt_co_await app->CoInstallService<ecommerce::CacheViewService<0>>(
                ConcurrentOptions().BindComponent<v2::RedisComponent>("cache"))) ||
            !(bbt_co_await app->CoInstallService<ecommerce::CacheViewService<1>>(
                ConcurrentOptions().BindComponent<v2::RedisComponent>("cache"))) ||
            !(bbt_co_await app->CoInstallService<ecommerce::CacheViewService<2>>(
                ConcurrentOptions().BindComponent<v2::RedisComponent>("aux-cache"))) ||
            !(bbt_co_await app->CoInstallService<ecommerce::CacheViewService<3>>(
                ConcurrentOptions().BindComponent<v2::RedisComponent>("aux-cache"))))
            return 65;
    }
    if (args.storage && (args.mode == "order" || args.mode == "settlement")) {
        const std::string collection =
            args.mode == "order" ? "orders" : "bills";
        v2::MongoDBComponent::Options mongo_options;
        mongo_options.config.uri = args.mongo;
        mongo_options.config.worker_threads = 2;
        mongo_options.config.max_queue = 64;
        mongo_options.config.server_selection_timeout =
            std::chrono::milliseconds{2000};
        mongo_options.config.connect_timeout = std::chrono::milliseconds{2000};
        mongo_options.config.socket_timeout = std::chrono::milliseconds{2000};
        mongo_options.config.wait_queue_timeout =
            std::chrono::milliseconds{2000};
        mongo_options.target.database = args.database;
        mongo_options.target.collection = collection;
        if (auto r = bbt_co_await app->CoInstallComponent<v2::MongoDBComponent>(
                collection, std::move(mongo_options));
            !r)
            return 65;
    }
#endif

    // 每个进程只装配本角色的服务：gateway 不监听业务下游，catalog 不装配上游。
    result<void> added = result<void>::ok();
#ifdef EC_STORAGE_AVAILABLE
    if (args.mode == "catalog") {
        added = bbt_co_await app->CoInstallService<ecommerce::CatalogService>(
            args.storage ? ConcurrentOptions().BindComponent<v2::RedisComponent>("cache") : ConcurrentOptions());
    } else if (args.mode == "order") {
        added = bbt_co_await app->CoInstallService<ecommerce::OrderService>(
            args.storage ? ConcurrentOptions().BindComponent<v2::MongoDBComponent>("orders") : ConcurrentOptions());
    } else if (args.mode == "settlement") {
        added = bbt_co_await app->CoInstallService<ecommerce::SettlementService>(
            args.storage ? ConcurrentOptions().BindComponent<v2::MongoDBComponent>("bills") : ConcurrentOptions());
    } else {
        added = bbt_co_await app->CoInstallService<ecommerce::GatewayService>(
            ConcurrentOptions());
    }
#else
    if (args.mode == "gateway")
        added = bbt_co_await app->CoInstallService<ecommerce::GatewayService>(
            ConcurrentOptions());
    else if (args.mode == "order")
        added = bbt_co_await app->CoInstallService<ecommerce::OrderService>(
            ConcurrentOptions());
    else if (args.mode == "settlement")
        added = bbt_co_await app->CoInstallService<ecommerce::SettlementService>(
            ConcurrentOptions());
    else
        added = bbt_co_await app->CoInstallService<ecommerce::CatalogService>(
            ConcurrentOptions());
#endif
    if (!added) {
        std::fprintf(stderr, "install service(%s) failed: %s\n",
                     args.mode.c_str(), added.error().message.c_str());
        return 4;
    }

    auto started = bbt_co_await app->CoStart();
    if (!started) {
        std::fprintf(stderr, "ecommerce_cluster: start failed: %s\n",
                     started.error().message.c_str());
        const int rc = started.error().code == ErrorCode::InvalidArgument ? 1 : 2;
        auto close_options = bbt::infra::CloseOptions::Immediate();
        close_options.wait_release = true;
        close_options.deadline = std::chrono::steady_clock::now() + std::chrono::seconds{8};
        auto closed = bbt_co_await app->CoStop(close_options);
        Journal("event=start_failure_cleanup mode=" + args.mode +
                " complete=" + (closed && app->PendingCleanup().empty() ? "1" : "0"));
        return rc;
    }

    auto* component = ConsulDiscoveryComponent::Current();
    if (component == nullptr) {
        auto closed = bbt_co_await app->CoStop();
        (void)closed;
        std::fprintf(stderr, "ecommerce_cluster: discovery component missing\n");
        return 6;
    }
    // CoStart 后共享 Engine 由 host hook 推进；watcher 的首个查询在此期间
    // 才可能完成。未就绪即如实失败收口，不输出 ready。
    auto snapshotted = bbt_co_await component->WaitFirstSnapshots(
        std::chrono::steady_clock::now() + std::chrono::milliseconds{8000});
    if (!snapshotted) {
        Journal("event=discovery_not_ready mode=" + args.mode +
                " error=" + snapshotted.error().message);
        auto closed = bbt_co_await app->CoStop();
        (void)closed;
        return 8;
    }

    const std::string endpoint = app->BoundEndpoint();
    if (endpoint.empty()) {
        auto closed = bbt_co_await app->CoStop();
        (void)closed;
        std::fprintf(stderr, "ecommerce_cluster: did not bind\n");
        return 5;
    }
    const std::size_t colon = endpoint.rfind(':');
    const std::uint16_t port = static_cast<std::uint16_t>(
        std::stoi(endpoint.substr(colon + 1)));

    Journal("event=dynamic_routes_installed mode=" + args.mode);

    if (!register_as.empty()) {
        auto registered = bbt_co_await component->RegisterSelf(
            register_as, ecommerce::Node().instance_id, "127.0.0.1", port);
        if (!registered) {
            Journal("event=register_failed mode=" + args.mode +
                    " error=" + registered.error().message);
            auto closed = bbt_co_await app->CoStop();
            (void)closed;
            return 7;
        }
    }

    Journal("event=ready mode=" + args.mode +
            " service=" + service +
            " instance=" + ecommerce::Node().instance_id +
            " endpoint=" + endpoint +
            " pid=" + pid +
            " registered=" + (register_as.empty() ? "0" : "1"));

    // stdin EOF = 驱动器要求优雅退出：非阻塞观察（不建 native 观察线程）。
    for (;;) {
        pollfd pfd{STDIN_FILENO, POLLIN, 0};
        const int ready = ::poll(&pfd, 1, 0);
        if (ready > 0) {
            char buffer[64];
            const ssize_t n = ::read(STDIN_FILENO, buffer, sizeof(buffer));
            if (n <= 0)
                break;
            continue;
        }
        if (ready < 0 && errno != EINTR)
            break;
        bbtco_sleep(20);
    }

    if (!register_as.empty()) {
        if (auto* component = ConsulDiscoveryComponent::Current()) {
            auto deregistered = bbt_co_await component->DeregisterSelf();
            if (!deregistered)
                Journal("event=deregister_failed mode=" + args.mode +
                        " error=" + deregistered.error().message);
        }
    }

    app->RequestShutdown();
    auto requested = bbt_co_await app->CoWaitShutdownRequest();
    if (!requested) {
        std::fprintf(stderr,
                     "ecommerce_cluster: wait shutdown request failed: %s\n",
                     requested.error().message.c_str());
    }
    auto close_options = bbt::infra::CloseOptions::Immediate();
    close_options.wait_release = true;
    close_options.deadline = std::chrono::steady_clock::now() + std::chrono::seconds{8};
    auto stopped = bbt_co_await app->CoStop(close_options);
    const int rc = stopped ? 0
                           : (stopped.error().code == ErrorCode::TimedOut ? 3
                                                                          : 4);
    Journal("event=stopped mode=" + args.mode +
            " instance=" + ecommerce::Node().instance_id +
            " rc=" + std::to_string(rc));
    return rc;
}

// 业务协程里做注册发现与嵌套出站，默认 12KB 栈不足；在 Start 前放大。
void setup_runtime() {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 256 * 1024;
}

// 入口 native host hook：共享 Engine 的 Engine::Run 在主线程执行一次。
int bbtco_main_host_hook(bbt::coroutine::HostHookContext& ctx) {
    return bbt::framework::HostGate::RunHostGate(ctx);
}

BBTCO_MAIN_WITH_SETUP_AND_HOST_HOOK();
