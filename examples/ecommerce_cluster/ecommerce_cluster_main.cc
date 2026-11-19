/**
 * 电商集群示例入口：按 argv 装配 gateway/order/catalog/settlement 角色。
 *
 * 四个角色是独立的真实进程（各自 CoApp、各自监听动态 loopback 端口）：
 * gateway  对外入口：只装配 GatewayService；动态发现 OrderService；
 * order    装配 OrderService；动态发现 CatalogService；把自己注册为 OrderService；
 * catalog  装配 CatalogService；把自己注册为 CatalogService。
 * settlement 装配独立结算服务；仅存储模式启用并拥有账单资源。
 *
 * 业务只给逻辑服务名，目标地址由宿主路由门从 DynamicRouteTable 解析；
 * 进程里没有任何实例地址注入（没有静态路由）。
 *
 * 装配顺序（与旧 add_resource 工厂/Start 相位一致）：Scheduler 启动后，
 * 先由宿主线程创建并启动 Consul 发现组件（首个权威快照失败即启动失败），
 * 再把服务装配投递进受管协程；CoStart 内组件先启动、服务随后启动、
 * 网络最后装配（绑定后才开始接纳）。自注册（若该角色需要）发生在
 * CoStart 返回、`event=ready` 之前；stdin EOF 触发「注销 → 优雅关闭」。
 * 注册中心地址只经 --consul 传入，业务代码不感知后端。
 */

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <unistd.h>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>

#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/DynamicRouteTable.hpp>
#include <bbt/framework/ExecutionPolicy.hpp>

#include "discovery.hpp"
#include "services/catalog/catalog_service.hpp"
#include "services/common/node_identity.hpp"
#include "services/gateway/gateway_service.hpp"
#include "services/order/order_service.hpp"
#include "services/settlement/settlement_service.hpp"
#include "services/common/storage.hpp"
#ifdef EC_STORAGE_AVAILABLE
#include <bbt/framework/RedisNetResource.hpp>
#include <bbt/framework/MongoDbResource.hpp>
#include <bbt/infra/mongo/Client.hpp>
#include <bbt/infra/net/Address.hpp>
#endif

using namespace bbt::framework;

namespace {

using ecommerce::ConsulDiscoveryComponent;

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

bool ParseArgs(int argc, char* argv[], Args* out) {
    if (argc < 2)
        return false;
    out->mode = argv[1];
    for (int i = 2; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--consul" && i + 1 < argc) {
            out->consul = argv[++i];
        } else if (arg == "--instance-id" && i + 1 < argc) {
            out->instance_id = argv[++i];
        } else if (arg == "--storage") {
            out->storage = true;
        } else if (arg == "--redis" && i + 1 < argc) {
            out->redis = argv[++i];
        } else if (arg == "--mongo" && i + 1 < argc) {
            out->mongo = argv[++i];
        } else if (arg == "--database" && i + 1 < argc) {
            out->database = argv[++i];
        } else if (arg == "--key-prefix" && i + 1 < argc) {
            out->key_prefix = argv[++i];
        } else {
            return false;
        }
    }
    if (out->consul.empty())
        return false;
    return out->mode == "gateway" || out->mode == "order" ||
           out->mode == "catalog" || (out->mode == "settlement" && out->storage);
}

void Journal(const std::string& line) {
    std::printf("%s\n", line.c_str());
    std::fflush(stdout);
}

ServiceOptions ConcurrentOptions() {
    return ServiceOptions{}
        .SetExecution(ExecutionPolicy::Concurrent)
        .SetMaxInflight(64);
}

/**
 * "host:port" 的端口段解析：只接受 1..65535 的数字串（与 old stoi 相同口径，
 * 但不抛异常）。返回 0 表示非法。
 */
std::uint16_t ParseEndpointPort(const std::string& endpoint) {
    const auto colon = endpoint.rfind(':');
    if (colon == std::string::npos)
        return 0;
    const std::string port_text = endpoint.substr(colon + 1);
    if (port_text.empty())
        return 0;
    unsigned long value = 0;
    for (const char c : port_text) {
        if (c < '0' || c > '9')
            return 0;
        value = value * 10 + static_cast<unsigned long>(c - '0');
        if (value > 65535)
            return 0;
    }
    return static_cast<std::uint16_t>(value);
}

} // namespace

int main(int argc, char* argv[]) {
    Args args;
    if (!ParseArgs(argc, argv, &args)) {
        std::fputs(kUsage, stderr);
        /**
         * EX_USAGE
         */
        return 90;
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

    /**
     * gateway 发现 order；order 发现 catalog；存储模式两者还发现 settlement。
     * catalog 和 settlement 为链路末端，仅登记自己。
     */
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

    /**
     * handler 内做 protobuf 编解码 + 出站调用，默认 12KB 栈不够；这是运行期
     * 资源配置，必须在 Scheduler Start 前设置（process-global，非 per-service）。
     */
    g_bbt_coroutine_config->m_cfg_stack_size = 512 * 1024;

    auto table = std::make_shared<DynamicRouteTable>();

    g_scheduler->Start();

    /**
     * 发现组件由 main 直接创建/启动/关闭（旧 add_resource 工厂与框架 Start
     * 相位的对应物）：Create + Start 都在宿主线程、Scheduler 已启动、服务
     * 装配与网络装配之前执行；首个权威快照失败即进程启动失败（fail-closed）。
     * 自注册/注销经受管协程 await 其公开方法；Close 在进程退出前由宿主线程
     * 调用（调度器仍能推进在途操作时）。
     */
    ConsulDiscoveryComponent::Options discovery_options;
    discovery_options.agent_url = args.consul;
    discovery_options.watch_services = watches;
    discovery_options.request_timeout = std::chrono::milliseconds{3000};
    discovery_options.query_wait = std::chrono::milliseconds{2000};
    std::shared_ptr<ConsulDiscoveryComponent> discovery;
    {
        auto created = ConsulDiscoveryComponent::Create(discovery_options, table);
        if (!created) {
            std::fprintf(stderr, "consul-discovery create failed: %s\n",
                         created.error().message.c_str());
            return 4;
        }
        discovery = std::move(created).value();
    }
    if (auto started = discovery->Start(); !started) {
        std::fprintf(stderr, "consul-discovery start failed: %s\n",
                     started.error().message.c_str());
        discovery->Close();
        return 4;
    }

#ifdef EC_STORAGE_AVAILABLE
    /**
     * 存储模式装配参数在宿主线程预先校验（旧工厂入参校验的对应物）：
     * Redis 只接受数值 IPv4/IPv6（net::Address::Ip 不解析主机名）。
     */
    std::optional<bbt::infra::net::Address> redis_address;
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
        auto address = bbt::infra::net::Address::Ip(args.redis.substr(0, colon), static_cast<std::uint16_t>(port), address_error);
        if (address_error || !address.Valid())
            return 65;
        redis_address = std::move(address);
    }
    bbt::infra::mongo::MongoRuntimeConfig mongo_config;
    bbt::infra::mongo::MongoTarget mongo_target;
    std::string mongo_collection;
    if (args.storage && (args.mode == "order" || args.mode == "settlement")) {
        mongo_collection = args.mode == "order" ? "orders" : "bills";
        mongo_config.uri = args.mongo;
        mongo_config.worker_threads = 2;
        mongo_config.max_queue = 64;
        mongo_config.server_selection_timeout = std::chrono::milliseconds{2000};
        mongo_config.connect_timeout = std::chrono::milliseconds{2000};
        mongo_config.socket_timeout = std::chrono::milliseconds{2000};
        mongo_config.wait_queue_timeout = std::chrono::milliseconds{2000};
        mongo_target.database = args.database;
        mongo_target.collection = mongo_collection;
    }
#endif

    /**
     * 装配/启动/等待关闭/收口全部在受管协程内（Co* 入口的 CoRuntimeOnly
     * 强约束）；App 指针发布给宿主线程只做 RequestShutdown（幂等 noexcept）；
     * 绑定端点与退出码经 promise 回传（0=优雅收口且 Stopped，2=装配/启动
     * 失败，4=收口未竟；promise 各只写一次）。
     */
    std::promise<std::string> bound_ep;
    std::promise<int> rc_slot;
    std::shared_ptr<CoApp> app_handle;
    std::mutex app_mtx;
    std::atomic<int> register_failed{0};

    AppLimits limits;
    limits.max_objects = 64;
    limits.max_bindings = 64;
    limits.max_observers = 64;
    limits.observer_queue_capacity = 64;
    bool submitted = false;
    g_scheduler->RegistCoroutineTask(
        [options = AppOptions{}
                       .SetAppLimits(limits)
                       .SetNetworkLimits(bbt::infra::NetworkLimits{
                           64, 64, 16 * 1024, 64 * 1024, std::chrono::milliseconds{30000}})
                       .Listen("127.0.0.1:0")
                       .SetShutdownBudget(std::chrono::milliseconds{8000})
                       .SetLifecycleBudget(std::chrono::milliseconds{30000})
                       /**
                         * V2 会话身份必填；每个进程角色使用自己的身份。
                         */
                       .SetLocalIdentity(std::string("ecommerce-") + args.mode)
                       .SetDynamicRoutes(table),
         discovery, mode = args.mode, service, register_as, pid,
#ifdef EC_STORAGE_AVAILABLE
         redis_address, mongo_config, mongo_target, mongo_collection,
#endif
         &bound_ep, &rc_slot, &app_handle, &app_mtx,
         &register_failed]() mutable {
            auto finish = [&](std::string endpoint, int rc) {
                bound_ep.set_value(std::move(endpoint));
                rc_slot.set_value(rc);
            };
            auto app = CoApp::Create(std::move(options), "ecommerce-" + mode);
            if (!app) {
                std::fprintf(stderr, "ecommerce_cluster: CoApp::Create failed\n");
                finish("", 4);
                return;
            }
            {
                std::lock_guard<std::mutex> lk(app_mtx);
                app_handle = app;
            }
#ifdef EC_STORAGE_AVAILABLE
            if (redis_address) {
                auto installed = bbt_co_await app->CoInstallComponent<RedisNetResource>(
                    "cache", RedisNetResource::Options{}.SetAddress(*redis_address));
                if (!installed) {
                    std::fprintf(stderr, "ecommerce_cluster: CoInstallComponent(cache) failed: %s\n",
                                 installed.error().message.c_str());
                    finish("", 2);
                    return;
                }
            }
            if (!mongo_collection.empty()) {
                auto installed = bbt_co_await app->CoInstallComponent<MongoDbResource>(
                    mongo_collection,
                    MongoDbResource::Options{}.SetConfig(mongo_config).SetTarget(mongo_target));
                if (!installed) {
                    std::fprintf(stderr, "ecommerce_cluster: CoInstallComponent(%s) failed: %s\n",
                                 mongo_collection.c_str(), installed.error().message.c_str());
                    finish("", 2);
                    return;
                }
            }
#endif
            /**
             * 每个进程只装配本角色的服务：gateway 不监听业务下游，catalog 不装配上游。
             * 存储模式下服务声明绑定自己拥有的资源组件。
             */
            auto svc_options = ConcurrentOptions();
#ifdef EC_STORAGE_AVAILABLE
            if (redis_address)
                svc_options.BindComponent<RedisNetResource>("cache");
            if (!mongo_collection.empty())
                svc_options.BindComponent<MongoDbResource>(mongo_collection);
#endif
            bbt::infra::result<void> svc_installed =
                mode == "gateway"
                    ? bbt_co_await app->CoInstallService<ecommerce::GatewayService>(svc_options)
                : mode == "order"
                    ? bbt_co_await app->CoInstallService<ecommerce::OrderService>(svc_options)
                : mode == "settlement"
                    ? bbt_co_await app->CoInstallService<ecommerce::SettlementService>(svc_options)
                    : bbt_co_await app->CoInstallService<ecommerce::CatalogService>(svc_options);
            if (!svc_installed) {
                std::fprintf(stderr, "ecommerce_cluster: CoInstallService(%s) failed: %s\n",
                             mode.c_str(), svc_installed.error().message.c_str());
                finish("", 2);
                return;
            }

            auto started = bbt_co_await app->CoStart(
                LifecycleOptions{}.SetDeadline(
                    std::chrono::steady_clock::now() + std::chrono::seconds{30}));
            if (!started) {
                std::fprintf(stderr, "ecommerce_cluster: CoStart failed: %s\n",
                             started.error().message.c_str());
                for (const auto& failure : app->LifecycleFailures())
                    std::fprintf(stderr, "ecommerce_cluster: lifecycle_failure: %s\n",
                                 failure.message.c_str());
                finish("", 2);
                return;
            }
            const std::string endpoint = app->BoundEndpoint();
            const std::uint16_t port = ParseEndpointPort(endpoint);
            if (endpoint.empty() || port == 0) {
                std::fprintf(stderr, "ecommerce_cluster: invalid bound endpoint '%s'\n",
                             endpoint.c_str());
                finish("", 4);
                return;
            }

            if (!register_as.empty()) {
                auto registered = bbt_co_await discovery->RegisterSelf(
                    register_as, ecommerce::Node().instance_id, "127.0.0.1", port);
                if (!registered) {
                    Journal("event=register_failed mode=" + mode +
                            " error=" + registered.error().message);
                    register_failed.store(1, std::memory_order_release);
                    app->RequestShutdown();
                }
            }

            Journal("event=ready mode=" + mode +
                    " service=" + service +
                    " instance=" + ecommerce::Node().instance_id +
                    " endpoint=" + endpoint +
                    " pid=" + pid +
                    " registered=" + (register_failed.load(std::memory_order_acquire) ? "0" : "1"));
            bound_ep.set_value(endpoint);

            const auto waited = bbt_co_await app->CoWaitShutdownRequest();
            (void)waited;

            /**
             * stdin EOF = 驱动器要求优雅退出：先注销，再收束宿主。
             */
            if (!register_as.empty()) {
                auto deregistered = bbt_co_await discovery->DeregisterSelf();
                if (!deregistered)
                    Journal("event=deregister_failed mode=" + mode +
                            " error=" + deregistered.error().message);
            }

            auto stopped = bbt_co_await app->CoStop(
                LifecycleOptions{}.SetDeadline(
                    std::chrono::steady_clock::now() + std::chrono::seconds{8}));
            for (const auto& failure : app->LifecycleFailures())
                std::fprintf(stderr, "ecommerce_cluster: lifecycle_failure: %s\n",
                             failure.message.c_str());
            rc_slot.set_value(
                stopped && app->State() == AppState::Stopped ? 0 : 4);
        },
        submitted);
    if (!submitted) {
        std::fprintf(stderr, "ecommerce_cluster: coroutine task rejected\n");
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
        const int rc = rc_slot.get_future().get();
        std::fprintf(stderr, "ecommerce_cluster: did not bind (rc=%d)\n", rc);
        discovery->Close();
        return 5;
    }
    const std::string endpoint = bound.get();
    if (endpoint.empty()) {
        /**
         * 装配/启动失败：协程侧已收口并发布真实 rc。
         */
        const int rc = rc_slot.get_future().get();
        std::fprintf(stderr, "ecommerce_cluster: startup failed (rc=%d)\n", rc);
        discovery->Close();
        return rc;
    }

    /**
     * stdin EOF = 驱动器要求优雅退出：native 线程发关闭请求（仅置位并唤醒
     * 唯一 waiter；主协程亲自注销并 CoStop 收口）。
     */
    char buffer[64];
    while (::read(STDIN_FILENO, buffer, sizeof(buffer)) > 0) {
    }
    {
        std::lock_guard<std::mutex> lk(app_mtx);
        if (app_handle)
            app_handle->RequestShutdown();
    }

    const int rc = rc_slot.get_future().get();
    if (register_failed.load(std::memory_order_acquire)) {
        discovery->Close();
        return 7;
    }
    Journal("event=stopped mode=" + args.mode +
            " instance=" + ecommerce::Node().instance_id +
            " rc=" + std::to_string(rc));
    discovery->Close();
    return rc;
}
