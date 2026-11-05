// 电商集群示例入口：按 argv 装配 gateway/order/catalog/settlement 角色。
//
// 四个角色是独立的真实进程（各自 CoApp、各自监听动态 loopback 端口）：
//   gateway  对外入口：只装配 GatewayService；动态发现 OrderService；
//   order    装配 OrderService；动态发现 CatalogService；把自己注册为 OrderService；
//   catalog  装配 CatalogService；把自己注册为 CatalogService。
//   settlement 装配独立结算服务；仅存储模式启用并拥有账单资源。
//
// 业务只给逻辑服务名，目标地址由 CoApp::find_route 从 DynamicRouteTable 解析；
// 进程里没有任何实例地址注入（没有 static_routes）。
//
// 生命周期：监听绑定后先自注册（若该角色需要），再输出 `event=ready`；stdin EOF
// 触发「注销 → 优雅关闭」。注册中心地址只经 --consul 传入，业务代码不感知后端。

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#include <unistd.h>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>

#include <bbt/framework/CoApp.hpp>

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
#endif

using namespace bbt::framework;

namespace {

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

bool ParseArgs(int argc, char* argv[], Args* out) {
    if (argc < 2) return false;
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
    if (out->consul.empty()) return false;
    return out->mode == "gateway" || out->mode == "order" ||
           out->mode == "catalog" || (out->mode == "settlement" && out->storage);
}

void Journal(const std::string& line) {
    std::printf("%s\n", line.c_str());
    std::fflush(stdout);
}

// 在受管协程里执行一次 await 型调用（注册/注销），宿主线程限时等待。
// 协程环境检查失败会抛 std::logic_error，这里如实转成失败而不是吞掉。
bool RunInCoroutine(const std::function<bbt::infra::result<void>()>& fn,
                    std::chrono::milliseconds budget, std::string* message) {
    struct Slot {
        std::mutex              mtx;
        std::condition_variable cv;
        bool                    done{false};
        bool                    ok{false};
        std::string             message;
    };
    auto slot = std::make_shared<Slot>();
    bool submitted = false;
    g_scheduler->RegistCoroutineTask(
        256 * 1024,
        [slot, fn] {
            try {
                auto r = fn();
                slot->ok = bool(r);
                if (!r) slot->message = r.error().message;
            } catch (const std::exception& e) {
                slot->message = std::string("coroutine exception: ") + e.what();
            }
            std::lock_guard<std::mutex> lk(slot->mtx);
            slot->done = true;
            slot->cv.notify_all();
        },
        submitted);
    if (!submitted) {
        *message = "coroutine registration failed";
        return false;
    }
    std::unique_lock<std::mutex> lk(slot->mtx);
    if (!slot->cv.wait_for(lk, budget, [&] { return slot->done; })) {
        *message = "coroutine did not finish before deadline";
        return false;
    }
    if (!slot->ok) *message = slot->message;
    return slot->ok;
}

ServiceOptions ConcurrentOptions() {
    ServiceOptions options;
    options.execution         = ExecutionPolicy::Concurrent;
    options.max_inflight      = 64;
    options.mailbox_capacity  = 0;
    options.max_actors        = 0;
    options.ordered_ingress   = false;
    options.max_ordered_streams      = 0;
    options.max_cached_results       = 0;
    options.max_cached_result_bytes  = 0;
    return options;
}

} // namespace

int main(int argc, char* argv[]) {
    Args args;
    if (!ParseArgs(argc, argv, &args)) {
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

    CoAppOptions options;
    options.network_limits = bbt::infra::NetworkLimits{
        64, 64, 16 * 1024, 64 * 1024, std::chrono::milliseconds{30000}};
    options.listen = bbt::infra::ListenAddress{"127.0.0.1", 0};
    options.shutdown_step_budget = std::chrono::milliseconds{8000};
    options.inbound_bridge       = RpcInboundBridge::ProtoWireV1;
    options.dynamic_routes       = table;

    CoApp app(options);
    // 每个进程只装配本角色的服务：gateway 不监听业务下游，catalog 不装配上游。
    bbt::infra::result<void> added =
        args.mode == "gateway"
            ? app.add_service<ecommerce::GatewayService>(ConcurrentOptions())
        : args.mode == "order"
            ? app.add_service<ecommerce::OrderService>(ConcurrentOptions())
        : args.mode == "settlement"
            ? app.add_service<ecommerce::SettlementService>(ConcurrentOptions())
            : app.add_service<ecommerce::CatalogService>(
                  ConcurrentOptions());
    if (!added) {
        std::fprintf(stderr, "add_service(%s) failed: %s\n", args.mode.c_str(),
                     added.error().message.c_str());
        return 4;
    }

    // 发现组件经资源工厂装配：Create 发生在 Scheduler 已启动、网络 Create 之前，
    // 其 Engine 线程才有可用的协程运行时；框架负责最终 Close。
    std::atomic<ConsulDiscoveryComponent*> discovery{nullptr};
    ConsulDiscoveryComponent::Options discovery_options;
    discovery_options.agent_url      = args.consul;
    discovery_options.watch_services = watches;
    discovery_options.request_timeout = std::chrono::milliseconds{3000};
    discovery_options.query_wait      = std::chrono::milliseconds{2000};
    if (auto r = app.add_resource<ConsulDiscoveryComponent>(
            "consul-discovery",
            [&]() -> bbt::infra::result<
                      std::shared_ptr<ConsulDiscoveryComponent>> {
                auto created = ConsulDiscoveryComponent::Create(
                    discovery_options, table);
                if (!created) return created;
                discovery.store(created.value().get());
                return created;
            });
        !r) {
        std::fprintf(stderr, "add_resource(consul-discovery) failed: %s\n",
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
        auto address = bbt::infra::net::Address::Ip(args.redis.substr(0, colon), static_cast<std::uint16_t>(port), address_error);
        if (address_error || !address.Valid())
            return 65;
        if (auto r = app.add_resource<RedisNetResource>("cache", [address] {
                auto created = RedisNetResource::Create(address);
                if (!created)
                    return created;
                auto client = created.value();
                std::string message;
                const auto budget = bbt::infra::CallOptions{std::chrono::steady_clock::now() + std::chrono::seconds{3}};
                if (!RunInCoroutine([client, budget] { return bbt_co_await client->Connect(budget); },
                                    std::chrono::seconds{4}, &message)) {
                    client->Close();
                    return bbt::infra::result<std::shared_ptr<RedisNetResource>>::err(
                        MakeError(ErrorCode::Unavailable, "redis startup: " + message));
                }
                return created;
            });
            !r)
            return 65;
    }
    if (args.storage && (args.mode == "order" || args.mode == "settlement")) {
        const std::string collection = args.mode == "order" ? "orders" : "bills";
        bbt::infra::mongo::MongoRuntimeConfig config{
            args.mongo, 2, 64, std::chrono::milliseconds{2000}, std::chrono::milliseconds{2000},
            std::chrono::milliseconds{2000}, std::chrono::milliseconds{2000}};
        const bbt::infra::mongo::MongoTarget target{args.database, collection};
        if (auto r = app.add_resource<MongoDbResource>(collection, [config, target] {
                auto created = MongoDbResource::Create(config, target);
                if (!created)
                    return created;
                auto db = created.value();
                std::string message;
                if (!RunInCoroutine([db] {
                        auto filter = bbt::infra::mongo::MongoDocument::FromJson("{}");
                        if (!filter)
                            return bbt::infra::result<void>::err(filter.error());
                        auto read = bbt_co_await db->FindOne(filter.value(), bbt::infra::CallOptions{
                                                                                 std::chrono::steady_clock::now() + std::chrono::seconds{3}});
                        if (!read)
                            return bbt::infra::result<void>::err(read.error());
                        return bbt::infra::result<void>::ok();
                    },
                                    std::chrono::seconds{4}, &message)) {
                    db->Close();
                    return bbt::infra::result<std::shared_ptr<MongoDbResource>>::err(
                        MakeError(ErrorCode::Unavailable, "mongo startup: " + message));
                }
                return created;
            });
            !r)
            return 65;
    }
#endif
    int rc = -1;
    std::thread runner([&] { rc = app.run(); });

    std::string endpoint;
    for (int i = 0; i < 400; ++i) {
        endpoint = app.bound_endpoint();
        if (!endpoint.empty()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds{25});
    }
    if (endpoint.empty()) {
        app.request_shutdown();
        if (runner.joinable()) runner.join();
        std::fprintf(stderr, "ecommerce_cluster: did not bind (rc=%d)\n", rc);
        return 5;
    }
    const std::size_t colon = endpoint.rfind(':');
    const std::uint16_t port = static_cast<std::uint16_t>(
        std::stoi(endpoint.substr(colon + 1)));

    if (!register_as.empty()) {
        ConsulDiscoveryComponent* component = discovery.load();
        if (component == nullptr) {
            app.request_shutdown();
            if (runner.joinable()) runner.join();
            std::fprintf(stderr, "ecommerce_cluster: discovery component missing\n");
            return 6;
        }
        std::string message;
        const bool registered = RunInCoroutine(
            [&] {
                return bbt_co_await component->RegisterSelf(
                    register_as, ecommerce::Node().instance_id, "127.0.0.1",
                    port);
            },
            std::chrono::milliseconds{8000}, &message);
        if (!registered) {
            Journal("event=register_failed mode=" + args.mode +
                    " error=" + message);
            app.request_shutdown();
            if (runner.joinable()) runner.join();
            return 7;
        }
    }

    Journal("event=ready mode=" + args.mode +
            " service=" + service +
            " instance=" + ecommerce::Node().instance_id +
            " endpoint=" + endpoint +
            " pid=" + pid +
            " registered=" + (register_as.empty() ? "0" : "1"));

    // stdin EOF = 驱动器要求优雅退出：先注销，再收束宿主。
    char buffer[64];
    while (::read(STDIN_FILENO, buffer, sizeof(buffer)) > 0) {
    }

    if (!register_as.empty() && discovery.load() != nullptr) {
        ConsulDiscoveryComponent* component = discovery.load();
        std::string message;
        const bool deregistered = RunInCoroutine(
            [&] { return bbt_co_await component->DeregisterSelf(); },
            std::chrono::milliseconds{8000}, &message);
        if (!deregistered)
            Journal("event=deregister_failed mode=" + args.mode +
                    " error=" + message);
    }

    app.request_shutdown();
    if (runner.joinable()) runner.join();
    Journal("event=stopped mode=" + args.mode +
            " instance=" + ecommerce::Node().instance_id +
            " rc=" + std::to_string(rc));
    return rc;
}
