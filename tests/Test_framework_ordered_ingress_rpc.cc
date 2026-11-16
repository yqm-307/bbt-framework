/**
 * OrderedIngress 真实 V2 会话接线验收。
 * 覆盖无票拒绝、grant、成功执行、重复重放、内容冲突与序号缺口；
 * 入站经真实 InfraHttpHost + V2 Call（SessionFrame），出站客户端是互异身份
 * 的第二个会话 owner；不直接调用业务 handler。
 *
 * 唯一公开 API 迁移：
 * - 服务端装配为 CoInstallService + grant_ordered_stream + CoStart（受管
 *   协程）；宿主经测试缝注入（本用例注入的是真实 InfraHttpHost 以便
 *   观察真实 loopback），入站接线由测试侧 admission 复刻器完成（生产
 *   InstallRpcAdmission 需要 m_runtime，候选公开面没有对应访问口——见
 *   交接报告「公共缺口」）；DispatchInboundForTest 不接受 on_dispatched，
 *   测试侧在挂起前先放行同步派发门（如实留档，本用例流量串行不依赖
 *   门内并发序）。
 */
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <utility>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/framework/internal/InfraHttpHost.hpp>
#include <bbt/framework/internal/RpcBridge.hpp>
#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/Framework.hpp>
#include <bbt/framework/internal/CoAppSeam.hpp>
#include <bbt/framework/internal/ErrorDomainRule.hpp>
#include <bbt/framework/internal/InboundDispatcher.hpp>
#include <bbt/framework/internal/MethodTable.hpp>
#include <bbt/framework/internal/NetworkHost.hpp>
#include <bbt/framework/internal/OrderedIngress.hpp>
#include <bbt/infra/rpc/Request.hpp>
#include <bbt/infra/rpc/Response.hpp>

namespace fw = bbt::framework;
namespace inf = bbt::infra;
namespace co = bbt::coroutine;
using Clock = std::chrono::steady_clock;

namespace {

std::atomic<int>* g_hits = nullptr;

class OrderedSvc final : public fw::CoService<> {
public:
    static constexpr std::string_view kServiceName = "ordered";
    explicit OrderedSvc(fw::detail::ConstructKey k) : fw::CoService<>(k) {}

    fw::CoRpcResp Echo(fw::CoRpcReq req) {
        auto args = req.Parse<std::string, std::int32_t>();
        if (!args)
            return fw::CoRpcResp::Error(args.error());
        if (g_hits != nullptr)
            g_hits->fetch_add(1, std::memory_order_acq_rel);
        const auto value = std::get<1>(args.value());
        if (value == -1)
            return fw::CoRpcResp::Error(
                fw::MakeError(fw::ErrorCode::RemoteError, "business failure"));
        if (value == -2 || value == -3 || value == -4) {
            auto error = fw::MakeError(fw::ErrorCode::RemoteError,
                                       "ordered business failure");
            error.domain = value == -4 ? "app.billing" : "framework.actor";
            error.domain_code = value == -4 ? "InvoiceDenied" : "SequenceGap";
            error.details.emplace_back(
                value == -4 ? "invoice_id" : "expected_sequence",
                value == -2 ? "not-a-number" : value == -3 ? "7"
                                                           : "INV-1");
            return fw::CoRpcResp::Error(std::move(error));
        }
        return fw::CoRpcResp::From(value);
    }

    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::ActorMethodAt<&OrderedSvc::Echo, std::string>("echo"));

protected:
    ~OrderedSvc() noexcept override = default;
private:
    friend class fw::detail::Registry;
};

/**
 * 取已安装服务的受管实例（锁内取 owner+指针，保活纪律与宿主一致）。
 */
template <class S>
S* ManagedService(fw::CoApp& app, std::shared_ptr<void>& owner) {
    const auto reg = app.RegistryForTest();
    std::lock_guard<std::mutex> lk(reg->Mutex());
    auto* e = reg->FindByTypeNameLocked(
        fw::ObjectKind::Service, S::kServiceName);
    if (!e) return nullptr;
    owner = e->object;
    return static_cast<S*>(e->object.get());
}

/**
 * 测试侧入站接线复刻器：与生产 InstallRpcAdmission 同一纪律——同步派发门
 * 在任何可挂起动作之前释放，回复经 ValidateErrorAtBoundary 边界校验。
 * app 经 shared_ptr 槽位间接持有（装配在受管协程内完成后回填）。
 */
void InstallAdmissionForTest(
    const std::shared_ptr<fw::InfraHttpHost>& host,
    const std::shared_ptr<std::shared_ptr<fw::CoApp>>& app_slot) {
    host->InstallRpcHandler(
        [app_slot](const inf::rpc::Request& request,
                   const std::function<void()>& dispatched)
            -> fw::result<inf::rpc::Response> {
            /**
             * DispatchInboundForTest 不透传 on_dispatched（候选缝签名缺口，
             * 见交接）：测试侧在挂起前先放行同步派发门；本文件流量串行，
             * 不依赖门内并发序。
             */
            dispatched();
            auto reply = fw::DispatchInboundForTest(**app_slot, request);
            if (!reply) {
                if (auto valid = fw::ValidateErrorAtBoundary(reply.error());
                    !valid)
                    return fw::result<inf::rpc::Response>::err(
                        std::move(valid.error()));
            }
            return reply;
        });
}

fw::CoRpcReq MakeRequest(const std::string& actor, std::int32_t value) {
    auto req = fw::CoRpcReq::From(std::tuple{actor, value});
    if (!req)
        throw std::logic_error("request encode failed");
    return std::move(req).value();
}

/**
 * V2 Call 请求：五关联字段 + 有序流 fw.* 元数据（framework 白名单内）。
 */
inf::rpc::Request MakeEnvelope(const std::string& rid,
                               const std::string& actor,
                               std::int32_t value,
                               std::optional<std::uint64_t> sequence) {
    inf::rpc::Request env;
    env.SetService("ordered")
        .SetMethod("echo")
        .SetRequestId(rid)
        .SetRequestSchema(std::string(fw::kCoRpcPositionalSchema))
        .SetResponseSchema(std::string(fw::kCoRpcPositionalSchema))
        .SetReply(inf::rpc::ReplyMode::Required)
        .SetTraffic(inf::rpc::TrafficClass::Actor);
    env.SetPayload(MakeRequest(actor, value).payload());
    if (sequence) {
        env.SetMetadata({
            {"fw.producer_id", "producer.test"},
            {"fw.producer_epoch", "producer-epoch-1"},
            {"fw.receiver_epoch", "receiver-epoch-1"},
            {"fw.sequence", std::to_string(*sequence)},
        });
    }
    return env;
}

struct CallState {
    std::optional<fw::result<inf::rpc::Response>> out;
    std::atomic<bool> done{false};
};

/**
 * 生命周期任务状态：装配在受管协程内完成（无效注册断言的证据经
 * invalid_code 回传控制线程断言）。
 */
struct ServerState {
    std::shared_ptr<std::shared_ptr<fw::CoApp>> app_slot =
        std::make_shared<std::shared_ptr<fw::CoApp>>();
    std::promise<bool>                       started;
    std::atomic<bool>                        done{false};
    int                                      rc{-1};
    fw::ErrorCode                            invalid_code{};
    bool                                     invalid_rejected{false};
};

struct Fixture {
    inf::NetworkLimits limits{64, 64, 16384, 65536,
                              std::chrono::milliseconds{30000}};
    std::shared_ptr<fw::InfraHttpHost> host;
    std::shared_ptr<fw::InfraHttpHost> client_runtime;
    std::shared_ptr<fw::RpcEgress> egress;
    std::shared_ptr<ServerState> server = std::make_shared<ServerState>();
    std::string endpoint;

    std::shared_ptr<fw::CoApp> App() const { return *server->app_slot; }

    void Start(std::atomic<int>& hits) {
        g_hits = &hits;
        host = std::make_shared<fw::InfraHttpHost>(
            limits, inf::ListenAddress{"127.0.0.1", 0}, "ordered-server");
        InstallAdmissionForTest(host, server->app_slot);

        /**
         * Registered driver capacity: server 2 + host egress 1 + test client 1 + runnable worker 1.
         */
        if (!g_scheduler->IsInitialized()) {
            g_bbt_coroutine_config->m_cfg_static_thread_num = 5;
            g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
            g_scheduler->Start();
        }

        {
            bool submitted = false;
            g_scheduler->RegistCoroutineTask(
                [this] {
                    auto st = server;
                    auto set_rc = [&](int rc) {
                        st->rc = rc;
                        st->done.store(true, std::memory_order_release);
                    };
                    try {
                        fw::AppOptions options;
                        options.SetLimits(64, 64, 64, 64)
                            .SetShutdownBudget(
                                std::chrono::milliseconds{2000})
                            .SetLifecycleBudget(
                                std::chrono::milliseconds{2000})
                            .SetNetworkLimits(limits)
                            .SetLocalIdentity("ordered-server");
                        *st->app_slot = fw::MakeCoAppForTest(
                            options, fw::CoAppSeam{host, {}});
                        auto& app = *st->app_slot;

                        const fw::ServiceOptions invalid_concurrent =
                            fw::ServiceOptions{}
                                .SetExecution(fw::ExecutionPolicy::Concurrent)
                                .SetMaxInflight(64)
                                .SetOrderedIngress(fw::OrderedIngressOptions{
                                    /*enabled*/ true, 8, 64, 65536});
                        const auto invalid_registration = bbt_co_await
                            app->CoInstallService<OrderedSvc>(
                                invalid_concurrent);
                        st->invalid_rejected = !invalid_registration;
                        if (invalid_registration)
                            st->invalid_code =
                                invalid_registration.error().code;

                        const fw::ServiceOptions service_options =
                            fw::ServiceOptions{}
                                .SetExecution(fw::ExecutionPolicy::ActorSerial)
                                .SetMaxInflight(64)
                                .SetMaxSerialKeys(8)
                                .SetSerialQueueCapacity(16)
                                .SetOrderedIngress(fw::OrderedIngressOptions{
                                    /*enabled*/ true, 8, 64, 65536});
                        auto r = bbt_co_await
                            app->CoInstallService<OrderedSvc>(service_options);
                        if (!r) { set_rc(2); return; }

                        fw::OrderedGrant grant;
                        grant.service = "ordered";
                        grant.actor_key = "acct-1";
                        grant.producer_id = "producer.test";
                        grant.producer_epoch = "producer-epoch-1";
                        grant.receiver_epoch = "receiver-epoch-1";
                        /**
                         * Infra HTTP loopback has no authenticated principal; empty means the
                         * anonymous loopback identity and is matched exactly by the ingress.
                         */
                        grant.peer_principal.clear();
                        if (auto g = app->grant_ordered_stream(std::move(grant));
                            !g) { set_rc(2); return; }

                        if (auto s = bbt_co_await app->CoStart(); !s) {
                            set_rc(2); return;
                        }
                        st->started.set_value(true);
                        { auto _r = bbt_co_await app->CoWaitShutdownRequest(); (void)_r; }
                        { auto _r = bbt_co_await app->CoStop(); (void)_r; }
                        set_rc(app->State() == fw::AppState::Stopped ? 0 : 4);
                    } catch (...) {
                        try { st->started.set_value(false); } catch (...) {}
                        set_rc(2);
                    }
                }, submitted);
            if (!submitted) {
                server->rc = 2;
                server->done.store(true, std::memory_order_release);
            }
        }

        auto started = server->started.get_future();
        BOOST_REQUIRE_MESSAGE(
            started.wait_for(std::chrono::seconds{15}) ==
                std::future_status::ready &&
                started.get(),
            "server task did not start");

        const auto until = Clock::now() + std::chrono::seconds{15};
        while (endpoint.empty() && !server->done.load() &&
               Clock::now() < until) {
            endpoint = host->bound_endpoint();
            std::this_thread::yield();
        }
        BOOST_REQUIRE_MESSAGE(!endpoint.empty(), "HTTP loopback did not bind");

        /**
         * 非法注册断言（候选选项违例在服务装配期如实拒绝）。
         */
        BOOST_REQUIRE(server->invalid_rejected);
        BOOST_CHECK_EQUAL(
            static_cast<int>(server->invalid_code),
            static_cast<int>(fw::ErrorCode::InvalidArgument));

        auto runtime = std::make_shared<fw::InfraHttpHost>(
            limits, inf::ListenAddress{"127.0.0.1", 0}, "ordered-client");
        BOOST_REQUIRE(runtime);
        client_runtime = runtime;
        BOOST_REQUIRE(client_runtime->Create());
        BOOST_REQUIRE(client_runtime->Start());
        BOOST_REQUIRE(client_runtime->rpc_client() != nullptr);
        egress = std::make_shared<fw::RpcEgress>(client_runtime);
    }

    fw::result<inf::rpc::Response> Call(const inf::rpc::Request& env) {
        auto state = std::make_shared<CallState>();
        const auto request = env;
        const std::string target = "tcp://" + endpoint;
        bool registered = false;
        g_scheduler->RegistCoroutineTask(
            [state, request, target, egress = egress] {
                inf::CallOptions options;
                options.deadline = Clock::now() + std::chrono::seconds{10};
                state->out.emplace(bbt_co_await egress->Call(target, request, options));
                state->done.store(true, std::memory_order_release);
            },
            registered);
        BOOST_REQUIRE(registered);
        const auto until = Clock::now() + std::chrono::seconds{10};
        while (!state->done.load(std::memory_order_acquire) &&
               Clock::now() < until)
            std::this_thread::yield();
        BOOST_REQUIRE(state->done.load(std::memory_order_acquire));
        return std::move(*state->out);
    }

    /**
     * 直接派发（机器面）：Context 由测试构造（无 infra 填充），断言
     * Dispatcher + OrderedIngress 组件语义核与重放边界。
     */
    fw::result<inf::rpc::Response> CallDirect(
        fw::InboundDispatcher& dispatcher, const inf::rpc::Request& env) {
        auto state = std::make_shared<CallState>();
        const auto request = env;
        bool registered = false;
        g_scheduler->RegistCoroutineTask(
            [&dispatcher, state, request] {
                state->out.emplace(dispatcher.Dispatch(request));
                state->done.store(true, std::memory_order_release);
            },
            registered);
        BOOST_REQUIRE(registered);
        const auto until = Clock::now() + std::chrono::seconds{10};
        while (!state->done.load(std::memory_order_acquire) &&
               Clock::now() < until)
            std::this_thread::yield();
        BOOST_REQUIRE(state->done.load(std::memory_order_acquire));
        return std::move(*state->out);
    }

    ~Fixture() {
        /**
         * infra 关闭是同步契约：Close() 返回即物理释放。
         */
        if (client_runtime)
            client_runtime->Close();
        if (App())
            App()->RequestShutdown();
        const auto until = Clock::now() + std::chrono::seconds{10};
        while (!server->done.load(std::memory_order_acquire) &&
               Clock::now() < until)
            std::this_thread::yield();
        if (host)
            host->Close();
        g_hits = nullptr;
    }
};

} // namespace

BOOST_AUTO_TEST_CASE(ordered_ingress_loopback_replays_and_rejects_conflicts) {
    std::atomic<int> hits{0};
    Fixture fixture;
    fixture.Start(hits);

    auto no_stamp = fixture.Call(MakeEnvelope("no-stamp", "acct-1", 10,
                                              std::nullopt));
    BOOST_REQUIRE(!no_stamp);
    BOOST_CHECK_EQUAL(no_stamp.error().domain_code, "OrderedStampRequired");
    BOOST_CHECK_EQUAL(hits.load(), 0);

    const auto first = MakeEnvelope("ordered-1", "acct-1", 10, 1);
    auto accepted = fixture.Call(first);
    BOOST_REQUIRE(accepted);
    BOOST_CHECK_EQUAL(hits.load(), 1);

    auto replay = fixture.Call(first);
    BOOST_REQUIRE(replay);
    BOOST_CHECK(replay.value().Payload() == accepted.value().Payload());
    BOOST_CHECK_EQUAL(hits.load(), 1);

    auto conflict = fixture.Call(
        MakeEnvelope("ordered-1", "acct-1", 11, 1));
    BOOST_REQUIRE(!conflict);
    BOOST_CHECK_EQUAL(conflict.error().domain_code, "SequenceConflict");
    BOOST_CHECK_EQUAL(hits.load(), 1);

    auto gap = fixture.Call(
        MakeEnvelope("ordered-3", "acct-1", 13, 3));
    BOOST_REQUIRE(!gap);
    BOOST_CHECK_EQUAL(gap.error().domain_code, "SequenceGap");
    BOOST_CHECK_EQUAL(hits.load(), 1);

    auto second = fixture.Call(
        MakeEnvelope("ordered-2", "acct-1", 12, 2));
    BOOST_REQUIRE(second);
    BOOST_CHECK_EQUAL(hits.load(), 2);

    auto failed = fixture.Call(
        MakeEnvelope("ordered-3", "acct-1", -1, 3));
    BOOST_REQUIRE(!failed);
    BOOST_CHECK_EQUAL(hits.load(), 3);
    auto failed_replay = fixture.Call(
        MakeEnvelope("ordered-3", "acct-1", -1, 3));
    BOOST_REQUIRE(!failed_replay);
    BOOST_CHECK_EQUAL(failed_replay.error().message, failed.error().message);
    BOOST_CHECK_EQUAL(hits.load(), 3);
}

BOOST_AUTO_TEST_CASE(ordered_direct_error_boundary_matches_replay) {
    std::atomic<int> hits{0};
    Fixture fixture;
    fixture.Start(hits);

    /**
     * 直挂分发表项：受管实例 + 独立 OrderedIngress（与生产装配同一配置
     * 来源）；无执行租约（无网络簿记的测试构造，lease 为空直接执行）。
     */
    std::shared_ptr<void> owner;
    auto* instance = ManagedService<OrderedSvc>(*fixture.App(), owner);
    BOOST_REQUIRE(instance != nullptr);

    auto table = std::make_shared<const fw::RpcMethodTable>(
        fw::BuildMethodTable<OrderedSvc>());
    auto created = fw::OrderedIngress::Create(
        fw::OrderedIngressConfig{"ordered", 8, 64, 65536});
    BOOST_REQUIRE(created);
    auto ingress = created.value();
    fw::OrderedGrant grant;
    grant.service = "ordered";
    grant.actor_key = "acct-1";
    grant.producer_id = "producer.test";
    grant.producer_epoch = "producer-epoch-1";
    grant.receiver_epoch = "receiver-epoch-1";
    BOOST_REQUIRE(ingress->GrantStream(grant));
    const fw::ServiceOptions opts =
        fw::ServiceOptions{}
            .SetExecution(fw::ExecutionPolicy::ActorSerial)
            .SetMaxInflight(64)
            .SetMaxSerialKeys(8)
            .SetSerialQueueCapacity(16)
            .SetOrderedIngress(fw::OrderedIngressOptions{
                /*enabled*/ true, 8, 64, 65536});
    std::map<std::string, fw::InboundDispatcher::ServiceEntry> services;
    fw::InboundDispatcher::ServiceEntry entry;
    entry.options = opts;
    entry.table = std::move(table);
    entry.instance = instance;
    entry.ordered_ingress = ingress;
    services.emplace("ordered", std::move(entry));
    fw::InboundDispatcher dispatcher(std::move(services));

    const auto invalid = MakeEnvelope("invalid-1", "acct-1", -2, 1);
    auto first = fixture.CallDirect(dispatcher, invalid);
    auto replay = fixture.CallDirect(dispatcher, invalid);
    BOOST_REQUIRE(!first);
    BOOST_REQUIRE(!replay);
    BOOST_CHECK(first.error().code == fw::ErrorCode::ProtocolError);
    BOOST_CHECK(replay.error().code == fw::ErrorCode::ProtocolError);
    BOOST_CHECK(first.error().details.empty());
    BOOST_CHECK(replay.error().details.empty());
    BOOST_CHECK_EQUAL(first.error().domain, replay.error().domain);
    BOOST_CHECK_EQUAL(hits.load(), 1);

    const auto actor = MakeEnvelope("valid-actor-2", "acct-1", -3, 2);
    auto actor_first = fixture.CallDirect(dispatcher, actor);
    auto actor_replay = fixture.CallDirect(dispatcher, actor);
    BOOST_REQUIRE(!actor_first);
    BOOST_REQUIRE(!actor_replay);
    BOOST_CHECK_EQUAL(actor_first.error().domain, "framework.actor");
    BOOST_CHECK_EQUAL(actor_first.error().domain_code, "SequenceGap");
    BOOST_REQUIRE_EQUAL(actor_first.error().details.size(), 1u);
    BOOST_CHECK_EQUAL(actor_first.error().details[0].second, "7");
    BOOST_CHECK(actor_replay.error().details == actor_first.error().details);

    const auto extension = MakeEnvelope("valid-extension-3", "acct-1", -4, 3);
    auto app_first = fixture.CallDirect(dispatcher, extension);
    auto app_replay = fixture.CallDirect(dispatcher, extension);
    BOOST_REQUIRE(!app_first);
    BOOST_REQUIRE(!app_replay);
    BOOST_CHECK_EQUAL(app_first.error().domain, "app.billing");
    BOOST_CHECK_EQUAL(app_first.error().domain_code, "InvoiceDenied");
    BOOST_REQUIRE_EQUAL(app_first.error().details.size(), 1u);
    BOOST_CHECK_EQUAL(app_first.error().details[0].second, "INV-1");
    BOOST_CHECK(app_replay.error().details == app_first.error().details);
    BOOST_CHECK_EQUAL(hits.load(), 3);
}

BOOST_AUTO_TEST_CASE(ordered_ingress_rejects_concurrent_service_options) {
    const fw::ServiceOptions invalid =
        fw::ServiceOptions{}
            .SetExecution(fw::ExecutionPolicy::Concurrent)
            .SetMaxInflight(64)
            .SetOrderedIngress(fw::OrderedIngressOptions{
                /*enabled*/ true, 8, 64, 65536});
    const auto validated = invalid.Validate();
    BOOST_REQUIRE(!validated);
    BOOST_CHECK_EQUAL(static_cast<int>(validated.error().code),
                      static_cast<int>(fw::ErrorCode::InvalidArgument));
    BOOST_CHECK(validated.error().message.find("ordered_ingress") !=
                std::string::npos);
}
