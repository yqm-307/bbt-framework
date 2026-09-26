// framework F2-b3：OrderedIngress 真实 HTTP loopback 接线验收。
// 覆盖无票拒绝、grant、成功执行、重复重放、内容冲突与序号缺口；
// 通过真实 InfraHttpHost + HttpClient，不直接调用 InboundDispatcher。
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <utility>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/infra/HttpClient.hpp>
#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/Framework.hpp>
#include <bbt/framework/internal/ActorRegistry.hpp>
#include <bbt/framework/internal/CoAppSeam.hpp>
#include <bbt/framework/internal/InboundDispatcher.hpp>
#include <bbt/framework/internal/MethodTable.hpp>
#include <bbt/framework/internal/OrderedIngress.hpp>
#include <bbt/framework/internal/InfraHttpHost.hpp>
#include <bbt/framework/internal/RpcHttpBridge.hpp>

namespace fw = bbt::framework;
namespace inf = bbt::infra;
namespace co = bbt::coroutine;
using Clock = std::chrono::steady_clock;

namespace {

std::atomic<int>* g_hits = nullptr;

class OrderedSvc final : public fw::CoService<OrderedSvc> {
public:
    static constexpr std::string_view kServiceName = "ordered";

    fw::CoRpcResp Echo(fw::CoRpcReq req) {
        auto args = req.Parse<std::string, std::int32_t>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        if (g_hits != nullptr) g_hits->fetch_add(1, std::memory_order_acq_rel);
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
                value == -2 ? "not-a-number" : value == -3 ? "7" : "INV-1");
            return fw::CoRpcResp::Error(std::move(error));
        }
        return fw::CoRpcResp::From(value);
    }

    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::ActorMethodAt<&OrderedSvc::Echo, std::string>("echo"));
};

fw::CoRpcReq MakeRequest(const std::string& actor, std::int32_t value) {
    auto req = fw::CoRpcReq::From(std::tuple{actor, value});
    if (!req)
        throw std::logic_error("request encode failed");
    return std::move(req).value();
}

inf::RpcEnvelope MakeEnvelope(const std::string& rid,
                              const std::string& actor,
                              std::int32_t value,
                              std::optional<std::uint64_t> sequence) {
    inf::RpcEnvelope env;
    env.service = "ordered";
    env.method = "echo";
    env.request_id = rid;
    env.request_schema = std::string(fw::kCoRpcPositionalSchema);
    env.response_schema = std::string(fw::kCoRpcPositionalSchema);
    env.payload = MakeRequest(actor, value).payload();
    if (sequence) {
        env.metadata.emplace_back("fw.producer_id", "producer.test");
        env.metadata.emplace_back("fw.producer_epoch", "producer-epoch-1");
        env.metadata.emplace_back("fw.receiver_epoch", "receiver-epoch-1");
        env.metadata.emplace_back("fw.sequence", std::to_string(*sequence));
    }
    return env;
}

struct CallState {
    std::optional<fw::result<inf::RpcEnvelope>> out;
    std::atomic<bool> done{false};
};

struct Fixture {
    inf::NetworkLimits limits{64, 64, 16384, 65536,
                               std::chrono::milliseconds{30000}};
    std::shared_ptr<fw::InfraHttpHost> host;
    std::shared_ptr<inf::NetworkRuntime> client_runtime;
    std::shared_ptr<inf::HttpClient> client;
    std::unique_ptr<fw::CoApp> app;
    std::thread run_thread;
    std::atomic<bool> run_done{false};
    int run_rc = -1;
    std::string endpoint;

    void Start(std::atomic<int>& hits) {
        g_hits = &hits;
        host = std::make_shared<fw::InfraHttpHost>(
            limits, inf::ListenAddress{"127.0.0.1", 0});

        fw::CoAppOptions options;
        options.network_limits = limits;
        options.listen = inf::ListenAddress{"127.0.0.1", 0};
        options.shutdown_step_budget = std::chrono::milliseconds{2000};

        fw::CoAppSeam seam;
        seam.host = host;
        app = fw::MakeCoAppForTest(options, std::move(seam));
        const fw::ServiceOptions invalid_concurrent{
            fw::ExecutionPolicy::Concurrent, 64, 0, 0,
            true, 8, 64, 65536};
        const auto invalid_registration =
            app->add_service<OrderedSvc>(invalid_concurrent);
        BOOST_REQUIRE(!invalid_registration);
        BOOST_CHECK_EQUAL(
            static_cast<int>(invalid_registration.error().code),
            static_cast<int>(fw::ErrorCode::InvalidArgument));

        const fw::ServiceOptions service_options{
            fw::ExecutionPolicy::ActorSerial, 64, 16, 8,
            true, 8, 64, 65536};
        BOOST_REQUIRE(app->add_service<OrderedSvc>(service_options));

        fw::OrderedGrant grant;
        grant.service = "ordered";
        grant.actor_key = "acct-1";
        grant.producer_id = "producer.test";
        grant.producer_epoch = "producer-epoch-1";
        grant.receiver_epoch = "receiver-epoch-1";
        // Infra HTTP loopback has no authenticated principal; empty means the
        // anonymous loopback identity and is matched exactly by the ingress.
        grant.peer_principal.clear();
        BOOST_REQUIRE(app->grant_ordered_stream(std::move(grant)));
        fw::InstallRpcHttpBridge(*host, *app);

        g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
        g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
        run_thread = std::thread([this] {
            run_rc = app->run();
            run_done.store(true, std::memory_order_release);
        });

        const auto until = Clock::now() + std::chrono::seconds{15};
        while (endpoint.empty() && !run_done.load(std::memory_order_acquire) &&
               Clock::now() < until) {
            endpoint = host->bound_endpoint();
            std::this_thread::yield();
        }
        BOOST_REQUIRE_MESSAGE(!endpoint.empty(), "HTTP loopback did not bind");

        auto runtime = inf::NetworkRuntime::Create(limits);
        BOOST_REQUIRE(runtime);
        client_runtime = runtime.value();
        BOOST_REQUIRE(client_runtime->Start());
        auto created = client_runtime->CreateHttpClient();
        BOOST_REQUIRE(created);
        client = created.value();
    }

    fw::result<inf::RpcEnvelope> Call(const inf::RpcEnvelope& env) {
        auto state = std::make_shared<CallState>();
        const auto request = env;
        bool registered = false;
        g_scheduler->RegistCoroutineTask(
            [this, state, request] {
                inf::CallOptions options;
                options.deadline = Clock::now() + std::chrono::seconds{10};
                auto response = client->Request(
                    fw::http_bridge::ToHttpRequest(
                        request, "http://" + endpoint + "/rpc"), options);
                if (!response)
                    state->out.emplace(fw::result<inf::RpcEnvelope>::err(
                        std::move(response.error())));
                else
                    state->out.emplace(fw::http_bridge::EnvelopeFromResponse(
                        response.value()));
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

    fw::result<inf::RpcEnvelope> CallDirect(
        fw::InboundDispatcher& dispatcher, const inf::RpcEnvelope& env) {
        auto state = std::make_shared<CallState>();
        const auto request = env;
        bool registered = false;
        g_scheduler->RegistCoroutineTask(
            [&dispatcher, state, request] {
                inf::IncomingCallContext incoming;
                incoming.deadline = Clock::now() + std::chrono::seconds{10};
                state->out.emplace(dispatcher.Dispatch(incoming, request));
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
        if (client) client->RequestClose();
        if (client_runtime) client_runtime->RequestClose();
        if (app) app->request_shutdown();
        if (run_thread.joinable()) run_thread.join();
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
    BOOST_CHECK(replay.value().payload == accepted.value().payload);
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

    fw::ActorRegistry registry;
    BOOST_REQUIRE(registry.RegisterFactory(
        "ordered", [] { return std::make_shared<OrderedSvc>(); }, 16));
    auto table = fw::BuildMethodTable<OrderedSvc>();
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
    const fw::ServiceOptions opts{
        fw::ExecutionPolicy::ActorSerial, 64, 16, 8,
        true, 8, 64, 65536};
    std::map<std::string, fw::InboundDispatcher::ServiceEntry> services;
    services.emplace("ordered", fw::InboundDispatcher::ServiceEntry{
        opts, &table, nullptr, &registry, ingress});
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
    const fw::ServiceOptions invalid{
        fw::ExecutionPolicy::Concurrent, 64, 0, 0,
        true, 8, 64, 65536};
    const auto validated = fw::ValidateServiceOptions(invalid);
    BOOST_REQUIRE(!validated);
    BOOST_CHECK_EQUAL(static_cast<int>(validated.error().code),
                      static_cast<int>(fw::ErrorCode::InvalidArgument));
    BOOST_CHECK(validated.error().message.find("ordered_ingress") !=
                std::string::npos);
}
