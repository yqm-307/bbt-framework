// OrderedIngress 接线验收（direct dispatch 层 + 装配校验）。
//
// 覆盖：ActorSerial 服务经 v2::CoApp 安装取实例、自装配
// ActorRegistry/OrderedIngress/InboundDispatcher 的真实分发路径（
// 不是生产 InstallRpcAdmission 路径——后者在 v2 _PublishNetworkState 里
// 尚未接 ActorRegistry/OrderedIngress（registry=nullptr），真实入站
// ActorSerial 调用在核心侧不可达，如实记录为前驱缺口）。无票拒绝、grant、
// 成功执行、重复重放、内容冲突与序号缺口在 direct dispatcher 层覆盖；
// 装配校验（Concurrent + ordered_ingress 拒配）经公开
// ValidateServiceOptions 承担。
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
#include <tuple>
#include <utility>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/framework/Framework.hpp>
#include <bbt/framework/V2.hpp>
#include <bbt/framework/internal/ActorRegistry.hpp>
#include <bbt/framework/internal/CoAppSeam.hpp>
#include <bbt/framework/internal/InboundDispatcher.hpp>
#include <bbt/framework/internal/MethodTable.hpp>
#include <bbt/framework/internal/OrderedIngress.hpp>
#include <bbt/infra/rpc/Request.hpp>
#include <bbt/infra/rpc/Response.hpp>

#include "framework_co_harness.hpp"

namespace fw = bbt::framework;
namespace v2 = bbt::framework::v2;
namespace inf = bbt::infra;
namespace co = bbt::coroutine;
using Clock = std::chrono::steady_clock;

namespace {

std::atomic<int>* g_hits = nullptr;

class OrderedSvc final : public v2::CoService<> {
public:
    static constexpr std::string_view kServiceName = "ordered";
    explicit OrderedSvc(fw::detail::ConstructKey k) : v2::CoService<>(k) {}

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
    friend class fw::detail::Registry;
};

fw::CoRpcReq MakeRequest(const std::string& actor, std::int32_t value) {
    auto req = fw::CoRpcReq::From(std::tuple{actor, value});
    if (!req)
        throw std::logic_error("request encode failed");
    return std::move(req).value();
}

// V2 Call 请求：五关联字段 + 有序流 fw.* 元数据（framework 白名单内）。
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

// 直接派发（机器面）：Context 由 infra 填充，测试不伪造入站事实。
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

} // namespace

// 装配校验：Concurrent + ordered_ingress 必须在副作用前被拒（装配校验，
// 不经受管对象）。
BOOST_AUTO_TEST_CASE(ordered_ingress_rejects_concurrent_service_options) {
    const auto o = fw::ServiceOptions{}
                       .SetMaxInflight(64)
                       .SetOrderedIngress(true, 8, 64, 65536);
    const auto validated = fw::ValidateServiceOptions(o);
    BOOST_REQUIRE(!validated);
    BOOST_CHECK_EQUAL(static_cast<int>(validated.error().code),
                      static_cast<int>(fw::ErrorCode::InvalidArgument));
    BOOST_CHECK(validated.error().message.find("ordered_ingress") !=
                std::string::npos);
}

// ActorSerial 服务实例的真实分发路径：受管 OrderedSvc（经 seam app
// CoInstallService 实例化，ConstructKey 为私有 passkey）+ 测试自有
// ActorRegistry/OrderedIngress/InboundDispatcher。不是生产 InstallRpcAdmission
// 路径（核心 _PublishNetworkState 目前对 ActorSerial/OrderedIngress 的 registry
// 未接线，入站 ActorSerial 在核心侧不可达——如实记录为前驱缺口，不以测试替身
// 冒充运行验收）。
BOOST_AUTO_TEST_CASE(ordered_direct_error_boundary_matches_replay) {
    std::atomic<int> hits{0};
    g_hits = &hits;

    framework_test::EnsureRuntime();
    auto app = framework_test::CreateV2App([] {
        return v2::CoApp::Create(
            fw::AppOptions{}
                .SetLimits(64, 64, 8, 64)
                .SetShutdownBudget(std::chrono::milliseconds{2000})
                .SetLifecycleBudget(std::chrono::milliseconds{2000}),
            "ordered-ingress-test");
    });
    BOOST_REQUIRE(app);
    framework_test::RunInCoTask([&] {
        BOOST_REQUIRE(static_cast<bool>(bbt_co_await app->CoInstallService<OrderedSvc>(
            fw::ServiceOptions{}.SetMaxInflight(64).SetActorSerial()
                                 .SetSerialQueueCapacity(16)
                                 .SetMaxSerialKeys(8)
                                 .SetOrderedIngress(true, 8, 64, 65536))));
    });

    auto instance = framework_test::ServiceInstance(*app, "ordered");
    BOOST_REQUIRE(instance);

    auto registry = std::make_shared<fw::ActorRegistry>();
    BOOST_REQUIRE(registry->RegisterFactory(
        "ordered", [&] { return instance; }, 16));
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
            .SetMaxInflight(64)
            .SetActorSerial()
            .SetSerialQueueCapacity(16)
            .SetMaxSerialKeys(8)
            .SetOrderedIngress(true, 8, 64, 65536);
    std::map<std::string, fw::InboundDispatcher::ServiceEntry> services;
    services.emplace("ordered", fw::InboundDispatcher::ServiceEntry{
                                    opts, table, nullptr, registry, ingress});
    fw::InboundDispatcher dispatcher(std::move(services));

    const auto invalid = MakeEnvelope("invalid-1", "acct-1", -2, 1);
    auto first = CallDirect(dispatcher, invalid);
    auto replay = CallDirect(dispatcher, invalid);
    BOOST_REQUIRE(!first);
    BOOST_REQUIRE(!replay);
    BOOST_CHECK(first.error().code == fw::ErrorCode::ProtocolError);
    BOOST_CHECK(replay.error().code == fw::ErrorCode::ProtocolError);
    BOOST_CHECK(first.error().details.empty());
    BOOST_CHECK(replay.error().details.empty());
    BOOST_CHECK_EQUAL(first.error().domain, replay.error().domain);
    BOOST_CHECK_EQUAL(hits.load(), 1);

    const auto actor = MakeEnvelope("valid-actor-2", "acct-1", -3, 2);
    auto actor_first = CallDirect(dispatcher, actor);
    auto actor_replay = CallDirect(dispatcher, actor);
    BOOST_REQUIRE(!actor_first);
    BOOST_REQUIRE(!actor_replay);
    BOOST_CHECK_EQUAL(actor_first.error().domain, "framework.actor");
    BOOST_CHECK_EQUAL(actor_first.error().domain_code, "SequenceGap");
    BOOST_REQUIRE_EQUAL(actor_first.error().details.size(), 1u);
    BOOST_CHECK_EQUAL(actor_first.error().details[0].second, "7");
    BOOST_CHECK(actor_replay.error().details == actor_first.error().details);

    const auto extension = MakeEnvelope("valid-extension-3", "acct-1", -4, 3);
    auto app_first = CallDirect(dispatcher, extension);
    auto app_replay = CallDirect(dispatcher, extension);
    BOOST_REQUIRE(!app_first);
    BOOST_REQUIRE(!app_replay);
    BOOST_CHECK_EQUAL(app_first.error().domain, "app.billing");
    BOOST_CHECK_EQUAL(app_first.error().domain_code, "InvoiceDenied");
    BOOST_REQUIRE_EQUAL(app_first.error().details.size(), 1u);
    BOOST_CHECK_EQUAL(app_first.error().details[0].second, "INV-1");
    BOOST_CHECK(app_replay.error().details == app_first.error().details);
    BOOST_CHECK_EQUAL(hits.load(), 3);

    framework_test::RunInCoTask([&] {
        BOOST_REQUIRE(static_cast<bool>(bbt_co_await app->CoStop()));
    });
}
