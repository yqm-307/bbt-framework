// F0：CoRpc 公共面、声明式方法表、错误边界、actor key、路由与资源缝。
// 不联网、不起真实 I/O、不启动 Scheduler。
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/Framework.hpp>
#include <bbt/framework/internal/MethodTable.hpp>

namespace fw = bbt::framework;

namespace {

struct PlayerRouteArg { std::string player_id; };

} // namespace

template <>
struct bbt::framework::RouteTraits<PlayerRouteArg> {
    static bbt::framework::result<bbt::framework::RouteFields>
    Encode(const PlayerRouteArg& arg) {
        bbt::framework::RouteFields fields;
        fields.values.emplace_back("player", arg.player_id);
        return bbt::framework::result<bbt::framework::RouteFields>::ok(
            std::move(fields));
    }
};

namespace {

template <class... Ts>
fw::CoRpcReq Request(Ts&&... values) {
    auto req = fw::CoRpcReq::From(
        std::tuple<std::decay_t<Ts>...>{std::forward<Ts>(values)...});
    if (!req)
        throw std::logic_error("test request encoding failed");
    return std::move(req).value();
}

class EchoService final : public fw::CoService<EchoService> {
public:
    static constexpr std::string_view kServiceName = "echo";

    fw::CoRpcResp Echo(fw::CoRpcReq req) {
        auto args = req.Parse<std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        return fw::CoRpcResp::From("echo:" + args.value());
    }
    fw::CoRpcResp Reset(fw::CoRpcReq req) {
        auto args = req.Parse<std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        return fw::CoRpcResp::From(std::tuple<>{});
    }
    fw::CoRpcResp Notify(fw::CoRpcReq req) {
        auto args = req.Parse<std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        m_last_notify = args.value();
        return fw::CoRpcResp::From(std::tuple<>{});
    }
    fw::CoRpcResp Fail(fw::CoRpcReq) {
        return fw::CoRpcResp::Error(
            fw::MakeError(fw::ErrorCode::InvalidArgument, "rejected"));
    }
    fw::CoRpcResp Boom(fw::CoRpcReq) {
        throw std::runtime_error("boom");
    }
    fw::CoRpcResp WhoAmI(fw::CoRpcReq req) {
        auto args = req.Parse<std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        return fw::CoRpcResp::From("player:" + args.value());
    }

    fw::result<fw::CoRpcResp> CallOut(fw::CoRpcReq req) {
        return this->call("other-service", "get", req);
    }

    const std::string& last_notify() const { return m_last_notify; }

    template <class R>
    std::shared_ptr<R> Resource() { return context().resource<R>(); }

    template <class R>
    std::shared_ptr<R> Resource(std::string_view name) {
        return context().resource<R>(name);
    }

    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&EchoService::Echo>("echo"),
        fw::Method<&EchoService::Reset>("reset"),
        fw::Method<&EchoService::Notify>("notify"),
        fw::Method<&EchoService::Fail>("fail"),
        fw::Method<&EchoService::Boom>("boom"),
        fw::ActorMethodAt<&EchoService::WhoAmI, std::string>("who-am-i"));

private:
    std::string m_last_notify;
};

class DupService final : public fw::ICoService {
public:
    std::string_view co_service_name() const noexcept override { return "dup"; }
    fw::CoRpcResp A(fw::CoRpcReq) {
        return fw::CoRpcResp::From(std::tuple<>{});
    }
    fw::CoRpcResp B(fw::CoRpcReq) {
        return fw::CoRpcResp::From(std::tuple<>{});
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&DupService::A>("same"),
        fw::Method<&DupService::B>("same"));
};

class EmptyNameService final : public fw::ICoService {
public:
    std::string_view co_service_name() const noexcept override { return "e"; }
    fw::CoRpcResp A(fw::CoRpcReq) {
        return fw::CoRpcResp::From(std::tuple<>{});
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&EmptyNameService::A>(""));
};

struct DummyResource { int n = 7; };

using EchoTraits = fw::rpc_traits<decltype(&EchoService::Echo)>;
static_assert(std::is_same_v<EchoTraits::service_type, EchoService>);
static_assert(std::is_same_v<EchoTraits::request_type, fw::CoRpcReq>);
static_assert(std::is_same_v<EchoTraits::reply_type, fw::CoRpcResp>);
static_assert(EchoTraits::kCoRpc);
static_assert(fw::has_rpc_methods_v<EchoService>);
static_assert(!fw::has_rpc_methods_v<DummyResource>);

BOOST_AUTO_TEST_SUITE(framework_f0)

BOOST_AUTO_TEST_CASE(method_table_binding) {
    const auto table = fw::BuildMethodTable<EchoService>();
    BOOST_TEST(table.size() == 6);

    const fw::RpcMethod* echo = table.find("echo");
    BOOST_REQUIRE(echo != nullptr);
    BOOST_TEST(echo->request_schema == std::string(fw::kCoRpcPositionalSchema));
    BOOST_TEST(echo->response_schema == std::string(fw::kCoRpcPositionalSchema));
    BOOST_TEST(!echo->actor_keyed);
    BOOST_CHECK(echo->extract_key == nullptr);

    const fw::RpcMethod* who = table.find("who-am-i");
    BOOST_REQUIRE(who != nullptr);
    BOOST_TEST(who->actor_keyed);
    BOOST_CHECK(who->extract_key != nullptr);
    BOOST_TEST(who->request_schema == std::string(fw::kCoRpcPositionalSchema));
    BOOST_TEST(table.find("no-such-method") == nullptr);
}

BOOST_AUTO_TEST_CASE(dispatch_roundtrip) {
    EchoService svc;
    const auto table = fw::BuildMethodTable<EchoService>();
    auto out = table.find("echo")->invoke(svc, Request(std::string{"hi"}).payload());
    BOOST_REQUIRE(out);
    auto reply = fw::CoRpcReq(std::move(out.value())).Parse<std::string>();
    BOOST_REQUIRE(reply);
    BOOST_TEST(reply.value() == "echo:hi");
}

BOOST_AUTO_TEST_CASE(empty_success_and_side_effect) {
    EchoService svc;
    const auto table = fw::BuildMethodTable<EchoService>();

    auto notify = table.find("notify")->invoke(
        svc, Request(std::string{"ping"}).payload());
    BOOST_REQUIRE(notify);
    BOOST_TEST(notify.value() == std::vector<std::uint8_t>({0, 0, 0, 0}));
    BOOST_TEST(svc.last_notify() == "ping");

    auto reset = table.find("reset")->invoke(
        svc, Request(std::string{"x"}).payload());
    BOOST_REQUIRE(reset);
    BOOST_TEST(reset.value() == std::vector<std::uint8_t>({0, 0, 0, 0}));
}

BOOST_AUTO_TEST_CASE(error_boundaries) {
    EchoService svc;
    const auto table = fw::BuildMethodTable<EchoService>();

    auto business = table.find("fail")->invoke(svc, Request(std::string{"x"}).payload());
    BOOST_REQUIRE(!business);
    BOOST_CHECK(business.error().code == fw::ErrorCode::InvalidArgument);
    BOOST_TEST(business.error().message == "rejected");

    auto thrown = table.find("boom")->invoke(svc, Request(std::string{"x"}).payload());
    BOOST_REQUIRE(!thrown);
    BOOST_CHECK(thrown.error().code == fw::ErrorCode::InternalError);

    EchoService untouched;
    auto malformed = table.find("notify")->invoke(untouched, {});
    BOOST_REQUIRE(!malformed);
    BOOST_CHECK(malformed.error().code == fw::ErrorCode::ProtocolError);
    BOOST_TEST(untouched.last_notify().empty());
}

BOOST_AUTO_TEST_CASE(actor_key_extraction) {
    const auto table = fw::BuildMethodTable<EchoService>();
    const fw::RpcMethod* who = table.find("who-am-i");
    BOOST_REQUIRE(who != nullptr);

    auto good = who->extract_key(Request(std::string{"p-42"}).payload());
    BOOST_REQUIRE(good);
    BOOST_TEST(good.value() == "p-42");

    auto bad = who->extract_key(Request(std::string{}).payload());
    BOOST_REQUIRE(!bad);
    BOOST_CHECK(bad.error().code == fw::ErrorCode::InvalidArgument);
}

BOOST_AUTO_TEST_CASE(duplicate_method_rejected) {
    BOOST_CHECK_THROW(fw::BuildMethodTable<DupService>(), std::invalid_argument);
    BOOST_CHECK_THROW(fw::BuildMethodTable<EmptyNameService>(), std::invalid_argument);
}

BOOST_AUTO_TEST_CASE(unhosted_identity_and_call) {
    EchoService svc;
    auto info = svc.GetObjectInfo();
    BOOST_TEST(info.id == 0);
    BOOST_TEST(info.name == "echo");
    BOOST_TEST(!svc.co_actor_key().has_value());

    auto result = svc.CallOut(Request(std::string{"q"}));
    BOOST_REQUIRE(!result);
    BOOST_CHECK(result.error().code == fw::ErrorCode::RuntimeUnavailable);
}

BOOST_AUTO_TEST_CASE(route_fields_and_traits) {
    auto fields = fw::RouteTraits<PlayerRouteArg>::Encode(PlayerRouteArg{"p7"});
    BOOST_REQUIRE(fields);
    BOOST_REQUIRE(fields.value().values.size() == 1);
    BOOST_TEST(fields.value().values[0].first == "player");
    BOOST_TEST(fields.value().values[0].second == "p7");

    fw::RpcRoute route{"echo", "echo", fields.value()};
    BOOST_TEST(route.service_name == "echo");
    BOOST_TEST(route.method_name == "echo");
    BOOST_TEST(route.custom.values.size() == 1);
}

BOOST_AUTO_TEST_CASE(call_options_defaults) {
    fw::CallOptions options;
    BOOST_TEST(!options.deadline.has_value());
    BOOST_TEST(!options.ordered.has_value());
    BOOST_TEST(!options.cancel.IsCancellationRequested());
}

BOOST_AUTO_TEST_CASE(positional_roundtrip_all_scalar_types) {
    const std::vector<std::uint8_t> bytes{0, 1, 2, 250, 255};
    auto request = fw::CoRpcReq::From(std::tuple{
        true, std::int8_t{-7}, std::int32_t{-123456},
        std::uint64_t{0xFEDCBA9876543210ULL}, 3.25,
        std::string{"位置参数"}, bytes});
    BOOST_REQUIRE(request);

    auto decoded = request.value().Parse<
        bool, std::int8_t, std::int32_t, std::uint64_t,
        double, std::string, std::vector<std::uint8_t>>();
    BOOST_REQUIRE(decoded);
    const auto& [flag, tiny, n, big, ratio, name, blob] = decoded.value();
    BOOST_TEST(flag);
    BOOST_TEST(tiny == -7);
    BOOST_TEST(n == -123456);
    BOOST_TEST(big == 0xFEDCBA9876543210ULL);
    BOOST_TEST(ratio == 3.25);
    BOOST_TEST(name == "位置参数");
    BOOST_TEST(blob == bytes);
}

BOOST_AUTO_TEST_CASE(positional_decode_rejects_malformed) {
    auto request = fw::CoRpcReq::From(std::int32_t{7});
    BOOST_REQUIRE(request);

    auto trailing = request.value().payload();
    trailing.push_back(0xA5);
    auto trailing_result = fw::CoRpcReq(std::move(trailing)).Parse<std::int32_t>();
    BOOST_REQUIRE(!trailing_result);
    BOOST_CHECK(trailing_result.error().code == fw::ErrorCode::ProtocolError);

    auto truncated = request.value().payload();
    truncated.pop_back();
    auto truncated_result = fw::CoRpcReq(std::move(truncated)).Parse<std::int32_t>();
    BOOST_REQUIRE(!truncated_result);
    BOOST_CHECK(truncated_result.error().code == fw::ErrorCode::ProtocolError);

    auto mismatch = request.value().Parse<std::string>();
    BOOST_REQUIRE(!mismatch);
    BOOST_CHECK(mismatch.error().code == fw::ErrorCode::TypeMismatch);

    auto count = request.value().Parse<std::int32_t, std::int32_t>();
    BOOST_REQUIRE(!count);
    BOOST_CHECK(count.error().code == fw::ErrorCode::InvalidArgument);
}

BOOST_AUTO_TEST_CASE(resource_seam_unbound_and_registration) {
    EchoService svc;
    BOOST_CHECK(svc.Resource<DummyResource>() == nullptr);

    fw::CoAppOptions options;
    options.network_limits = bbt::infra::NetworkLimits{
        64, 64, 16384, 65536, std::chrono::milliseconds{30000}};
    options.listen = bbt::infra::ListenAddress{"127.0.0.1", 0};
    options.shutdown_step_budget = std::chrono::milliseconds{2000};
    fw::CoApp app(options);

    auto resource = std::make_shared<DummyResource>();
    BOOST_CHECK(app.add_resource(resource));
    auto duplicate = app.add_resource(std::make_shared<DummyResource>());
    BOOST_REQUIRE(!duplicate);
    BOOST_CHECK(duplicate.error().code == fw::ErrorCode::InvalidArgument);
    auto null_resource = app.add_resource(std::shared_ptr<DummyResource>{});
    BOOST_REQUIRE(!null_resource);
    BOOST_CHECK(null_resource.error().code == fw::ErrorCode::InvalidArgument);
}

BOOST_AUTO_TEST_CASE(named_resource_registration_and_lookup) {
    EchoService svc;
    BOOST_CHECK(svc.Resource<DummyResource>("missing") == nullptr);

    fw::CoAppOptions options;
    options.network_limits = bbt::infra::NetworkLimits{
        64, 64, 16384, 65536, std::chrono::milliseconds{30000}};
    options.listen = bbt::infra::ListenAddress{"127.0.0.1", 0};
    options.shutdown_step_budget = std::chrono::milliseconds{2000};
    fw::CoApp app(options);

    auto primary = std::make_shared<DummyResource>();
    primary->n = 11;
    auto replica = std::make_shared<DummyResource>();
    replica->n = 22;

    BOOST_CHECK(app.add_resource<DummyResource>("primary", primary));
    BOOST_CHECK(app.add_resource<DummyResource>("replica", replica));

    auto duplicate = app.add_resource<DummyResource>(
        "primary", std::make_shared<DummyResource>());
    BOOST_REQUIRE(!duplicate);
    BOOST_CHECK(duplicate.error().code == fw::ErrorCode::InvalidArgument);

    auto empty_name = app.add_resource<DummyResource>("", primary);
    BOOST_REQUIRE(!empty_name);
    BOOST_CHECK(empty_name.error().code == fw::ErrorCode::InvalidArgument);
}

BOOST_AUTO_TEST_SUITE_END()

} // namespace
