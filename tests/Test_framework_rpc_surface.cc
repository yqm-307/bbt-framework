// CoRpc 公共面、声明式方法表、错误边界、actor key、路由与出站发送缝。
// 不联网、不起真实 I/O；需要真实受管实例的用例在受管协程内经
// v2::CoApp::CoInstallService 装配（ConstructKey 为私有 passkey，
// 测试不能栈构造受管服务）。
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

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/framework/App.hpp>
#include <bbt/framework/Framework.hpp>
#include <bbt/framework/internal/MethodTable.hpp>

#include "framework_co_harness.hpp"

namespace fw = bbt::framework;
namespace v2 = bbt::framework::v2;

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

class EchoService final : public v2::CoService<> {
public:
    static constexpr std::string_view kServiceName = "echo";

    explicit EchoService(fw::detail::ConstructKey k) : v2::CoService<>(k) {}

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

    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&EchoService::Echo>("echo"),
        fw::Method<&EchoService::Reset>("reset"),
        fw::Method<&EchoService::Notify>("notify"),
        fw::Method<&EchoService::Fail>("fail"),
        fw::Method<&EchoService::Boom>("boom"),
        fw::ActorMethodAt<&EchoService::WhoAmI, std::string>("who-am-i"));

protected:
    ~EchoService() noexcept override = default;

private:
    std::string m_last_notify;
    friend class fw::detail::Registry;
};

class DupService final : public v2::CoService<> {
public:
    static constexpr std::string_view kServiceName = "dup";

    explicit DupService(fw::detail::ConstructKey k) : v2::CoService<>(k) {}

    fw::CoRpcResp A(fw::CoRpcReq) {
        return fw::CoRpcResp::From(std::tuple<>{});
    }
    fw::CoRpcResp B(fw::CoRpcReq) {
        return fw::CoRpcResp::From(std::tuple<>{});
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&DupService::A>("same"),
        fw::Method<&DupService::B>("same"));

protected:
    ~DupService() noexcept override = default;
    friend class fw::detail::Registry;
};

class EmptyNameService final : public v2::CoService<> {
public:
    static constexpr std::string_view kServiceName = "e";

    explicit EmptyNameService(fw::detail::ConstructKey k)
        : v2::CoService<>(k) {}

    fw::CoRpcResp A(fw::CoRpcReq) {
        return fw::CoRpcResp::From(std::tuple<>{});
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&EmptyNameService::A>(""));

protected:
    ~EmptyNameService() noexcept override = default;
    friend class fw::detail::Registry;
};

using EchoTraits = fw::rpc_traits<decltype(&EchoService::Echo)>;
static_assert(std::is_same_v<EchoTraits::service_type, EchoService>);
static_assert(std::is_same_v<EchoTraits::request_type, fw::CoRpcReq>);
static_assert(std::is_same_v<EchoTraits::reply_type, fw::CoRpcResp>);
static_assert(EchoTraits::kCoRpc);
static_assert(fw::has_rpc_methods_v<EchoService>);

fw::AppOptions MakeAppOptions() {
    return fw::AppOptions{}
        .SetLimits(64, 64, 8, 16)
        .SetShutdownBudget(std::chrono::milliseconds{2000})
        .SetLifecycleBudget(std::chrono::milliseconds{2000});
}

// 在受管协程内装配一个 seam app（无网络宿主、无出站缝），安装 S 并把其受管
// 实例交给 body。invoke 的形参类型是 v2::CoServiceBase&，实例由 Registry
// 强持有并在 CoStop/app 释放时销毁。
template <class S, class F>
void WithService(F&& body) {
    bool ran = framework_test::RunInCoTask(
        [&] {
            auto app = v2::CoApp::Create(MakeAppOptions());
            fw::result<void> installed =
                bbt_co_await app->CoInstallService<S>(
                    fw::ServiceOptions{}.SetMaxInflight(8));
            if (!installed)
                throw std::runtime_error("install failed: " +
                                         installed.error().message);
            auto instance =
                framework_test::ServiceInstance(*app, S::kServiceName);
            if (!instance)
                throw std::runtime_error("service instance missing");
            body(static_cast<S&>(*instance));
            BOOST_REQUIRE(static_cast<bool>(bbt_co_await app->CoStop()));
        },
        std::chrono::seconds{30});
    if (!ran)
        throw std::runtime_error("service task did not settle");
}

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
    const auto table = fw::BuildMethodTable<EchoService>();
    WithService<EchoService>([&](EchoService& svc) {
        auto out = table.find("echo")->invoke(
            svc, Request(std::string{"hi"}).payload());
        BOOST_REQUIRE(out);
        auto reply =
            fw::CoRpcReq(std::move(out.value())).Parse<std::string>();
        BOOST_REQUIRE(reply);
        BOOST_TEST(reply.value() == "echo:hi");
    });
}

BOOST_AUTO_TEST_CASE(empty_success_and_side_effect) {
    const auto table = fw::BuildMethodTable<EchoService>();
    WithService<EchoService>([&](EchoService& svc) {
        auto notify = table.find("notify")->invoke(
            svc, Request(std::string{"ping"}).payload());
        BOOST_REQUIRE(notify);
        BOOST_TEST(notify.value() == std::vector<std::uint8_t>({0, 0, 0, 0}));
        BOOST_TEST(svc.last_notify() == "ping");

        auto reset = table.find("reset")->invoke(
            svc, Request(std::string{"x"}).payload());
        BOOST_REQUIRE(reset);
        BOOST_TEST(reset.value() == std::vector<std::uint8_t>({0, 0, 0, 0}));
    });
}

BOOST_AUTO_TEST_CASE(error_boundaries) {
    const auto table = fw::BuildMethodTable<EchoService>();
    WithService<EchoService>([&](EchoService& svc) {
        auto business = table.find("fail")->invoke(
            svc, Request(std::string{"x"}).payload());
        BOOST_REQUIRE(!business);
        BOOST_CHECK(business.error().code == fw::ErrorCode::InvalidArgument);
        BOOST_TEST(business.error().message == "rejected");

        auto thrown = table.find("boom")->invoke(
            svc, Request(std::string{"x"}).payload());
        BOOST_REQUIRE(!thrown);
        BOOST_CHECK(thrown.error().code == fw::ErrorCode::InternalError);

        auto malformed = table.find("notify")->invoke(svc, {});
        BOOST_REQUIRE(!malformed);
        BOOST_CHECK(malformed.error().code == fw::ErrorCode::ProtocolError);
        BOOST_TEST(svc.last_notify().empty());
    });
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
    BOOST_CHECK_THROW(fw::BuildMethodTable<EmptyNameService>(),
                      std::invalid_argument);
}

BOOST_AUTO_TEST_CASE(unhosted_call_fails_before_io) {
    // _bind_runtime 在 CoInstallService 时已建立（含发送闭包与对象身份）；
    // 受管 handler 在无请求上下文时 this->call 先在适配层失败（不发 I/O）。
    // seam app 无 seam_send/默认宿主：上下文落地后发送也如实
    // RuntimeUnavailable 而不是伪造成功。
    WithService<EchoService>([](EchoService& svc) {
        auto result = svc.CallOut(Request(std::string{"q"}));
        BOOST_REQUIRE(!result);
        BOOST_CHECK(result.error().code == fw::ErrorCode::InvalidContext ||
                    result.error().code == fw::ErrorCode::RuntimeUnavailable ||
                    result.error().code == fw::ErrorCode::NotFound);
    });
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

BOOST_AUTO_TEST_SUITE_END()

} // namespace
