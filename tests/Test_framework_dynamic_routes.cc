// DynamicRouteTable 与 CoApp::find_route 的动态优先语义。
//
// 不联网、不启动 Scheduler：只断言公共面（Replace/Resolve/MarkUnavailable/
// HasService 与 find_route 的静态回退边界）。真实注册中心链路由
// examples.ecommerce_cluster 在真实 Consul 上验收。
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/DynamicRouteTable.hpp>
#include <bbt/framework/internal/AppRuntimeState.hpp>

namespace fw = bbt::framework;

namespace {

using std::chrono::milliseconds;

// V2 出站目标只有 endpoint 字符串（host:port）；transport 由 infra 会话持有。
std::string Endpoint(std::uint16_t port) {
    return "127.0.0.1:" + std::to_string(port);
}

std::vector<std::string> Endpoints(std::uint16_t a, std::uint16_t b) {
    return {Endpoint(a), Endpoint(b)};
}

} // namespace

BOOST_AUTO_TEST_SUITE(framework_dynamic_routes)

BOOST_AUTO_TEST_CASE(table_replace_resolve_round_robin) {
    fw::DynamicRouteTable table;
    BOOST_CHECK(!table.HasService("catalog"));
    auto unknown = table.Resolve("catalog");
    BOOST_REQUIRE(!unknown);
    BOOST_CHECK(unknown.error().code == fw::ErrorCode::NotFound);

    BOOST_REQUIRE(table.Replace("catalog", 7, Endpoints(5001, 5002)));
    BOOST_CHECK(table.HasService("catalog"));

    // 每服务 round-robin：连续 4 次解析两实例各命中两次。
    int first = 0;
    int second = 0;
    for (int i = 0; i < 4; ++i) {
        auto ep = table.Resolve("catalog");
        BOOST_REQUIRE(ep);
        if (ep.value() == "127.0.0.1:5001")
            ++first;
        else if (ep.value() == "127.0.0.1:5002")
            ++second;
        else
            BOOST_FAIL("unexpected endpoint " + ep.value());
    }
    BOOST_CHECK_EQUAL(first, 2);
    BOOST_CHECK_EQUAL(second, 2);

    // 完整替换（不是增量）：新集合替换旧集合，游标重置。
    BOOST_REQUIRE(table.Replace("catalog", 8, {Endpoint(6001)}));
    for (int i = 0; i < 3; ++i) {
        auto ep = table.Resolve("catalog");
        BOOST_REQUIRE(ep);
        BOOST_CHECK_EQUAL(ep.value(), "127.0.0.1:6001");
    }
}

BOOST_AUTO_TEST_CASE(table_rejects_invalid_and_dedupes) {
    fw::DynamicRouteTable table;
    auto empty_service = table.Replace("", 1, {Endpoint(5001)});
    BOOST_REQUIRE(!empty_service);
    BOOST_CHECK(empty_service.error().code == fw::ErrorCode::InvalidArgument);

    auto bad_ep = table.Replace(
        "catalog", 1, {std::string{}});
    BOOST_REQUIRE(!bad_ep);
    BOOST_CHECK(bad_ep.error().code == fw::ErrorCode::InvalidArgument);

    // 重复 endpoint 去重保序，不产生偏心的 round-robin 槽位。
    BOOST_REQUIRE(table.Replace(
        "catalog", 1, {Endpoint(5001), Endpoint(5002), Endpoint(5001)}));
    std::vector<std::string> seen;
    for (int i = 0; i < 4; ++i) {
        auto ep = table.Resolve("catalog");
        BOOST_REQUIRE(ep);
        seen.push_back(ep.value());
    }
    BOOST_CHECK_EQUAL(seen[0], "127.0.0.1:5001");
    BOOST_CHECK_EQUAL(seen[1], "127.0.0.1:5002");
    BOOST_CHECK_EQUAL(seen[2], "127.0.0.1:5001");
    BOOST_CHECK_EQUAL(seen[3], "127.0.0.1:5002");
}

BOOST_AUTO_TEST_CASE(table_empty_view_is_not_found_and_mark_unavailable) {
    fw::DynamicRouteTable table;
    // 合法空健康列表：立即发布空视图，不保留旧实例。
    BOOST_REQUIRE(table.Replace("catalog", 9, Endpoints(5001, 5002)));
    BOOST_REQUIRE(table.Replace("catalog", 10, {}));
    BOOST_CHECK(table.HasService("catalog"));
    auto empty = table.Resolve("catalog");
    BOOST_REQUIRE(!empty);
    BOOST_CHECK(empty.error().code == fw::ErrorCode::NotFound);

    BOOST_REQUIRE(table.Replace("catalog", 11, Endpoints(5001, 5002)));
    table.MarkUnavailable("catalog");
    auto gone = table.Resolve("catalog");
    BOOST_REQUIRE(!gone);
    BOOST_CHECK(gone.error().code == fw::ErrorCode::RuntimeUnavailable);
    // 未配置的服务名 MarkUnavailable 不创建条目。
    table.MarkUnavailable("order");
    BOOST_CHECK(!table.HasService("order"));
}

BOOST_AUTO_TEST_CASE(table_same_view_refreshes_freshness_then_expires) {
    // 缩短陈旧边界让用例快：语义与默认 5s 相同。
    fw::DynamicRouteTable table(milliseconds{200});
    BOOST_REQUIRE(table.Replace("catalog", 5, Endpoints(5001, 5002)));
    std::this_thread::sleep_for(milliseconds{120});
    BOOST_CHECK(table.Resolve("catalog"));

    // 同 revision、同 endpoint 的权威查询仍必须刷新观测时间：否则稳定的健康
    // 服务会在 200ms 后被误判陈旧。
    BOOST_REQUIRE(table.Replace("catalog", 5, Endpoints(5001, 5002)));
    std::this_thread::sleep_for(milliseconds{120});
    BOOST_CHECK(table.Resolve("catalog"));   // 距首次 240ms，距刷新 120ms

    // 超过边界且无刷新 → RuntimeUnavailable，不返回陈旧 endpoint。
    std::this_thread::sleep_for(milliseconds{260});
    auto stale = table.Resolve("catalog");
    BOOST_REQUIRE(!stale);
    BOOST_CHECK(stale.error().code == fw::ErrorCode::RuntimeUnavailable);

    // 恢复一次成功查询即可重新可用。
    BOOST_REQUIRE(table.Replace("catalog", 6, {Endpoint(5003)}));
    auto recovered = table.Resolve("catalog");
    BOOST_REQUIRE(recovered);
    BOOST_CHECK_EQUAL(recovered.value(), "127.0.0.1:5003");
}

BOOST_AUTO_TEST_CASE(table_same_view_refresh_preserves_round_robin_cursor) {
    fw::DynamicRouteTable table;
    const auto endpoints = Endpoints(5001, 5002);
    BOOST_REQUIRE(table.Replace("catalog", 7, endpoints));

    auto first = table.Resolve("catalog");
    BOOST_REQUIRE(first);
    BOOST_CHECK_EQUAL(first.value(), "127.0.0.1:5001");

    // Identical revision and view refresh freshness without restarting rotation.
    BOOST_REQUIRE(table.Replace("catalog", 7, endpoints));
    auto second = table.Resolve("catalog");
    BOOST_REQUIRE(second);
    BOOST_CHECK_EQUAL(second.value(), "127.0.0.1:5002");

    // Revision can advance while the passing endpoint view stays identical.
    BOOST_REQUIRE(table.Replace("catalog", 8, endpoints));
    auto third = table.Resolve("catalog");
    BOOST_REQUIRE(third);
    BOOST_CHECK_EQUAL(third.value(), "127.0.0.1:5001");
    BOOST_REQUIRE(table.Replace("catalog", 9, endpoints));
    auto fourth = table.Resolve("catalog");
    BOOST_REQUIRE(fourth);
    BOOST_CHECK_EQUAL(fourth.value(), "127.0.0.1:5002");
}

BOOST_AUTO_TEST_CASE(table_revision_regression_installs_snapshot) {
    fw::DynamicRouteTable table;
    BOOST_REQUIRE(table.Replace("catalog", 100, {Endpoint(5001)}));
    // revision 回退（provider 重置 epoch）：照装刚取的完整权威快照，不拒绝。
    BOOST_REQUIRE(table.Replace("catalog", 42, Endpoints(7001, 7002)));
    auto first = table.Resolve("catalog");
    BOOST_REQUIRE(first);
    BOOST_CHECK_EQUAL(first.value(), "127.0.0.1:7001");
    auto second = table.Resolve("catalog");
    BOOST_REQUIRE(second);
    BOOST_CHECK_EQUAL(second.value(), "127.0.0.1:7002");
}

BOOST_AUTO_TEST_CASE(coapp_find_route_prefers_dynamic_without_static_fallback) {
    auto table = std::make_shared<fw::DynamicRouteTable>();

    fw::CoAppOptions options;
    options.network_limits = bbt::infra::NetworkLimits{
        8, 8, 16 * 1024, 64 * 1024, milliseconds{5000}};
    options.listen = bbt::infra::ListenAddress{"127.0.0.1", 0};
    options.shutdown_step_budget = milliseconds{1000};
    options.static_routes = {
        fw::StaticRoute{"catalog", Endpoint(4999)},
        fw::StaticRoute{"order", Endpoint(4998)},
    };
    options.dynamic_routes = table;
    fw::CoApp app(options);

    // 静态路由语义保持：未进动态表的名字走静态项。
    auto order = app.find_route("order");
    BOOST_REQUIRE(order);
    BOOST_CHECK_EQUAL(order.value(), "127.0.0.1:4998");

    auto unknown = app.find_route("inventory");
    BOOST_REQUIRE(!unknown);
    BOOST_CHECK(unknown.error().code == fw::ErrorCode::NotFound);

    // 动态表配置了同名服务 → 动态优先，静态 endpoint 不再可见。
    BOOST_REQUIRE(table->Replace("catalog", 1, {Endpoint(5001)}));
    auto dynamic_hit = app.find_route("catalog");
    BOOST_REQUIRE(dynamic_hit);
    BOOST_CHECK_EQUAL(dynamic_hit.value(), "127.0.0.1:5001");

    // 已配置动态服务变空 → NotFound，绝不回退到静态 4999。
    BOOST_REQUIRE(table->Replace("catalog", 2, {}));
    auto empty = app.find_route("catalog");
    BOOST_REQUIRE(!empty);
    BOOST_CHECK(empty.error().code == fw::ErrorCode::NotFound);
    BOOST_CHECK_NE(empty.error().message.find("no healthy instance"),
                   std::string::npos);

    // 动态视图被标记不可用 → RuntimeUnavailable（同样不回退）。
    BOOST_REQUIRE(table->Replace("catalog", 3, Endpoints(5001, 5002)));
    table->MarkUnavailable("catalog");
    auto unavailable = app.find_route("catalog");
    BOOST_REQUIRE(!unavailable);
    BOOST_CHECK(unavailable.error().code == fw::ErrorCode::RuntimeUnavailable);
}

BOOST_AUTO_TEST_CASE(runtime_send_uses_live_dynamic_view_before_policy) {
    struct Policy final : fw::RpcPolicy {
        int calls = 0;
        std::string endpoint;
        bool reject = false;

        fw::result<fw::RpcPolicyDecision> Preflight(const fw::RpcCallView& call) override {
            ++calls;
            endpoint = call.target;
            fw::RpcPolicyDecision decision;
            if (reject) {
                decision.kind = fw::RpcPolicyDecision::Kind::Reject;
                decision.reject_error = fw::MakeError(fw::ErrorCode::Overloaded, "policy rejected");
            }
            return fw::result<fw::RpcPolicyDecision>::ok(decision);
        }
    };

    fw::AppRuntimeState state;
    auto table = std::make_shared<fw::DynamicRouteTable>(milliseconds{0});
    state.dynamic_routes = table;
    state.routes.emplace("catalog", Endpoint(4999));
    state.routes.emplace("order", Endpoint(4998));
    auto policy = std::make_shared<Policy>();
    state.rpc_policy = policy;
    std::vector<std::string> sent;
    state.seam_send = [&](std::string_view target,
                          const bbt::infra::rpc::Request& env,
                          const bbt::infra::CallOptions&) {
        BOOST_CHECK_EQUAL(policy->endpoint, std::string(target));
        sent.push_back(std::string(target));
        return fw::result<bbt::infra::rpc::Response>::ok(
            bbt::infra::rpc::Response::FromRequest(env));
    };
    bbt::infra::rpc::Request env;
    env.SetService("order")
        .SetReply(bbt::infra::rpc::ReplyMode::Required)
        .SetTraffic(bbt::infra::rpc::TrafficClass::Service);
    bbt::infra::CallOptions options{std::chrono::steady_clock::now() + milliseconds{1000}};
    BOOST_REQUIRE(state.Send(env, options));
    BOOST_CHECK_EQUAL(sent.back(), "tcp://127.0.0.1:4998");

    // 空/陈旧/不可用动态视图必须在策略与 I/O 前失败，不能走同名静态项。
    env.SetService("catalog");
    BOOST_REQUIRE(table->Replace("catalog", 1, {}));
    auto empty = state.Send(env, options);
    BOOST_REQUIRE(!empty);
    BOOST_CHECK(empty.error().code == fw::ErrorCode::NotFound);
    BOOST_REQUIRE(table->Replace("catalog", 2, {Endpoint(5001)}));
    std::this_thread::sleep_for(milliseconds{1});
    auto stale = state.Send(env, options);
    BOOST_REQUIRE(!stale);
    BOOST_CHECK(stale.error().code == fw::ErrorCode::RuntimeUnavailable);
    table->MarkUnavailable("catalog");
    auto unavailable = state.Send(env, options);
    BOOST_REQUIRE(!unavailable);
    BOOST_CHECK(unavailable.error().code == fw::ErrorCode::RuntimeUnavailable);
    env.SetService("unknown");
    auto unknown = state.Send(env, options);
    BOOST_REQUIRE(!unknown);
    BOOST_CHECK(unknown.error().code == fw::ErrorCode::NotFound);
    BOOST_CHECK_EQUAL(policy->calls, 1);
    BOOST_CHECK_EQUAL(sent.size(), 1u);

    // 长寿命出站状态读取后续发布，不缓存首次解析结果。
    state.dynamic_routes = std::make_shared<fw::DynamicRouteTable>();
    table = state.dynamic_routes;
    env.SetService("catalog");
    BOOST_REQUIRE(table->Replace("catalog", 3, Endpoints(5001, 5002)));
    BOOST_REQUIRE(state.Send(env, options));
    BOOST_CHECK_EQUAL(sent.back(), "tcp://127.0.0.1:5001");
    BOOST_REQUIRE(state.Send(env, options));
    BOOST_CHECK_EQUAL(sent.back(), "tcp://127.0.0.1:5002");
    BOOST_REQUIRE(table->Replace("catalog", 4, {Endpoint(5003)}));
    BOOST_REQUIRE(state.Send(env, options));
    BOOST_CHECK_EQUAL(sent.back(), "tcp://127.0.0.1:5003");
    policy->reject = true;
    auto rejected = state.Send(env, options);
    BOOST_REQUIRE(!rejected);
    BOOST_CHECK(rejected.error().code == fw::ErrorCode::Overloaded);
    BOOST_CHECK_EQUAL(policy->calls, 5);
    BOOST_CHECK_EQUAL(sent.size(), 4u);
}

BOOST_AUTO_TEST_SUITE_END()
