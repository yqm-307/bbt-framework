// 发现组件（v2::CoComponent）的 Co 生命周期回归（无后端、无 live Consul）。
//
// 覆盖：
//  T1 no_watch_component_co_lifecycle
//      CoInstallComponent 只装配（无 watcher、无拨号）；CoStart 无 watch 直接
//      成功且幂等（重复 OnStart 语义下状态仍 Running）；CoStop 同一
//      CloseOptions 收口；组件按登记销毁。
//  T2 watch_first_snapshot_failure_marks_failed
//      测试自持的「已 bind 未 listen」loopback 地址（连接必被拒），首快照
//      失败使 OnStart 如实失败（组件 Failed）；CoStop 仍能停 watcher 并收口。
//      不访问任何共享后端。
//  T3 onstop_requires_finite_graceful_deadline
//      Graceful 无期限是非法组合，CoStop(CloseOptions) 在任何副作用前返回
//      InvalidArgument（框架统一前置校验）；随后用有限期限正常收口。
//
// watcher 协程由组件 OnStart 登记在受管 runtime 上；共享 Engine 由 app 持有，
// 测试线程扮演等价入口 host hook（Engine::Run 在 native 线程执行一次），
// native 测试线程只做有界编排等待，不参与业务关闭语义。

#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <memory>
#include <string>

#include <bbt/coroutine/sync/WaitTypes.hpp>
#include <bbt/infra/CloseOptions.hpp>
#include <bbt/infra/NetworkTypes.hpp>

#include <bbt/framework/DynamicRouteTable.hpp>
#include <bbt/framework/V2.hpp>

#include "discovery.hpp"
#include "framework_co_harness.hpp"

namespace {

using namespace std::chrono_literals;

using Clock = std::chrono::steady_clock;
namespace fw = bbt::framework;
namespace v2 = bbt::framework::v2;
namespace inf = bbt::infra;
using ecommerce::ConsulDiscoveryComponent;

// 自持端口：bind 后不 listen，connect 会被内核拒绝；fd 活到用例结束，
// 保证该地址不会被其它进程复用。不启动任何服务。
struct RefusedLoopbackPort {
    int fd{-1};
    std::uint16_t port{0};

    RefusedLoopbackPort() {
        fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0)
            return;
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
            ::close(fd);
            fd = -1;
            return;
        }
        socklen_t len = sizeof(addr);
        if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
            ::close(fd);
            fd = -1;
            return;
        }
        port = ntohs(addr.sin_port);
    }

    ~RefusedLoopbackPort() {
        if (fd >= 0)
            ::close(fd);
    }

    RefusedLoopbackPort(const RefusedLoopbackPort&) = delete;
    RefusedLoopbackPort& operator=(const RefusedLoopbackPort&) = delete;
};

fw::AppOptions MakeAppOptions() {
    fw::AppNetworkOptions network;
    network.SetLimits(inf::NetworkLimits{
                          64, 64, 16 * 1024, 64 * 1024,
                          std::chrono::milliseconds{30000}})
        .SetListenAddress(inf::ListenAddress{"127.0.0.1", 0})
        .SetLocalAppId("discovery-co-test");
    return fw::AppOptions{}
        .SetLimits(64, 64, 8, 64)
        .SetShutdownBudget(10s)
        .SetLifecycleBudget(10s)
        .SetNetworkOptions(std::move(network));
}

// 在受管协程内装配 app + 共享 Engine（等价入口 host hook 在 native 线程跑
// 一次 Run），安装发现组件并交回 owner。
struct Fixture {
    std::shared_ptr<v2::CoApp> app;
    std::shared_ptr<fw::DynamicRouteTable> table;
    inf::ListenAddress listen{};

    void Create(inf::ListenAddress addr = inf::ListenAddress{"127.0.0.1", 0}) {
        table = std::make_shared<fw::DynamicRouteTable>();
        listen = std::move(addr);
        auto table_ptr = table;
        app = framework_test::CreateV2App(
            [table_ptr] {
                return v2::CoApp::Create(MakeAppOptions(),
                                         "discovery-co-test");
            });
        BOOST_REQUIRE(app);
        BOOST_REQUIRE(app->engine());
    }

    // Install 先于共享 Engine 的等价入口 hook：http/consul client 的 Create 要求
    // Engine 仍 Idle；客户端装配完成后才由 native 线程发起唯一一次 Run。
    void Install(ConsulDiscoveryComponent::Options options) {
        BOOST_REQUIRE(app);
        fw::result<void> installed = fw::result<void>::err(
            fw::MakeError(fw::ErrorCode::RuntimeUnavailable, "not run"));
        bool ran = framework_test::RunInCoTask([&] {
            options.engine = app->engine();
            options.table = table;
            installed = bbt_co_await
                app->CoInstallComponent<ConsulDiscoveryComponent>(
                    "consul-discovery", std::move(options));
        });
        BOOST_REQUIRE(ran);
        BOOST_REQUIRE_MESSAGE(static_cast<bool>(installed),
                              "install failed: " << (installed ? "none" : installed.error().message));
        // 应用根共享 Engine 的等价入口 host hook：宿主 Create 装配完成后经
        // HostGate::Pending() 为标记发起唯一一次 Run（生产由
        // bbtco_main_host_hook 承担；Engine 的 Idle 装配在 Install/CoStart 前
        // 必须已完成——Run 不得先于宿主 Create）。在 CoStart 之前挂 watcher，
        // 保证「组件 Install（Idle）→ 宿主 Create → Publish → Run → Ready」的
        // 真实次序。
        std::thread([&] {
            (void)framework_test::RunAppEngineWhenPublished(
                app->engine(), listen);
        }).detach();
    }

    void Destroy() {
        framework_test::RunInCoTask([&] {
            BOOST_REQUIRE(static_cast<bool>(bbt_co_await app->CoStop(
                inf::CloseOptions::Graceful(Clock::now() + 10s))));
        });
        framework_test::CloseSharedEngine(app ? app->engine() : nullptr);
        app.reset();
    }

    ~Fixture() { Destroy(); }
};

ConsulDiscoveryComponent::Options NoWatchOptions() {
    ConsulDiscoveryComponent::Options options;
    options.agent_url = "http://127.0.0.1:1"; // 无 watch 时不拨号
    options.request_timeout = 2000ms;
    options.query_wait = 500ms;
    return options;
}

} // namespace

BOOST_AUTO_TEST_CASE(no_watch_component_co_lifecycle) {
    Fixture f;
    f.Create();
    f.Install(NoWatchOptions());

    auto comp = f.app->FindComponent<ConsulDiscoveryComponent>(
        "consul-discovery");
    BOOST_REQUIRE(static_cast<bool>(comp));

    // CoStart：无 watch 的组件直接 Running（共享 Engine 已由等价入口 hook
    // 跑起，组件不接触它）。
    auto started = framework_test::CoStart(*f.app);
    BOOST_REQUIRE(static_cast<bool>(started));
    auto info = comp.value().Info();
    BOOST_REQUIRE(static_cast<bool>(info));
    BOOST_CHECK(info.value().state == fw::ObjectState::Running);

    // CoStop：同一 CloseOptions 收口；watcher 从未登记，drain 直接成立。
    auto stopped = framework_test::CoEvalVoid([&] {
        return bbt_co_await f.app->CoStop(
            inf::CloseOptions::Graceful(Clock::now() + 10s));
    });
    BOOST_REQUIRE(static_cast<bool>(stopped));
    BOOST_CHECK(f.app->State() == fw::AppState::Stopped);
    BOOST_TEST(ConsulDiscoveryComponent::Current() == nullptr);
    BOOST_TEST(!static_cast<bool>(f.app->FindComponent<ConsulDiscoveryComponent>(
        "consul-discovery")));
}

BOOST_AUTO_TEST_CASE(watch_first_snapshot_failure_is_reported_before_ready) {
    RefusedLoopbackPort refused;
    BOOST_REQUIRE(refused.fd >= 0);
    BOOST_REQUIRE(refused.port != 0);

    Fixture f;
    f.Create();
    auto options = NoWatchOptions();
    options.agent_url =
        "http://127.0.0.1:" + std::to_string(refused.port);
    options.watch_services.emplace_back("ecommerce-test-order");
    f.Install(std::move(options));

    // watcher 在共享 Engine Ready 后推进；首快照失败由显式就绪等待返回。
    auto started = framework_test::CoStart(*f.app);
    BOOST_REQUIRE(static_cast<bool>(started));
    auto snapshot = framework_test::CoEvalVoid([&] {
        return bbt_co_await ConsulDiscoveryComponent::Current() -> WaitFirstSnapshots(Clock::now() + 3s);
    });
    BOOST_REQUIRE(!static_cast<bool>(snapshot));
    auto comp = f.app->FindComponent<ConsulDiscoveryComponent>(
        "consul-discovery");
    BOOST_REQUIRE(static_cast<bool>(comp));
    auto info = comp.value().Info();
    BOOST_REQUIRE(static_cast<bool>(info));
    BOOST_CHECK(info.value().state == fw::ObjectState::Running);
    if (auto* component = ConsulDiscoveryComponent::Current())
        BOOST_TEST(component->LastError().find("ecommerce-test-order") !=
                   std::string::npos);
    // 首快照失败不发布可用视图。
    auto resolved = f.table->Resolve("ecommerce-test-order");
    BOOST_TEST(!static_cast<bool>(resolved));

    // CoStop 仍能收口：watcher 已停、consul 封口、组件销毁。
    auto stopped = framework_test::CoEvalVoid([&] {
        return bbt_co_await f.app->CoStop(
            inf::CloseOptions::Graceful(Clock::now() + 10s));
    });
    BOOST_REQUIRE(static_cast<bool>(stopped));
    BOOST_TEST(ConsulDiscoveryComponent::Current() == nullptr);
}

BOOST_AUTO_TEST_CASE(onstop_requires_finite_graceful_deadline) {
    Fixture f;
    f.Create();
    f.Install(NoWatchOptions());
    auto started = framework_test::CoStart(*f.app);
    BOOST_REQUIRE(static_cast<bool>(started));

    // Graceful + 无限期限是非法组合：必须在产生任何副作用前拒绝。
    auto invalid = framework_test::CoEvalVoid([&] {
        return bbt_co_await f.app->CoStop(
            inf::CloseOptions::Graceful(bbt::coroutine::Deadline::max()));
    });
    BOOST_REQUIRE(!static_cast<bool>(invalid));
    BOOST_TEST(static_cast<int>(invalid.error().code) ==
               static_cast<int>(fw::ErrorCode::InvalidArgument));
    BOOST_CHECK(f.app->State() == fw::AppState::Running);

    auto stopped = framework_test::CoEvalVoid([&] {
        return bbt_co_await f.app->CoStop(
            inf::CloseOptions::Graceful(Clock::now() + 10s));
    });
    BOOST_REQUIRE(static_cast<bool>(stopped));
    BOOST_CHECK(f.app->State() == fw::AppState::Stopped);
}
