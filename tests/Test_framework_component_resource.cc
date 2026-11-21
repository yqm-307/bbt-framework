/**
 * 内置资源组件的关闭收口与强引用寿命（framework.component_resource）：
 * RedisNetResource 经真实 CoApp 装配、启动、绑定；持有组件强引用
 * （lock() 分享安装控制块）不阻塞成功关闭，关闭后经同一强引用发起
 * 命令得到 Closed——关闭只释放资源，对象寿命仍由最后强引用决定。
 *
 * 不连接真实后端：Closed 判定发生在已发布客户端清空之后（组件关闭
 * 即同步物理收口），装配与启停只涉及本组件自有网络线程；真实后端
 * 往返验收由 framework.resource_live_* 承担。
 */

#define BOOST_TEST_MODULE framework.component_resource
#include <boost/test/included/unit_test.hpp>

#include <chrono>
#include <condition_variable>
#include <exception>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/framework/Framework.hpp>
#include <bbt/framework/RedisNetResource.hpp>

namespace fw = bbt::framework;
namespace inf = bbt::infra;
using fw::ErrorCode;

namespace {

/**
 * 声明 Redis 资源依赖的最小服务：仅作为绑定 subject。
 */
class RedisUser final : public fw::CoService<fw::RedisNetResource> {
public:
    static constexpr std::string_view kServiceName = "redis_user";
    explicit RedisUser(fw::detail::ConstructKey k)
        : fw::CoService<fw::RedisNetResource>(k) {}

protected:
    ~RedisUser() noexcept override = default;
    friend class fw::detail::Registry;
};

fw::AppOptions MakeAppOptions() {
    return fw::AppOptions{}
        .SetLimits(64, 64, 64, 64)
        .SetShutdownBudget(std::chrono::milliseconds(200))
        .SetLifecycleBudget(std::chrono::milliseconds(200));
}

/**
 * 在受管协程内执行 fn；与 v2_core 相同的宿主线程 CV 等待模式。
 */
template <class F>
bool RunInManagedCoroutine(F&& fn,
                           std::chrono::milliseconds budget = std::chrono::milliseconds{5000}) {
    if (bbt::coroutine::IsInCoroutineContext()) {
        fn();
        return true;
    }
    if (!g_scheduler->IsInitialized())
        g_scheduler->Start();
    struct Slot {
        std::mutex mtx;
        std::condition_variable cv;
        bool done = false;
        std::exception_ptr error;
    };
    auto slot = std::make_shared<Slot>();
    auto f = std::forward<F>(fn);
    bool submitted = false;
    g_scheduler->RegistCoroutineTask(
        [slot, f = std::move(f)]() mutable {
            try {
                f();
            } catch (...) { slot->error = std::current_exception(); }
            std::lock_guard<std::mutex> lk(slot->mtx);
            slot->done = true;
            slot->cv.notify_all();
        },
        submitted);
    if (!submitted)
        throw std::runtime_error("test coroutine submission rejected");
    std::unique_lock<std::mutex> lk(slot->mtx);
    const bool done = slot->cv.wait_for(lk, budget, [&] { return slot->done; });
    if (!done)
        std::terminate();
    if (slot->error)
        std::rethrow_exception(slot->error);
    return done;
}

} // namespace

static void resource_close_releases_published_client_body() {
    auto app = fw::CoApp::Create(MakeAppOptions());

    /**
     * 数值地址（不拨号）：组件启动只装配自有网络线程与客户端，不连接。
     */
    std::error_code addr_ec;
    auto address = inf::net::Address::Ip("127.0.0.1", 6379, addr_ec);
    BOOST_REQUIRE_MESSAGE(!addr_ec,
                          "地址须为数值 IPv4/IPv6: " << addr_ec.message());

    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<fw::RedisNetResource>(
        "cache", fw::RedisNetResource::Options{}.SetAddress(address)));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<RedisUser>(
        fw::ServiceOptions{}.SetMaxInflight(1)));
    BOOST_REQUIRE(bbt_co_await app->CoStart());

    auto svc = app->FindService("redis_user");
    BOOST_REQUIRE(svc);
    auto h = bbt_co_await app->CoBindComponent<fw::RedisNetResource>(svc.value(), "cache");
    BOOST_REQUIRE(h);
    BOOST_CHECK(h.value().IsBound());

    /**
     * 普通强引用：跨关闭保活对象，不阻塞收口。
     */
    auto owner = h.value().lock();
    BOOST_REQUIRE(owner);

    /**
     * 默认解绑摘除关系（立即成功，不等普通 owner）；
     * 仍有挂载关系的卸载返回 Overloaded（契约规则）。
     */
    auto blocked = bbt_co_await app->CoUninstallComponent<fw::RedisNetResource>("cache");
    BOOST_CHECK(!blocked && blocked.error().code == ErrorCode::Overloaded);
    BOOST_REQUIRE(bbt_co_await app->CoUnbindComponent<fw::RedisNetResource>(svc.value(), "cache"));

    /**
     * 成功关闭（卸载推进 OnStop 同步 Close，普通 owner 不参与排空）。
     */
    auto uninstalled = bbt_co_await app->CoUninstallComponent<fw::RedisNetResource>("cache");
    BOOST_REQUIRE_MESSAGE(uninstalled,
                          "卸载失败: code=" << (uninstalled ? 0 : static_cast<int>(uninstalled.error().code))
                                            << " msg=" << (uninstalled ? std::string{} : uninstalled.error().message));
    BOOST_CHECK(!h.value().IsValid());
    BOOST_CHECK(!h.value().IsBound());

    /**
     * 强引用仍保活同一 C++ 对象；关闭后命令返回 Closed（不放宽为任意错误）。
     */
    BOOST_CHECK(owner);
    BOOST_CHECK(h.value().lock());
    auto late = bbt_co_await owner->Ping(inf::CallOptions{
        std::chrono::steady_clock::now() + std::chrono::milliseconds(200)});
    BOOST_REQUIRE(!late);
    BOOST_CHECK(late.error().code == ErrorCode::Closed);
}

BOOST_AUTO_TEST_CASE(resource_close_releases_published_client) {
    BOOST_REQUIRE(
        RunInManagedCoroutine(resource_close_releases_published_client_body));
}

/** 动态卸载 Service 收口其 Service-scope 组件：目标仍有挂载关系时卸载被拒
 * （Overloaded）；解绑后卸载服务，其 Service-scope 组件连同服务一并销毁；
 * 旧强引用上的命令返回 Closed，App-scope 组件不受影响、仍可绑定借用。
 * 不连接真实后端。
 */
static void service_uninstall_collects_service_scope_component_body() {
    auto app = fw::CoApp::Create(MakeAppOptions());

    std::error_code addr_ec;
    auto address = inf::net::Address::Ip("127.0.0.1", 6379, addr_ec);
    BOOST_REQUIRE_MESSAGE(!addr_ec,
                          "地址须为数值 IPv4/IPv6: " << addr_ec.message());

    /** RedisUser 声明 RedisNetResource，作为 Service-scope 组件的 owner。 */
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<RedisUser>(
        fw::ServiceOptions{}.SetMaxInflight(1)));
    auto svc = app->FindService("redis_user");
    BOOST_REQUIRE(svc);

    fw::ComponentOptions placement;
    placement.SetScope(fw::ComponentScope::Service).SetOwner(svc.value());
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<fw::RedisNetResource>(
        "cache", fw::RedisNetResource::Options{}.SetAddress(address), placement));

    /** App-scope 组件须在服务卸载前已存在，才能证明不受服务卸载影响。 */
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<fw::RedisNetResource>(
        "shared", fw::RedisNetResource::Options{}.SetAddress(address)));

    /** 保留第一次绑定句柄，不再次绑定同一槽位。 */
    auto h = bbt_co_await app->CoBindComponent<fw::RedisNetResource>(
        svc.value(), "cache", placement);
    BOOST_REQUIRE(h);
    BOOST_CHECK(h.value().IsBound());
    BOOST_REQUIRE(bbt_co_await app->CoStart());

    /** 旧强引用：跨服务卸载保活对象，不参与资源排空。 */
    auto owner = h.value().lock();
    BOOST_REQUIRE(owner);

    /** 目标仍有挂载关系：卸载被拒（Overloaded），必须先解绑。 */
    auto blocked = bbt_co_await app->CoUninstallComponent<fw::RedisNetResource>(
        "cache", placement);
    BOOST_CHECK(!blocked && blocked.error().code == ErrorCode::Overloaded);

    BOOST_REQUIRE(bbt_co_await app->CoUnbindComponent<fw::RedisNetResource>(
        svc.value(), "cache", placement));
    BOOST_CHECK(!h.value().IsBound());

    /** 卸载服务：收口其 Service-scope 组件，服务与组件均销毁。 */
    BOOST_REQUIRE(bbt_co_await app->CoUninstallService<RedisUser>());
    BOOST_CHECK(!app->FindService("redis_user"));
    /** Service placement 查找：Service-scope 组件已随服务销毁。 */
    auto gone = app->FindComponent<fw::RedisNetResource>("cache", placement);
    BOOST_CHECK(!gone);

    /** 旧强引用上的命令返回 Closed（资源面已关闭）。 */
    auto late = bbt_co_await owner->Ping(inf::CallOptions{
        std::chrono::steady_clock::now() + std::chrono::milliseconds(200)});
    BOOST_REQUIRE(!late);
    BOOST_CHECK(late.error().code == ErrorCode::Closed);

    /** App-scope 组件不受服务卸载影响，仍可绑定借用：重装同类服务后
     * 仍可将其绑定到该 App-scope 组件。 */
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<RedisUser>(
        fw::ServiceOptions{}.SetMaxInflight(1)));
    auto svc2 = app->FindService("redis_user");
    BOOST_REQUIRE(svc2);
    auto h2 = bbt_co_await app->CoBindComponent<fw::RedisNetResource>(
        svc2.value(), "shared");
    BOOST_REQUIRE(h2);
    BOOST_CHECK(h2.value().IsBound());
    auto borrow2 = h2.value().Borrow();
    BOOST_REQUIRE(borrow2);
}

BOOST_AUTO_TEST_CASE(service_uninstall_collects_service_scope_component) {
    BOOST_REQUIRE(
        RunInManagedCoroutine(service_uninstall_collects_service_scope_component_body));
}
