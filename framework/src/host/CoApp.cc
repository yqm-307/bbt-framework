#include <bbt/framework/CoApp.hpp>

#include <exception>

#include <bbt/coroutine/object/CoObject.hpp>

#include <bbt/framework/internal/ActorRegistry.hpp>
#include <bbt/framework/internal/CoAppSeam.hpp>
#include <bbt/framework/internal/HostLifecycle.hpp>
#include <bbt/framework/internal/InboundDispatcher.hpp>
#include <bbt/framework/internal/InfraHttpHost.hpp>
#include <bbt/framework/internal/RpcHttpBridge.hpp>

namespace bbt::framework {

namespace {

// 进程内活跃宿主登记：契约「默认一个进程共享现有单例 Scheduler；不宣称
// 多个 CoApp 可各自创建独立 Scheduler」。本切片口径：同一时刻至多一个
// CoApp 处于 run 内；完整收束后允许另一实例顺序复用同一单例。
std::mutex g_active_app_mtx;
CoApp*     g_active_app = nullptr;

struct ActiveAppGuard {
    ~ActiveAppGuard() {
        std::lock_guard<std::mutex> lk(g_active_app_mtx);
        g_active_app = nullptr;
    }
};

result<void> FailInvalid(std::string what) {
    return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                       std::move(what)));
}

} // namespace

CoApp::CoApp(CoAppOptions options)
    : m_options(std::move(options)) {
    // 默认装配：真实 HTTP 宿主 + envelope↔HTTP 线桥 + 默认出站。
    // InstallHandler 只存 handler（Start 在 run 内），此处安装是安全的；
    // handler 闭包持有 this，只在 run 存活期内被调用。
    m_default_host = std::make_shared<InfraHttpHost>(
        m_options.network_limits, m_options.listen);
    m_network_host = m_default_host;
    InstallRpcHttpBridge(*m_default_host, *this);
    m_default_egress = std::make_shared<HttpEgress>(m_default_host);
    for (const auto& r : m_options.static_routes)
        m_routes.emplace(r.service_name, r.address);   // 重复名保先见，run 校验拒绝
}

// 测试缝构造：显式注入宿主与出站发送（private，经 MakeCoAppForTest）。
CoApp::CoApp(CoAppOptions options, CoAppSeam seam)
    : m_options(std::move(options)),
      m_network_host(std::move(seam.host)),
      m_seam_send(std::move(seam.rpc_send)) {
    for (const auto& r : m_options.static_routes)
        m_routes.emplace(r.service_name, r.address);
}

CoApp::~CoApp() = default;

std::unique_ptr<CoApp> MakeCoAppForTest(CoAppOptions options,
                                        CoAppSeam seam) {
    // 测试缝构造是 private：唯一构造口在本处（CoApp 的友元）。
    return std::unique_ptr<CoApp>(
        new CoApp(std::move(options), std::move(seam)));
}

int CoApp::run() {
    {
        std::lock_guard<std::mutex> lk(g_active_app_mtx);
        if (g_active_app != nullptr)
            return HostLifecycle::kExitRejected;   // 已有活跃宿主
        g_active_app = this;
    }
    ActiveAppGuard release_active;

    if (m_ran_once.exchange(true, std::memory_order_acq_rel))
        return HostLifecycle::kExitRejected;       // run() 一次性

    {
        std::lock_guard<std::mutex> lk(m_mtx);
        m_registration_open = false;
    }

    // run 前校验：任何失败都在启动任何组件之前返回。
    if (auto r = _ValidateConfig(); !r) {
        std::lock_guard<std::mutex> lk(m_mtx);
        m_early_failures.push_back(r.error().message);
        return HostLifecycle::kExitRejected;
    }

    HostLifecycle lifecycle(m_network_host,
                            m_options.shutdown_step_budget,
                            &m_shutdown_req);
    m_lifecycle.store(&lifecycle, std::memory_order_release);

    HostLifecycle::Hooks hooks;
    hooks.on_scheduler_started = [this] { return _BindServices(); };
    hooks.on_release           = [this] { _ReleaseServices(); };

    const int rc = lifecycle.Run(hooks);

    // 生命周期对象随 run 结束销毁前，把失败清单与最终状态留档，
    // 供 lifecycle_failures()/shutdown_state() 在 run 后仍可读。
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        auto f = lifecycle.Failures();
        m_early_failures.insert(m_early_failures.end(),
                                std::make_move_iterator(f.begin()),
                                std::make_move_iterator(f.end()));
    }
    m_parked_state.store(lifecycle.State(), std::memory_order_release);
    m_lifecycle.store(nullptr, std::memory_order_release);
    return rc;
}

void CoApp::request_shutdown() noexcept {
    m_shutdown_req.store(true, std::memory_order_release);
    if (auto* lc = m_lifecycle.load(std::memory_order_acquire))
        lc->RequestShutdown();
    else {
        // run 尚未开始：对外表达「已进入关闭意图」；已结束则保持 Closed。
        auto expected = ShutdownState::Running;
        m_parked_state.compare_exchange_strong(
            expected, ShutdownState::Closing, std::memory_order_release);
    }
}

ShutdownState CoApp::shutdown_state() const noexcept {
    if (auto* lc = m_lifecycle.load(std::memory_order_acquire))
        return lc->State();
    return m_parked_state.load(std::memory_order_acquire);
}

std::vector<std::string> CoApp::pending_cleanup() const {
    if (auto* lc = m_lifecycle.load(std::memory_order_acquire))
        return lc->IncompleteSteps();
    return {};
}

std::vector<std::string> CoApp::lifecycle_failures() const {
    if (auto* lc = m_lifecycle.load(std::memory_order_acquire))
        return lc->Failures();
    std::lock_guard<std::mutex> lk(m_mtx);
    return m_early_failures;
}

result<bbt::infra::RpcAddress> CoApp::find_route(
    std::string_view service_name) const {
    std::lock_guard<std::mutex> lk(m_mtx);
    const auto it = m_routes.find(std::string(service_name));
    if (it == m_routes.end())
        return result<bbt::infra::RpcAddress>::err(MakeError(
            ErrorCode::NotFound,
            "find_route: no configured target for service '" +
                std::string(service_name) + "'"));
    return result<bbt::infra::RpcAddress>::ok(it->second);
}

result<std::shared_ptr<ICoService>> CoApp::find_service(
    std::string_view service_name) const {
    std::lock_guard<std::mutex> lk(m_mtx);
    const auto it = m_services.find(std::string(service_name));
    if (it == m_services.end())
        return result<std::shared_ptr<ICoService>>::err(MakeError(
            ErrorCode::NotFound,
            "find_service: unregistered service '" +
                std::string(service_name) + "'"));
    if (it->second.options.execution == ExecutionPolicy::ActorSerial)
        return result<std::shared_ptr<ICoService>>::err(MakeError(
            ErrorCode::InvalidArgument,
            "find_service: actor-keyed service '" +
                std::string(service_name) +
                "'; use actor_registry()"));
    const auto inst = m_instances.find(std::string(service_name));
    if (inst == m_instances.end())
        return result<std::shared_ptr<ICoService>>::err(MakeError(
            ErrorCode::RuntimeUnavailable,
            "find_service: service '" + std::string(service_name) +
                "' not bound (run not in progress)"));
    return result<std::shared_ptr<ICoService>>::ok(inst->second);
}

ActorRegistry* CoApp::actor_registry() const noexcept {
    std::lock_guard<std::mutex> lk(m_mtx);
    return m_actor_registry.get();
}

std::string CoApp::bound_endpoint() const {
    return m_default_host ? m_default_host->bound_endpoint()
                          : std::string{};
}

result<void> CoApp::_ValidateConfig() const {
    if (!m_network_host)
        return FailInvalid("CoApp: network host is not configured");
    if (auto r = bbt::infra::ValidateNetworkLimits(m_options.network_limits); !r)
        return r;
    if (m_options.listen.host.empty())
        return FailInvalid("CoApp: listen host is empty");
    if (m_options.shutdown_step_budget <= std::chrono::milliseconds{0})
        return FailInvalid("CoApp: shutdown_step_budget must be > 0");
    for (const auto& r : m_options.static_routes) {
        if (r.service_name.empty())
            return FailInvalid("CoApp: static route with empty service_name");
        if (r.address.transport.empty() || r.address.endpoint.empty())
            return FailInvalid("CoApp: static route '" + r.service_name +
                               "' has empty transport/endpoint");
    }
    // 重复 service_name 的静态路由 → 拒绝（map 已保先见，此处显式计数）。
    if (m_routes.size() != m_options.static_routes.size())
        return FailInvalid("CoApp: duplicate static route service_name");
    return result<void>::ok();
}

result<void> CoApp::_BindServices() {
    // Scheduler::Start 之后调用（generation 已就位）。Concurrent → 单实例
    // 绑定；ActorSerial → 注册表工厂在激活时绑定身份，actor key 由注册表
    // 经 _bind_actor_key 绑定。所有实例共享同一出站注入点（经路由门）
    // 与同一应用级资源表（context().resource<R>() 的数据源）。
    const ICoService::RpcSendFn egress = _MakeEgress();
    decltype(m_instances) instances;
    auto registry = std::make_unique<ActorRegistry>();
    std::map<std::string, InboundDispatcher::ServiceEntry> dispatch;
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        for (const auto& [name, entry] : m_services) {
            try {
                if (entry.options.execution == ExecutionPolicy::Concurrent) {
                    auto inst = entry.factory();
                    inst->_bind_runtime(
                        bbt::coroutine::CreateObjectInfo("service", name),
                        std::nullopt, egress, m_resources);
                    instances.emplace(name, inst);
                    dispatch.emplace(name,
                        InboundDispatcher::ServiceEntry{
                            entry.options, &entry.table,
                            std::move(inst), nullptr});
                } else {
                    const auto factory = entry.factory;
                    const auto max_actors = entry.options.max_actors;
                    const auto resources = m_resources;
                    auto r = registry->RegisterFactory(name,
                        [factory, name, egress, resources]()
                            -> std::shared_ptr<ICoService> {
                            auto s = factory();
                            if (s)
                                s->_bind_runtime(
                                    bbt::coroutine::CreateObjectInfo(
                                        "service", name),
                                    std::nullopt, egress, resources);
                            return s;
                        },
                        max_actors);
                    if (!r)
                        return r;
                    dispatch.emplace(name,
                        InboundDispatcher::ServiceEntry{
                            entry.options, &entry.table,
                            nullptr, registry.get()});
                }
            } catch (const std::exception& e) {
                return result<void>::err(MakeError(ErrorCode::InternalError,
                    "CoApp bind '" + name + "': " + e.what()));
            }
        }
        m_instances      = std::move(instances);
        m_actor_registry = std::move(registry);
        m_dispatcher = std::make_unique<InboundDispatcher>(
            std::move(dispatch));
    }
    return result<void>::ok();
}

void CoApp::_ReleaseServices() noexcept {
    std::lock_guard<std::mutex> lk(m_mtx);
    // 分发器先于注册表/实例销毁：不再接受的入站才不再有执行入口。
    m_dispatcher.reset();
    m_instances.clear();
    m_actor_registry.reset();
}

ICoService::RpcSendFn CoApp::_MakeEgress() {
    // 路由门在宿主侧：业务只给逻辑 service 名，实际目标必须命中显式
    // 静态路由；发送实现 = 默认 HttpEgress（业务路径）或测试缝注入的
    // rpc_send。两者都缺失 → 如实 RuntimeUnavailable。
    return [this](const bbt::infra::RpcEnvelope& env,
                  const bbt::infra::CallOptions& options)
        -> result<bbt::infra::RpcEnvelope> {
        auto addr = find_route(env.service);
        if (!addr)
            return result<bbt::infra::RpcEnvelope>::err(addr.error());
        if (m_default_egress)
            return m_default_egress->Send(addr.value(), env, options);
        if (!m_seam_send)
            return result<bbt::infra::RpcEnvelope>::err(MakeError(
                ErrorCode::RuntimeUnavailable,
                "CoApp: rpc_send egress not configured"));
        return m_seam_send(addr.value(), env, options);
    };
}

result<bbt::infra::RpcEnvelope> CoApp::dispatch_inbound(
    const bbt::infra::IncomingCallContext& incoming,
    bbt::infra::RpcEnvelope request) {
    InboundDispatcher* dispatcher;
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        dispatcher = m_dispatcher.get();
    }
    if (dispatcher == nullptr)
        return result<bbt::infra::RpcEnvelope>::err(MakeError(
            ErrorCode::RuntimeUnavailable,
            "dispatch_inbound: app not running"));
    return dispatcher->Dispatch(incoming, std::move(request));
}

} // namespace bbt::framework
