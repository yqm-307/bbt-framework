/**
 * framework CoApp 生命周期与受管对象装配。
 *
 * 条目引用纪律：RegistryEntry* 只在「持锁取指针 + 同锁内读写」的同一临界区
 * 使用；任何锁外 hook/事件派发/析构都只携带 ObjectId 与对象 owner（
 * shared_ptr<void> 的 callback lease），回到锁内再 FindEntry 重查——deque
 * erase 会使全部指针/引用失效。
 *
 * 锁外业务代码访问纪律：owner 保证 C++ 对象寿命；绑定通知还需在锁内
 * 取得回调资格，保证收口等通知的真实终态，不能只靠 shared_ptr。
 */

#include <algorithm>
#include <chrono>
#include <mutex>
#include <unordered_set>
#include <utility>
#include <stdexcept>
#include <thread>

#include <boost/scope/scope_exit.hpp>
#include <boost/scope/scope_fail.hpp>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/internal/AppRuntimeState.hpp>
#include <bbt/framework/internal/CoAppSeam.hpp>
#include <bbt/framework/internal/InboundDispatcher.hpp>
#include <bbt/framework/internal/InfraHttpHost.hpp>
#include <bbt/framework/internal/RpcBridge.hpp>

namespace bbt::framework {

namespace {

bool IsStopState(AppState state) {
    return state == AppState::Stopping || state == AppState::StopIncomplete || state == AppState::Stopped;
}

/**
 * hook 错误捕获：业务 hook 的错误返回原样透出；异常 → InternalError。
 */
template <class F>
result<void> RunHookCaught(F&& fn) {
    try {
        return fn();
    } catch (const std::exception& ex) {
        Error e = MakeError(ErrorCode::InternalError,
                            std::string("hook threw: ") + ex.what());
        e.domain = "framework";
        return result<void>::err(std::move(e));
    } catch (...) {
        Error e = MakeError(ErrorCode::InternalError, "hook threw (non-std)");
        e.domain = "framework";
        return result<void>::err(std::move(e));
    }
}

} // namespace

/**
 * ---- 构造/析构（构造 private；唯一入口 CoApp::Create，见 App.hpp）----
 */

std::shared_ptr<CoApp> CoApp::Create(AppOptions options, std::string name) {
    return bbt::coroutine::MakeCoObject<CoApp>(
        "framework.CoApp", std::move(name),
        [opts = std::move(options)]() mutable {
            return std::shared_ptr<CoApp>(new CoApp(std::move(opts)));
        });
}

namespace {

/**
 * Registry 构造的公共尾（两个受控构造共用；业务 Registry 簿记先于网络装配）。
 */
std::shared_ptr<detail::Registry> MakeRegistry(const AppOptions& options) {
    detail::Registry::Limits limits;
    limits.max_objects = options.MaxObjects();
    limits.max_bindings = options.MaxBindings();
    limits.max_observers = options.MaxObservers();
    limits.lifecycle_budget = options.LifecycleBudget();
    limits.shutdown_budget = options.ShutdownBudget();
    return std::make_shared<detail::Registry>(limits);
}

} // namespace

CoApp::CoApp(AppOptions options)
    : m_options(std::move(options)) {
    bbt::coroutine::RequireCoroutineContext();
    m_registry = MakeRegistry(m_options);
    auto registered = _RegisterAppObject();
    if (!registered)
        throw std::runtime_error(registered.error().message);

    /**
     * 默认装配：真实网络宿主 + 入站线桥 + 默认出站。构造期只装配不联网
     * （Create/Start 在 CoStart 内）；InstallHandler/InstallRpcHandler 只存
     * 业务入口，此处安装是安全的；handler 闭包按值复制运行时状态的
     * shared_ptr，不借用宿主/CoApp 成员、不捕获 this（见
     * internal/AppRuntimeState.hpp 的寿命契约）。
     */
    m_default_host = std::make_shared<InfraHttpHost>(
        m_options.NetworkLimits(), m_options.ListenAddr(),
        m_options.LocalIdentity());
    m_network_host = m_default_host;
    m_runtime = std::make_shared<AppRuntimeState>();
    InstallRpcAdmission(*m_default_host, m_runtime);
    m_runtime->default_egress = std::make_shared<RpcEgress>(m_default_host);
    m_runtime->rpc_policy = m_options.RpcPolicyPtr();
    m_runtime->dynamic_routes = m_options.DynamicRoutes();
    for (const auto& r : m_options.StaticRoutes())
        /**
         * 重复名保先见，CoStart 校验拒绝
         */
        m_runtime->routes.emplace(r.service_name, r.address);
}

CoApp::CoApp(AppOptions options, struct CoAppSeam seam)
    : m_options(std::move(options)) {
    bbt::coroutine::RequireCoroutineContext();
    m_registry = MakeRegistry(m_options);
    auto registered = _RegisterAppObject();
    if (!registered)
        throw std::runtime_error(registered.error().message);

    /**
     * 测试缝装配：显式注入宿主与出站发送；路由/策略仍来自 AppOptions。
     */
    m_network_host = std::move(seam.host);
    m_seam_host = true;
    m_runtime = std::make_shared<AppRuntimeState>();
    m_runtime->seam_send = std::move(seam.rpc_send);
    m_runtime->rpc_policy = m_options.RpcPolicyPtr();
    m_runtime->dynamic_routes = m_options.DynamicRoutes();
    for (const auto& r : m_options.StaticRoutes())
        m_runtime->routes.emplace(r.service_name, r.address);
}

std::shared_ptr<CoApp> MakeCoAppForTest(AppOptions options, CoAppSeam seam) {
    /**
     * 测试缝构造是 private：唯一构造口在本处（CoApp 的友元）。
     */
    return std::shared_ptr<CoApp>(
        new CoApp(std::move(options), std::move(seam)));
}

result<bbt::infra::rpc::Response> DispatchInboundForTest(
    CoApp& app,
    const bbt::infra::rpc::Request& request) {
    /**
     * 转发私有 dispatch_inbound：测试侧拿到真实 Dispatcher/RequestContext
     * 链路（envelope 校验 → 上下文落地 → 执行策略分发 → 回复封包），
     * 不绕过 InboundDispatcher 直接触 handler。
     */
    return app.dispatch_inbound(request);
}

/**
 * 只释放自身 owner，不隐式执行 hook 或等待。
 */
CoApp::~CoApp() = default;

result<void> CoApp::_RegisterAppObject() {
    std::unique_lock<std::mutex> lk(m_registry->Mutex());
    auto r = m_registry->CreateEntryLocked(
        ObjectKind::App, "app", "", ComponentScope::App, {},
        typeid(CoApp), std::shared_ptr<void>{});
    if (!r)
        return result<void>::err(std::move(r.error()));
    auto* e = r.value();
    auto events = m_registry->SetStateLocked(*e, ObjectState::Running);
    m_self = m_registry->MakeHandleLocked(*e);
    lk.unlock();
    m_registry->DispatchEvents(std::move(events));
    m_app_entry_registered = true;
    return result<void>::ok();
}

result<ObjectHandle> CoApp::FindService(std::string_view service) const {
    std::unique_lock<std::mutex> lk(m_registry->Mutex());
    auto* e = m_registry->FindByTypeNameLocked(ObjectKind::Service, service);
    if (!e)
        return result<ObjectHandle>::err(MakeError(ErrorCode::NotFound,
                                                   "FindService: not found"));
    return result<ObjectHandle>::ok(m_registry->MakeHandleLocked(*e));
}

result<ObjectHandle> CoApp::FindActor(const ActorId& id) const {
    std::unique_lock<std::mutex> lk(m_registry->Mutex());
    auto* e = m_registry->FindActorLocked(id.first, id.second);
    if (!e)
        return result<ObjectHandle>::err(MakeError(ErrorCode::NotFound,
                                                   "FindActor: not found"));
    return result<ObjectHandle>::ok(m_registry->MakeHandleLocked(*e));
}

result<ObjectHandle> CoApp::_FindComponentImpl(
    const std::type_info& ctype, std::string_view name,
    const ComponentOptions& placement) const {
    ObjectId owner_id;
    if (auto r = const_cast<CoApp*>(this)->_ValidateOwner(placement, &owner_id);
        !r)
        return result<ObjectHandle>::err(std::move(r.error()));
    std::unique_lock<std::mutex> lk(m_registry->Mutex());
    auto* e = m_registry->FindComponentLocked(placement.Scope(), owner_id,
                                              ctype, name);
    if (!e)
        return result<ObjectHandle>::err(MakeError(ErrorCode::NotFound,
                                                   "FindComponent: not found"));
    return result<ObjectHandle>::ok(m_registry->MakeHandleLocked(*e));
}

bbt_co_async(result<ObserverToken>) CoApp::CoObserve(const ObjectHandle& observer,
                                                     const ObjectHandle& target,
                                                     ObjectObserver callback) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<ObserverToken>::err(std::move(ctx.error()));
    return m_registry->Observe(observer, target, std::move(callback));
}

ObjectHandle CoApp::Handle() const {
    return m_self;
}

std::vector<CleanupInfo> CoApp::PendingCleanup() const {
    return m_registry->PendingCleanup();
}

std::vector<LifecycleFailure> CoApp::LifecycleFailures() const {
    return m_registry->Failures();
}

result<void> CoApp::_CheckCoContext() const {
    const auto co = bbt::coroutine::GetLocalCoroutineId();
    if (co == 0)
        return result<void>::err(MakeError(ErrorCode::InvalidContext,
                                           "lifecycle: Co* entry points require a managed coroutine context"));
    if (m_hook_co->load(std::memory_order_acquire) == co ||
        m_registry->InBindingCallback(co))
        return result<void>::err(MakeError(ErrorCode::InvalidContext,
                                           "lifecycle: a lifecycle entry point called by its own hook would wait on itself"));
    return result<void>::ok();
}

/**
 * 宿主 hook/start/drain 共用的一次结果交付槽：发布方锁内写一次
 * 结果与 done、锁外 Notify；调用协程按同一绝对 deadline 在 waiter
 * 上挂起（不阻塞 worker）。Notify 与「读取 done」的先后竞争由等待
 * 侧 on_registered 复查关闭（CoWaiter 对挂起前到达的 Notify 走
 * PENDING 早到路径，不丢唤醒）；等待超时返回后发布方仍可写槽——
 * 槽由发布方经 shared_ptr 保活。槽只做结果交付：线程 join 与迟到
 * 结果仍归各调用方（hook 任务、start/drain 辅助线程各自回收）。
 */
struct Completion {
    std::mutex mtx;
    bool done = false;
    result<void> rc = result<void>::ok();
    bbt::coroutine::sync::CoWaiter::SPtr waiter =
        bbt::coroutine::sync::CoWaiter::Create();

    /**
     * 发布终态：仅发布方调用一次；锁内写结果，锁外唤醒等待协程。
     */
    void Publish(result<void> r) {
        {
            std::lock_guard<std::mutex> lk(mtx);
            rc = std::move(r);
            done = true;
        }
        waiter->Notify();
    }

    /**
     * 取回结果（等待侧确认 Completed 后调用）。
     */
    result<void> Take() {
        std::lock_guard<std::mutex> lk(mtx);
        return std::move(rc);
    }
};

/**
 * 一次结果等待：登记成功、尚未挂起前复查 done——发布方在此窗内
 * 已完成的直接自唤醒（PENDING 早到路径），关闭「先完成丢失 Notify」
 * 竞态；deadline 到期只结束本次等待。
 */
bbt::coroutine::WaitStatus AwaitCompletion(
    const std::shared_ptr<Completion>& c,
    bbt::coroutine::Deadline deadline) {
    bbt::coroutine::WaitOptions wo;
    wo.deadline = deadline;
    return c->waiter->WaitWithCallback(wo, [c] {
        std::lock_guard<std::mutex> lk(c->mtx);
        if (c->done)
            c->waiter->Notify();
        return true;
    });
}

/**
 * 协程等待结果 → result：Completed 取回 hook 结果；TimedOut 记超时
 * 并返回（超时只结束等待：hook 协程与 hooks_in_flight 责任继续到
 * 真实终态）；Cancelled/InvalidContext/RuntimeUnavailable 如实透出。
 */
result<void> FinishCoHookWait(const std::shared_ptr<Completion>& slot,
                              bbt::coroutine::WaitStatus ws) {
    using WS = bbt::coroutine::WaitStatus;
    switch (ws) {
    case WS::Completed:
        return slot->Take();
    case WS::TimedOut:
        return result<void>::err(MakeError(ErrorCode::TimedOut,
                                           "lifecycle hook deadline exceeded"));
    case WS::Cancelled:
        return result<void>::err(MakeError(ErrorCode::Cancelled,
                                           "lifecycle hook wait cancelled"));
    case WS::InvalidContext:
        return result<void>::err(MakeError(ErrorCode::InvalidContext,
                                           "lifecycle: CoWaiter must be awaited from a managed coroutine"));
    case WS::AlreadyWaiting:
        return result<void>::err(MakeError(ErrorCode::Overloaded,
                                           "lifecycle: hook wait slot already occupied"));
    case WS::RuntimeUnavailable:
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
                                           "lifecycle: coroutine wait runtime unavailable"));
    }
    return result<void>::err(MakeError(ErrorCode::InternalError,
                                       "lifecycle: unknown wait status"));
}

/**
 * 在受管协程内等待指定对象 hooks_in_flight 归零，deadline 与本次
 * 生命周期转换共享同一绝对时间。Completed → ok；TimedOut → 在途
 * hook 责任照旧保留，如实返回 TimedOut；其他状态如实映射。
 * 不重入在途 hook：只在「等待归零」语义上续等，hook 本身不被强杀。
 */
result<void> WaitHooksSettledCo(
    const std::shared_ptr<detail::Registry>& reg, const ObjectId& id,
    bbt::coroutine::Deadline deadline) {
    using WS = bbt::coroutine::WaitStatus;
    WS ws;
    do {
        std::shared_ptr<bbt::coroutine::sync::CoWaiter> w;
        {
            std::unique_lock<std::mutex> lk(reg->Mutex());
            auto* e = reg->FindEntryLocked(id);
            if (!e || e->hooks_in_flight == 0)
                return result<void>::ok();
            w = reg->HookSettledWaiterLocked(*e);
        }
        bbt::coroutine::WaitOptions wo;
        wo.deadline = deadline;
        /**
         * on_registered 复查：若在「取 waiter → 登记等待」窗口内前序 hook
         * 已归零，自唤醒（PENDING 早到路径）避免空等到 deadline。
         */
        ws = w->WaitWithCallback(wo, [reg, id, w] {
            bool settled = false;
            {
                std::lock_guard<std::mutex> lk(reg->Mutex());
                auto* e = reg->FindEntryLocked(id);
                settled = (!e || e->hooks_in_flight == 0);
            }
            if (settled)
                w->Notify();
            return true;
        });
        // 唤醒后复查计数：上一批归零的通知可能晚于下一批的接纳。
    } while (ws == WS::Completed);
    switch (ws) {
    case WS::Completed:
        return result<void>::ok();
    case WS::TimedOut:
        return result<void>::err(MakeError(ErrorCode::TimedOut,
                                           "lifecycle: prior hook still in flight at deadline"));
    case WS::Cancelled:
        return result<void>::err(MakeError(ErrorCode::Cancelled,
                                           "lifecycle: hook-settle wait cancelled"));
    case WS::InvalidContext:
        return result<void>::err(MakeError(ErrorCode::InvalidContext,
                                           "lifecycle: hook-settle wait requires a managed coroutine"));
    case WS::AlreadyWaiting:
        return result<void>::err(MakeError(ErrorCode::Overloaded,
                                           "lifecycle: another coroutine is already waiting for this object’s hooks to settle"));
    case WS::RuntimeUnavailable:
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
                                           "lifecycle: coroutine wait runtime unavailable"));
    }
    return result<void>::err(MakeError(ErrorCode::InternalError,
                                       "lifecycle: unknown wait status"));
}

/**
 * m_lifecycle_active 的 RAII 释放：任一出口（含提前 return）统一复位，
 * 避免在各分支手写 store(false)。
 */
struct LifecycleActiveGuard {
    std::atomic_bool& active;
    explicit LifecycleActiveGuard(std::atomic_bool& a) : active(a) {}
    ~LifecycleActiveGuard() { active.store(false, std::memory_order_release); }
};

bbt_co_async(result<void>) CoApp::CoStart(const LifecycleOptions& life) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<void>::err(std::move(ctx.error()));
    return _StartImpl(life);
}

result<void> CoApp::_StartImpl(const LifecycleOptions& life) {
    /**
     * 协程入口不得阻塞 worker 等转换锁：冲突转换进行中 → Busy。
     */
    if (m_lifecycle_active.exchange(true, std::memory_order_acq_rel))
        return result<void>::err(MakeError(ErrorCode::Overloaded,
                                           "Start: lifecycle transition in progress"));
    LifecycleActiveGuard release{m_lifecycle_active};
    const AppState s = m_state.load();
    if (s == AppState::Stopped || s == AppState::StopIncomplete)
        return result<void>::err(MakeError(ErrorCode::Closed,
                                           "Start: app already stopped"));
    if (s != AppState::Created && s != AppState::Paused)
        return result<void>::err(MakeError(ErrorCode::Overloaded,
                                           "Start: app not in Created/Paused state"));
    if (auto r = m_options.Validate(); !r) {
        LifecycleFailure f;
        f.object_id = Handle().Id();
        f.operation = "start";
        f.phase = "validate";
        f.code = r.error().code;
        f.at = std::chrono::system_clock::now();
        f.message = r.error().message;
        m_registry->RecordFailure(std::move(f));
        return r;
    }

    const auto deadline = m_registry->ResolveDeadline(life);
    /**
     * 组件启动前拒绝非法监听、限额、身份或静态路由，
     * 记一条 LifecycleFailure 供公开观测口读取，组件 OnStart 一律不执行。
     * 网络相位（分发表发布/宿主 Create/Start）仍在服务启动之后。
     */
    if (m_network_host != nullptr &&
        (m_default_host == nullptr || m_options.ListenUri().has_value())) {
        if (auto r = _ValidateNetworkConfig(); !r) {
            LifecycleFailure f;
            f.object_id = Handle().Id();
            f.operation = "start";
            f.phase = "validate";
            f.code = r.error().code;
            f.at = std::chrono::system_clock::now();
            f.message = r.error().message;
            m_registry->RecordFailure(std::move(f));
            _RollbackStart({}, deadline);
            return r;
        }
    }

    m_state.store(AppState::Starting);

    /**
     * 组件先启动：按依赖拓扑（依赖先于使用者）；服务随后按安装顺序。
     */
    std::vector<ObjectId> comps, svcs;
    {
        std::unique_lock<std::mutex> lk(m_registry->Mutex());
        bool had_cycle = false;
        auto order = m_registry->ComponentStartOrderLocked(m_registry->ComponentDependenciesLocked(), &had_cycle);
        if (had_cycle) {
            LifecycleFailure f;
            f.operation = "start";
            f.phase = "topo";
            f.code = ErrorCode::InternalError;
            f.at = std::chrono::system_clock::now();
            f.message = "component dependency cycle residual";
            m_registry->RecordFailure(std::move(f));
        }
        /**
         * 只启动 Installed 态组件（保留拓扑序）。
         */
        for (const auto& id : order) {
            auto* e = m_registry->FindEntryLocked(id);
            if (e && e->state == ObjectState::Installed)
                comps.push_back(id);
        }
        svcs = m_registry->ListIdsLocked(ObjectKind::Service,
                                         ObjectState::Installed);
    }
    /**
     * 已启动对象逆序记录（组件 + 服务），供网络相位失败的启动回退使用。
     * hook 失败不走回退（冻结契约：首个 hook 失败原错误透出、该对象记
     * Failed，App 仍置 Running——部分启动诚实反映，不虚报事务回滚）。
     */
    std::vector<ObjectId> started;
    for (const auto& id : comps) {
        m_registry->SetStateAndDispatch(id, ObjectState::Starting);
        auto r = _RunHookCo(id, &CoComponent::OnStart, "start",
                            deadline);
        if (!r) {
            m_registry->SetStateAndDispatch(
                id, ObjectState::Failed,
                ObjectError{r.error().code, "start",
                            r.error().message});
            /**
             * 启动序列就此终止：已启动对象保留（不回退），未启动对象保持
             * Installed（可单独卸载），网络相位不再进入；错误原样透出。
             */
            m_state.store(AppState::Running);
            return r;
        }
        m_registry->SetStateAndDispatch(id, ObjectState::Running);
        started.push_back(id);
    }
    for (const auto& id : svcs) {
        m_registry->SetStateAndDispatch(id, ObjectState::Starting);
        auto r = _RunHookCo(id, &CoServiceBase::OnStart, "start",
                            deadline);
        if (!r) {
            m_registry->SetStateAndDispatch(
                id, ObjectState::Failed,
                ObjectError{r.error().code, "start",
                            r.error().message});
            m_state.store(AppState::Running);
            return r;
        }
        m_registry->SetStateAndDispatch(id, ObjectState::Running);
        started.push_back(id);
    }

    /**
     * 网络相位：配置校验 → 分发表发布 → 宿主 Create/Start（开始接纳）。
     * 资源/服务就绪后再接纳（装配顺序约束）。任一步失败同样按启动失败
     * 回退（宿主 Close + 已启动对象按当前依赖收口）。
     */
    if (m_network_host != nullptr &&
        (m_default_host == nullptr || m_options.ListenUri().has_value())) {
        if (auto r = _ValidateNetworkConfig(); !r) {
            _RollbackStart(started, deadline);
            return r;
        }
        if (auto r = _StartNetworkPhase(deadline); !r) {
            _RollbackStart(started, deadline);
            return r;
        }
    }
    m_state.store(AppState::Running);
    return result<void>::ok();
}

bbt_co_async(result<void>) CoApp::CoPause(const LifecycleOptions& life) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<void>::err(std::move(ctx.error()));
    return _PauseImpl(life);
}

result<void> CoApp::_PauseImpl(const LifecycleOptions& life) {
    if (m_lifecycle_active.exchange(true, std::memory_order_acq_rel))
        return result<void>::err(MakeError(ErrorCode::Overloaded,
                                           "Pause: lifecycle transition in progress"));
    LifecycleActiveGuard release{m_lifecycle_active};
    if (m_state.load() != AppState::Running)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                           "Pause: app not Running"));
    const auto deadline = m_registry->ResolveDeadline(life);

    std::vector<ObjectId> svcs;
    {
        std::unique_lock<std::mutex> lk(m_registry->Mutex());
        svcs = m_registry->ListIdsLocked(ObjectKind::Service,
                                         ObjectState::Running);
    }
    for (const auto& id : svcs) {
        auto r = _RunHookCo(id, &CoServiceBase::OnPause, "pause",
                            deadline);
        if (!r) {
            m_registry->SetStateAndDispatch(
                id, ObjectState::Failed,
                ObjectError{r.error().code, "pause",
                            r.error().message});
            return r;
        }
        m_registry->SetStateAndDispatch(id, ObjectState::Paused);
    }
    m_state.store(AppState::Paused);
    return result<void>::ok();
}

bbt_co_async(result<void>) CoApp::CoResume(const LifecycleOptions& life) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<void>::err(std::move(ctx.error()));
    return _ResumeImpl(life);
}

result<void> CoApp::_ResumeImpl(const LifecycleOptions& life) {
    if (m_lifecycle_active.exchange(true, std::memory_order_acq_rel))
        return result<void>::err(MakeError(ErrorCode::Overloaded,
                                           "Resume: lifecycle transition in progress"));
    LifecycleActiveGuard release{m_lifecycle_active};
    if (m_state.load() != AppState::Paused)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                           "Resume: app not Paused"));
    const auto deadline = m_registry->ResolveDeadline(life);

    std::vector<ObjectId> svcs;
    {
        std::unique_lock<std::mutex> lk(m_registry->Mutex());
        svcs = m_registry->ListIdsLocked(ObjectKind::Service,
                                         ObjectState::Paused);
    }
    for (const auto& id : svcs) {
        auto r = _RunHookCo(id, &CoServiceBase::OnResume, "resume",
                            deadline);
        if (!r) {
            m_registry->SetStateAndDispatch(
                id, ObjectState::Failed,
                ObjectError{r.error().code, "resume",
                            r.error().message});
            return r;
        }
        m_registry->SetStateAndDispatch(id, ObjectState::Running);
    }
    m_state.store(AppState::Running);
    return result<void>::ok();
}

bbt_co_async(result<void>) CoApp::CoStop(const LifecycleOptions& life) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<void>::err(std::move(ctx.error()));
    return _StopImpl(life);
}

result<void> CoApp::_StopImpl(const LifecycleOptions& life) {
    if (m_lifecycle_active.exchange(true, std::memory_order_acq_rel))
        return result<void>::err(MakeError(ErrorCode::Overloaded,
                                           "Stop: lifecycle transition in progress"));
    LifecycleActiveGuard release{m_lifecycle_active};
    const AppState s = m_state.load();
    if (s == AppState::Stopped)
        return result<void>::ok();
    {
        // 与绑定发布使用同一把锁，停止快照之后不会出现新依赖。
        std::lock_guard<std::mutex> lk(m_registry->Mutex());
        m_state.store(AppState::Stopping);
    }
    const auto deadline = m_registry->ResolveShutdownDeadline(life);

    /**
     * 网络收口先于业务对象（旧宿主顺序的对应物）：拒新接纳 → 等已接纳
     * handler 排空。本轮关闭的总期限是上方单一绝对 deadline（未显式
     * SetDeadline 时为 shutdown_budget）：_DrainNetwork 与 _StopAll 的
     * 全部子步骤共享它，不得重置。排空被证实才允许继续对象收口；排空
     * 未被证实（预算耗尽/宿主 Close 打断/其他错误）→ 记失败项、撤销
     * 接纳、同步 Close 网络宿主，但**不销毁业务对象**：owner 与资源版本
     * 保留，App 置 StopIncomplete，允许再次以更晚期限 CoStop 推进
     * （真实排空责任不因超时消失）。
     */
    if (m_network_host != nullptr) {
        /**
         * 宿主已 Close（前次排空超时的收口或启动回退）：WaitHandlersDone
         * 只会立即返回已发布 close_reason，重试排空永远到不了对象收口。
         * 拒新已由 Close 完成，迟到 handler 的真实终态由 _StopAll 各对象
         * 收口的 borrow/hook 等待负责——直接以本次新 deadline 推进
         * （Release 已在前次失败分支完成，幂等）。
         */
        // 未发布网络的 StopIncomplete（包括启动回退）只需继续对象收口。
        if (!(s == AppState::StopIncomplete && !m_network_published) &&
            !m_network_host->CloseReason().has_value()) {
            m_network_host->StopAccepting();
            if (auto drained = _DrainNetwork(deadline); !drained) {
                LifecycleFailure f;
                f.object_id = Handle().Id();
                f.operation = "stop";
                f.phase = "wait_handlers_done";
                f.code = drained.error().code;
                f.at = std::chrono::system_clock::now();
                f.message = drained.error().message;
                m_registry->RecordFailure(std::move(f));
                m_runtime->Release();
                m_network_host->Close();
                m_state.store(AppState::StopIncomplete);
                return result<void>::ok();
            }
            /**
             * 排空已证实：撤销接纳（Publish/Release 配对），此后业务对象
             * 收口不再有新入站在途。
             */
            m_runtime->Release();
        }
    }

    _StopAll(deadline);
    const bool incomplete = !m_registry->PendingCleanup().empty();
    m_state.store(incomplete ? AppState::StopIncomplete : AppState::Stopped);
    return result<void>::ok();
}

void CoApp::RequestShutdown() noexcept {
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> waiter;
    {
        std::lock_guard<std::mutex> lk(m_run_mtx);
        m_shutdown_req.store(true, std::memory_order_release);
        waiter = m_shutdown_waiter;
    }
    if (waiter)
        waiter->Notify();
}

bbt_co_async(result<void>) CoApp::CoWaitShutdownRequest(
    const bbt::coroutine::WaitOptions& wait) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return ctx;
    auto waiter = bbt::coroutine::sync::CoWaiter::Create();
    {
        std::lock_guard<std::mutex> lk(m_run_mtx);
        if (m_shutdown_waiter)
            return result<void>::err(MakeError(ErrorCode::Overloaded,
                                               "shutdown request already has a waiter"));
        if (m_shutdown_req.load(std::memory_order_acquire))
            return result<void>::ok();
        m_shutdown_waiter = waiter;
    }
    const auto status = waiter->WaitWithCallback(wait, [this, waiter] {
        if (m_shutdown_req.load(std::memory_order_acquire))
            waiter->Notify();
        return true;
    });
    {
        std::lock_guard<std::mutex> lk(m_run_mtx);
        m_shutdown_waiter.reset();
    }
    if (status == bbt::coroutine::WaitStatus::Completed)
        return result<void>::ok();
    if (status == bbt::coroutine::WaitStatus::TimedOut)
        return result<void>::err(MakeError(ErrorCode::TimedOut, "shutdown request timed out"));
    if (status == bbt::coroutine::WaitStatus::Cancelled)
        return result<void>::err(MakeError(ErrorCode::Cancelled, "shutdown request cancelled"));
    return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
                                       "shutdown request wait unavailable"));
}

namespace {

class ComponentClosePass {
public:
    explicit ComponentClosePass(const detail::Registry::ComponentDependencies& deps)
        : m_deps(deps) {}

    template <class Close>
    std::optional<Error> Run(const std::vector<ObjectId>& order, Close close) {
        std::optional<Error> first_error;
        for (const auto& id : order) {
            if (m_kept.count(id))
                continue;
            if (auto r = close(id); !r) {
                KeepDependencies(id);
                if (!first_error)
                    first_error = std::move(r.error());
            }
        }
        return first_error;
    }

private:
    void KeepDependencies(const ObjectId& id) {
        for (const auto& dep : m_deps.at(id)) {
            if (m_kept.insert(dep).second)
                KeepDependencies(dep);
        }
    }

    const detail::Registry::ComponentDependencies& m_deps;
    std::unordered_set<ObjectId> m_kept;
};

// 只释放本次已取得的收口资格；回退异常不能让下一次 CoStop 永久 Busy。
void ReleaseInterruptedClose(detail::Registry& registry, const ObjectId& id, ObjectState state) noexcept {
    try {
        std::lock_guard<std::mutex> lk(registry.Mutex());
        if (auto* e = registry.FindEntryLocked(id)) {
            registry.EndOpLocked(*e);
            e->state = state;
        }
    } catch (...) {
        // 避免异常守卫在栈展开期间再次抛出并终止进程。
    }
}

bool ServiceOwnerReady(detail::Registry& registry, const ObjectId& id,
                       const std::vector<const ObjectId*>& ready) {
    std::lock_guard<std::mutex> lk(registry.Mutex());
    const auto* e = registry.FindEntryLocked(id);
    return !e || e->scope != ComponentScope::Service || !registry.FindEntryLocked(e->owner_id) ||
           std::any_of(ready.begin(), ready.end(), [&](const ObjectId* owner) { return *owner == e->owner_id; });
}

} // namespace

void CoApp::_RollbackStart(const std::vector<ObjectId>& started,
                           bbt::coroutine::Deadline deadline) noexcept {
    try {
        {
            // 与绑定发布共锁：Close 或业务 hook 运行前就封住新绑定。
            std::lock_guard<std::mutex> lk(m_registry->Mutex());
            m_state.store(AppState::Stopping);
        }
        if (m_network_host != nullptr)
            m_network_host->Close();
        if (m_network_published) {
            m_runtime->Release();
            m_network_published = false;
        }
        if (started.empty()) {
            m_state.store(AppState::Stopped);
            return;
        }

        detail::Registry::ComponentDependencies deps;
        std::vector<ObjectId> services, components;
        {
            std::lock_guard<std::mutex> lk(m_registry->Mutex());
            const std::unordered_set<ObjectId> started_ids(started.begin(), started.end());
            deps = m_registry->ComponentDependenciesLocked();
            const auto order = m_registry->ComponentStartOrderLocked(deps);
            for (auto it = order.rbegin(); it != order.rend(); ++it)
                if (started_ids.count(*it))
                    components.push_back(*it);
            for (auto it = started.rbegin(); it != started.rend(); ++it) {
                const auto* entry = m_registry->FindEntryLocked(*it);
                if (entry && entry->kind == ObjectKind::Service)
                    services.push_back(*it);
            }
        }

        bool incomplete = false;
        std::vector<const ObjectId*> services_to_finish;
        services_to_finish.reserve(services.size());
        auto release = boost::scope::make_scope_fail([&] {
            for (const auto* id : services_to_finish)
                if (id)
                    ReleaseInterruptedClose(*m_registry, *id, ObjectState::StopIncomplete);
        });
        for (const auto& id : services) {
            if (_DestroyService(id, deadline))
                services_to_finish.push_back(&id);
            else
                incomplete = true;
        }
        ComponentClosePass pass(deps);
        if (pass.Run(components, [&](const ObjectId& id) {
                if (!ServiceOwnerReady(*m_registry, id, services_to_finish))
                    return result<void>::err(MakeError(ErrorCode::Overloaded, "component cleanup: service owner not settled"));
                return _DestroyComponent(id, deadline);
            }))
            incomplete = true;
        for (auto*& id : services_to_finish) {
            if (!_FinishService(*id, "stop", ObjectState::StopIncomplete))
                incomplete = true;
            id = nullptr;
        }
        m_state.store(incomplete ? AppState::StopIncomplete : AppState::Stopped);
    } catch (...) {
        // 保留原始启动错误；绝不能将未证实完成的回退标为 Stopped。
        m_state.store(AppState::StopIncomplete);
    }
}

void CoApp::_StopAll(bbt::coroutine::Deadline deadline) {
    // 同一快照贯穿 scoped/App 两段；scoped 失败也能保留其共享依赖。
    detail::Registry::ComponentDependencies deps;
    std::vector<ObjectId> svcs, actors, scoped, app_scope;
    {
        std::lock_guard<std::mutex> lk(m_registry->Mutex());
        svcs = m_registry->ListIdsByKindLocked(ObjectKind::Service);
        actors = m_registry->ListIdsByKindLocked(ObjectKind::Actor);
        deps = m_registry->ComponentDependenciesLocked();
        const auto order = m_registry->ComponentStartOrderLocked(deps);
        for (auto it = order.rbegin(); it != order.rend(); ++it) {
            const auto* e = m_registry->FindEntryLocked(*it);
            (e->scope == ComponentScope::App ? app_scope : scoped).push_back(*it);
        }
    }

    std::vector<const ObjectId*> services_to_finish;
    services_to_finish.reserve(svcs.size());
    auto release = boost::scope::make_scope_fail([&] {
        for (const auto* id : services_to_finish)
            if (id)
                ReleaseInterruptedClose(*m_registry, *id, ObjectState::StopIncomplete);
    });
    for (const auto& id : svcs) {
        if (_DestroyService(id, deadline))
            services_to_finish.push_back(&id);
    }
    for (const auto& id : actors)
        _DestroyActor(id, deadline);
    ComponentClosePass pass(deps);
    (void)pass.Run(scoped, [&](const ObjectId& id) {
        // owner 尚有 hook 或并发卸载时，不接管其 owned 组件。
        if (!ServiceOwnerReady(*m_registry, id, services_to_finish))
            return result<void>::err(MakeError(ErrorCode::Overloaded, "component cleanup: service owner not settled"));
        return _DestroyComponent(id, deadline);
    });
    for (auto*& id : services_to_finish) {
        (void)_FinishService(*id, "stop", ObjectState::StopIncomplete);
        id = nullptr;
    }
    release.set_active(false);
    (void)pass.Run(app_scope, [&](const ObjectId& id) { return _DestroyComponent(id, deadline); });
}

/**
 * 收口步失败统一出口：记 LifecycleFailure、标 incomplete 态、EndOp 后返回。
 * owner/资源版本保留——incomplete 态对象可 Find、可再次收口；不在此销毁。
 */
void CoApp::_AbortDestroy(const ObjectId& id, const char* operation,
                          const char* phase, const Error& err,
                          ObjectState incomplete_state) noexcept {
    try {
        detail::ObserverEvents events;
        {
            std::lock_guard<std::mutex> lk(m_registry->Mutex());
            auto* e = m_registry->FindEntryLocked(id);
            if (e) {
                m_registry->EndOpLocked(*e);
                e->state = incomplete_state;
                e->destroy_phase = phase;
                events = m_registry->SetStateLocked(*e, incomplete_state);
            }
            m_registry->RecordFailure(LifecycleFailure{id, e ? e->generation : 0, operation, phase,
                                                       err.code, std::chrono::system_clock::now(), err.message});
        }
        m_registry->DispatchEvents(std::move(events));
    } catch (...) {
        // 诊断或事件分配失败不改变已保留的 owner 和重试状态。
    }
}

/**
 * OnStop 与「前序 hook 已达真实终态」的统一收口步：OnStop 失败或仍有
 * hooks_in_flight 时经 abort 收尾。abort 负责 EndOp 与 incomplete 状态
 * 记录（各调用点操作名/状态不同，由参数带入）；返回 abort 产出的错误。
 */
template <class T, class Abort>
result<void> CoApp::_EnsureHookSettled(
    const ObjectId& id, result<void> (T::*hook)(),
    const char* op_name, bbt::coroutine::Deadline deadline,
    Abort&& abort) {
    {
        auto r = _RunHookCo(id, hook, "stop", deadline, true);
        if (!r) {
            Error err = std::move(r.error());
            abort("onstop", err);
            return result<void>::err(std::move(err));
        }
    }
    {
        std::unique_lock<std::mutex> lk(m_registry->Mutex());
        auto* e = m_registry->FindEntryLocked(id);
        if (e && e->hooks_in_flight > 0) {
            Error err = MakeError(ErrorCode::TimedOut,
                                  std::string(op_name) + ": prior hook still in flight");
            lk.unlock();
            abort("hook_in_flight", err);
            return result<void>::err(std::move(err));
        }
    }
    return result<void>::ok();
}

void CoApp::_FinishObject(const ObjectId& id) {
    std::shared_ptr<void> obj;
    detail::ObserverEvents events;
    {
        std::unique_lock<std::mutex> lk(m_registry->Mutex());
        m_registry->RevokeObserversOfLocked(id);
        auto* e = m_registry->FindEntryLocked(id);
        if (e) {
            e->destroy_phase.clear();
            m_registry->EndOpLocked(*e);
            auto [o, ev] = m_registry->FinishDestroyLocked(*e);
            obj = std::move(o);
            events = std::move(ev);
        }
    }
    /**
     * 在途 observer 回调 lease 先排空再释放 owner（回调闭包的捕获对象
     * 寿命不得短于被观察对象）。
     */
    m_registry->DrainInactiveObserversOf(id);
    m_registry->DispatchEvents(std::move(events));
    /**
     * 业务析构锁外
     */
    obj.reset();
}

void CoApp::_NotifyBindingChanged(ObjectKind kind, void* subj,
                                  const BindingEvent& be, const char* op) {
    try {
        if (kind == ObjectKind::Service)
            static_cast<CoServiceBase*>(subj)->OnBindingChanged(be);
        else if (kind == ObjectKind::Component)
            static_cast<CoComponent*>(subj)->OnBindingChanged(be);
        else if (kind == ObjectKind::Actor)
            static_cast<CoActor*>(subj)->OnBindingChanged(be);
    } catch (...) {
        LifecycleFailure f;
        f.object_id = be.subject_id;
        f.operation = op;
        f.phase = "binding_changed_hook";
        f.code = ErrorCode::InternalError;
        f.at = std::chrono::system_clock::now();
        f.message = "OnBindingChanged threw";
        std::unique_lock<std::mutex> lk(m_registry->Mutex());
        m_registry->RecordFailure(std::move(f));
    }
}

result<void> CoApp::_DestroyService(const ObjectId& id,
                                    bbt::coroutine::Deadline deadline,
                                    bool uninstall) {
    const char* operation = uninstall ? "uninstall" : "stop";
    const auto incomplete = uninstall ? ObjectState::UninstallIncomplete : ObjectState::StopIncomplete;
    const auto abort = [&](const char* phase, const Error& err) {
        _AbortDestroy(id, operation, phase, err, incomplete);
    };
    bool acquired = false;
    auto release = boost::scope::make_scope_fail([&] {
        if (acquired)
            ReleaseInterruptedClose(*m_registry, id, incomplete);
    });
    std::string service_name;
    bool uninstall_hook_done;
    {
        std::unique_lock<std::mutex> lk(m_registry->Mutex());
        auto r = m_registry->BeginDestroyLocked(id);
        if (!r)
            return result<void>::err(std::move(r.error()));
        acquired = true;
        auto* e = r.value();
        service_name = e->type_name;
        uninstall_hook_done = e->uninstall_hook_done;
        e->destroy_phase = operation;
        m_registry->RetireSubjectBindingsLocked(id);
        m_registry->RevokeObserversOfLocked(id);
        auto ev = m_registry->SetStateLocked(*e, uninstall ? ObjectState::Uninstalling : ObjectState::Stopping);
        lk.unlock();
        m_runtime->RemoveDispatch(service_name);
        m_registry->DispatchEvents(std::move(ev));
    }

    if (auto r = m_registry->DrainObserversOf(id, deadline); !r) {
        abort("observer_drain", r.error());
        return r;
    }
    if (auto r = m_registry->DrainSubjectBorrows(id, deadline); !r) {
        abort("borrow_drain", r.error());
        return r;
    }
    if (auto r = _EnsureHookSettled(id, &CoServiceBase::OnStop, operation, deadline, abort); !r)
        return r;
    if (!uninstall_hook_done) {
        _RunVoidHook(id, &CoServiceBase::OnUninstall, "uninstall");
        std::lock_guard<std::mutex> lk(m_registry->Mutex());
        m_registry->FindEntryLocked(id)->uninstall_hook_done = true;
    }
    return result<void>::ok();
}

result<void> CoApp::_FinishService(const ObjectId& id, const char* operation,
                                   ObjectState incomplete_state) {
    std::string service_name;
    {
        std::unique_lock<std::mutex> lk(m_registry->Mutex());
        for (const auto& component : m_registry->ListIdsByKindLocked(ObjectKind::Component)) {
            const auto* e = m_registry->FindEntryLocked(component);
            if (e->scope == ComponentScope::Service && e->owner_id == id) {
                lk.unlock();
                auto err = MakeError(ErrorCode::Overloaded, "service cleanup: owned components remain");
                _AbortDestroy(id, operation, "owned_components", err, incomplete_state);
                return result<void>::err(std::move(err));
            }
        }
        service_name = m_registry->FindEntryLocked(id)->type_name;
    }
    _FinishObject(id);
    {
        std::lock_guard<std::mutex> lk(m_net_mtx);
        m_service_descriptions.erase(service_name);
    }
    return result<void>::ok();
}

void CoApp::_DestroyActor(const ObjectId& id, bbt::coroutine::Deadline deadline) {
    {
        std::lock_guard<std::mutex> lk(m_registry->Mutex());
        auto* entry = m_registry->FindEntryLocked(id);
        if (!entry || !m_registry->BeginOpLocked(*entry))
            return;
        entry->gate_closed = true;
        m_registry->RetireSubjectBindingsLocked(id);
    }
    if (auto r = m_registry->DrainSubjectBorrows(id, deadline); !r) {
        _AbortDestroy(id, "destroy", "borrow_drain", r.error(), ObjectState::StopIncomplete);
        return;
    }
    if (auto r = WaitHooksSettledCo(m_registry, id, deadline); !r) {
        _AbortDestroy(id, "destroy", "hook_in_flight", r.error(), ObjectState::StopIncomplete);
        return;
    }
    _RunActorDestroy(id);
    _FinishObject(id);
}

result<void> CoApp::_DestroyComponent(const ObjectId& id,
                                      bbt::coroutine::Deadline deadline) {
    bool acquired = false;
    auto release = boost::scope::make_scope_fail([&] {
        if (acquired)
            ReleaseInterruptedClose(*m_registry, id, ObjectState::StopIncomplete);
    });
    {
        std::unique_lock<std::mutex> lk(m_registry->Mutex());
        auto* e = m_registry->FindEntryLocked(id);
        if (!e)
            return result<void>::ok();
        if (m_registry->HasComponentDependentsLocked(*e))
            return result<void>::err(MakeError(ErrorCode::Overloaded, "component cleanup: dependent components remain"));
        if (!m_registry->BeginOpLocked(*e)) {
            if (e->destroy_phase.empty())
                e->destroy_phase = "stop";
            return result<void>::err(MakeError(ErrorCode::Overloaded,
                                               "component cleanup: operation in progress"));
        }
        acquired = true;
        e->destroy_phase = "stop";
        e->gate_closed = true;
        m_registry->RetireTargetBindingsLocked(*e);
        m_registry->RetireSubjectBindingsLocked(id);
        auto ev = m_registry->SetStateLocked(*e, ObjectState::Stopping);
        lk.unlock();
        m_registry->DispatchEvents(std::move(ev));
    }
    /**
     * 顺序（简单 Close）：borrow 排空 → OnStop → OnUninstall → 移除登记。
     * 旧 borrow 必须在 OnStop 关闭资源前自然终态；OnStop 失败/超时同样
     * 保留 owner、不继续收口。
     */
    {
        auto r = m_registry->WaitBorrowsDrained(id, deadline);
        if (!r) {
            _AbortDestroy(id, "stop", "borrow_drain", r.error(),
                          ObjectState::StopIncomplete);
            return r;
        }
    }
    if (auto r = m_registry->DrainSubjectBorrows(id, deadline); !r) {
        _AbortDestroy(id, "stop", "borrow_drain", r.error(), ObjectState::StopIncomplete);
        return r;
    }
    /**
     * OnStop + 前序 hook 终态检查：未达真实终态不跑 OnUninstall、
     * 不销毁（owner 保留）。
     */
    if (auto r = _EnsureHookSettled(
            id, &CoComponent::OnStop, "stop", deadline,
            [&](const char* ph, const Error& e) {
                _AbortDestroy(id, "stop", ph, e,
                              ObjectState::StopIncomplete);
            });
        !r)
        return r;
    _RunVoidHook(id, &CoComponent::OnUninstall, "uninstall");
    _FinishObject(id);
    return result<void>::ok();
}

/**
 * ---- hook 驱动（按 id 查条目，锁内取对象 owner，锁外调用）----
 *
 * 生命周期 hook 在既有调度器内执行，共享调用方的绝对 deadline。
 * 任务持 Registry 与对象 owner（callback lease：object 一定先于任务结束
 * 才可能销毁）；超时结束等待，不悬空 App/对象引用。
 */

template <class T>
result<void> CoApp::_RunHookCo(
    const ObjectId& id, result<void> (T::*hook)(),
    const char* phase, bbt::coroutine::Deadline deadline, bool remember_stop) {
    T* object = nullptr;
    std::shared_ptr<void> owner;
    auto slot = std::make_shared<Completion>();
    /**
     * Co* 入口前序在途 hook：协程挂起续等至真实终态（同一绝对
     * deadline），不重入前序 hook。
     */
    if (auto r = WaitHooksSettledCo(m_registry, id, deadline); !r)
        return r;
    {
        std::unique_lock<std::mutex> lk(m_registry->Mutex());

        auto* e = m_registry->FindEntryLocked(id);
        /**
         * 已消失：跳过
         */
        if (!e || (remember_stop && e->stop_hook_done))
            return result<void>::ok();
        object = static_cast<T*>(e->object.get());
        owner = e->object;
        ++e->hooks_in_flight;
    }
    auto record_fail = [&](const result<void>& r) {
        LifecycleFailure f;
        f.object_id = id;
        f.operation = phase;
        f.phase = phase;
        f.code = r.error().code;
        f.at = std::chrono::system_clock::now();
        f.message = r.error().message;
        std::unique_lock<std::mutex> lk(m_registry->Mutex());
        m_registry->RecordFailure(std::move(f));
    };

    /**
     * hook 投递给调度器的真实协程任务：任务体持有对象 owner（回调 lease），
     * 调用协程在 CoWaiter 上按同一 deadline 挂起等待；超时只结束等待，
     * hook 继续受管执行至真实终态。
     */
    auto reg = m_registry;
    auto hook_co = m_hook_co;
    auto task = [object, hook, slot, reg, id, hook_co, owner, remember_stop]() {
        hook_co->store(bbt::coroutine::GetLocalCoroutineId(),
                       std::memory_order_release);
        auto r = RunHookCaught([&] { return (object->*hook)(); });
        hook_co->store(0, std::memory_order_release);
        reg->NoteHookSettled(id, remember_stop && static_cast<bool>(r));
        slot->Publish(std::move(r));
    };
    try {
        bool submitted = false;
        g_scheduler->RegistCoroutineTask(task, submitted);
        if (!submitted) {
            m_registry->NoteHookSettled(id);
            return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
                                               "lifecycle: hook task rejected"));
        }
    } catch (const std::exception& e) {
        m_registry->NoteHookSettled(id);
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
                                           std::string("lifecycle: hook task regist failed: ") +
                                               e.what()));
    } catch (...) {
        m_registry->NoteHookSettled(id);
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
                                           "lifecycle: hook task regist failed (non-std)"));
    }

    /**
     * 同一绝对 deadline：挂起到 hook 完成或到期（先发布后等与
     * 到期窗口由 AwaitCompletion 统一处理）。
     */
    const auto ws = AwaitCompletion(slot, deadline);
    auto r = FinishCoHookWait(slot, ws);
    if (!r)
        record_fail(r);
    return r;
}

template <class T>
result<void> CoApp::_RunHook(
    const ObjectId& id, result<void> (T::*hook)(),
    const char* phase) {
    T* object = nullptr;
    /**
     * callback lease：锁外 hook 期保活对象
     */
    std::shared_ptr<void> owner;
    {
        std::unique_lock<std::mutex> lk(m_registry->Mutex());
        auto* e = m_registry->FindEntryLocked(id);
        /**
         * 已消失：跳过
         */
        if (!e)
            return result<void>::ok();
        object = static_cast<T*>(e->object.get());
        owner = e->object;
    }
    auto r = RunHookCaught([&] { return (object->*hook)(); });
    if (!r) {
        LifecycleFailure f;
        f.object_id = id;
        f.operation = phase;
        f.phase = phase;
        f.code = r.error().code;
        f.at = std::chrono::system_clock::now();
        f.message = r.error().message;
        std::unique_lock<std::mutex> lk(m_registry->Mutex());
        m_registry->RecordFailure(std::move(f));
    }
    return r;
}

template <class T>
void CoApp::_RunVoidHook(
    const ObjectId& id, void (T::*hook)(),
    const char* phase) noexcept {
    T* object = nullptr;
    std::shared_ptr<void> owner;
    {
        std::unique_lock<std::mutex> lk(m_registry->Mutex());
        auto* e = m_registry->FindEntryLocked(id);
        if (!e)
            return;
        object = static_cast<T*>(e->object.get());
        owner = e->object;
    }
    try {
        (object->*hook)();
    } catch (const std::exception& ex) {
        LifecycleFailure f;
        f.object_id = id;
        f.operation = phase;
        f.phase = phase;
        f.code = ErrorCode::InternalError;
        f.at = std::chrono::system_clock::now();
        f.message = std::string("void hook threw: ") + ex.what();
        std::unique_lock<std::mutex> lk(m_registry->Mutex());
        m_registry->RecordFailure(std::move(f));
    } catch (...) {
        LifecycleFailure f;
        f.object_id = id;
        f.operation = phase;
        f.phase = phase;
        f.code = ErrorCode::InternalError;
        f.at = std::chrono::system_clock::now();
        f.message = "void hook threw (non-std)";
        std::unique_lock<std::mutex> lk(m_registry->Mutex());
        m_registry->RecordFailure(std::move(f));
    }
}

void CoApp::_RunActorDestroy(const ObjectId& id) noexcept {
    CoActor* a = nullptr;
    std::shared_ptr<void> owner;
    {
        std::unique_lock<std::mutex> lk(m_registry->Mutex());
        auto* e = m_registry->FindEntryLocked(id);
        if (!e)
            return;
        a = static_cast<CoActor*>(e->object.get());
        owner = e->object;
    }
    /**
     * Actor type 登记条目无实例：无 OnDestroy 可跑，
     */
    if (!a)
        return;
    /**
                       * 条目收口由 _DestroyActor 完成。
                       */
    try {
        a->OnDestroy();
    } catch (...) {
        LifecycleFailure f;
        f.object_id = id;
        f.operation = "destroy";
        f.phase = "destroy";
        f.code = ErrorCode::InternalError;
        f.at = std::chrono::system_clock::now();
        f.message = "OnDestroy threw";
        std::unique_lock<std::mutex> lk(m_registry->Mutex());
        m_registry->RecordFailure(std::move(f));
    }
}

result<void> CoApp::_RunActorCreate(const ObjectId& id) {
    CoActor* a = nullptr;
    std::shared_ptr<void> owner;
    {
        std::unique_lock<std::mutex> lk(m_registry->Mutex());
        auto* e = m_registry->FindEntryLocked(id);
        if (!e)
            return result<void>::err(MakeError(ErrorCode::NotFound,
                                               "OnCreate: actor entry gone"));
        a = static_cast<CoActor*>(e->object.get());
        owner = e->object;
    }
    if (!a)
        return result<void>::err(MakeError(ErrorCode::NotFound,
                                           "OnCreate: actor type has no instance"));
    return RunHookCaught([&] { return a->OnCreate(); });
}

result<void> CoApp::_ValidateOwner(const ComponentOptions& placement,
                                   ObjectId* out_owner_id) {
    out_owner_id->clear();
    const auto scope = placement.Scope();
    if (scope == ComponentScope::App) {
        if (placement.Owner().has_value())
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                               "ComponentOptions: App scope does not allow an explicit owner"));
        return result<void>::ok();
    }
    if (!placement.Owner().has_value())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                           "ComponentOptions: Service/Actor scope requires an explicit owner"));

    const ObjectHandle& owner = placement.Owner().value();
    /**
     * 跨 App 句柄拒绝：owner 必须属于本 Registry（局部 id/generation
     * 可能与他 App 句柄巧合相同，身份校验不可省）。
     */
    if (!m_registry->OwnsHandle(owner))
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                           "ComponentOptions: owner handle belongs to another Registry"));
    std::unique_lock<std::mutex> lk(m_registry->Mutex());
    const auto* e = m_registry->FindEntryLocked(owner.Id());
    if (!e || e->generation != owner.Generation())
        return result<void>::err(MakeError(ErrorCode::NotFound,
                                           "ComponentOptions: owner is not registered or its generation changed"));
    if (scope == ComponentScope::Service && e->kind != ObjectKind::Service)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                           "ComponentOptions: Service scope owner must be a Service"));
    if (scope == ComponentScope::Actor && e->kind != ObjectKind::Actor)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                           "ComponentOptions: Actor scope owner must be an Actor"));
    *out_owner_id = owner.Id();
    return result<void>::ok();
}

/**
 * ---- 组件→组件依赖环 ----
 *
 * 依赖方向：subject（绑定发起方）依赖 target（被绑组件）。沿依赖边做
 * DFS：target 能回到 subject 即成环。在 m_registry->Mutex() 已持锁的
 * 临界区内调用（只读 m_entries/bindings，无锁外访问）。
 */
bool CoApp::_WouldFormCycleLocked(const ObjectId& subject_id,
                                  const ObjectId& target_id) const {
    const auto deps = m_registry->ComponentDependenciesLocked();
    std::unordered_set<ObjectId> visited;
    std::function<bool(const ObjectId&)> reach = [&](const ObjectId& id) {
        if (id == subject_id)
            return true;
        if (!visited.insert(id).second)
            return false;
        for (const auto& dep : deps.at(id))
            if (reach(dep))
                return true;
        return false;
    };
    return reach(target_id);
}

result<void> CoApp::_InstallComponentImpl(
    const std::type_info& ctype, std::string name,
    const ComponentOptions& placement, const LifecycleOptions& life,
    std::function<CoComponent*(detail::ConstructKey)> make,
    std::function<void(CoComponent*)> destroy) {
    if (name.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                           "InstallComponent: empty name"));

    ObjectId owner_id;
    if (auto r = _ValidateOwner(placement, &owner_id); !r)
        return r;

    std::shared_ptr<void> obj;
    try {
        obj = bbt::coroutine::MakeCoObject<CoComponent>(
            "framework.CoComponent", name, [&] {
                return std::shared_ptr<CoComponent>(
                    make(detail::ConstructKey{}), std::move(destroy));
            });
    } catch (const std::exception& e) {
        return result<void>::err(MakeError(ErrorCode::InternalError,
                                           std::string("InstallComponent: ctor threw: ") + e.what()));
    } catch (...) {
        return result<void>::err(MakeError(ErrorCode::InternalError,
                                           "InstallComponent: ctor threw (non-std)"));
    }

    ObjectId id;
    auto end_install = boost::scope::make_scope_exit([&] {
        if (!id.empty()) {
            std::lock_guard<std::mutex> lk(m_registry->Mutex());
            m_registry->EndOpLocked(*m_registry->FindEntryLocked(id));
        }
    });
    {
        std::unique_lock<std::mutex> lk(m_registry->Mutex());
        if (IsStopState(m_state.load()))
            return result<void>::err(MakeError(ErrorCode::Closed,
                                               "InstallComponent: app is stopping or stopped"));
        if (placement.Owner()) {
            const auto* owner = m_registry->FindEntryLocked(owner_id);
            if (!owner || owner->generation != placement.Owner()->Generation() || owner->gate_closed)
                return result<void>::err(MakeError(ErrorCode::Closed,
                                                   "InstallComponent: scope owner is closing"));
        }
        if (m_registry->FindComponentLocked(placement.Scope(), owner_id,
                                            ctype, name))
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                               "InstallComponent: duplicate component '" + name + "'"));
        auto r = m_registry->CreateEntryLocked(
            ObjectKind::Component, ctype.name(), name, placement.Scope(),
            owner_id, ctype, std::move(obj));
        if (!r)
            return result<void>::err(std::move(r.error()));
        auto* e = r.value();
        id = e->id;
        // 收口若遇到已登记而尚未结束的安装，保留条目供后续重试。
        m_registry->BeginOpLocked(*e);
        static_cast<CoComponent*>(e->object.get())
            ->_Bind(m_registry->MakeHandleLocked(*e));
        auto ev = m_registry->SetStateLocked(*e, ObjectState::Installing);
        lk.unlock();
        m_registry->DispatchEvents(std::move(ev));
    }

    auto r = _RunHook(id, &CoComponent::OnInstall, "install");
    if (!r) {
        m_registry->SetStateAndDispatch(
            id, ObjectState::InstallFailed,
            ObjectError{r.error().code, "install",
                        r.error().message});
        return r;
    }
    m_registry->SetStateAndDispatch(id, ObjectState::Installed);

    if (m_state.load() == AppState::Running) {
        m_registry->SetStateAndDispatch(id, ObjectState::Starting);
        r = _RunHookCo(id, &CoComponent::OnStart, "start",
                       m_registry->ResolveDeadline(life));
        if (!r) {
            m_registry->SetStateAndDispatch(
                id, ObjectState::Failed,
                ObjectError{r.error().code, "start",
                            r.error().message});
            return r;
        }
        m_registry->SetStateAndDispatch(id, ObjectState::Running);
    }
    return result<void>::ok();
}

result<void> CoApp::_UninstallComponentImpl(
    const std::type_info& ctype, std::string_view name,
    const ComponentOptions& placement, const LifecycleOptions& life) {
    ObjectId owner_id;
    if (auto r = _ValidateOwner(placement, &owner_id); !r)
        return r;

    ObjectId id;
    {
        std::unique_lock<std::mutex> lk(m_registry->Mutex());
        auto* e = m_registry->FindComponentLocked(placement.Scope(),
                                                  owner_id, ctype, name);
        if (!e)
            return result<void>::err(MakeError(ErrorCode::NotFound,
                                               "UninstallComponent: not found"));
        auto r = m_registry->BeginDestroyLocked(e->id);
        if (!r)
            return result<void>::err(std::move(r.error()));
        id = e->id;
        e->destroy_phase = "uninstall";
        m_registry->RetireSubjectBindingsLocked(id);
        auto ev = m_registry->SetStateLocked(*e, ObjectState::Uninstalling);
        lk.unlock();
        m_registry->DispatchEvents(std::move(ev));
    }

    const auto deadline = m_registry->ResolveDeadline(life);
    /**
     * 顺序（简单 Close）：borrow 排空 → OnStop → OnUninstall → 移除登记。
     */
    {
        auto r = m_registry->WaitBorrowsDrained(id, deadline);
        if (!r) {
            _AbortDestroy(id, "uninstall", "borrow_drain", r.error(),
                          ObjectState::UninstallIncomplete);
            return r;
        }
    }
    if (auto r = m_registry->DrainSubjectBorrows(id, deadline); !r) {
        _AbortDestroy(id, "uninstall", "borrow_drain", r.error(), ObjectState::UninstallIncomplete);
        return r;
    }
    /**
     * OnStop + 前序 hook 终态检查：未达真实终态不跑 OnUninstall、
     * 不销毁（owner 保留）。
     */
    if (auto r = _EnsureHookSettled(
            id, &CoComponent::OnStop, "uninstall", deadline,
            [&](const char* ph, const Error& e) {
                _AbortDestroy(id, "uninstall", ph, e,
                              ObjectState::UninstallIncomplete);
            });
        !r)
        return r;
    _RunVoidHook(id, &CoComponent::OnUninstall, "uninstall");
    _FinishObject(id);
    return result<void>::ok();
}

result<void> CoApp::_InstallService(
    const std::type_info& type, std::string name, ServiceOptions options,
    RpcMethodTable (*build_table)(),
    std::function<CoServiceBase*(detail::ConstructKey)> make,
    std::function<void(CoServiceBase*)> destroy) {
    if (name.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                           "CoInstallService: S must declare a non-empty kServiceName"));

    std::shared_ptr<const RpcMethodTable> table;
    try {
        table = std::make_shared<const RpcMethodTable>(build_table());
    } catch (const std::invalid_argument& e) {
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                           std::string("CoInstallService: ") + e.what()));
    } catch (const std::exception& e) {
        return result<void>::err(MakeError(ErrorCode::InternalError,
                                           std::string("CoInstallService: method table failed: ") + e.what()));
    }
    if (auto r = ValidateActorKeying(*table, options.Execution()); !r)
        return r;

    const auto decls = options.ComponentBindings();
    auto r = _InstallServiceImpl(type, name, options, std::move(make),
                                 std::move(destroy), decls);
    if (!r)
        return r;

    // Hot-install hooks run before egress is wired. Publish the description first.
    {
        using Description = InboundDispatcher::ServiceDescription;
        auto description = std::make_shared<Description>(
            Description{std::move(options), std::move(table), nullptr});
        std::lock_guard<std::mutex> lk(m_net_mtx);
        if (!m_service_descriptions.emplace(name, std::move(description)).second)
            return result<void>::err(MakeError(ErrorCode::InternalError,
                                               "CoInstallService: duplicate service record '" + name + "'"));
    }
    _BindEgressToService(name);
    return result<void>::ok();
}

result<void> CoApp::_InstallServiceImpl(
    const std::type_info& stype, std::string service_name,
    const ServiceOptions& options,
    std::function<CoServiceBase*(detail::ConstructKey)> make,
    std::function<void(CoServiceBase*)> destroy,
    const std::vector<detail::ComponentBindingDecl>& decls) {
    if (m_state.load() == AppState::Stopped)
        return result<void>::err(MakeError(ErrorCode::Closed,
                                           "InstallService: app stopped"));

    std::shared_ptr<void> obj;
    try {
        obj = bbt::coroutine::MakeCoObject<CoServiceBase>(
            "framework.CoService", service_name, [&] {
                return std::shared_ptr<CoServiceBase>(
                    make(detail::ConstructKey{}), std::move(destroy));
            });
    } catch (const std::exception& e) {
        return result<void>::err(MakeError(ErrorCode::InternalError,
                                           std::string("InstallService: ctor threw: ") + e.what()));
    } catch (...) {
        return result<void>::err(MakeError(ErrorCode::InternalError,
                                           "InstallService: ctor threw (non-std)"));
    }

    ObjectId id;
    ObjectHandle svc_handle;
    {
        std::unique_lock<std::mutex> lk(m_registry->Mutex());
        if (m_registry->FindByTypeNameLocked(ObjectKind::Service,
                                             service_name))
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                               "InstallService: duplicate service '" + service_name + "'"));
        auto r = m_registry->CreateEntryLocked(
            ObjectKind::Service, service_name, service_name,
            ComponentScope::App, {}, stype, std::move(obj));
        if (!r)
            return result<void>::err(std::move(r.error()));
        auto* e = r.value();
        id = e->id;
        svc_handle = m_registry->MakeHandleLocked(*e);
        static_cast<CoServiceBase*>(e->object.get())->_Bind(svc_handle);
        auto ev = m_registry->SetStateLocked(*e, ObjectState::Installing);
        lk.unlock();
        m_registry->DispatchEvents(std::move(ev));
    }

    if (auto r = _ApplyServiceBindings(svc_handle, decls, options.Life());
        !r) {
        m_registry->SetStateAndDispatch(
            id, ObjectState::InstallFailed,
            ObjectError{r.error().code, "install",
                        r.error().message});
        return r;
    }

    auto r = _RunHook(id, &CoServiceBase::OnInstall, "install");
    if (!r) {
        m_registry->SetStateAndDispatch(
            id, ObjectState::InstallFailed,
            ObjectError{r.error().code, "install",
                        r.error().message});
        return r;
    }
    m_registry->SetStateAndDispatch(id, ObjectState::Installed);

    if (m_state.load() == AppState::Running) {
        m_registry->SetStateAndDispatch(id, ObjectState::Starting);
        r = _RunHookCo(id, &CoServiceBase::OnStart, "start",
                       m_registry->ResolveDeadline(options.Life()));
        if (!r) {
            m_registry->SetStateAndDispatch(
                id, ObjectState::Failed,
                ObjectError{r.error().code, "start",
                            r.error().message});
            return r;
        }
        m_registry->SetStateAndDispatch(id, ObjectState::Running);
    }
    return result<void>::ok();
}

result<void> CoApp::_ApplyServiceBindings(
    const ObjectHandle& svc_handle,
    const std::vector<detail::ComponentBindingDecl>& decls,
    const LifecycleOptions& life) {
    CoServiceBase* svc = nullptr;
    /**
     * callback lease：声明绑定展开期保活 service
     */
    std::shared_ptr<void> owner;
    {
        std::unique_lock<std::mutex> lk(m_registry->Mutex());
        auto* e = m_registry->FindEntryLocked(svc_handle.Id());
        if (e) {
            svc = static_cast<CoServiceBase*>(e->object.get());
            owner = e->object;
        }
    }
    if (!svc)
        return result<void>::err(MakeError(ErrorCode::InternalError,
                                           "_ApplyServiceBindings: service entry vanished"));

    std::vector<const detail::ComponentBindingDecl*> done;
    for (const auto& d : decls) {
        if (!svc->_AllowsComponent(*d.component_type)) {
            for (const auto* x : done)
                (void)_UnbindComponentImpl(*x->component_type, svc_handle,
                                           x->name, {}, &life);
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                               "BindComponent: component type is not declared in this Service’s allowed set"));
        }
        auto r = _BindComponentImpl(*d.component_type, svc_handle,
                                    d.name, d.name, {}, false);
        if (!r) {
            for (const auto* x : done)
                (void)_UnbindComponentImpl(*x->component_type, svc_handle,
                                           x->name, {}, &life);
            return result<void>::err(std::move(r.error()));
        }
        done.push_back(&d);
    }
    return result<void>::ok();
}

result<void> CoApp::_UninstallServiceImpl(std::string_view type_name,
                                          const LifecycleOptions& life) {
    ObjectId id;
    {
        std::lock_guard<std::mutex> lk(m_registry->Mutex());
        auto* e = m_registry->FindByTypeNameLocked(ObjectKind::Service, type_name);
        if (!e)
            return result<void>::err(MakeError(ErrorCode::NotFound, "UninstallService: not found"));
        id = e->id;
    }
    const auto deadline = m_registry->ResolveDeadline(life);
    if (auto r = _DestroyService(id, deadline, true); !r)
        return r;
    auto release = boost::scope::make_scope_fail([&] {
        ReleaseInterruptedClose(*m_registry, id, ObjectState::UninstallIncomplete);
    });

    detail::Registry::ComponentDependencies deps;
    std::vector<ObjectId> owned;
    {
        std::lock_guard<std::mutex> lk(m_registry->Mutex());
        deps = m_registry->ComponentDependenciesLocked();
        const auto order = m_registry->ComponentStartOrderLocked(deps);
        for (auto it = order.rbegin(); it != order.rend(); ++it) {
            const auto* e = m_registry->FindEntryLocked(*it);
            if (e->scope == ComponentScope::Service && e->owner_id == id)
                owned.push_back(*it);
        }
    }
    ComponentClosePass pass(deps);
    if (auto failure = pass.Run(owned, [&](const ObjectId& component) {
            return _DestroyComponent(component, deadline);
        })) {
        _AbortDestroy(id, "uninstall", "owned_components", *failure, ObjectState::UninstallIncomplete);
        release.set_active(false);
        return result<void>::err(std::move(*failure));
    }
    auto finish_result = _FinishService(id, "uninstall", ObjectState::UninstallIncomplete);
    release.set_active(false);
    return finish_result;
}

result<void> CoApp::_RegisterActorType(const std::type_info& atype, std::string actor_type) {
    if (m_state.load() == AppState::Stopped)
        return result<void>::err(MakeError(ErrorCode::Closed,
                                           "InstallActor: app stopped"));

    std::unique_lock<std::mutex> lk(m_registry->Mutex());
    if (m_registry->FindByTypeNameLocked(ObjectKind::Actor, actor_type))
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                           "InstallActor: duplicate actor type '" + actor_type + "'"));

    auto r = m_registry->CreateEntryLocked(
        ObjectKind::Actor, actor_type, "", ComponentScope::App, {},
        atype, std::shared_ptr<void>{});
    if (!r)
        return result<void>::err(std::move(r.error()));
    auto* e = r.value();
    auto ev = m_registry->SetStateLocked(*e, ObjectState::Installed);
    lk.unlock();
    m_registry->DispatchEvents(std::move(ev));
    return result<void>::ok();
}

result<void> CoApp::_UninstallActorTypeImpl(std::string_view actor_type,
                                            const LifecycleOptions& life) {
    (void)life;
    ObjectId id;
    {
        std::unique_lock<std::mutex> lk(m_registry->Mutex());
        auto* e = m_registry->FindByTypeNameLocked(ObjectKind::Actor,
                                                   actor_type);
        if (!e)
            return result<void>::err(MakeError(ErrorCode::NotFound,
                                               "UninstallActor: type not registered"));
        for (const auto& xid :
             m_registry->ListIdsByKindLocked(ObjectKind::Actor)) {
            if (xid == e->id)
                continue;
            auto* x = m_registry->FindEntryLocked(xid);
            if (x && x->type_name == actor_type && !x->name.empty())
                return result<void>::err(MakeError(ErrorCode::Overloaded,
                                                   "UninstallActor: live actor instances remain"));
        }
        auto r = m_registry->BeginDestroyLocked(e->id);
        if (!r)
            return result<void>::err(std::move(r.error()));
        id = e->id;
        auto ev = m_registry->SetStateLocked(*e, ObjectState::Uninstalling);
        lk.unlock();
        m_registry->DispatchEvents(std::move(ev));
    }

    /**
     * 类型登记条目不撤销其签发的 observer token：actor 类型收口不触及
     * 观察簿记（实例注销由各 _DestroyActor 各自负责），只抹除登记并释放
     * owner。
     */
    std::shared_ptr<void> obj;
    detail::ObserverEvents events;
    {
        std::unique_lock<std::mutex> lk(m_registry->Mutex());
        auto* e = m_registry->FindEntryLocked(id);
        if (e) {
            e->destroy_phase.clear();
            m_registry->EndOpLocked(*e);
            auto [o, ev] = m_registry->FinishDestroyLocked(*e);
            obj = std::move(o);
            events = std::move(ev);
        }
    }
    m_registry->DispatchEvents(std::move(events));
    obj.reset();
    return result<void>::ok();
}

namespace {
BindingEvent MakeBindingEvent(const detail::RegistryEntry& subject,
                              const detail::RegistryEntry& target,
                              std::string_view slot, std::uint64_t binding_generation,
                              bool bound) {
    BindingEvent event;
    event.subject_id = subject.id;
    event.subject_generation = subject.generation;
    event.target_id = target.id;
    event.generation = target.generation;
    event.binding_generation = binding_generation;
    event.slot = slot;
    event.bound = bound;
    event.revision = target.revision;
    event.observed_at = std::chrono::system_clock::now();
    return event;
}
} // namespace

result<detail::ComponentBindingSnapshot> CoApp::_BindComponentImpl(
    const std::type_info& ctype, const ObjectHandle& subject,
    std::string_view slot, std::string_view target_name,
    const ComponentOptions& placement, bool replace) {
    using BindResult = result<detail::ComponentBindingSnapshot>;
    if (!m_registry->OwnsHandle(subject))
        return BindResult::err(MakeError(ErrorCode::InvalidArgument,
                                         "BindComponent: subject handle belongs to another Registry"));
    ObjectId owner_id;
    if (auto r = _ValidateOwner(placement, &owner_id); !r)
        return BindResult::err(std::move(r.error()));

    detail::ComponentBindingSnapshot snapshot;
    BindingEvent bound;
    std::optional<BindingEvent> removed;
    const auto co = bbt::coroutine::GetLocalCoroutineId();
    std::shared_ptr<void> subject_owner;
    ObjectKind subject_kind;
    auto release = boost::scope::make_scope_exit([&] {
        if (subject_owner)
            m_registry->ReleaseBindingCallback(bound.subject_id, co);
    });
    {
        std::lock_guard<std::mutex> lk(m_registry->Mutex());
        if (IsStopState(m_state.load()))
            return BindResult::err(MakeError(ErrorCode::Closed, "BindComponent: app is stopping or stopped"));
        auto* subj = m_registry->FindEntryLocked(subject.Id());
        if (!subj || subj->generation != subject.Generation())
            return BindResult::err(MakeError(ErrorCode::NotFound, "BindComponent: subject stale"));
        if (subj->kind == ObjectKind::App || !subj->object)
            return BindResult::err(MakeError(ErrorCode::InvalidArgument,
                                             "BindComponent: subject must be an object instance"));
        if (subj->kind == ObjectKind::Service &&
            !static_cast<CoServiceBase*>(subj->object.get())->_AllowsComponent(ctype))
            return BindResult::err(MakeError(ErrorCode::InvalidArgument,
                                             "BindComponent: component type is not declared in this Service’s allowed set"));

        auto* target = m_registry->FindComponentLocked(placement.Scope(), owner_id,
                                                       ctype, target_name);
        if (!target)
            return BindResult::err(MakeError(ErrorCode::NotFound, "BindComponent: component not found"));
        const auto owner_closed = [&](const detail::RegistryEntry& entry) {
            if (entry.scope == ComponentScope::App)
                return false;
            const auto* owner = m_registry->FindEntryLocked(entry.owner_id);
            return !owner || owner->gate_closed;
        };
        if (owner_closed(*subj) || owner_closed(*target))
            return BindResult::err(MakeError(ErrorCode::Closed, "BindComponent: scope owner is closing"));
        if (target->scope != ComponentScope::App && target->owner_id != subj->id &&
            (subj->kind != ObjectKind::Component || subj->scope != target->scope ||
             subj->owner_id != target->owner_id))
            return BindResult::err(MakeError(ErrorCode::InvalidArgument,
                                             "BindComponent: component is outside the subject's scope"));
        auto [previous, previous_binding] = m_registry->FindBindingLocked(subj->id, ctype, slot);
        if (!replace && previous)
            return BindResult::err(MakeError(ErrorCode::InvalidArgument,
                                             "BindComponent: slot already bound"));
        if (replace && !previous)
            return BindResult::err(MakeError(ErrorCode::NotFound, "RebindComponent: slot not bound"));
        if (previous == target)
            return BindResult::err(MakeError(ErrorCode::InvalidArgument,
                                             "RebindComponent: slot already points to target"));
        if (subj->kind == ObjectKind::Component && _WouldFormCycleLocked(subj->id, target->id))
            return BindResult::err(MakeError(ErrorCode::InvalidArgument,
                                             "BindComponent: dependency would form a cycle"));

        snapshot = m_registry->SnapshotBindingLocked(*target, 0);
        bound = MakeBindingEvent(*subj, *target, slot, 0, true);
        if (previous)
            removed = MakeBindingEvent(*subj, *previous, slot, previous_binding->generation, false);
        if (subj->gate_closed)
            return BindResult::err(MakeError(ErrorCode::Closed, "BindComponent: subject is closing"));
        m_registry->AcquireBindingCallbackLocked(*subj, co);
        subject_owner = subj->object;
        subject_kind = subj->kind;
        auto generation = m_registry->BindLocked(*target, *subj, slot, previous, previous_binding);
        if (!generation)
            return BindResult::err(std::move(generation.error()));
        snapshot.binding_generation = generation.value();
        bound.binding_generation = generation.value();
    }
    if (removed)
        _NotifyBindingChanged(subject_kind, subject_owner.get(), *removed, "rebind");
    _NotifyBindingChanged(subject_kind, subject_owner.get(), bound, replace ? "rebind" : "bind");
    return BindResult::ok(std::move(snapshot));
}

result<void> CoApp::_UnbindComponentImpl(
    const std::type_info& ctype, const ObjectHandle& subject,
    std::string_view slot, const ComponentOptions& placement,
    const LifecycleOptions* drain) {
    if (!m_registry->OwnsHandle(subject))
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                           "UnbindComponent: subject handle belongs to another Registry"));
    ObjectId owner_id;
    if (auto r = _ValidateOwner(placement, &owner_id); !r)
        return r;

    BindingEvent removed;
    const auto co = bbt::coroutine::GetLocalCoroutineId();
    std::shared_ptr<void> subject_owner;
    ObjectKind subject_kind;
    auto release = boost::scope::make_scope_exit([&] {
        if (subject_owner)
            m_registry->ReleaseBindingCallback(removed.subject_id, co);
    });
    const auto qualify_locked = [&](detail::RegistryEntry& subj) {
        if (!subj.gate_closed) {
            m_registry->AcquireBindingCallbackLocked(subj, co);
            subject_owner = subj.object;
            subject_kind = subj.kind;
        }
    };
    {
        std::lock_guard<std::mutex> lk(m_registry->Mutex());
        auto* subj = m_registry->FindEntryLocked(subject.Id());
        if (!subj || subj->generation != subject.Generation())
            return result<void>::err(MakeError(ErrorCode::NotFound, "UnbindComponent: subject stale"));
        if (subj->kind == ObjectKind::App || !subj->object)
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                               "UnbindComponent: subject must be an object instance"));

        // 先续等原代，避免超时后重试摘掉同槽位的新绑定。
        auto [target, binding] = drain
                                     ? m_registry->FindBindingLocked(subj->id, ctype, slot, &placement, true)
                                     : std::pair<detail::RegistryEntry*, detail::Binding*>{};
        if (!target) {
            auto active = m_registry->FindBindingLocked(subj->id, ctype, slot, &placement);
            target = active.first;
            binding = active.second;
        }
        if (!target)
            return result<void>::err(MakeError(ErrorCode::NotFound, "UnbindComponent: binding not found"));
        removed = MakeBindingEvent(*subj, *target, slot, binding->generation, false);
        if (!drain)
            qualify_locked(*subj);
        if (!binding->retiring)
            m_registry->RetireBindingLocked(*target, *binding, drain != nullptr);
    }

    if (drain) {
        auto r = m_registry->WaitBindingDrained(removed.target_id, removed.subject_id,
                                                removed.binding_generation,
                                                m_registry->ResolveDeadline(*drain));
        std::lock_guard<std::mutex> lk(m_registry->Mutex());
        if (!r) {
            m_registry->RecordFailure(LifecycleFailure{
                removed.target_id, removed.generation, "unbind", "borrow_drain", r.error().code,
                std::chrono::system_clock::now(), r.error().message});
            return r;
        }
        auto* target = m_registry->FindEntryLocked(removed.target_id);
        if (target && m_registry->EraseBindingLocked(*target, removed.subject_id,
                                                     removed.binding_generation)) {
            // 排空期间收口可以接管旧代；封门后不得再取得新通知资格。
            if (auto* subj = m_registry->FindEntryLocked(removed.subject_id))
                qualify_locked(*subj);
        }
    }
    if (subject_owner)
        _NotifyBindingChanged(subject_kind, subject_owner.get(), removed, "unbind");
    return result<void>::ok();
}

bbt_co_async(result<void>) CoApp::CoBindObject(const ObjectHandle& subject,
                                               const ObjectHandle& target) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<void>::err(std::move(ctx.error()));
    (void)subject;
    (void)target;
    return result<void>::err(MakeError(ErrorCode::UnsupportedRoute,
                                       "BindObject: non-owning relationship edges are not supported"));
}

bbt_co_async(result<void>) CoApp::CoUnbindObject(const ObjectHandle& subject,
                                                 const ObjectHandle& target) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<void>::err(std::move(ctx.error()));
    (void)subject;
    (void)target;
    return result<void>::err(MakeError(ErrorCode::UnsupportedRoute,
                                       "UnbindObject: non-owning relationship edges are not supported"));
}

result<void> CoApp::_RunNetworkTask(std::function<result<void>()> task,
                                    bbt::coroutine::Deadline deadline,
                                    const char* timeout_message) {
    auto slot = std::make_shared<Completion>();
    std::thread runner([slot, task = std::move(task)] {
        slot->Publish(task());
    });
    const auto status = AwaitCompletion(slot, deadline);
    runner.join();
    if (status != bbt::coroutine::WaitStatus::Completed)
        return result<void>::err(MakeError(ErrorCode::TimedOut,
                                           std::string(timeout_message) + " (status " +
                                               std::to_string(static_cast<int>(status)) + ")"));
    return slot->Take();
}

std::shared_ptr<detail::Registry> RegistryForTest(const CoApp& app) {
    return app.m_registry;
}

} // namespace bbt::framework
