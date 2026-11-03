#pragma once
// Co-only 应用装配根：管理对象、绑定和生命周期，不启动或停止 runtime。
// 所有 Co 入口在副作用前检查当前协程；只读快照和 RequestShutdown 可跨域调用。
//
// CoApp 唯一创建路径是静态工厂 Create（内部经 coroutine MakeCoObject 真实
// creator/统计接管）；构造为 private，业务不能栈构造或直接 new。
// 关闭语义（简单 Close）：CoStop/CoUninstall* 直接收口——拒绝新操作，
// 在有限 deadline 内收口后在途责任（borrow/hook）至真实终态并销毁对象；
// deadline 耗尽保留 owner、返回诚实的不完整状态（StopIncomplete/
// UninstallIncomplete），允许再次以新的期限调用推进；框架不新增
// drain/receipt/多阶段协议，等待顺序与业务任务编排由用户控制。

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <type_traits>
#include <typeinfo>
#include <utility>
#include <vector>

#include <bbt/coroutine/syntax/Async.hpp>
#include <bbt/coroutine/object/CoObject.hpp>
#include <bbt/coroutine/object/CoRuntimeOnly.hpp>

#include <bbt/framework/Address.hpp>
#include <bbt/framework/Object.hpp>
#include <bbt/framework/Result.hpp>
#include <bbt/framework/Service.hpp>
#include <bbt/framework/internal/Registry.hpp>

namespace bbt::framework::v2 {

// Registry 拥有业务对象；App self entry 与观察索引均不反向保活 App。
class CoApp : public bbt::coroutine::CoObject,
              public bbt::coroutine::CoRuntimeOnly {
public:
    /**
     * @brief 唯一创建入口：经 coroutine MakeCoObject 接管真实 creator 与
     *        created/destroyed 统计，返回强引用 owner。
     * @pre 必须在受管协程上下文调用（CoRuntimeOnly 强约束，构造期即检查）。
     * @param options 装配选项；AppOptions::Validate 在 Start 时强制校验。
     * @param name 观测名（CoObjectInfo.name；默认 "app"）。
     * @return shared_ptr<CoApp>（业务 owner 强引用边界）。
     */
    static std::shared_ptr<CoApp> Create(AppOptions options,
                                         std::string name = "app") {
        return bbt::coroutine::MakeCoObject<CoApp>(
            "framework.CoApp", std::move(name),
            [opts = std::move(options)]() mutable {
                return std::shared_ptr<CoApp>(new CoApp(std::move(opts)));
            });
    }

    ~CoApp(); // 只释放自身资源，不隐式执行生命周期 hook。

    CoApp(const CoApp&) = delete;
    CoApp& operator=(const CoApp&) = delete;

    // ---- Install/Uninstall ----
    /**
     * @brief 安装受管组件（App/Service/Actor scope）。
     * @tparam C 组件类型，必须继承 CoComponent、构造只接受 ConstructKey、
     *         析构 protected/private（static_assert 强制）。
     * @param name 组件逻辑名（同一 scope+owner+type 下唯一；空名 InvalidArgument）。
     * @param options C::Options 构造参数（按值传递，装配期移交给受控构造）。
     * @param placement 归属范围与 owner（Service/Actor scope 必须 SetOwner，
     *        App scope 禁止显式 owner）。
     * @param life 生命周期选项。
     * @return 成功 ok；App 已 Stopped 返回 Closed，重名/placement 非法
     *         InvalidArgument，构造异常 InternalError，OnInstall 失败原错误
     *         透出且组件进入 InstallFailed（可 Find/卸载，不自动回滚）。
     *         App 已 Running 时成功安装后继续 OnStart，失败组件进入 Failed。
     */
    template <class C>
    bbt_co_async(result<void>) CoInstallComponent(
        std::string name, typename C::Options options,
        const ComponentOptions& placement = {},
        const LifecycleOptions& life = {});
    /**
     * @brief 卸载受管组件。
     * @tparam C 组件类型。
     * @param name 组件逻辑名。
     * @param placement 归属范围与 owner。
     * @param life 生命周期选项（收口期限）。
     * @return 成功 ok；未找到 NotFound；仍有未解绑边或控制面操作进行中
     *         Overloaded；收口（borrow 排空/OnStop）超时组件记
     *         UninstallIncomplete 并记 LifecycleFailure，保留 owner 待
     *         再次调用推进；OnStop 失败同样保留 owner 记 UninstallIncomplete。
     */
    template <class C>
    bbt_co_async(result<void>) CoUninstallComponent(
        std::string_view name, const ComponentOptions& placement = {},
        const LifecycleOptions& life = {});
    /**
     * @brief 安装服务。
     * @tparam S 服务类型，必须继承 CoService<...>（CoServiceBase）、构造只接受
     *         ConstructKey、析构 protected/private；须声明非空 kServiceName。
     * @param options ServiceOptions（Validate 要求 max_inflight 有限正值）；
     *        其中声明的 BindComponent 条目在安装时逐个展开绑定。
     * @return 成功 ok；App 已 Stopped 返回 Closed，重名/缺 kServiceName
     *         InvalidArgument；声明绑定失败原错误透出且已完成绑定被解绑；
     *         OnInstall 失败服务进入 InstallFailed（已成功的绑定不自动解绑）。
     *         App 已 Running 时成功后继续 OnStart，失败服务进入 Failed。
     */
    template <class S>
    bbt_co_async(result<void>) CoInstallService(ServiceOptions options = {});
    /**
     * @brief 卸载服务。
     * @tparam S 服务类型（以 kServiceName 定位）。
     * @param life 生命周期选项（收口期限）。
     * @return 成功 ok；未注册 NotFound。收口顺序：封该服务为 subject 的绑定
     *         并等 borrow 排空 → OnStop → OnUninstall → 移除登记；
     *         任一收口步超时/失败保留 owner 记 UninstallIncomplete。
     */
    template <class S>
    bbt_co_async(result<void>) CoUninstallService(const LifecycleOptions& life = {});
    /**
     * @brief 登记 Actor 类型（不创建实例）。
     * @tparam A Actor 类型，必须继承 CoActor 并声明非空 kActorType。
     * @param options ActorOptions（Validate 要求三项额度有限正值）。
     * @return 成功 ok；App 已 Stopped 返回 Closed，类型重复/缺 kActorType
     *         InvalidArgument。实例创建经后续 CoCreateActor 切片。
     */
    template <class A>
    bbt_co_async(result<void>) CoInstallActor(ActorOptions options = {});
    /**
     * @brief 卸载 Actor 类型。
     * @tparam A Actor 类型。
     * @return 成功 ok；类型未注册 NotFound；仍有存活实例 Overloaded。
     */
    template <class A>
    bbt_co_async(result<void>) CoUninstallActor(const LifecycleOptions& life = {});

    // ---- Find ----
    /**
     * @brief 按服务名查非拥有句柄。
     * @param service 服务名（kServiceName）。
     * @return 成功 ObjectHandle；未注册 NotFound。
     */
    result<ObjectHandle> FindService(std::string_view service) const;
    /**
     * @brief 按 ActorId 查非拥有句柄。
     * @param id (type, key)。
     * @return 成功 ObjectHandle；未注册 NotFound。
     */
    result<ObjectHandle> FindActor(const ActorId& id) const;
    /**
     * @brief 按类型+名字+归属查组件句柄。
     * @tparam C 组件类型。
     * @param name 组件逻辑名。
     * @param placement 归属范围与 owner。
     * @return 成功 ObjectHandle；placement 非法 InvalidArgument，未找到 NotFound。
     */
    template <class C>
    result<ObjectHandle> FindComponent(
        std::string_view name, const ComponentOptions& placement = {}) const;

    // ---- Bind/Unbind ----
    /**
     * @brief 把组件绑定给 subject 并发放类型化句柄。
     * @tparam C 组件类型；subject 为 Service 时 C 必须在其声明集合内。
     * @param subject 绑定发起方（Service/Actor/其他组件的 ObjectHandle；
     *        必须属于本 App Registry，App handle 非法）。
     * @param name 目标组件逻辑名。
     * @param placement 目标组件归属。
     * @param life 生命周期选项。
     * @return 成功 ComponentHandle<C>（携带 binding generation）；subject
     *         失效 NotFound，subject 属其他 Registry InvalidArgument，
     *         类型未在 Service 声明集合内 InvalidArgument，目标未找到
     *         NotFound，重复绑定 InvalidArgument，额度满 Overloaded，
     *         目标封门 Closed。绑定成功后 subject 的 OnBindingChanged 在
     *         Registry 锁外同步调用（subject owner 以 callback lease
     *         保活整个锁外 hook），hook 抛异常只记 LifecycleFailure，
     *         绑定不回滚。
     */
    template <class C>
    bbt_co_async(result<ComponentHandle<C>>) CoBindComponent(
        const ObjectHandle& subject, std::string_view name,
        const ComponentOptions& placement = {},
        const LifecycleOptions& life = {});
    /**
     * @brief 解绑组件：封该绑定新 borrow 并等已有 borrow 排空。
     * @return 成功 ok；目标/subject 未找到 NotFound，subject 属其他
     *         Registry InvalidArgument；排空超时原错误透出并记
     *         LifecycleFailure（retiring 边保留待再次调用收口）。
     *         排空完成后锁外派发 bound=false 事件。
     */
    template <class C>
    bbt_co_async(result<void>) CoUnbindComponent(
        const ObjectHandle& subject, std::string_view name,
        const ComponentOptions& placement = {},
        const LifecycleOptions& life = {});
    /**
     * @brief 非拥有关系边（本切片未实现）。
     * @return 恒 UnsupportedRoute。
     */
    bbt_co_async(result<void>) CoBindObject(const ObjectHandle& subject,
                            const ObjectHandle& target);
    /**
     * @brief 解绑非拥有关系边（本切片未实现）。
     * @return 恒 UnsupportedRoute。
     */
    bbt_co_async(result<void>) CoUnbindObject(const ObjectHandle& subject,
                              const ObjectHandle& target);

    /**
     * @brief 订阅 target 的状态事件。
     * @param observer 观察者对象句柄（卸载时其 token 自动撤销）。
     * @param target 被观察对象句柄。两个句柄都必须属于本 App Registry。
     * @param callback 事件回调（锁外执行，载荷为 owning ObjectStateEvent；
     *        Revoke 后不再新派发，已派发的在途回调由 callback lease 排空）。
     * @return 成功 ObserverToken（析构即 Revoke）；额度满/对象失效返回错误。
     */
    bbt_co_async(result<ObserverToken>) CoObserve(const ObjectHandle& observer,
                                  const ObjectHandle& target,
                                  ObjectObserver callback);

    /// @brief 本 App 在 Registry 中的非拥有句柄。
    ObjectHandle Handle() const;

    bbt_co_async(result<void>) CoWaitShutdownRequest(
        const bbt::coroutine::WaitOptions& wait = {});

    // ---- 生命周期 ----
    /**
     * @brief 启动：组件按依赖拓扑 OnStart，再 Service 按安装顺序 OnStart。
     * @param life 生命周期选项；本步所有 hook 共享同一有限 deadline。
     * @return 全部成功 ok；已 Stopped/StopIncomplete 返回 Closed，非
     *         Created/Paused 返回 Overloaded；协程上下文调用返回
     *         InvalidContext。首个 hook 失败原错误透出、该对象记 Failed，
     *         App 仍置 Running（部分启动诚实反映）。
     */
    /**
     * @brief 暂停：对 Running 服务逐个 OnPause（受管协程）。
     * @return 全部成功 ok；非 Running 返回 InvalidArgument；协程上下文
     *         InvalidContext；首个失败即停止并透出错误，其后服务不再暂停，
     *         该服务记 Failed。
     */
    /**
     * @brief 恢复：对 Paused 服务逐个 OnResume（受管协程）。
     * @return 全部成功 ok；非 Paused 返回 InvalidArgument；协程上下文
     *         InvalidContext；首个失败即停止并透出错误，其后服务不再恢复，
     *         该服务记 Failed。
     */
    /**
     * @brief 停止（简单 Close）：拒新后逆序收口 Service → Actor → scoped
     *        Component → App Component；每个对象先排空 borrow 再 OnStop。
     *        各分支独立推进，失败记 LifecycleFailure 并继续。
     * @return ok（收口未竟时 App 置 StopIncomplete，经 PendingCleanup 观察，
     *        允许再次 CoStop 以更晚期限推进）。协程上下文 InvalidContext。
     *        Stop 后不能 Start；框架内不新增 drain/receipt/多阶段协议，
     *        等待顺序与业务任务编排由用户控制。
     */
    /**
     * @brief 受管协程启动入口：与 Start 相同语义，在当前受管协程内执行。
     * @pre 必须在受管协程上下文调用；宿主/非协程上下文返回 InvalidContext。
     *      本 App hook 执行中的协程再调同对象生命周期入口同样
     *      InvalidContext（自等待）。
     */
    bbt_co_async(result<void>) CoStart(const LifecycleOptions& life = {});
    /// @brief 受管协程暂停入口（语义同 Pause；上下文约束同 CoStart）。
    bbt_co_async(result<void>) CoPause(const LifecycleOptions& life = {});
    /// @brief 受管协程恢复入口（语义同 Resume；上下文约束同 CoStart）。
    bbt_co_async(result<void>) CoResume(const LifecycleOptions& life = {});
    /// @brief 受管协程停止入口（语义同 Stop；上下文约束同 CoStart）。
    bbt_co_async(result<void>) CoStop(const LifecycleOptions& life = {});
    /// @brief 外部线程请求关闭（幂等、noexcept；唤醒 shutdown 等待，不执行 Stop）。
    void RequestShutdown() noexcept;
    /// @brief App 当前状态。
    AppState State() const noexcept { return m_state.load(); }

    /// @brief 收口未完成项快照（bounded ring）。
    std::vector<CleanupInfo> PendingCleanup() const;
    /// @brief 生命周期失败记录快照（bounded ring）。
    std::vector<LifecycleFailure> LifecycleFailures() const;

    /// @brief 测试观察口（非稳定 API）：供本仓单测断言内部簿记。
    std::shared_ptr<detail::Registry> RegistryForTest() const {
        return m_registry;
    }

private:
    /// @brief 受控构造：仅 Create 工厂可调用（private + friend 自身静态成员）。
    explicit CoApp(AppOptions options);
    /// @brief shared_ptr 本体的删除同样只经 Create 返回的 owner（不开放 new/delete）。
    friend struct std::default_delete<CoApp>;

    // 生命周期 hook 在既有协程调度器执行，调用方用 CoWaiter 挂起等待。
    result<void> _RunServiceHookCo(
        const ObjectId& id, result<void> (CoServiceBase::*hook)(),
        const char* phase, bbt::coroutine::Deadline deadline);
    result<void> _RunComponentHookCo(
        const ObjectId& id, result<void> (CoComponent::*hook)(),
        const char* phase, bbt::coroutine::Deadline deadline);
    // 安装/卸载 hook 在已通过门禁的调用协程内、Registry 锁外执行。
    result<void> _RunServiceHook(
        const ObjectId& id, result<void> (CoServiceBase::*hook)(),
        const char* phase);
    result<void> _RunComponentHook(
        const ObjectId& id, result<void> (CoComponent::*hook)(),
        const char* phase);
    void _RunServiceVoidHook(
        const ObjectId& id, void (CoServiceBase::*hook)(),
        const char* phase) noexcept;
    void _RunComponentVoidHook(
        const ObjectId& id, void (CoComponent::*hook)(),
        const char* phase) noexcept;
    result<void> _RunActorCreate(const ObjectId& id);
    void         _RunActorDestroy(const ObjectId& id) noexcept;

    // Install/Uninstall 的类型擦除执行体。
    result<void> _InstallComponentImpl(
        const std::type_info& ctype, std::string name,
        const ComponentOptions& placement, const LifecycleOptions& life,
        std::function<CoComponent*(detail::ConstructKey)> make,
        std::function<void(CoComponent*)> destroy);
    result<void> _UninstallComponentImpl(
        const std::type_info& ctype, std::string_view name,
        const ComponentOptions& placement, const LifecycleOptions& life);

    result<void> _InstallServiceImpl(
        const std::type_info& stype, std::string service_name,
        const ServiceOptions& options,
        std::function<CoServiceBase*(detail::ConstructKey)> make,
        std::function<void(CoServiceBase*)> destroy,
        const std::vector<detail::ComponentBindingDecl>& decls);
    result<void> _UninstallServiceImpl(std::string_view type_name,
                                       const LifecycleOptions& life);

    result<void> _InstallActorTypeImpl(
        const std::type_info& atype, std::string actor_type,
        const ActorOptions& options,
        std::function<CoActor*(detail::ConstructKey)> make,
        std::function<void(CoActor*)> destroy);
    result<void> _UninstallActorTypeImpl(std::string_view actor_type,
                                         const LifecycleOptions& life);

    result<std::pair<ObjectHandle, std::uint64_t>> _BindComponentImpl(
        const std::type_info& ctype, const ObjectHandle& subject,
        std::string_view name, const ComponentOptions& placement,
        const LifecycleOptions& life);
    result<void> _UnbindComponentImpl(
        const std::type_info& ctype, const ObjectHandle& subject,
        std::string_view name, const ComponentOptions& placement,
        const LifecycleOptions& life);
    result<ObjectHandle> _FindComponentImpl(
        const std::type_info& ctype, std::string_view name,
        const ComponentOptions& placement) const;

    // Service 声明绑定：按 decls 逐个 CoBindComponent(subject=service)；
    // 任一失败把本服务已完成的绑定解绑（契约：必需依赖缺失 → NotFound/
    // 原错误，不虚报事务回滚——已解绑动作本身完成并记录）。
    result<void> _ApplyServiceBindings(
        const ObjectHandle& service_handle,
        const std::vector<detail::ComponentBindingDecl>& decls,
        const LifecycleOptions& life);

    // 生命周期转换体：仅由已通过门禁的 Co 入口调用。
    result<void> _StartImpl(const LifecycleOptions& life);
    result<void> _PauseImpl(const LifecycleOptions& life);
    result<void> _ResumeImpl(const LifecycleOptions& life);
    result<void> _StopImpl(const LifecycleOptions& life);

    // 上下文校验：Co 入口在非协程上下文 /
    // 生命周期 hook 执行中同协程再调生命周期入口 → InvalidContext。
    result<void> _CheckCoContext() const;


    // Stop 逆序收口：Service → Actor 实例 → scoped Component → App
    // Component（后两者按依赖逆拓扑）。各分支独立推进，失败记
    // LifecycleFailure 并继续。
    void _StopAll(bbt::coroutine::Deadline deadline);
    void _DestroyService(const ObjectId& id,
                         bbt::coroutine::Deadline deadline);
    void _DestroyComponent(const ObjectId& id,
                           bbt::coroutine::Deadline deadline);
    void _DestroyActor(const ObjectId& id);
    // 收口步失败统一出口：记 LifecycleFailure、标 incomplete 态、EndOp。
    void _AbortDestroy(const ObjectId& id, const char* operation,
                       const char* phase, const Error& err,
                       ObjectState incomplete_state) noexcept;

    result<void> _RegisterAppObject();
    // owner 校验：句柄必须属于本 Registry；Service scope 的 owner 必须是
    // Service；Actor scope 的 owner 必须是已创建 actor 实例；App scope
    // 不允许显式 owner。
    result<void> _ValidateOwner(const ComponentOptions& placement,
                                ObjectId* out_owner_id);
    // 组件→组件依赖环检查（锁内）：subject 是 CoComponent 时，加入
    // (subject → target) 边不得成环。返回 true 表示成环。
    bool _WouldFormCycleLocked(const ObjectId& subject_id,
                               const ObjectId& target_id) const;

    const AppOptions                  m_options;
    std::shared_ptr<detail::Registry> m_registry;
    std::atomic<AppState>             m_state{AppState::Created};
    std::atomic_bool                  m_shutdown_req{false};
    std::atomic_bool                  m_lifecycle_active{false};
    ObjectHandle                      m_self;
    bool                              m_app_entry_registered = false;

    // 请求只置持久 flag 并通知单一 Co waiter，不执行 Stop。
    std::mutex                        m_run_mtx;
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> m_shutdown_waiter;

    // 生命周期 hook 当前执行协程 id（0=无）：hook 内同协程再调生命周期
    // 入口据此判自等待 → InvalidContext。非协程上下文恒 0。
    std::shared_ptr<std::atomic<std::uint64_t>> m_hook_co =
        std::make_shared<std::atomic<std::uint64_t>>(0);
};

// ---- 模板实现区：上段类定义只声明，以下类外定义保持对使用点可见 ----

template <class C>
bbt_co_async(result<void>) CoApp::CoInstallComponent(
    std::string name, typename C::Options options,
    const ComponentOptions& placement, const LifecycleOptions& life) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<void>::err(std::move(ctx.error()));
    static_assert(std::is_base_of_v<CoComponent, C>,
        "CoInstallComponent: C 必须继承 framework::v2::CoComponent");
    static_assert(!std::is_default_constructible_v<C>,
        "CoInstallComponent: C 构造必须只接受 ConstructKey（业务不可直接构造）");
    static_assert(!std::is_destructible_v<C>,
        "CoInstallComponent: C 析构必须 protected/private（Registry 独有销毁权）");
    return _InstallComponentImpl(
        typeid(C), std::move(name), placement, life,
        [opts = std::move(options)](detail::ConstructKey k) mutable
            -> CoComponent* {
            return detail::Registry::NewManaged<C>(k, std::move(opts));
        },
        [](CoComponent* p) { detail::Registry::DeleteManaged<C>(
            static_cast<C*>(p)); });
}

template <class C>
bbt_co_async(result<void>) CoApp::CoUninstallComponent(
    std::string_view name, const ComponentOptions& placement,
    const LifecycleOptions& life) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<void>::err(std::move(ctx.error()));
    static_assert(std::is_base_of_v<CoComponent, C>,
        "CoUninstallComponent: C 必须继承 framework::v2::CoComponent");
    return _UninstallComponentImpl(typeid(C), name, placement, life);
}

template <class S>
bbt_co_async(result<void>) CoApp::CoInstallService(ServiceOptions options) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<void>::err(std::move(ctx.error()));
    static_assert(std::is_base_of_v<CoServiceBase, S>,
        "CoInstallService: S 必须继承 framework::v2::CoService<...>");
    static_assert(!std::is_default_constructible_v<S>,
        "CoInstallService: S 构造必须只接受 ConstructKey（业务不可直接构造）");
    static_assert(!std::is_destructible_v<S>,
        "CoInstallService: S 析构必须 protected/private（Registry 独有销毁权）");
    if (auto r = options.Validate(); !r) return r;

    std::string name = CoServiceStaticName<S>::Get();
    if (name.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "CoInstallService: S 缺 kServiceName 声明或为空"));

    const auto decls = options.ComponentBindings();
    return _InstallServiceImpl(
        typeid(S), std::move(name), options,
        [](detail::ConstructKey k) -> CoServiceBase* {
            return detail::Registry::NewManaged<S>(k);
        },
        [](CoServiceBase* p) { detail::Registry::DeleteManaged<S>(
            static_cast<S*>(p)); }, decls);
}

template <class S>
bbt_co_async(result<void>) CoApp::CoUninstallService(const LifecycleOptions& life) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<void>::err(std::move(ctx.error()));
    static_assert(std::is_base_of_v<CoServiceBase, S>,
        "CoUninstallService: S 必须继承 framework::v2::CoService<...>");
    std::string name = CoServiceStaticName<S>::Get();
    if (name.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "CoUninstallService: S 缺 kServiceName 声明"));
    return _UninstallServiceImpl(name, life);
}

template <class A>
bbt_co_async(result<void>) CoApp::CoInstallActor(ActorOptions options) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<void>::err(std::move(ctx.error()));
    static_assert(std::is_base_of_v<CoActor, A>,
        "CoInstallActor: A 必须继承 framework::v2::CoActor");
    static_assert(!std::is_default_constructible_v<A>,
        "CoInstallActor: A 构造必须只接受 ConstructKey（业务不可直接构造）");
    static_assert(!std::is_destructible_v<A>,
        "CoInstallActor: A 析构必须 protected/private（Registry 独有销毁权）");
    if (auto r = options.Validate(); !r) return r;

    std::string type = CoActorStaticName<A>::Get();
    if (type.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "CoInstallActor: A 缺 kActorType 声明或为空"));

    return _InstallActorTypeImpl(
        typeid(A), std::move(type), options,
        [](detail::ConstructKey k) -> CoActor* {
            return detail::Registry::NewManaged<A>(k);
        },
        [](CoActor* p) { detail::Registry::DeleteManaged<A>(
            static_cast<A*>(p)); });
}

template <class A>
bbt_co_async(result<void>) CoApp::CoUninstallActor(const LifecycleOptions& life) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<void>::err(std::move(ctx.error()));
    static_assert(std::is_base_of_v<CoActor, A>,
        "CoUninstallActor: A 必须继承 framework::v2::CoActor");
    std::string type = CoActorStaticName<A>::Get();
    if (type.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "CoUninstallActor: A 缺 kActorType 声明"));
    return _UninstallActorTypeImpl(type, life);
}

template <class C>
result<ObjectHandle> CoApp::FindComponent(
    std::string_view name, const ComponentOptions& placement) const {
    static_assert(std::is_base_of_v<CoComponent, C>,
        "FindComponent: C 必须继承 framework::v2::CoComponent");
    return _FindComponentImpl(typeid(C), name, placement);
}

template <class C>
bbt_co_async(result<ComponentHandle<C>>) CoApp::CoBindComponent(
    const ObjectHandle& subject, std::string_view name,
    const ComponentOptions& placement, const LifecycleOptions& life) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<ComponentHandle<C>>::err(std::move(ctx.error()));
    static_assert(std::is_base_of_v<CoComponent, C>,
        "CoBindComponent: C 必须继承 framework::v2::CoComponent");
    auto r = _BindComponentImpl(typeid(C), subject, name, placement, life);
    if (!r) return result<ComponentHandle<C>>::err(std::move(r.error()));
    return result<ComponentHandle<C>>::ok(
        detail::MakeComponentHandle<C>(r.value().first, r.value().second));
}

template <class C>
bbt_co_async(result<void>) CoApp::CoUnbindComponent(
    const ObjectHandle& subject, std::string_view name,
    const ComponentOptions& placement, const LifecycleOptions& life) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<void>::err(std::move(ctx.error()));
    static_assert(std::is_base_of_v<CoComponent, C>,
        "CoUnbindComponent: C 必须继承 framework::v2::CoComponent");
    return _UnbindComponentImpl(typeid(C), subject, name, placement, life);
}

} // namespace bbt::framework::v2
