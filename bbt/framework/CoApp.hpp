#pragma once
/**
 * 应用装配根（唯一公开宿主）：受管对象、绑定、生命周期与真实网络面。
 * 所有 Co 入口在副作用前检查当前协程；只读快照和 RequestShutdown 可跨域调用。
 * 网络装配：CoStart 校验网络配置后装配 InfraHttpHost + RPC admission +
 * RpcEgress（V2 双向会话），服务方法表经 Registry 受管对象分发；未配置
 * 监听则不装配网络宿主（纯受管对象面，分发/出站如实 RuntimeUnavailable）。
 * 出站路由门：先查已装配动态路由表，再回退静态路由，两者皆无 → NotFound。
 *
 * CoApp 唯一创建路径是静态工厂 Create（内部经 coroutine MakeCoObject 真实
 * creator/统计接管）；构造为 private，业务不能栈构造或直接 new。
 * 关闭语义（简单 Close）：CoStop/CoUninstall* 直接收口——拒绝新操作，
 * 在有限 deadline 内收口后在途责任（borrow/hook）至真实终态并销毁对象；
 * deadline 耗尽保留 owner、返回诚实的不完整状态（StopIncomplete/
 * UninstallIncomplete），允许再次以新的期限调用推进；框架不新增
 * drain/receipt/多阶段协议，等待顺序与业务任务编排由用户控制。
 */

#include <atomic>
#include <functional>
#include <map>
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
#include <bbt/framework/CoService.hpp>
#include <bbt/framework/internal/MethodTable.hpp>
#include <bbt/framework/internal/Registry.hpp>

namespace bbt::framework {

class ActorRegistry;
class INetworkHost;
class InfraHttpHost;
class OrderedIngress;
struct AppRuntimeState;

/**
 * 内部入口友元（internal/ 下声明）：测试缝构造与测试侧入站分发观察口。
 */
std::shared_ptr<CoApp> MakeCoAppForTest(AppOptions options, struct CoAppSeam seam);
result<bbt::infra::rpc::Response> DispatchInboundForTest(
    CoApp&, const bbt::infra::rpc::Request&);

/**
 * Registry 拥有业务对象；App self entry 与观察索引均不反向保活 App。
 */
class CoApp : public bbt::coroutine::CoObject,
              public bbt::coroutine::CoRuntimeOnly {
    /**
     * 测试缝构造与测试侧入站分发观察口（internal/CoAppSeam.hpp）。
     */
    friend std::shared_ptr<CoApp> MakeCoAppForTest(AppOptions, struct CoAppSeam);
    friend result<bbt::infra::rpc::Response> DispatchInboundForTest(
        CoApp&, const bbt::infra::rpc::Request&);

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
                                         std::string name = "app");

    /**
     * 只释放自身资源，不隐式执行生命周期 hook。
     */
    ~CoApp();

    CoApp(const CoApp&) = delete;
    CoApp& operator=(const CoApp&) = delete;

    /**
     * ---- Install/Uninstall ----
     */
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
     * @tparam S 服务类型，必须继承 CoService\<...\>（CoServiceBase）、构造只接受
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

    /**
     * ---- Find ----
     */
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

    /**
     * ---- Bind/Unbind ----
     */
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

    /**
     * @brief 本 App 在 Registry 中的非拥有句柄。
     */
    ObjectHandle Handle() const;

    bbt_co_async(result<void>) CoWaitShutdownRequest(
        const bbt::coroutine::WaitOptions& wait = {});

    /**
     * @brief 安装接收端有序流授权。
     *
     * 仅允许在 CoStart 前调用；已启动后返回 Closed。service 必须已安装
     * 且 ServiceOptions 启用有序入口，授权字段由调用方提供，不自动创建
     * 未知服务或隐式放宽其他匹配条件。
     */
    result<void> grant_ordered_stream(OrderedGrant grant);

    /**
     * @brief 默认宿主的实际绑定接入点（"host:port"）。
     *
     * 仅默认装配路径有值；未监听/测试缝 → 空串。
     */
    std::string BoundEndpoint() const;

    /**
     * ---- 生命周期 ----
     */
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
    /**
     * @brief 受管协程暂停入口（语义同 Pause；上下文约束同 CoStart）。
     */
    bbt_co_async(result<void>) CoPause(const LifecycleOptions& life = {});
    /**
     * @brief 受管协程恢复入口（语义同 Resume；上下文约束同 CoStart）。
     */
    bbt_co_async(result<void>) CoResume(const LifecycleOptions& life = {});
    /**
     * @brief 受管协程停止入口（语义同 Stop；上下文约束同 CoStart）。
     */
    bbt_co_async(result<void>) CoStop(const LifecycleOptions& life = {});
    /**
     * @brief 外部线程请求关闭（幂等、noexcept；唤醒 shutdown 等待，不执行 Stop）。
     */
    void RequestShutdown() noexcept;
    /**
     * @brief App 当前状态。
     */
    AppState State() const noexcept { return m_state.load(); }

    /**
     * @brief 收口未完成项快照（bounded ring）。
     */
    std::vector<CleanupInfo> PendingCleanup() const;
    /**
     * @brief 生命周期失败记录快照（bounded ring）。
     */
    std::vector<LifecycleFailure> LifecycleFailures() const;

    /**
     * @brief 测试观察口（非稳定 API）：供本仓单测断言内部簿记。
     */
    std::shared_ptr<detail::Registry> RegistryForTest() const {
        return m_registry;
    }

private:
    /**
     * @brief 受控构造：仅 Create 工厂可调用（private + friend 自身静态成员）。
     */
    explicit CoApp(AppOptions options);
    /**
     * 测试缝构造：显式注入宿主与出站发送；仅 MakeCoAppForTest 可达。
     */
    CoApp(AppOptions options, struct CoAppSeam seam);
    /**
     * @brief shared_ptr 本体的删除同样只经 Create 返回的 owner（不开放 new/delete）。
     */
    friend struct std::default_delete<CoApp>;

    /**
     * Service/Component 共用执行体，仅在 App.cc 实例化；hook 成员指针保留类型。
     * 调度器执行的 hook 由任务持有 owner，调用方用 CoWaiter 挂起等待。
     */
    template <class T>
    result<void> _RunHookCo(
        const ObjectId& id, result<void> (T::*hook)(),
        const char* phase, bbt::coroutine::Deadline deadline);
    /**
     * 安装/卸载 hook 在已通过门禁的调用协程内、Registry 锁外执行。
     */
    template <class T>
    result<void> _RunHook(
        const ObjectId& id, result<void> (T::*hook)(),
        const char* phase);
    template <class T>
    void _RunVoidHook(
        const ObjectId& id, void (T::*hook)(),
        const char* phase) noexcept;
    result<void> _RunActorCreate(const ObjectId& id);
    void         _RunActorDestroy(const ObjectId& id) noexcept;
    /**
     * OnStop 执行 + 前序 hook 在途检查。成功 ok；失败/前序未达真实终态时
     * 经 `abort` 记录并完成收口失败收尾（abort 已带 EndOp 与状态转换）。
     * `abort` 由调用方给出，用于区分「返回错误」与「void 收口路径仅记录」
     * 两种退出形态。
     */
    template <class T, class Abort>
    result<void> _EnsureHookSettled(
        const ObjectId& id, result<void> (T::*hook)(),
        const char* op_name, bbt::coroutine::Deadline deadline,
        Abort&& abort);
    /**
     * 对象收口尾部：撤销其签发的 observer token、FinishDestroy 移除登记、
     * 排空 inactive observer lease、锁外派发事件并归还 owner。
     * `on_entry`（可空）在持锁且条目存在时执行服务侧清理。
     */
    void _FinishObject(
        const ObjectId& id,
        const std::function<void(detail::RegistryEntry&)>& on_entry = {});
    /**
     * OnBindingChanged 锁外派发：subject 经 owner lease 保活（并发卸载不能
     * 先于本调用销毁对象），异常只记 LifecycleFailure——绑定/解绑不回滚。
     */
    void _NotifyBindingChanged(ObjectKind kind, void* subj,
                               const BindingEvent& be, const char* op);

    /**
     * Install/Uninstall 的类型擦除执行体。
     */
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

    /**
     * Service 声明绑定：按 decls 逐个 CoBindComponent(subject=service)；
     * 任一失败把本服务已完成的绑定解绑（契约：必需依赖缺失 → NotFound/
     * 原错误，不虚报事务回滚——已解绑动作本身完成并记录）。
     */
    result<void> _ApplyServiceBindings(
        const ObjectHandle& service_handle,
        const std::vector<detail::ComponentBindingDecl>& decls,
        const LifecycleOptions& life);

    /**
     * 生命周期转换体：仅由已通过门禁的 Co 入口调用。
     */
    result<void> _StartImpl(const LifecycleOptions& life);
    /**
     * 启动失败回退：对已启动对象（组件/服务）按逆序执行完整收口
     * （borrow 排空 → OnStop → OnUninstall → 销毁），不销毁失败对象与
     * 未启动对象；各步独立推进，失败只记 LifecycleFailure 不中断回退。
     */
    void _RollbackStart(const std::vector<ObjectId>& started,
                        bbt::coroutine::Deadline deadline) noexcept;
    result<void> _PauseImpl(const LifecycleOptions& life);
    result<void> _ResumeImpl(const LifecycleOptions& life);
    result<void> _StopImpl(const LifecycleOptions& life);

    /**
     * ---- 网络面（装配/收口，机器面实现见 host/CoApp.cc）----
     */
    /**
     * run 前校验的对应物：listen/限额/身份/静态路由在启动网络宿主前校验，
     * 任何失败都在启动任何组件回退之前返回。
     */
    result<void> _ValidateNetworkConfig() const;
    /**
     * 出站发送注入点（CoServiceBase::RpcSendFn 的供给者）：回调复制
     * AppRuntimeState 的 shared_ptr——不捕获 this、不借用 CoApp 成员；
     * 路由门/策略/发送实现全部在状态内。真实出站可挂起当前协程。
     */
    CoServiceBase::RpcSendFn _MakeEgress();
    /**
     * 把出站注入点绑定到指定服务对象（安装路径与运行期补装共用）。
     */
    void _BindEgressToService(const std::string& service_name);
    /**
     * 启动期网络绑定：按 Registry 服务对象构建分发表项（含有序入口装配与
     * 授权流应用）、Publish 运行时状态、宿主 Create/Start。任一步失败由
     * 调用方按启动失败回退（已发布状态先撤销接纳）。宿主 Create/Start 经
     * native 辅助线程执行（infra rpc 服务器 Start 要求 native host
     * context），deadline 由调用方给出。
     */
    result<void> _StartNetworkPhase(bbt::coroutine::Deadline deadline);
    /**
     * 收口期网络步骤：StopAccepting → 在 native 辅助线程等宿主排空
     * （协程路径不经 std::cv 阻塞 worker），排空被证实返回 ok；预算耗尽
     * 或宿主 Close 打断都表示「排空未被证实」，如实透出错误。
     */
    result<void> _DrainNetwork(bbt::coroutine::Deadline deadline);
    /**
     * 入站分发入口（机器面）：由入站线桥（InstallRpcAdmission）在
     * 受管协程内调用；未启动/已收束 → err(RuntimeUnavailable)。
     * 业务不直接调用本入口。
     */
    result<bbt::infra::rpc::Response> dispatch_inbound(
        const bbt::infra::rpc::Request& request);

    /**
     * 上下文校验：Co 入口在非协程上下文 /
     * 生命周期 hook 执行中同协程再调生命周期入口 → InvalidContext。
     */
    result<void> _CheckCoContext() const;

    /**
     * Stop 逆序收口：Service → Actor 实例 → scoped Component → App
     * Component（后两者按依赖逆拓扑）。各分支独立推进，失败记
     * LifecycleFailure 并继续。
     */
    void _StopAll(bbt::coroutine::Deadline deadline);
    void _DestroyService(const ObjectId& id,
                         bbt::coroutine::Deadline deadline);
    void _DestroyComponent(const ObjectId& id,
                           bbt::coroutine::Deadline deadline);
    void _DestroyActor(const ObjectId& id);
    /**
     * 收口步失败统一出口：记 LifecycleFailure、标 incomplete 态、EndOp。
     */
    void _AbortDestroy(const ObjectId& id, const char* operation,
                       const char* phase, const Error& err,
                       ObjectState incomplete_state) noexcept;

    result<void> _RegisterAppObject();
    /**
     * owner 校验：句柄必须属于本 Registry；Service scope 的 owner 必须是
     * Service；Actor scope 的 owner 必须是已创建 actor 实例；App scope
     * 不允许显式 owner。
     */
    result<void> _ValidateOwner(const ComponentOptions& placement,
                                ObjectId* out_owner_id);
    /**
     * 组件→组件依赖环检查（锁内）：subject 是 CoComponent 时，加入
     * (subject → target) 边不得成环。返回 true 表示成环。
     */
    bool _WouldFormCycleLocked(const ObjectId& subject_id,
                               const ObjectId& target_id) const;

    const AppOptions                  m_options;
    std::shared_ptr<detail::Registry> m_registry;
    std::atomic<AppState>             m_state{AppState::Created};

    /**
     * ---- 网络面（构造期装配，CoStart 期启动，CoStop 期收口）----
     * 运行期持有的受管对象放进 m_state（AppRuntimeState）：入站 handler
     * 闭包与出站回调复制该 shared_ptr，不借用本对象成员、不捕获 this。
     * ShutdownIncomplete/StopIncomplete 期间仍由 m_state 强持有出站依赖。
     */
    mutable std::mutex                m_net_mtx;
    std::shared_ptr<INetworkHost>     m_network_host;
    /**
     * 默认装配路径的具体宿主（弱持有观察；生命周期归 m_network_host）。
     * 测试缝注入路径为空。
     */
    std::shared_ptr<InfraHttpHost>    m_default_host;
    /**
     * 测试缝注入路径标记：缝宿主不要求显式 listen（桩宿主自证）。
     */
    bool                              m_seam_host{false};
    std::shared_ptr<AppRuntimeState>  m_runtime;
    /**
     * 服务网络簿记：方法表与安装选项（安装期写入；CoStart 构建
     * 分发表项；grant 校验消费）。与 Registry 登记一致以服务名为键。
     */
    struct ServiceRecord {
        ServiceOptions options;
        RpcMethodTable table;
        std::shared_ptr<OrderedIngress> ordered_ingress;
    };
    std::map<std::string, ServiceRecord> m_service_records;
    std::vector<OrderedGrant>            m_pending_ordered_grants;
    bool                                 m_network_published{false};
    std::atomic_bool                  m_shutdown_req{false};
    std::atomic_bool                  m_lifecycle_active{false};
    ObjectHandle                      m_self;
    bool                              m_app_entry_registered = false;

    /**
     * 请求只置持久 flag 并通知单一 Co waiter，不执行 Stop。
     */
    std::mutex                        m_run_mtx;
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> m_shutdown_waiter;

    /**
     * 生命周期 hook 当前执行协程 id（0=无）：hook 内同协程再调生命周期
     * 入口据此判自等待 → InvalidContext。非协程上下文恒 0。
     */
    std::shared_ptr<std::atomic<std::uint64_t>> m_hook_co =
        std::make_shared<std::atomic<std::uint64_t>>(0);
};

/**
 * ---- 模板实现区：上段类定义只声明，以下类外定义保持对使用点可见 ----
 */

template <class C>
bbt_co_async(result<void>) CoApp::CoInstallComponent(
    std::string name, typename C::Options options,
    const ComponentOptions& placement, const LifecycleOptions& life) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<void>::err(std::move(ctx.error()));
    static_assert(std::is_base_of_v<CoComponent, C>,
        "CoInstallComponent: C must derive from CoComponent");
    static_assert(!std::is_default_constructible_v<C>,
        "CoInstallComponent: C constructor must accept only ConstructKey");
    static_assert(!std::is_destructible_v<C>,
        "CoInstallComponent: C destructor must be protected or private");
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
        "CoUninstallComponent: C must derive from CoComponent");
    return _UninstallComponentImpl(typeid(C), name, placement, life);
}

template <class S>
bbt_co_async(result<void>) CoApp::CoInstallService(ServiceOptions options) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<void>::err(std::move(ctx.error()));
    static_assert(std::is_base_of_v<CoServiceBase, S>,
        "CoInstallService: S must derive from CoService<...>");
    static_assert(!std::is_default_constructible_v<S>,
        "CoInstallService: S constructor must accept only ConstructKey");
    static_assert(!std::is_destructible_v<S>,
        "CoInstallService: S destructor must be protected or private");
    if (auto r = options.Validate(); !r) return r;

    std::string name = CoServiceStaticName<S>::Get();
    if (name.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "CoInstallService: S must declare a non-empty kServiceName"));

    /**
     * 方法表在安装期展开：非法签名走编译错误，重复/空方法名在此处
     * 抛 std::invalid_argument（启动期暴露，不等首个请求）。
     * ActorSerial 服务的每个方法都必须经 ActorMethodAt 安装 key 提取器。
     */
    RpcMethodTable table;
    try {
        table = BuildMethodTable<S>();
    } catch (const std::invalid_argument& e) {
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            std::string("CoInstallService: ") + e.what()));
    } catch (const std::exception& e) {
        return result<void>::err(MakeError(ErrorCode::InternalError,
            std::string("CoInstallService: method table failed: ") +
                e.what()));
    }
    if (auto r = ValidateActorKeying(table, options.Execution()); !r)
        return r;

    const auto decls = options.ComponentBindings();
    auto r = _InstallServiceImpl(
        typeid(S), name, options,
        [](detail::ConstructKey k) -> CoServiceBase* {
            return detail::Registry::NewManaged<S>(k);
        },
        [](CoServiceBase* p) { detail::Registry::DeleteManaged<S>(
            static_cast<S*>(p)); }, decls);
    if (!r) return r;

    /**
     * 网络簿记登记 + 出站注入点绑定：受管校验/envelope/路由门/发送
     * 全部在框架内部；服务对象一经安装即可经 bbt_co_await call 出站
     *（未启动时由路由门/宿主如实 RuntimeUnavailable）。Registry 已拒绝同名
     * 重复登记，此处 emplace 失败属簿记不变量破坏，如实失败。
     */
    {
        std::lock_guard<std::mutex> lk(m_net_mtx);
        if (!m_service_records.emplace(name,
                ServiceRecord{options, std::move(table), nullptr}).second)
            return result<void>::err(MakeError(ErrorCode::InternalError,
                "CoInstallService: duplicate service record '" + name + "'"));
    }
    _BindEgressToService(name);
    return result<void>::ok();
}

template <class S>
bbt_co_async(result<void>) CoApp::CoUninstallService(const LifecycleOptions& life) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<void>::err(std::move(ctx.error()));
    static_assert(std::is_base_of_v<CoServiceBase, S>,
        "CoUninstallService: S must derive from CoService<...>");
    std::string name = CoServiceStaticName<S>::Get();
    if (name.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "CoUninstallService: S must declare kServiceName"));
    return _UninstallServiceImpl(name, life);
}

template <class A>
bbt_co_async(result<void>) CoApp::CoInstallActor(ActorOptions options) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<void>::err(std::move(ctx.error()));
    static_assert(std::is_base_of_v<CoActor, A>,
        "CoInstallActor: A must derive from CoActor");
    static_assert(!std::is_default_constructible_v<A>,
        "CoInstallActor: A constructor must accept only ConstructKey");
    static_assert(!std::is_destructible_v<A>,
        "CoInstallActor: A destructor must be protected or private");
    if (auto r = options.Validate(); !r) return r;

    std::string type = CoActorStaticName<A>::Get();
    if (type.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "CoInstallActor: A must declare a non-empty kActorType"));

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
        "CoUninstallActor: A must derive from CoActor");
    std::string type = CoActorStaticName<A>::Get();
    if (type.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "CoUninstallActor: A must declare kActorType"));
    return _UninstallActorTypeImpl(type, life);
}

template <class C>
result<ObjectHandle> CoApp::FindComponent(
    std::string_view name, const ComponentOptions& placement) const {
    static_assert(std::is_base_of_v<CoComponent, C>,
        "FindComponent: C must derive from CoComponent");
    return _FindComponentImpl(typeid(C), name, placement);
}

template <class C>
bbt_co_async(result<ComponentHandle<C>>) CoApp::CoBindComponent(
    const ObjectHandle& subject, std::string_view name,
    const ComponentOptions& placement, const LifecycleOptions& life) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<ComponentHandle<C>>::err(std::move(ctx.error()));
    static_assert(std::is_base_of_v<CoComponent, C>,
        "CoBindComponent: C must derive from CoComponent");
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
        "CoUnbindComponent: C must derive from CoComponent");
    return _UnbindComponentImpl(typeid(C), subject, name, placement, life);
}

} // namespace bbt::framework
