#pragma once
/**
 * @brief 在受管协程中安装组件和服务，管理它们的启动、绑定和停止。
 *
 * Co 接口在改动对象前检查协程上下文；其他线程可请求关闭或读取快照。没有配置监听地址时不启动网络宿主。
 * 停止或卸载会先拒绝新操作，再等待已有借用归还、回调结束。超时后保留对象，可传入新期限重试。
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
#include <bbt/framework/internal/InboundDispatcher.hpp>
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
    friend std::shared_ptr<detail::Registry> RegistryForTest(const CoApp&);
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
     * @return 成功 ok；非协程返回 InvalidContext，App 已 Stopped 返回 Closed，
     *         选项无效、类型重复或缺 kActorType 返回 InvalidArgument。
     */
    template <class A>
    bbt_co_async(result<void>) CoRegisterActorType(ActorOptions options = {});

    /** @brief CoRegisterActorType 的兼容入口。 */
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
     *         目标封门 Closed。绑定先于 OnBindingChanged 生效，subject 可在
     *         通知中查询该绑定。通知在 Registry 锁外同步调用并保活 subject。
     *         通知抛异常只记 LifecycleFailure，绑定结果保持不变。
     */
    template <class C>
    bbt_co_async(result<ComponentHandle<C>>) CoBindComponent(
        const ObjectHandle& subject, std::string_view name,
        const ComponentOptions& placement = {},
        const LifecycleOptions& life = {});
    /**
     * @brief 原子替换 subject 的 (C, slot) 所指实例。
     * @return 新绑定的快照；验证失败时保留原关系。
     */
    template <class C>
    bbt_co_async(result<ComponentHandle<C>>) CoRebindComponent(
        const ObjectHandle& subject, std::string_view slot, std::string_view target_name,
        const ComponentOptions& placement = {});
    /**
     * @brief 摘除 subject 的 (C, slot) 关系并锁外通知。
     * placement 匹配当前目标归属。已取得的 Borrow 可继续使用原资源。
     */
    template <class C>
    bbt_co_async(result<void>) CoUnbindComponent(
        const ObjectHandle& subject, std::string_view slot,
        const ComponentOptions& placement = {});
    /**
     * @brief 摘除绑定并排空该代的显式 Borrow，排空后才通知。
     * 超时保留该代以供四参数入口重试。普通 shared_ptr 不参与排空。
     */
    template <class C>
    bbt_co_async(result<void>) CoUnbindComponent(
        const ObjectHandle& subject, std::string_view slot,
        const ComponentOptions& placement, const LifecycleOptions& life);
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
     * @brief 停止接收请求并等待在途请求结束，再依次清理服务、Actor、局部组件和应用级组件。
     * 服务按逆序停止，组件按依赖的逆序停止；先等待对象的借用归还，再调用停止回调。
     *
     * @return 不在受管协程中返回 InvalidContext；已有生命周期操作在进行则返回 Overloaded。
     * 返回 ok 后仍须检查 State()：Stopped 表示清理完成；网络未排空或仍有对象待清理时为 StopIncomplete，可用新期限重试。
     * 可通过 PendingCleanup 和 LifecycleFailures 查看原因。
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
     * @brief 查看尚未清理完的对象；只保留最近的记录。
     */
    std::vector<CleanupInfo> PendingCleanup() const;
    /**
     * @brief 生命周期失败记录快照（bounded ring）。
     */
    std::vector<LifecycleFailure> LifecycleFailures() const;

    /**
     * @brief 兼容既有测试。新测试使用 internal/CoAppSeam.hpp。
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
        const char* phase, bbt::coroutine::Deadline deadline,
        bool remember_stop = false);
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
    void _RunActorDestroy(const ObjectId& id) noexcept;
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
     */
    void _FinishObject(const ObjectId& id);
    /**
     * OnBindingChanged 锁外派发：调用方在锁内取得 subject 的回调资格，
     * 收口等已接纳通知退出。异常只记 LifecycleFailure，不回滚关系。
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

    result<void> _InstallService(
        const std::type_info& type, std::string name, ServiceOptions options,
        RpcMethodTable (*build_table)(),
        std::function<CoServiceBase*(detail::ConstructKey)> make,
        std::function<void(CoServiceBase*)> destroy);
    result<void> _InstallServiceImpl(
        const std::type_info& stype, std::string service_name,
        const ServiceOptions& options,
        std::function<CoServiceBase*(detail::ConstructKey)> make,
        std::function<void(CoServiceBase*)> destroy,
        const std::vector<detail::ComponentBindingDecl>& decls);
    result<void> _UninstallServiceImpl(std::string_view type_name,
                                       const LifecycleOptions& life);

    result<void> _RegisterActorType(const std::type_info& atype, std::string actor_type);
    result<void> _UninstallActorTypeImpl(std::string_view actor_type,
                                         const LifecycleOptions& life);

    result<detail::ComponentBindingSnapshot> _BindComponentImpl(
        const std::type_info& ctype, const ObjectHandle& subject,
        std::string_view slot, std::string_view target_name,
        const ComponentOptions& placement, bool replace);
    result<void> _UnbindComponentImpl(
        const std::type_info& ctype, const ObjectHandle& subject,
        std::string_view slot, const ComponentOptions& placement,
        const LifecycleOptions* drain);
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
    static result<void> _RunNetworkTask(std::function<result<void>()> task,
                                        bbt::coroutine::Deadline deadline,
                                        const char* timeout_message);
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
    result<void> _DestroyService(const ObjectId& id,
                                 bbt::coroutine::Deadline deadline,
                                 bool uninstall = false);
    result<void> _FinishService(const ObjectId& id, const char* operation,
                                ObjectState incomplete_state);
    result<void> _DestroyComponent(const ObjectId& id,
                                   bbt::coroutine::Deadline deadline);
    void _DestroyActor(const ObjectId& id, bbt::coroutine::Deadline deadline);
    /**
     * 记录清理失败的原因，标记未完成并结束本次操作。
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

    const AppOptions m_options;
    std::shared_ptr<detail::Registry> m_registry;
    std::atomic<AppState> m_state{AppState::Created};

    /**
     * ---- 网络面（构造期装配，CoStart 期启动，CoStop 期收口）----
     * 运行期持有的受管对象放进 m_state（AppRuntimeState）：入站 handler
     * 闭包与出站回调复制该 shared_ptr，不借用本对象成员、不捕获 this。
     * ShutdownIncomplete/StopIncomplete 期间仍由 m_state 强持有出站依赖。
     */
    mutable std::mutex m_net_mtx;
    std::shared_ptr<INetworkHost> m_network_host;
    /**
     * 默认装配路径的具体宿主（弱持有观察；生命周期归 m_network_host）。
     * 测试缝注入路径为空。
     */
    std::shared_ptr<InfraHttpHost> m_default_host;
    /**
     * 测试缝注入路径标记：缝宿主不要求显式 listen（桩宿主自证）。
     */
    bool m_seam_host{false};
    std::shared_ptr<AppRuntimeState> m_runtime;
    /**
     * 安装簿持有描述直到服务移除。安装写选项和方法表，第一次网络装配
     * 在 m_net_mtx 下挂上已授权的有序入口，各次发布只读共享该描述。
     */
    std::map<std::string, std::shared_ptr<InboundDispatcher::ServiceDescription>>
        m_service_descriptions;
    std::vector<OrderedGrant> m_pending_ordered_grants;
    bool m_network_published{false};
    std::atomic_bool m_shutdown_req{false};
    std::atomic_bool m_lifecycle_active{false};
    ObjectHandle m_self;
    bool m_app_entry_registered = false;

    /**
     * 请求只置持久 flag 并通知单一 Co waiter，不执行 Stop。
     */
    std::mutex m_run_mtx;
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> m_shutdown_waiter;

    /**
     * 生命周期 hook 当前执行协程 id（0=无）：hook 内同协程再调生命周期
     * 入口据此判自等待 → InvalidContext。非协程上下文恒 0。
     */
    std::shared_ptr<std::atomic<std::uint64_t>> m_hook_co =
        std::make_shared<std::atomic<std::uint64_t>>(0);
};

} // namespace bbt::framework

#include <bbt/framework/__TCoApp.hpp>
