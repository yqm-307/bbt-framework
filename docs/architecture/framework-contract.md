# framework：对象、绑定、生命周期与监控契约

本文是 framework 对象模型、绑定、生命周期与监控的**契约真源**，分两类内容，读者须先分清：

- **已实现**（`bbt/framework/` 源码与对应 CTest 覆盖）：`CoApp` 受管装配与生命周期（`CoInstall*`、
  `CoUninstall*`、`CoBindComponent`/`CoUnbindComponent`、`CoObserve`、`CoStart`/`CoStop`、
  `RequestShutdown`/`CoWaitShutdownRequest`）、`ComponentHandle::Borrow`、V2 RPC 入站/出站、
  静态与动态路由门。已实现项在下文以函数名和行为给出，与源码一致。
- **目标设计**（本文多数签名与章节，尚未实现）：标注为“目标”或在 §11 验收台账中列出的项，包括
  `MonitorService`、`AppInspector`、`AppSession`、`Resolver`、`CoJoin`、`HttpComponent`/`RedisComponent`
  等内置组件、Actor 实例创建与消息分发、`BindObject`/`UnbindObject` 非拥有边。

调用未实现项返回的错误：

| 未实现项 | 当前行为 |
|---|---|
| `CoBindObject` / `CoUnbindObject`（非拥有关系边） | 恒返回 `ErrorCode::UnsupportedRoute`（`host/CoApp.cc`） |
| `CoCreateActor` / `CoDestroyActor` / Actor 消息分发 | 未接线；`CoInstallActor` 只登记类型，不创建实例 |
| `MonitorService`、`AppInspector`、`CoJoin`、`HttpComponent` | 源码中不存在；不得依赖 |

## 1. 权威、基线与取代关系

- 架构基调见 [distributed-framework-baseline.md](distributed-framework-baseline.md)；本文冻结具体对象接口，不替换分布式保证的边界。
- 依赖的 infra 消费契约为 `bbtools-infra/docs/decisions/` 下的 ADR：`0014-app-session.md`（逻辑会话与 admission 语义）、
  `0022-rpc-single-protocol.md`（RPC 只保留 V2 会话）。该仓不在本仓库内，本文不重复其内容；本文不重审或修改已验收 infra ADR，接口以 infra 公开头为准。
- 依赖版本与 SHA 以 `deps.lock` 为准；构建须使用其中的精确 pin，不以本文档中的任何历史 SHA 为依据。
- C++17，命名空间 `bbt::framework`；以下代码块是公共声明目标，不是可直接编译的完整头文件。`result/Error/ErrorCode` 沿用 infra 体系，错误名不另造第二套异常或错误协议。
- 旧公共面（`CoApp::add_service`/`add_resource`/`run()`、`CoAppOptions`、`ICoService`、`context().resource`、`ShutdownState`、`find_route` 等）已删除，不保留兼容壳；对应关系见 §12.1。
- `CoTell` 只返回 `result<void>`，需要业务结果只用 `CoCall`；不保留 WaitReply/Ask 第二入口。
- 顺序只在同一会话 epoch、同一发送方到同一目标的接纳域内成立（ADR 0014 §3.4、§5.3）；断线后不承诺跨 epoch 保序。
- framework 的 `AppSession` 只是非拥有的逻辑关系/观察句柄；物理 session 状态、连接与 epoch 协商归注入 Server 的同一 infra Client。
- 运行时 OnInstall 注册 HTTP Router 须在 Freeze 前完成；Freeze 后绑定返回 `Busy`（ADR 0014 §8），不提供动态 match-tree 修改。
- HTTP/RPC 监控出口默认关闭；显式开启且具备授权检查才可暴露。绑定 `HttpComponent` 不等于开启 HTTP。
- 唯一 framework 组件名为 `MongoDBComponent`（目标）；infra 侧仍为 `mongo::Database`。

## 2. 对象职责与所有权

| 对象 | 创建/强 owner | 可公开能力 | 不负责/禁止 |
|---|---|---|---|
| CoApp | main/宿主 | 通用 Install、Uninstall、Bind、Unbind、Observe、生命周期、Inspector | 不增加 EnableMonitor/InstallRedis 等能力特例；不停止进程寿命 coroutine runtime。 |
| Service | Registry（统一登记表），最终归 CoApp | 方法表、并发或按 key 串行、消息、受控 handle、hook | 不拥有其他 Service/Actor/Component，不自行关闭共享客户端。 |
| Actor | Registry（统一登记表），最终归 CoApp | 不变 ActorId、MailBox、串行消息处理、显式创建/销毁 | 不暴露 this、成员引用或可直接调用的 Actor 指针；不承诺集群唯一。 |
| Component | Registry（统一登记表），最终归 CoApp | 能力生命周期、元数据、实际 borrow 内的 infra Client/Database/Router | 不反向拥有绑定者，不隐式共享，不截断 infra 命令集。 |
| AppNetwork | CoApp 私有持有 | 本 App 的 Engine/Client/Server 装配及只读汇总 | 不成为业务 Service，不重造 socket、reader、writer、pool。 |
| AppSession | AppNetwork 管理的逻辑视图 | RemoteAppId、State、Epoch、Stats，均不泄漏连接 | 不拥有第二份 infra 会话表；不供业务创建/Close；观测缺失不编造值。 |

Registry 可以为借用/队列/回调保留内部强引用，但它们都属于同一个 CoApp 管理域，不能向业务交出 `shared_ptr<T>`。对象不可复制、不可移动。三种受管对象的构造都要求不能由业务制造的 ConstructKey——其私有构造显式提供（非 `= default`），不是 aggregate，业务不能以 `detail::ConstructKey{}` 聚合初始化绕过 Registry creator；基类与具体派生类析构均 protected/private 且 noexcept；具体类型声明对应 Registry 为友元，由其类型化 deleter 销毁。Install 对公开可析构的具体类型做负向静态检查，不能只保护基类析构却让 `delete C*` 合法。受管对象的类型擦除 owner 一律由「静态类型指针 + 类型化 deleter」构成，不经 `void*` 反向 `static_cast` 派生调整；`ComponentHandle::Borrow` 的对象地址以 `CoComponent*` 交付，由 `CoComponent*→C*` 的编译器转换完成多继承下的正确指针调整。业务不得 delete 从 borrow 借出的地址；C++ 访问控制不防御恶意类型自身的 delete-this/UB，也不能替代线上授权。

App scope 键为 `(component type, logical name)`；Service/Actor scope 键还包括所属对象的 id/generation。相同类型、不同名字不共享；同名不同类型不是同一实例。scope 是默认可见范围和收口策略，不是 C++ 所有权。App scope 在最后一个绑定解除后仍存在，直到显式 Uninstall；Service/Actor scope 只能绑定所属对象以及它受控的依赖，不得逃到其他对象。Component 依赖可绑定 App scope 或相同 scope/owner 的 Component。没有匹配的显式安装项则 NotFound，不自动降级到另一个 scope。

## 3. 身份、handle、borrow 与状态值

以下附属值类型随所属公共头声明，不新增 Registry/Session manager 等业务实体。时间：Deadline 为 `bbt::coroutine::Deadline`，本地单调时钟；导出时间为 UTC RFC 3339 字符串。

```cpp
using ObjectId = std::string; // 稳定逻辑 id，不是指针、fd 或 coroutine id
using ActorId = std::pair<std::string, std::string>; // type, key
enum class ObjectState {
    Created, Installing, Installed, Starting, Running, Paused, Stopping, Stopped,
    InstallFailed, Failed, StopIncomplete, Uninstalling, UninstallIncomplete,
    Destroyed, Invalidated, Unknown
};
enum class ComponentScope { App, Service, Actor };
enum class AppState {
    Created, Starting, Running, Paused, Stopping, StopIncomplete, Stopped
};

class ObjectHandle {
public:
    const ObjectId& Id() const noexcept;
    std::uint64_t Generation() const noexcept;
    bool IsValid() const noexcept;       // 仅观察，不能据此随后解引用
    result<ObjectInfo> Info() const;     // owning 值快照；没有 T*/T&/shared_ptr
    result<MailBox> GetMailBox() const; // 仅 Service/Actor
};

template<class C> class ComponentBorrow {
public:
    ComponentBorrow(ComponentBorrow&&) noexcept;
    ComponentBorrow& operator=(ComponentBorrow&&) noexcept;
    ~ComponentBorrow() noexcept;
    C& Get() const noexcept;             // 只在本 borrow 生存期内有效
    ComponentBorrow(const ComponentBorrow&) = delete;
    ComponentBorrow& operator=(const ComponentBorrow&) = delete;
};

template<class C> class ComponentHandle {
public:
    ObjectHandle Object() const;
    bool IsValid() const noexcept;
    result<ComponentBorrow<C>> Borrow() const;
};

class ObserverToken {
public:
    ObserverToken(ObserverToken&&) noexcept;
    ObserverToken& operator=(ObserverToken&&) noexcept;
    ~ObserverToken() noexcept;           // 逻辑 Revoke；非强杀在途回调
    void Revoke() noexcept;
    bool IsActive() const noexcept;
    ObserverToken(const ObserverToken&) = delete;
    ObserverToken& operator=(const ObserverToken&) = delete;
};
```

`ObjectInfo` 是值快照：id、generation、kind、type、name、scope、owner_id、state、最后错误的稳定 code/phase、observed_at；`ActorInfo` 另含 ActorId 和 MailBox，不含 Actor 引用。ObjectState 唯一集合为 Created、Installing、Installed、Starting、Running、Paused、Stopping、Stopped、InstallFailed、Failed、StopIncomplete、Uninstalling、UninstallIncomplete、Destroyed、Invalidated、Unknown。Actor 的 Creating/Destroying 分别映射 Installing/Uninstalling，JSON 使用 snake_case。失败状态不等于已释放。App node 同样允许 StopIncomplete，JSON 为 stop_incomplete。ObjectHandle 仅针对本 App 已登记对象；远端目标使用 AppObjectAddress/MailBox 值及 AppSession 可达性观察，不凭远端断线推断远端对象已销毁，也不将地址值当对象 borrow。

### 3.1 必须实现的实际寿命规则

1. `ComponentHandle` 只保存 registry 身份、对象 generation、binding generation 和失效状态，不拥有 C。`Borrow()` 在同一临界区内校验对象/绑定 generation、状态和 closing gate，并增加实际 borrow 计数/固定资源版本，成功后才返回引用。禁止 `weak.lock/check → unlock → raw pointer use`。
2. 安装期 OnInstall 可借 Installed 的依赖做纯内存装配；普通业务仅可借 Running 且 binding active 的组件。其他阶段返回明确 Closed/Busy/NotFound；不得把半启动的客户端当可用。
3. Unbind 原子关闭该 binding 的新 borrow，旧 handle 随 binding generation 失效；已经取得的 borrow 继续保有同一资源版本。Unbind 等到该 binding 的 borrow 归零才返回成功；deadline 到则返回 TimedOut，边为 invalidated，仍保留 retiring 记录和 owner，后续同一操作继续收口。不得在旧 borrow 仍运行时 Close 底层客户端。
4. UninstallComponent 若还有任何 active 或 retiring binding，返回 Busy，**不自动解绑所有使用者**。无绑定后封对象 gate，等待其他内部 lease/在途调用，再 OnStop/OnUninstall 和析构。共享组件的其他使用者不因一个使用者解绑而中断。
5. Service/Actor 不提供 public borrow 或对象指针。接纳队列持有内部执行 lease；Call 延续到 handler 和 reply 写责任结束，Tell 延续到框架 mailbox 中的业务处理结束。观察回调、Router handler、自定义采集任务分别持有内部 callback lease。
6. Borrow 可跨协程挂起，但必须保持在该受管任务的 RAII 栈上；禁止把 Get() 的引用、Client() 返回值、this 或成员引用交给不持该 lease 的任务。派生 Component 若创建后台任务，必须登记在自己的受管任务/计数内，OnStop 等待真实终态。不允许用 detached 裸捕获绕过 owner。
7. 析构条件是 admission 已封、业务 queue/inflight 为零、borrow/callback/route lease 为零、底层物理完成已确认，不是 `IsClosed()==true` 或 weak 已失效。超时保留 Registry owner、资源版本和 cleanup 记录，继续驱动；宿主必须保留 CoApp，不能用析构或硬释放伪装完成。
8. 控制面不得在持 Registry 锁时调用用户 hook、回调、Close、join 或析构。控制面操作串行发布不可变 dispatch/binding 快照，锁外执行 hook；同一对象转换期间冲突操作 Busy。hook 内递归同步调用同一控制面返回 Busy，避免自等待。

## 4. CoApp：唯一装配和动态控制接口

```cpp
class CoApp {
public:
    // 唯一创建入口：经 coroutine MakeCoObject 真实 creator 与统计接管。
    // 构造 private（业务不能栈构造或直接 new）；返回业务 owner 强引用。
    static std::shared_ptr<CoApp> Create(AppOptions options,
                                         std::string name = "app");
    ~CoApp(); // 宿主必须先完成 Stop；未完成时不得提前析构
    CoApp(const CoApp&) = delete;
    CoApp& operator=(const CoApp&) = delete;

    template<class C> result<void> InstallComponent(
        std::string name, typename C::Options options,
        const ComponentOptions& placement = {}, const LifecycleOptions& life = {});
    template<class C> result<void> UninstallComponent(
        std::string_view name, const ComponentOptions& placement = {},
        const LifecycleOptions& life = {});
    template<class S, class... Args> result<void> InstallService(
        ServiceOptions options = {}, Args&&... args);
    template<class S> result<void> UninstallService(const LifecycleOptions& life = {});
    template<class A> result<void> InstallActor(ActorOptions options = {}); // 登记类型
    template<class A> result<void> UninstallActor(const LifecycleOptions& life = {});

    result<ObjectHandle> FindService(std::string_view service) const;
    result<ObjectHandle> FindActor(const ActorId& id) const;
    template<class C> result<ObjectHandle> FindComponent(
        std::string_view name, const ComponentOptions& placement = {}) const;
    template<class C> result<ComponentHandle<C>> BindComponent(
        const ObjectHandle& subject, std::string_view name,
        const ComponentOptions& placement = {}, const LifecycleOptions& life = {});
    template<class C> result<void> UnbindComponent(
        const ObjectHandle& subject, std::string_view name,
        const ComponentOptions& placement = {}, const LifecycleOptions& life = {});
    result<void> BindObject(const ObjectHandle& subject, const ObjectHandle& target);
    result<void> UnbindObject(const ObjectHandle& subject, const ObjectHandle& target);
    result<ObserverToken> Observe(const ObjectHandle& observer,
        const ObjectHandle& target, ObjectObserver callback);
    ObjectHandle Handle() const;                 // App 自身的非拥有身份
    AppInspector Inspector() const;             // 非拥有、只读，App 失效后 Closed

    result<void> Start(const LifecycleOptions& life = {});
    result<void> Pause(const LifecycleOptions& life = {});
    result<void> Resume(const LifecycleOptions& life = {});
    result<void> Stop(const LifecycleOptions& life = {});
    bbt_co_async(result<void>) CoStart(const LifecycleOptions& life = {});
    bbt_co_async(result<void>) CoPause(const LifecycleOptions& life = {});
    bbt_co_async(result<void>) CoResume(const LifecycleOptions& life = {});
    bbt_co_async(result<void>) CoStop(const LifecycleOptions& life = {});
    int Run();
    void RequestShutdown() noexcept;
    AppState State() const noexcept;
    result<std::string> BoundEndpoint() const;
    std::vector<CleanupInfo> PendingCleanup() const;
    std::vector<LifecycleFailure> LifecycleFailures() const;
};
```

`ObjectObserver = std::function<void(const ObjectStateEvent&)>`；事件是 owning 值，含 id、generation、state、revision、observed_at、稳定错误 code/phase，以及可选 ActorInfo，不含引用或 payload。Component 依赖也是 BindComponent 的 subject；BindObject 只记录非拥有的关系，不授权直接调用对象、不隐式建立组件启动依赖。Find 只查已存在对象，失败安装对象仍可通过名字找到以清理；Actor Find 不会激活对象。UninstallActor 先拒新建，按预算销毁已建实例，再撤登记；失败保留类型及残留实例记录。

上面安装、卸载、绑定、Observe 是非协程宿主控制面，误上下文返回 InvalidContext；运行期操作同步完成局部 Start/Stop/绑定 hook 后返回。动态创建/销毁 Actor 只能通过下节 Co 入口在受管协程执行，不在 admission reader 内执行。读值快照和 RequestShutdown 可任意线程调用。需要异步停机时用 RequestShutdown，不从本 App handler 调会等待自身的 CoStop/CoPause；这种调用返回 InvalidContext。

### 4.1 Options 的完整装配面

下表中的 setter 均返回所属 Options 的 `&`；`Validate() const -> result<void>` 无副作用，Install/Start 内再次验证。参数声明里的默认值只为便于装配，不允许无限等待。

| 类型 | setter/查询的目标签名 | 默认/边界 |
|---|---|---|
| LifecycleOptions | `SetDeadline(Deadline)`；`Deadline() const noexcept -> Deadline` | 未设时使用 App 配置的有限 lifecycle budget；每步不得重置总 deadline。 |
| ComponentOptions | `SetScope(ComponentScope)`；`SetOwner(ObjectHandle)` | App 默认且 owner 为空；Service/Actor 必须有本 App、相符类型且同代的 owner。 |
| ServiceOptions | `SetLifecycleOptions(LifecycleOptions)`；`SetMaxInflight(size_t)`；`SetMaxSerialKeys(size_t)`；`SetSerialQueueCapacity(size_t)`；`SetOrderedIngress(OrderedIngressOptions)`；`BindComponent<C>(string name)`；`SetServiceOptions(O options)` | BindComponent 明确指向 App scope 已安装项；`SetServiceOptions` 的 O 必须为 S::Options，不隐式转换/序列化。默认无组件。 |
| ActorOptions | `SetLifecycleOptions(LifecycleOptions)`；`SetMaxActors(size_t)`；`SetMailboxCapacity(size_t)`；`SetAggregateQueueCapacity(size_t)`；`BindComponent<C>(string name)` | 默认无组件；所有额度有限正值，Actor scope 需对显式创建后的实例安装/绑定。 |
| CreateOptions | `SetDeadline(Deadline)` | 继承并收紧父预算，顶层必须有限。 |
| CallOptions | `SetDeadline(Deadline)`；`SetRouteKey(string)`；`AddRouteField(string,string)`；`SetOrdered(OrderedStamp)` | 父子 min；顶层无预算拒绝；默认不重试。 |
| TellOptions | `SetDeadline(Deadline)`；`RequireRemoteAcceptance(bool=true)` | 默认 false（仅本地接纳）；不提供 WaitReply。 |
| AppOptions | `Listen(string uri)`；`AddRoute(string service,string uri)`；`SetNetworkLimits(infra::NetworkLimits)`；`SetLimits(size_t max_objects,size_t max_bindings,size_t max_observers,size_t observer_queue_capacity)`；`SetShutdownBudget(milliseconds)`；`SetRpcPolicy(shared_ptr<RpcPolicy>)`；`SetResolver(shared_ptr<Resolver>)` | 只有一种 RPC 通道（V2 `tcp://` 会话，见 §7.1），无 profile 选择；默认无监听/静态路由，未配置目标 NotFound；`SetNetworkLimits` 各项须显式非零正值（`infra::ValidateNetworkLimits` 校验，全零默认被 `CoStart` 网络装配拒绝），未配置监听则不消费；SetShutdownBudget 必须为正有限值，未设则 Start/Run 校验失败；不自动安装资源或 Monitor。 |
| AppNetworkOptions | `SetLocalAppId(string)`；`SetTrustedNetworkPlaintext(bool)`；`SetMaxSessions(size_t)`；`SetMaxFrameBytes(size_t)`；`SetSystemQueueBytes(size_t)`；`SetServiceQueueBytes(size_t)`；`SetActorQueueBytes(size_t)`；`SetServiceWeight(uint32_t)`；`SetActorWeight(uint32_t)`；`SetControlDataSplit(bool)` | `SetLocalAppId` 即 infra `rpc::ClientOptions::LocalIdentity`（非空 ≤256 UTF-8，同值自连被拒）；明文 trusted-network 默认 false；未有可信 adapter 且未显式启用时按 ADR 返回 UnsupportedRoute；split 默认 false，未实现不能忽略 true。 |
| AppSessionOptions | `SetEndpoint(string)`；`SetProfile(string)`；`SetDeadline(Deadline)`；`Validate()` | 只是 Resolver/网络装配的值，不有 Create/Close/物理连接选项，不允许 Service 私建 session。profile/security 决定共享键，不能每业务请求换身份。 |

Service/Actor 可以声明允许使用的 Component 类型；动态 BindComponent 也必须满足类型声明和 scope 限制。对于 `Service<>`，没有声明的类型绑定返回 InvalidArgument。MonitorService 声明 HttpComponent 为可选能力，没有绑定也可安装；“可用类型”不等于“必需依赖”。必需依赖由安装配置显式列出，缺失在 Install/Bind 返回 NotFound。Component 的依赖关系通过 BindComponent 记录，启动按拓扑、停止按逆拓扑；环返回 InvalidArgument 并保留失败记录，不自动打断或修复。

## 5. Service、Actor、Component 与消息

```cpp
template<class... Components> class Service : public detail::ObjectBase {
public:
    ObjectHandle Handle() const;
    ObjectInfo Info() const;
protected:
    explicit Service(ConstructKey key);
    virtual ~Service() noexcept = default;
    template<class C> result<ComponentHandle<C>> GetComponent(std::string_view name) const;
    template<class Resp = Payload, class Req>
    bbt_co_async(result<Resp>) CoCall(const AppObjectAddress& target,
        std::string_view method, const Req& request, const CallOptions& options = {});
    bbt_co_async(result<void>) CoTell(const AppObjectAddress& target,
        std::string_view method, const Payload& message, const TellOptions& options = {});
    template<class A> bbt_co_async(result<MailBox>) CoCreateActor(
        std::string key = {}, const CreateOptions& options = {});
    bbt_co_async(result<void>) CoDestroyActor(const MailBox&, const LifecycleOptions& life = {});
    std::vector<ActorInfo> ListActors(std::string_view type) const;
    virtual result<void> OnInstall() { return {}; }  // 宿主，无 I/O
    virtual result<void> OnStart() { return {}; }    // 协程，可 bbt_co_await
    virtual result<void> OnPause() { return {}; }    // 协程
    virtual result<void> OnResume() { return {}; }   // 协程
    virtual result<void> OnStop() { return {}; }     // 协程
    virtual void OnUninstall() {}                   // 宿主，无 I/O
    virtual void OnActorCreated(const ActorInfo&) {}
    virtual void OnActorDestroyed(const ActorInfo&) {}
    virtual void OnMonitorCollect(MonitorWriter&) {} // 受管协程，默认空
    virtual void OnBindingChanged(const BindingEvent&) {} // 宿主，无 I/O
};

class Actor : public detail::ObjectBase {
public:
    const ActorId& GetId() const noexcept;
    const MailBox& GetMailBox() const noexcept;
    ObjectHandle Handle() const;
    ActorInfo Info() const;
protected:
    explicit Actor(ConstructKey key);
    virtual ~Actor() noexcept = default;
    template<class C> result<ComponentHandle<C>> GetComponent(std::string_view name) const;
    template<class Resp = Payload, class Req>
    bbt_co_async(result<Resp>) CoCall(const AppObjectAddress& target,
        std::string_view method, const Req& request, const CallOptions& options = {});
    bbt_co_async(result<void>) CoTell(const AppObjectAddress& target,
        std::string_view method, const Payload& message, const TellOptions& options = {});
    template<class A> bbt_co_async(result<MailBox>) CoCreateActor(
        std::string key = {}, const CreateOptions& options = {});
    bbt_co_async(result<void>) CoDestroyActor(const MailBox&, const LifecycleOptions& life = {});
    bbt_co_async(result<void>) CoDestroySelf(); // 当前消息终态后安排，不等待自身
    virtual result<void> OnCreate() { return {}; } // 协程，第一条业务消息前
    virtual void OnDestroy() {}                    // 协程，业务排空后
    virtual void OnMonitorCollect(MonitorWriter&) {}
    virtual void OnBindingChanged(const BindingEvent&) {} // 宿主，无 I/O
};

class Component {
public:
    ObjectHandle Handle() const;
    ObjectInfo Info() const;       // Registry 身份不可由派生类改写
protected:
    explicit Component(ConstructKey key);
    virtual ~Component() noexcept = default;
    template<class C> result<ComponentHandle<C>> GetComponent(std::string_view name) const;
    bbt::infra::net::Engine& NetEngine(); // 仅受管 hook/borrow；不得存出管理域
    virtual result<void> OnInstall() { return {}; }
    virtual result<void> OnStart() { return {}; }
    virtual result<void> OnStop() { return {}; }
    virtual void OnUninstall() {}
    virtual void OnMonitorCollect(MonitorWriter&) {}
    virtual void OnBindingChanged(const BindingEvent&) {}
};
```

`detail::ObjectBase` 为 Service/Actor 复用 ICoObject 信息、请求上下文和消息实现，不是新公共实体。它及 Component 显式删除 copy/move；各基类将对应 Registry 设为友元。Registry 调用入口 private；业务只覆盖 protected hook。ConstructKey 是构造函数私有、仅 `detail::Registry` 为友元的 passkey 值，由 Registryry 独占，禁止泄给业务。新 hook 只能兼容式新增并带空默认实现。

`BindingEvent` 含 subject/target id、generation、绑定状态、revision 和 observed_at。绑定处理先准备但不发布新边，调用 OnBindingChanged 校验/接线成功后才发布 active；失败保留 failed 记录，不恢复其他已完成操作。解绑先封 gate/invalidate，再在 borrow 排空后通知，hook 失败标记清理 incomplete。回调看到的对象身份不授予裸指针访问权。

### 5.1 唯一地址、消息和调用语义

```cpp
class AppObjectAddress {
public:
    enum class Kind { Service, Actor };
    static AppObjectAddress Service(std::string name, std::string app = {});
    static AppObjectAddress Actor(std::string type, std::string key, std::string app = {});
    Kind ObjectKind() const noexcept;
    const std::string& Type() const noexcept;
    const std::string& Key() const noexcept;
    const std::string& App() const noexcept;
    result<void> Validate() const;
};
class MailBox { // 可复制地址值，不是队列/Actor 引用
public:
    explicit MailBox(AppObjectAddress address);
    const AppObjectAddress& Address() const noexcept;
};
enum class AppTrafficClass : std::uint8_t { System = 0, Service = 1, Actor = 2 };
enum class ReplyMode : std::uint8_t { None = 0, Required = 1 };
struct AppMessage {
    AppObjectAddress source, target;
    std::string method, request_id, message_id;
    ReplyMode reply;
    std::uint64_t sequence;
    Payload payload;
};
```

- Service/Actor 均声明显式线上名字的方法表：Service 的 `kServiceName`、`kRpcMethods = RpcMethods(Method<&S::F>("name"), ...)`；Actor 的 `kActorType`、`kMessages = ActorMessages(Message<&A::F>("name"), ...)`。位置参数、proto、原始 Payload 编解码归 framework；handler 返回 `result<T>`。SerialBy<I>/SerialByRouteKey 是 Service 方法策略，不为每 key 构造 Service。
- `Payload::From(Args...) -> Payload`、`FromProto(const P&) -> Payload`、`As<T...>() const -> result<tuple<T...>>`、`ParseProto<P>() const -> result<P>`；编码错误在构造/发送边界转 result 错误，不抛出到业务调度边界。原始字节形式不得获得 transport owner。
- CoCall 设 Reply Required，返回 handler 的业务结果；CoTell 设 None，默认只表示有界本地接纳。RequireRemoteAcceptance(true) 只等待远端 queue/mailbox 接纳，不等业务执行。两者同一目标共用发送顺序和同一接纳 FIFO，不另造 Ask 协议。
- 本地快速路径也走授权、budget、相同 mailbox/Service dispatch 接纳和错误边界，禁止直接调用对象方法绕过策略。
- Service 默认可并发；按 key 串行仅锁同 key，Actor 一实例一次一条，挂起不释放执行资格。A 等 B、B 等 A 以及同 key 回调不具备自动重入/死锁修复，只能避免调用环并受 deadline 约束。
- CoCreateActor 仅使用已 InstallActor 的本地类型，空 key 生成唯一 key；本节点重复 id 为 AlreadyExists，额度满 Overloaded。CoDestroyActor 先封该实例的新消息，再按预算排空和 OnDestroy；超时保留 owner，不销毁在途栈。远程创建/销毁不是隐式能力，应由显式控制 Service 授权处理。
- Resolver 保留 `Resolve(const ResolveRequest&) -> result<string>`，ResolveRequest 是 owning 的 target、route key、route fields 值。RpcPolicy 保留 `Preflight(const CallView&) -> PolicyDecision`，只能拒绝/收紧预算。无 Resolver 时静态 Service 路由有效，Actor 仅本地；不承诺集群 placement/唯一性。
- `RequestContext::Current() -> result<shared_ptr<const RequestContext>>`；只读 RequestId/RouteKey/Deadline/PeerPrincipal/TraceId。上下文绑定逻辑协程，不使用普通 thread_local 跨挂起缓存；peer principal 只能来自可信 adapter，绝不从 route/fw metadata 推断。

framework 适配器对 Request 的业务映射也只有一份：Service target 的 `Service=target.Type()`；Actor target 的 `Service="actor."+target.Type()`；`Method=显式方法名`；`route.target_kind=service|actor`、Actor 的 `route.target_key`、可选 `route.target_app`，以及 `fw.source_kind`、`fw.source_type`、`fw.source_key`、`fw.source_app` 表达源地址。字段采用规范化 UTF-8，Actor key/type/app 超过 ADR metadata 单项256字节时在发送前 InvalidArgument；这些字段占用同一32项额度、不得重复，source 字段仅路由/保序用途，不作为认证。框架保留上述键，业务 AddRouteField 不得覆盖；接收端检查 target_kind、Service 字段和目标类型的一致性，认证/授权独立使用 principal。schema 名来自方法表，Call Response 原样回显 request_id/service/method/request_schema/response_schema。`ordering_key` 是源对象身份与目标身份的规范化长度前缀组合；超过 ADR256字节则发送前拒绝，不截断导致碰撞。相同 ordering key 的 sequence 在 AppNetwork 的提交临界区分配且不回绕；并发 Service 协程只按该提交顺序定义先后。AppMessage 不另带可与 Request 冲突的 traffic 字段。

### 5.2 内置 Component 不截断能力

`RedisComponent(ConstructKey, infra::redis::ClientOptions)` 暴露 `Client() -> infra::redis::Client&`；`MongoDBComponent(ConstructKey, infra::mongo::DatabaseOptions)` 暴露 `Database() -> infra::mongo::Database&`；`HttpComponent(ConstructKey, HttpComponent::Options)` 暴露 `Router() -> infra::http::Router&`。构造只 Registry 可用，Options 按组件归属头定义；Mongo 配置类型在 infra 稳定头落地前不得假称已可编译。

这些引用仅在实际 ComponentBorrow 的寿命内使用；业务不得 Close 客户端、改共享池配置、移动所有权或保存引用。组件只做 Create/Start/Stop、错误映射，不自建轮询线程，不包装有限命令子集。HttpComponent 的 Options 明确 listen、server/router 限额及 lifecycle budget；只有用户显式安装且启动该 Component 才有 HTTP listener，Monitor 不暗建 Server。

### 5.3 核心头文件、模板与注释组织

本节规定核心整理的目标与验收条件；未全部完成时以各文件现状为准，只调整代码组织和注释，不改变上文的接口语义、所有权、受控构造或生命周期保证。

**按独立概念拆文件。** `Component` 自带默认 hook，已经承担组件基类的职责。确实需要独立纯接口时，命名与目录采用 `third-party/bbtools-pub/docs/cpp/naming-and-types.md` 的约定。

以下是目标文件归属，尚不是现有文件清单；路径相对于 `bbt/framework/`：

| 文件 | 职责 |
|---|---|
| `Component.hpp` | Component 基类、组件生命周期和扩展 hook；不再混放在 Service.hpp。 |
| `Service.hpp` | `Service<Components...>` 的类定义和模板成员声明，业务能够先读清接口。 |
| `internal/ServiceBase.hpp` | 已有非模板类型擦除基类的内部声明；不新增一套服务抽象。 |
| `Actor.hpp` | Actor 基类和自身 hook，不与 Component/Service 的类定义混排。 |
| `App.hpp` | CoApp 装配与控制接口；模板成员声明与实现分区。 |

公共声明直接包含实际依赖，不靠偶然的传递 include；通过前置声明和单向依赖消除循环引用。附属 Options、枚举、别名按所属主类归档，不机械拆成大量小文件；现有共享值类型的单独文件仅在承担真实共享职责时保留。拆分后的聚合入口可以继续统一导出公共类型，但不能用聚合入口掩盖单头不能独立包含的问题。

**模板声明与实现分离。** 默认采用“头文件上半段是完整类定义、成员只作声明；下半段是类外模板成员定义”。较长实现按 `third-party/bbtools-pub/docs/cpp/class-and-headers.md` 拆至同目录实现头，由主头末尾包含。实现头是本公共头的实现细节，不作为调用者的独立入口。模板定义必须保持对使用点可见，不能直接搬到普通 `.cc` 导致消费者实例化缺失；非模板实现仍进入配对编译单元。类型 trait、常量和 `= default` 等简短声明可随类保留，不为分区增加空壳或重复转发层。

**Doxygen 放在声明处。** 公开类型、函数、模板及受保护的业务扩展 hook 使用 `/** ... */` 块（排版见 `third-party/bbtools-pub/docs/cpp/comments.md`），不能只写无法导出的普通 `//`。类型按职责卡说明 `@brief`、`@par 职责`、`@par 不负责`、`@par 约束`；函数按实际签名补齐 `@brief`、存在的 `@tparam`/`@param` 和有意义的 `@return`。调用上下文、线程/协程限制、借用寿命、错误与关闭行为按需写 `@pre`、`@par` 或正文，不为空参数或 void 返回机械生成空标签，也不编造尚未兑现的保证。实现文件和模板实现区解释 WHY、算法和时序，不复制公共声明的 Doxygen 契约。

**整理验收：**

- 阅读 Component/Service/Actor 不再需要在一个头中翻找多个独立基类；保留原有受控构造和所有权边界。
- 含逻辑的模板成员不再与类内接口声明混排；独立包含检查、真实消费者模板实例化和直接相关行为测试通过，无循环 include、重复定义或链接缺失。
- Doxygen 实际导出包含上述类型、函数和 hook，并核对参数、模板参数、返回说明与职责卡；缺工具或未运行必须标为未验证，不能把普通编译成功当作注释通过。既有文档门禁不得为通过而放宽；安装工具、修改 CI 或发布文档仍遵守原授权边界。
- 当前源码的混排和普通注释必须在后续核心整理中补齐。实现、构建测试、文档导出分别给证据，不能用最小功能切片的通过代替本节完成。

## 6. 动态控制、观察与失败兜底

- Install：无副作用参数/额度/名字校验 → Registry 登记 Installing → 构造 → 显式依赖检查 → OnInstall → Installed；App 已 Running 时再 OnStart 成功才发布业务 dispatch。构造失败不发布空实例；hook 失败已登记对象为 InstallFailed，可 Find/卸载，不能返回“全部回滚”。
- 组件先 Install 并按依赖拓扑启动，Service 再 Start；Actor 创建先确认依赖 ready，再 OnCreate，最后允许消息。Stop 反序：拒新 → 每个对象先封其 subject 绑定边并排**空在途 borrow** → 再 Service OnStop/Actor OnDestroy → scoped Component → App Component（旧 borrow 不得在 OnStop 关闭资源后仍运行同一资源版本；OnStop 失败/超时同样保留 owner 记 LifecycleFailure 并继续其他对象收口）。**排空粒度是该 subject 自己的 binding 边**（per-(component, subject, generation) `WaitBindingDrained`），不是组件全量 borrow：共享组件上其他 subject 的合法借用不阻塞本对象收口。subject 已封口（gate_closed/op_in_progress/关闭语义状态）后在同一 Registry 临界区内拒绝新增 Bind——销毁路径只擦除 retiring 边，关闭期间新增的 active 边不会被收口看到。
- Pause：拒新并排空在途后 OnPause；Resume 的 OnResume 成功后重新接纳。Stop 后不能 Start；StopIncomplete 允许再次 Stop 使用更晚期限推进 cleanup，不重开入口。
- UninstallService：移除新 dispatch、撤自己的 observer/Router token、等 execution/callback/borrow 终态、OnStop/OnUninstall、解绑、收口 owned-scope Component、最后销毁。各安全独立清理分支继续运行，任一未完为 UninstallIncomplete。逆序指实际已成功登记/启动的依赖图，不只反转名字列表。
- `CleanupInfo` 为 object id/generation、phase、pending borrows/tasks/callbacks、stable error code；`LifecycleFailure` 为 object id/generation、operation、phase、code、发生时间及本地脱敏说明。诊断列表受 App 的 max_objects/有界 ring 限制，溢出计 dropped；原始异常文本、URI、凭据不得直接导出 Monitor。
- 所有 hook 的 error return 和 std/非 std 异常均捕获，转 InternalError/原 error 并记录 phase；OnUninstall/OnDestroy 虽 void，抛异常仍记录并继续其他安全清理。析构 noexcept。try/catch 不保证 UB、数据竞争、死循环、进程崩溃或泄漏协程可恢复。
- Observe 只建立非拥有边。ObserverToken 绑定 observer generation，observer 卸载时 Registry 自动撤销；target 销毁时先发布一次终结事件再使 token 失效。事件队列有界、best-effort，终结事件也可能因队列满/观察者退出未交付；不能靠“必收 Destroyed”维持安全，handle gate 的实际失效才是依据。回调在锁外受管执行，回调抛异常计数，不拖垮业务。**在途 callback lease**：派发方在锁内校验 active 并取得 lease（在途计数）后才执行业务回调；Revoke 先标 inactive 阻断新派发，等待已派发 lease 归零后抹除记录与闭包——闭包捕获对象不会在回调仍在途时被销毁，撤销返回时闭包已确定释放。通知/序列缺口后用 Inspector 重新取快照。
- 自定义 Actor 采集 hook/状态通知若访问实例状态，必须经它同一 mailbox/执行资格串行调度，不在另一个监控协程直接读成员；队列满则省略并记录 observation_unavailable。Service/Component 的并发 hook 要么读取它们发布的不可变值快照，要么由业务以自身同步保护，不借 Monitor 消除数据竞争。采集超时只结束等待，仍运行的 hook 持有 callback lease 和 capture 额度至真实退出，不提前归还并发额度，也不无限创建下一轮采集。
- AppNetwork hook 使用同一 ObserverToken/有界通知机制：`AppMonitorHooks` 的默认空 `OnObjectStateChanged(const ObjectStateEvent&)`、`OnBindingChanged(const BindingEvent&)`、`OnSessionStateChanged(const SessionStateEvent&)`、`OnTrafficSample(const TrafficSample&)`。`AppInspector::Observe(ObjectHandle observer, shared_ptr<AppMonitorHooks>) -> result<ObserverToken>`；shared_ptr 只拥有订阅回调状态，不拥有受管对象，撤销后等在途 callback lease。

## 7. AppNetwork 与已验收 infra 的逐项映射

AppNetwork 为私有装配类：一个共享 Engine、一个共享 CoRpcClient，以及注入此 CoRpcClient 的 CoRpcServer；业务不获取它的强指针。默认每个 `(remote opaque identity, profile, security context)` 一个双向 link。System 只由 infra 生成控制帧；普通健康检查/Monitor RPC 仍为 Service traffic，不借 System 绕额度。

```cpp
class AppSession { // AppNetwork 发出的非拥有值句柄，不能业务构造
public:
    std::string RemoteAppId() const;
    ObjectState State() const; // 仅有证据的逻辑状态，否则 Unknown
    result<std::uint64_t> Epoch() const;
    result<AppMonitorSession> Stats() const;
};
class AppInspector {
public:
    result<AppMonitorSnapshot> Capture(const MonitorSnapshotOptions&) const;
    result<ObserverToken> Observe(ObjectHandle observer, std::shared_ptr<AppMonitorHooks>);
};
```

**观测可用性限制**：ADR 的稳定 ClientStats/ServerStats 是 owner 汇总，没有 public 的逐 peer 枚举、epoch 或逐 peer Stats getter。这里不发明 `Client::Sessions()`、detail 访问或第二份 session registry。Framework 可以记录 Resolver/自身目标关系、admission 和调用终态，不能由一次 Call 成功猜 epoch/link 数。Epoch 无源返回 UnsupportedRoute；Stats 只在有源时返回真实字段，缺项列入 `unavailable`，完全失效则 Closed。当前 adapter 不具备的逐 session 统计/hook 标记为实现证据缺口，而非修改既有 ADR。Schema 允许 unavailable 表达此边界；infra owner 汇总与 framework 本地计数可以正常导出。

| ADR 0014 章节 | framework 唯一映射/责任 | 实施验收依据（不是本文档的通过证据） |
|---|---|---|
| ADR 0014 §1–2.1 | infra 类型不含 App/Actor；AppObjectAddress 映射到 Request Service/Method/Metadata/Payload；Traffic 由 target kind 唯一决定，Reply 由 CoCall/CoTell 唯一决定；Context 只读 | 稳定头独立 include，禁旧 transport/Socket/AppLink 泄漏；四种消息方向。 |
| ADR 0014 §2.2 | CoRpcClient::Create(engine, options)，CoCall → Call(target,Request,infra CallOptions)，CoTell → Send(target,Request,SendOptions)；剩余预算只收紧；HTTP Send 为 UnsupportedRoute | 顶层无 deadline、本地满、HTTP Tell 拒绝；无自动 retry。 |
| ADR 0014 §2.3–2.4 | CoRpcServer::Create(engine,同一个CoRpcClient,AdmissionHandler,options)；同步 handler 只校验/授权/有界转移 owning AdmissionItem，不挂起、不执行业务 | Call/Send 单 FIFO，queue 满立即 Overloaded；不得在 reader 上跑 hook/Resolver I/O。 |
| ADR 0014 §4.2 | Call 在独立协程填相同 correlation 字段，经 ReplyCompletion::Complete/Fail 写终态后 Release AdmissionLease；Send 成功入队后 infra 收投递 lease，framework 自有 lease 留到 Tell 执行终态 | 请求各一次终态，lease 各一次释放；infra 会话责任结清不能冒充框架 Tell 排空。 |
| ADR 0014 §3 | 本地 id 来自 AppNetworkOptions；endpoint 只是位置；自报 identity/epoch 非授权凭据；同时拨号/握手/fencing 归 infra，App 不另实现 | 默认双进程单 link、同时拨号、旧 epoch 不 admission；跨重启/迁移顺序不承诺。 |
| ADR 0014 §4.1、§4.3 | RequireRemoteAcceptance 对应 SendOptions 同名值及 wire acceptance_requested，不把 SendAccepted 当业务结果；mailbox 满计 dead-letter/drop，确认式投递 Overloaded | 满一 Actor 不阻塞共享 reader/System/Service/其他 Actor；发送方不自动补发。 |
| ADR 0014 §5 | Codec/正式 v2 proto 与 u32be 归 infra；framework 只做 Payload/schema、route/fw 字段和 opaque ordering_key/sequence。metadata 仅 fw/route/trace，32项、key64/value256字节且无重复 | Call1→Tell2→Call3 同目标按序接纳；字段/长度/过期/关联拒绝；默认 frame8MiB，硬上限1GiB，握手≤64KiB。 |
| ADR 0014 §5.3–§6 | 同发送方/目标以发送队列接纳为序；旧 epoch 不能复用新代额度；已 mailbox 接纳的不回滚。OutcomeUnknown 保留 request_phase，不推断未执行 | 部分写退役、不重放；旧 callback 只能结算既有责任，不捕获 framework 裸指针。 |
| ADR 0014 §2.3、§6 | 宿主 StopAccepting 关闭业务门 → WaitHandlersDone 等宿主在途责任归零 → CoRpcServer::Shutdown() → 宿主 Close 对 RPC server/client 调用 Shutdown(reason) 并收口 Engine；HTTP 分支独立遵循其关闭接口 | RPC Shutdown/IsClosed 不证明物理释放；尚未结束的驱动线程仍由宿主持有并最终 join，StopIncomplete 可追踪。 |
| ADR 0014 §8 | 仅消费稳定 net/http/rpc/redis/mongo 面；HTTP Router/Request/Response/stream/pool、Redis Acquire/AUTH/SELECT、Mongo Collection/Document/worker 语义原样保留；Payload codec 归 framework | 模块 owner 实施对应 api/include/真实后端回归；不把本轮文档当作上述模块完成。 |
| ADR 0014 §8 | HttpComponent Freeze 前结构注册，token owner 为注册 Service；Revoke 先封新 dispatch，再等 handler lease；运行时新 path Busy | 并发撤销/Uninstall 不悬空；不偷偷重建 Router 或增加 route。 |
| ADR 0014 §9–§11 | 授权/Resolver/mailbox/重试/业务 idempotency 归 framework；Monitor 只读、显式、opt-in；删除 LegacyHeaders/x-bbt-*、旧 envelope/transport/WaitStopped 稳定入口 | examples/API consumer、正反向 include、Monitor/schema、真实两 App 和关闭验收。 |
| ADR 0014 §7、§12 | 只消费 infra 已验收的公开面，不重新打开 infra 契约；net/HTTP/Redis/Mongo 后续工作归属 infra | 目标文档不等同代码证据；未落地头/方法保持未验证。 |

### 7.1 RPC V2 唯一消费面（ADR 0022 现行面）

RPC 只按 V2 会话消费：`LegacyHeaders`/`ProtoWireV1`/`TcpV1` 模式选择、`x-bbt-*` 编解码、`RpcEnvelope`/`RpcAddress`/旧 `RpcHandler` 与两座旧桥（`RpcWireBridge`/`RpcHttpBridge`）已按 `bbtools-infra/docs/decisions/0022-rpc-single-protocol.md` 删除；`bbt/framework/host/` 现由 `RpcBridge.{hpp,cc}` 承担 V2 入站/出站装配。下表旧列只表示迁移历史，当前代码不存在这些旧面，也不保留兼容壳：

| 旧 | 新 | 责任方 |
|---|---|---|
| `CoAppOptions::inbound_bridge`（`RpcInboundBridge`） | 删除；`CoAppOptions::local_identity`（映射 `rpc::ClientOptions::SetLocalIdentity`） | framework 装配 |
| `InstallRpcHttpBridge` / `InstallRpcWireBridge` | `InstallRpcAdmission(InfraHttpHost&, shared_ptr<AppRuntimeState>)` | framework 入站 |
| `InfraHttpHost::InstallRpcHandler` + `SetRpcInboundTransport` | `rpc::CoRpcServer::Create(engine, client, AdmissionHandler, ServerOptions)` | framework 宿主 |
| `IncomingCallContext` + `RpcEnvelope`（入站） | `rpc::AdmissionItem`（`request` + `reply` + `lease`） | infra → framework |
| `RpcAddress{transport,endpoint}` | `std::string target`（`"tcp://host:port"`）；路由表存 endpoint 字符串 | framework 路由 |
| `HttpEgress` + `RpcEgressProfile` | `RpcEgress`：`CoRpcClient::Call(target, Request, CallOptions)` / `CoRpcClient::Send(target, Request, SendOptions)` | framework 出站 |
| `AppRuntimeState::Send(const RpcEnvelope&, ...)` | `Send(const rpc::Request&, const bbt::infra::CallOptions&) -> bbt_co_async(result<rpc::Response>)` | framework 路由门 + 策略 |
| `InboundDispatcher::Dispatch(ctx, RpcEnvelope)` | `Dispatch(const rpc::Request&) -> result<rpc::Response>` | framework 分发 |
| `RpcCallView{target: RpcAddress, request: RpcEnvelope}` | `RpcCallView{target: std::string, request: rpc::Request}` | framework 策略 |
| `RpcSendAppFn(RpcAddress, RpcEnvelope, CallOptions)` | `RpcSendAppFn(std::string_view, const rpc::Request&, const bbt::infra::CallOptions&) -> bbt_co_async(result<rpc::Response>)` | 测试缝 |

不变量：framework **不注册** `Server::RegisterRpc`——注册路由会为每帧派生独立协程并绕过单 FIFO admission 与 `OrderedIngress` 的唯一按序接纳（ADR 0014 §4.2）；全部入站（Call 与 Send）走 admission handler。入站 `Traffic=Service`，出站 `Call` 为 `Required`、`Tell` 为 `None`；V2 `OrderingKey`/`Sequence` 不填（`sequence=0` 即 infra 不启用顺序检查），业务有序票据继续走 `fw.*` 元数据。`peer_principal` 在明文 trusted-network 下恒空，授权仍由 `RpcPolicy` 与业务层承担。

表中「新」列的 infra 类型与 framework 侧 `InstallRpcAdmission`、`RpcEgress`、`AppOptions::SetLocalIdentity`（取代旧 `CoAppOptions::local_identity`）、`InboundDispatcher::Dispatch(const rpc::Request&)`、`RpcSendAppFn(std::string_view, ...)` 均已落地；消费者按本表消费，不另起别名。

## 8. MonitorService：显式安装、访问控制与撤销

Monitor 是框架提供的普通 Service，不是 CoApp 内核开关，不标记 builtin，不通过轮询业务 RPC 采集；唯一服务名为字符串 `__builtin.monitor`（不是 C++ 保留标识符）。`MonitorService : Service<HttpComponent>`、`using Options = MonitorOptions`，构造仅 ConstructKey；安装通过普通 ServiceOptions 的 SetServiceOptions 传值。

```cpp
enum class MonitorFailurePolicy { FailAppStart, DisableMonitorAndContinue };
enum class HealthState { Healthy, Degraded, Unhealthy, Unknown };
using MonitorAccessCheck = std::function<result<void>(const MonitorAccessContext&)>;
class MonitorOptions {
public:
    MonitorOptions& ExposeRpc(bool value);
    MonitorOptions& ExposeHttp(bool value);
    MonitorOptions& SetHttpPath(std::string path);
    MonitorOptions& SetAccessCheck(MonitorAccessCheck check);
    MonitorOptions& SetFailurePolicy(MonitorFailurePolicy policy);
    MonitorOptions& SetSampleInterval(std::chrono::milliseconds interval);
    MonitorOptions& SetHistoryLength(std::size_t count);
    MonitorOptions& SetLimits(std::size_t max_nodes, std::size_t max_edges,
        std::size_t max_sessions, std::size_t max_output_bytes,
        std::size_t max_concurrent_captures);
    MonitorOptions& SetCaptureBudget(std::chrono::milliseconds budget);
    result<void> Validate() const;
};
class MonitorService : public Service<HttpComponent> {
public:
    using Options = MonitorOptions;
    static constexpr std::string_view kServiceName = "__builtin.monitor";
    bbt_co_async(result<AppMonitorSnapshot>) CoGetSnapshot(const MonitorSnapshotOptions& = {});
    bbt_co_async(result<std::string>) CoGetSnapshotJson(const MonitorSnapshotOptions& = {});
    bbt_co_async(result<AppMonitorHealth>) CoGetHealth();
    bbt_co_async(result<AppMonitorMetrics>) CoGetMetrics(const MonitorQuery& = {});
protected:
    explicit MonitorService(ConstructKey key);
    ~MonitorService() noexcept override;
    friend class detail::ServiceRegistry;
};
class MonitorSnapshotOptions {
public:
    MonitorSnapshotOptions& SetDeadline(Deadline);
    MonitorSnapshotOptions& IncludeMetrics(bool);
    MonitorSnapshotOptions& SetNodeLimit(std::size_t);
    MonitorSnapshotOptions& SetEdgeLimit(std::size_t);
};
class MonitorQuery {
public:
    MonitorQuery& SetDeadline(Deadline);
    MonitorQuery& SetObjectId(std::string id); // 授权后仅过滤已导出非敏感 id
};
class MonitorWriter {
public:
    result<void> Metadata(std::string_view key, std::string_view safe_value);
    result<void> Gauge(std::string_view name, double value);
    result<void> Counter(std::string_view name, std::uint64_t value);
    result<void> Health(HealthState state, std::string_view stable_reason_code);
};
```

MonitorAccessContext 是只读值：transport（local/http/rpc）、已验证 principal、operation（snapshot/health/metrics）、可选请求的 object export id；不含 authorization header、cookie、凭据或 payload。本地 Inspector 仅宿主/受管控制面可取，不开放远程调用。对外 MonitorAccessCheck 对每次请求运行，默认拒绝；提供 `result<void>` 成功才授权，异常按拒绝记录。内部监控方法也不得绕过对相应调用来源的检查。

默认 `ExposeRpc=false`、`ExposeHttp=false`、path `/__monitor`、failure policy `DisableMonitorAndContinue`、采样关闭（interval=0）、history=0。没有安装/订阅时不启动采样任务或历史缓存。安装后只按需 Capture；interval>0 的显式采样才缓存，history≤64，interval 若开启必须≥100ms。默认上限：nodes=1024、edges=4096、sessions=64、output=1MiB、并发 capture=1、capture budget=100ms。可配置但不超过 Schema 硬上限 nodes=4096、edges=16384、sessions=256、output=4MiB、并发 capture=4、budget≤1s。hook、observer queue、history 总内存也必须有预算；history 总序列化字节≤16MiB，满按最老样本淘汰并计数。Schema 不代替字节、时间和 CPU 预算。

### 8.1 访问边界与端点

| 路径/调用者 | 认证来源 | 每次授权/结果 |
|---|---|---|
| 宿主 AppInspector、受管本地 Monitor 调用（internal） | 本地受管 App 身份，不由网络字段构造 | 本地 ACL/AccessCheck；不把 local 当远程通行证。Inspector 捕获只读，不能改 Registry。 |
| `GET <path>/snapshot`、`/health`、`/metrics`（machine/operator） | HttpComponent 信任配置产生的 principal；普通 header 自报无效 | 各 operation 都 AccessCheck；object filter 同样授权后处理，禁止有过滤器就跳过检查。 |
| Monitor RPC snapshot/health/metrics（machine/operator） | RequestContext::PeerPrincipal 的认证结果 | 每方法同一 AccessCheck；trusted-network 明文 principal 为空，不足以启用 RPC Monitor。 |

若启用某网络出口却无可信认证来源或 AccessCheck，Install 在注册任何 route 前失败；关闭出口不要求部署认证。当前 ADR 明文 RPC 装配不能提供可信 principal，因此无其他已验收可信 adapter 时 `ExposeRpc(true)` 返回 UnsupportedRoute，不提供“内网所以免鉴权”的例外。HTTP 同样要求已经实际接入的可信认证，未落地则明确 UnsupportedRoute。不给凭据默认值、不假设未来 TLS/verifier 已存在。

三个成功出口统一 JSON envelope：`schema`、`view`、`snapshot`、`app`，分别附图/health/metrics，正式真源是同目录 Schema；无分页/游标/持久化。失败 envelope 为 `{"schema":"bbt.app.monitor/v1","view":"error","error":{"code":"..."}}`，不得携带 raw Error::message/details。鉴权先于采集/查对象，未授权对象与不存在对象对外统一无对象存在性提示。仅只读 GET 和对应只读 RPC；不支持写、卸载或远程控制。

| 失败 | HTTP / RPC error | 能否重试 | 记录/返回限制 |
|---|---|---|---|
| 关闭出口/未安装 | 404 / NotFound | 否 | 不暴露对象存在性。 |
| 无认证 | 401 / Unauthorized | 否 | error code only，不回显凭据。 |
| 已认证但无权限 | 403 / PermissionDenied | 否 | code only，不返回 graph/health。 |
| 参数/过滤器超限 | 400 / InvalidArgument | 修正输入 | 不回显原参数。 |
| capture 并发额度满 | 429 / Overloaded | 有界退避 | 不创建补偿队列。 |
| 对象转换/Router 已 Freeze 后新绑定 | 控制面 Busy；已存在出口暂不可用为503 / Busy | 等待控制面 | 不修改 topology、不绕过 lease。 |
| 采集超时 | 504 / TimedOut | 总预算内 | 不强杀仍执行 hook、不释放其 lease。 |
| 采集/序列化/hook 异常 | 503 / InternalError | 总预算内 | 本地脱敏诊断；不回空 healthy 结果。 |
| 服务关闭/资源不可用 | 503 / Closed 或 UnsupportedRoute | 配置/生命周期恢复后 | Unknown/明确错误，不伪装 Healthy。 |

这里 HTTP/RPC 的授权 code 为监控出口的稳定语义映射；如现有 infra ErrorCode 未提供同名枚举，用 Error 的 framework domain/domain_code 携带该名字，不改已冻结 ErrorCode 编号。Schema error.code 就是这些稳定名字。

### 8.2 HttpComponent 绑定与 route token

1. `ExposeHttp=true` 必须已显式 BindComponent<HttpComponent>(name)，否则 NotFound；`ExposeHttp=false` 时即使绑定也不注册任何 monitor route。未绑定时 Monitor 可用于本地或已授权 RPC，不创建 HTTP listener。
2. Freeze 前 OnInstall 或动态 OnBindingChanged 借 Router 注册三条路由并持有各自 `RouteRegistrationToken`；方法、完整规范化 path、冲突、AccessCheck 都在发布前校验。注册一部分失败则撤已取得 token，记录失败，不宣称其他对象回滚。
3. 动态绑定发生在 HttpComponent 已 Start/Router Freeze 后：本契约选最小实现，返回 Busy，保留 failed binding 记录、RPC/本地功能不变。**不采用 ADR 允许但本项目未定义的预注册静态 dispatch 扩展**。要改变 path/增加路由，必须显式停服并重新装配 Router；组件不支持重开则卸载重建。
4. Unbind、Uninstall、依赖失效都先 Revoke token（拒新 dispatch），再等待旧 route/callback/capture lease 排空，最后释放 handler、解除 HttpComponent borrow 和 binding。token 析构不是在途 handler 强杀；预算耗尽保留 token owner 和 UninstallIncomplete。
5. 路由闭包只捕获 Registry generation/内部 callback state，不捕获会失效的裸 Service 指针。Router/HttpComponent 不能先于持有它的 handler borrow 被销毁。没有 route token 实现前不宣称可撤销出口通过。
6. OnInstall/Start 失败：FailAppStart 参与普通启动失败收口；默认 DisableMonitorAndContinue 撤出口/订阅、标记 Monitor InstallFailed/Failed，记录诊断后继续启动独立业务。Monitor 故障不标 App 全部 Healthy；也不声称能够隔离进程崩溃或无限阻塞 hook。 Monitor 在 OnInstall 订阅 Registry/Network，OnStop 封监控入口和采样但保留 cleanup 观察至 OnUninstall；要观察用户对象完整启动过程，装配方先安装 Monitor 再安装用户 Service，按普通安装顺序启动/逆序停止，不给 CoApp 增加隐藏优先级或自动安装。

## 9. 正式监控数据契约

唯一 JSON 形状由 [app-monitor.schema.json](../schemas/app-monitor.schema.json) 定义，JSON Schema Draft 2020-12，`$ref` 仅允许该文件内的 `#/$defs/...`，验证不得联网解析外部资源。`schema = bbt.app.monitor/v1`，`view = snapshot | health | metrics | error`。不同 view 是同一有版本的输出族，不维护第二份样例式“schema”。新增不兼容字段/枚举须新版本，不静默改变 v1 的 closed object；当前未发布，不承诺旧讨论稿 JSON 的逐字兼容。

typed AppMonitorSnapshot 与 JSON 一一对应：snapshot(id,app_revision,captured_at,consistency,truncated,omitted)，app(id,name,state,framework_version)，nodes、edges、sessions、health、metrics。AppMonitorHealth 含 state/reasons/observed_at；AppMonitorMetrics 使用 Schema 的同名对象。AppMonitorJsonWriter 的 `Write(const AppMonitorSnapshot&) -> result<string>` 及 health/metrics 对应重载统一写 envelope；序列化失败显式错误，禁止拼接未经转义的自定义字符串。AppInspector::Capture 返回 result，取代 F15 不可表达失败的裸值签名。

- node kinds：app/service/actor/component/session；session 详情在 sessions 数组中，必须以同 id 的 session node 出现在完整图。app 自身也是 node。`owns` 表示最终 Registry 所有权（app→Service/Actor/Component/session），scope owner_id 不把 Service 变成 C++ owner。
- edge kinds：owns/binds/observes/routes/calls/tells；state 为 active/invalidated/failed。routes 来自显式路由视图，calls/tells 只来自已启用且实际采样的有界计数关系，不推测未发生调用，不记录 payload/request id。未启用时这类边可以为空。
- id 格式为 `app:<id>`、`service:<app>/<name>`、`actor:<app>/<type>/<export_key>`、`component:<app>/<scope>/<type>/<name>[/<owner_export_key>]`、`session:<app>/<remote>[/<profile>/<security_alias>]`。仅安全逻辑标识，必要部分做规范化百分号编码；重复创建同逻辑对象用 generation 区分，不能把历史节点的 active 边接到新代。session 后缀用于不同 profile/security context，不能导出 secret 或身份凭据。
- Actor 原始业务 key、用户信息、连接 URI/密码、http header/cookie、payload/body、raw exception、socket/fd、线程 id、内存地址、trace/request/operation id 均不导出。Registry 必须用部署方确认非敏感的稳定 export alias 替换敏感业务 id/name；没有 alias 就不导出该项，计 omitted，并将快照标 truncated，不能直接“encode 一下”当脱敏。
- metadata 不是任意 map：Schema 仅允许 version、role、description、export_alias，值还必须来自安装方 allowlist 的非敏感自述。MonitorWriter 只接受这些 key；自定义数值指标放 custom，最多32个预登记低基数名字，不允许用户 id、actor key、request id 作标签或 key。Schema 不能证明任意字符串不含秘密，采集源和 writer 必须在序列化前按 allowlist 验证。
- 数值计数/epoch/revision/generation 限于 JSON 安全整数 `0..9007199254740991`，epoch/generation 非零。达到上限饱和并暴露非健康 reason，不回绕。任意浮点必须有限，禁止 NaN/Inf。Gauge/计数、latency_us 单位按字段名，不能将不存在的样本补0。
- session epoch/links/traffic 三项要么实际有值，要么对应列入 unavailable；Schema 结构检查这个互斥条件。traffic 的 System/Service/Actor 六个字段来自同一采样源，分别为 queued/dequeued/inflight/finished/dropped/bytes_queued；framework dead_letters/errors 在自己的 metrics 中记，不冒充 infra finished。
- health 为 healthy/degraded/unhealthy/unknown，reasons 含稳定 code、来源 node id 和 observed_at。有自定义 Unhealthy 则聚合 Unhealthy，Degraded 次之；缺采样/超时/丢事件且无更严重证据为 Unknown，不能以进程活着推断 Healthy。失效关键依赖为 Unhealthy，非关键资源压力为 Degraded；是否关键由显式部署配置而非 Monitor 猜测。
- 快照 best_effort，不跨 Registry/Network/业务 hook 做原子事务。app_revision 只标本地控制面快照版本，不伪称整个采集期冻结；节点/session 独立 observed_at。收集 owning 值后释放 Registry 锁，再序列化；过期关系允许 failed/invalidated，已 destroyed 节点不当 active 节点。
- 选择顺序：app node 优先，其他 id 排序选有界集合，再只选端点都存在的边及已入图 session 详情。生成前按 generation/revision 消除重复和陈旧 active 边；采集期间缺失的端点不作为悬空边导出。截断时 `truncated=true`、omitted 给实际被省略数或已知下界（计数采用下界时 reason 标记），不伪称完整。再裁至 output bytes 上限仍放不下最小 envelope 就返回 Overloaded。
- Schema 不会校验跨数组 id 唯一性/边端点引用/同 app归属/generation一致/真实 ACL/secret-free/总字节/时限；这些是 writer 和集成验收的语义断言。Schema 示例是**合成契约测试输入**，不是实际 App 运行数据。

## 10. 装配与借用示例（目标，不代表基线可运行）

```cpp
namespace fw = bbt::framework;
class GoodsService : public fw::Service<fw::RedisComponent> {
public:
    using Service::Service;
    static constexpr std::string_view kServiceName = "goods";
    result<fw::Payload> Read(const fw::Payload& request) {
        auto handle = GetComponent<fw::RedisComponent>("main");
        if (!handle) return handle.error();
        auto lease = handle.value().Borrow();
        if (!lease) return lease.error();
        // 所有 Client() 操作及其挂起都在 lease 生存期内；不能存出此引用。
        // 真实 Redis 命令按 infra 稳定 Client 的签名调用，不在此另造包装。
        return request;
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&GoodsService::Read>("read"));
protected:
    ~GoodsService() noexcept = default;
    friend class fw::detail::ServiceRegistry;
};

fw::CoApp app(app_options); // runtime 由宿主先初始化
if (auto r = app.InstallComponent<fw::RedisComponent>("main", redis_options); !r)
    return Report(r.error());
if (auto r = app.InstallService<GoodsService>(
        fw::ServiceOptions{}.BindComponent<fw::RedisComponent>("main")); !r)
    return Report(r.error());
// OrderService 也显式绑定 "main"，才共享同一池；不写绑定不会隐式创建。

// 有已配置认证的 HttpComponent 时，在 Router Freeze 之前安装 Monitor。
if (auto r = app.InstallComponent<fw::HttpComponent>("admin", admin_http_options); !r)
    return Report(r.error());
if (auto r = app.InstallService<fw::MonitorService>(fw::ServiceOptions{}
        .BindComponent<fw::HttpComponent>("admin")
        .SetServiceOptions(fw::MonitorOptions{}
            .ExposeHttp(true).ExposeRpc(false)
            .SetAccessCheck(operator_access_check))); !r)
    return Report(r.error());
return app.Run();
```

`Report`、redis_options、admin_http_options、operator_access_check 是应用提供且显式校验的配置/错误处理，不是框架新增入口。生产示例必须真实处理 result，不能照抄忽略失败。

运行期动态 Actor-scope 组件使用同一接口：先 `FindActor(id)` 取得 ObjectHandle，`ComponentOptions{}.SetScope(ComponentScope::Actor).SetOwner(handle)`，再 InstallComponent、BindComponent；Service-scope 同理。解绑一个使用者不能影响另一个；仍有 binding 的 App Component Uninstall 返回 Busy。动态绑定成功只替换该 binding generation，不迁移业务状态。

## 11. 验收台账与验证边界

本节列出每项契约要求的判定证据。“已实现”项的证据见对应测试（`tests/`、`examples/` 的 CTest 名）；
“目标”项尚未实现，其证据是后续实现的验收条件，不得视为通过。

| 要求 | 本文位置 | 判定证据 / 当前状态 |
|---|---|---|
| 唯一强 owner、禁业务构造/销毁 | §2–4 ConstructKey/Registry/handle | 已实现：`framework.v2_core`、`framework.rpc_surface`。负向编译（栈构造、make_shared、copy/move、delete）为目标。 |
| scope、显式共享、Busy | §2–4、§10 | 目标：两 Service 同 type/name 同实例，一方 Unbind 不关闭池；最后解绑仍存在；三 scope 隔离、错误 owner、环依赖、缺依赖。 |
| 真实 borrow，非 weak 检查 | §3.1 | 目标：Borrow 已取、解绑封口、旧任务挂起三阶段交错后，新 borrow 拒绝、旧借用可完成、最后引用结束前无 Close/析构。 |
| 观察无指针、自动撤销 | §3、§6 | 目标：target/observer 同时销毁、回调在途、队列满、回调抛异常；handle 即刻失效，callback lease 排空后释放。 |
| 动态失败兜底 | §4、§6 | 目标：OnInstall/OnStart/OnStop/OnUninstall 异常与错误返回，部分成功记录、逆序独立清理、StopIncomplete/UninstallIncomplete，不虚报事务回滚。 |
| 消息与网络 | §5、§7 | 目标：真实两 App、四方向 CoCall/CoTell、单 link、Call1/Tell2/Call3、Actor 满队列隔离、epoch 缺口/未知终态、lease/drain/物理收口。 |
| 显式 MonitorService | §8 | 目标（未实现）：未 Install 不存在/不采样/无 route；安装才入图；默认两出口关闭；无 Http 无隐藏 listener。 |
| 图与正式 Schema | §9、JSON Schema | 目标（未实现）：三 scope 共享图、owns/binds/observes/routes/calls/tells、id/generation/引用语义、schema 正反例、实际字节上限。 |
| hook/健康/一致性/失败 | §6、§8–9 | 目标（未实现）：稳定 metadata 和数值 custom、业务 hook 降级来源、缺观测 Unknown；hook 慢/抛错后业务继续。 |
| 授权、无敏感、资源有界 | §8–9 | 目标（未实现）：401/403 无数据、plain RPC 不能冒充认证、三端点同 ACL、敏感标记注入被拒/省略、截断/字节/并发/history/队列硬上限。 |
| 能力组合，不增特例实体 | §2、§4、§6、§8 | 目标：新自定义 Component/Service 只实现声明和 hook；CoApp 无特例 API；动态卸载无悬空 hook/route。 |
| 核心代码组织与 Doxygen | §5.3 | 目标：Component/Service/Actor 分文件；模板声明与定义分区或末尾 include；独立包含和真实模板实例化通过；实际 Doxygen 导出核对。 |
| HTTP freeze/token 与动态决定 | §8.2 | 目标（未实现）：Freeze 前注册、后绑定 Busy，撤销和在途 HTTP 并发，timeout 保留 owner。 |

本文档的签名与行为是否与源码一致，以 `bbt/framework/` 头文件和 `tests/`、`examples/` 的 CTest 结果为准；
本文档本身不是编译或运行证据。

已知依赖：infra v2 稳定头、HTTP Router/RouteRegistrationToken 与可信认证 adapter 需由 infra 的 owner 提供真实实现；
逐 session epoch/统计的公共观测源未由已验收签名给出，按 §7 的 `UnsupportedRoute`/unavailable 表达。
未提供认证时网络 Monitor 出口必须关闭；未实现 Router token 时不能宣称可撤销 HTTP Monitor 已就绪。

迁移影响：仓内 `examples/` 与 `tests/` 已切换到当前公共面；旧接口不保留兼容壳。
外部 fork/consumer 未穷举，不声称无人使用；按一次性不兼容迁移处理，不保留长期兼容壳。

## 12. 受管协程生命周期与旧接口对应

### 12.1 生命周期入口与旧接口对应

业务生命周期入口只有带 `Co` 前缀的协程方法；它们须在副作用、Registry 写入、hook 投递或等待登记前检查
协程上下文，宿主/非协程上下文返回 `InvalidContext`。宿主只做装配与等待，由协程执行收口。

| 旧接口（已删除） | 当前对应 | 边界 |
|---|---|---|
| `CoAppOptions` + 栈构造 `CoApp{opts}` | `CoApp::Create(AppOptions, name)` 返回 `shared_ptr<CoApp>` | 构造 private；只能经 `Create` 创建 |
| `add_service<S>(opts)` / `add_resource<R>(name, ...)` | `CoInstallService<S>(opts)` / `CoInstallComponent<C>(name, options)` | 须在协程内调用 |
| `context().resource<R>(name)` | `CoService::GetComponent<C>(name)` + `ComponentHandle::Borrow()` | 借用寿命仅限 lease 期间 |
| `ServiceOptions{...}` 位置参数 | `ServiceOptions{}.SetExecution(...).SetMaxInflight(...).BindComponent<C>(name)` | 链式 setter |
| `run()` / `request_shutdown()` | `CoWaitShutdownRequest()` + `CoStop()`；外部线程 `RequestShutdown()` | `RequestShutdown` 只置位，不执行 hook、不等待 |
| `shutdown_state()` | `State()`（`AppState`） | 枚举值：Created/Starting/Running/Paused/Stopping/StopIncomplete/Stopped |
| `ICoCloseable` 资源 | `CoComponent::OnStop()` | 组件收口在 borrow 排空之后、销毁之前 |
| `find_route` 静态路由 | `AppOptions::AddRoute(service, endpoint)`；动态路由 `AppOptions::SetDynamicRoutes` | 动态配置的服务名优先且不回退；未配置则 `NotFound` |

### 12.2 `RequestShutdown` 与 `CoWaitShutdownRequest`

- `RequestShutdown() noexcept` 幂等，可在任意线程调用：只置位并唤醒唯一 shutdown-request waiter，
  不执行 hook、不调用 `CoStop`、不等待自身。
- `CoWaitShutdownRequest(wait)` 使用持久标志，先请求后等待也不丢通知。同一时刻只允许一个活动 waiter，
  第二个返回 `Busy`；超时返回 `TimedOut` 且不清除请求、不销毁 App。
- 收到请求只证明请求到达：App 仍可能处于 `Running`。收口须由协程显式 `CoStop()`，成功后 `State() == Stopped`。

### 12.3 收口未竟与重试

- `CoStop` 在有限 deadline 内收口；耗尽时保留 owner、记录 `LifecycleFailure`，App 置 `StopIncomplete`，
  `PendingCleanup()` 可观察未完成项。
- 网络排空超时后，宿主已被同步 `Close`。再次以更晚期限 `CoStop` 时，若宿主已记录关闭原因，则跳过重复排空，
  直接推进对象收口（`host/CoApp.cc::_StopImpl`）。
- 每次调用的 deadline 独立计算，不续期前一次调用的 deadline。

### 12.4 已否决方案（历史决定，非实现契约）

早期草案曾提出让 framework 注册并驱动 native Engine 收口（`RunMainTask`、`MainNativeControl`、
`AdvanceNativeControl`、`MainEngineControl` 及专用 `bbtco_main` 入口）。该方案已被否决：
宿主入口是普通 `main` + 既有 `bbtco` 注册，`Engine::Create` / `Engine::Run` 由 infra 拥有，
framework 只经 `INetworkHost` seam 与 infra 公开 `Close` / `WaitHandlersDone` 语义完成网络收口。
实现中不存在上述类型。

### 12.5 目标设计中的对象命名（未实现）

契约 §12 之前的讨论提出把对象类型改名为 `CoService<...>`、`CoComponent`、`CoActor`，并用
`MakeCoObject<T>` 统一创建与观测计数。当前源码已使用 `CoService<...>` 与 `CoComponent`；`CoActor` 类型存在但
实例创建未接线。`MakeCoObject` 已用于 `CoApp` 与组件创建（`host/CoApp.cc`），`framework.CoApp` /
`framework.CoComponent` 为 CoObject 的 kind 标识。
