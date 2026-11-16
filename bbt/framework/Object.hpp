#pragma once
#include <bbt/coroutine/object/CoRuntimeOnly.hpp>
/**
 * framework 受管对象公共值层：id/generation/状态/info/handle/borrow/token。
 *
 * 本头只承载契约 §3 的值类型与句柄面；Registry/生命周期实现见
 * internal/Registry.hpp，CoApp 装配面见 CoApp.hpp。所有 handle 只携带
 * 「Registry 弱引用 + id + generation + 失效位」，
 * 不持有对象所有权；对象由对应 Registry 强持有并最终归 CoApp。
 */

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <typeinfo>
#include <utility>
#include <vector>

#include <bbt/coroutine/sync/WaitTypes.hpp>

#include <bbt/infra/NetworkTypes.hpp>

#include <bbt/framework/Address.hpp>
#include <bbt/framework/DynamicRouteTable.hpp>
#include <bbt/framework/ExecutionPolicy.hpp>
#include <bbt/framework/Result.hpp>
#include <bbt/framework/RpcPolicy.hpp>

namespace bbt::framework {

/**
 * @brief 受管对象装配根；Registry 与受管对象的唯一创建/生命周期入口。
 */
class CoApp;
/**
 * @brief 能力组件基类（Component.hpp）。
 */
class CoComponent;

/**
 * @brief 受管对象的稳定逻辑标识；不是指针、fd 或协程 id。
 */
using ObjectId = std::string;
/**
 * @brief Actor 实例标识：(actor type, key)。
 */
using ActorId = std::pair<std::string, std::string>;

/**
 * @brief 受管对象生命周期状态。
 *
 * 状态推进由 Registry 在锁内完成并向观察者派发 ObjectStateEvent；
 * InstallFailed/Failed 是可 Find 的终前状态，不是自动回滚。
 */
enum class ObjectState {
    /**
     * 已构造登记、未开始安装 hook。
     */
    Created,
    /**
     * 登记完成、OnInstall/绑定装配进行中。
     */
    Installing,
    /**
     * OnInstall 成功；App 未 Running 时停在此态。
     */
    Installed,
    /**
     * OnStart 进行中。
     */
    Starting,
    /**
     * 已启动，可派发/借用。
     */
    Running,
    /**
     * OnPause 成功，拒新。
     */
    Paused,
    /**
     * 收口进行中。
     */
    Stopping,
    /**
     * 收口完成。
     */
    Stopped,
    /**
     * OnInstall 或声明绑定失败；仍可 Find/卸载。
     */
    InstallFailed,
    /**
     * OnStart/Pause/Resume 等运行期 hook 失败。
     */
    Failed,
    /**
     * Stop 超时或有 pending borrow；允许再次 Stop 推进。
     */
    StopIncomplete,
    /**
     * 卸载进行中。
     */
    Uninstalling,
    /**
     * 卸载超时（borrow 未归零等）。
     */
    UninstallIncomplete,
    /**
     * 已从 Registry 移除并销毁。
     */
    Destroyed,
    /**
     * 观察端视角：句柄 generation 已失配。
     */
    Invalidated,
    /**
     * 无证据状态。
     */
    Unknown
};

/**
 * @brief 组件归属范围：App 共享、Service 私有或 Actor 实例私有。
 */
enum class ComponentScope { App, Service, Actor };

/**
 * @brief CoApp 自身生命周期状态。
 */
enum class AppState {
    /**
     * 已构造未启动。
     */
    Created,
    /**
     * Start 进行中。
     */
    Starting,
    /**
     * 已启动，接纳安装/绑定操作。
     */
    Running,
    /**
     * Pause 成功，拒新。
     */
    Paused,
    /**
     * Stop 收口进行中。
     */
    Stopping,
    /**
     * 收口未竟（pending cleanup），可再次 Stop 推进。
     */
    StopIncomplete,
    /**
     * 收口完成；Stop 后不能 Start。
     */
    Stopped
};

/**
 * @brief 受管对象类别。
 */
enum class ObjectKind { App, Service, Actor, Component };

class ObjectHandle;
class ObjectInfo;
class ObserverToken;
class AppObjectAddress;
class MailBox;

template <class C> class ComponentBorrow;
template <class C> class ComponentHandle;

namespace detail {
class Registry;
struct RegistryEntry;
struct BindingLease;

/**
 * @brief 受控构造钥匙（passkey）。
 *
 * 只有 Registry 与装配根 CoApp 能构造/销毁受管对象；测试要构造受管类型，
 * 同样必须经 CoApp/Registry 走正式安装路径。私有构造显式提供（非
 * `= default`），使本类不是 aggregate，业务不能以
 * `detail::ConstructKey{}` 聚合初始化绕过受控创建。
 */
class ConstructKey {
private:
    ConstructKey() {}
    friend class Registry;
    friend class ::bbt::framework::CoApp;
};

/**
 * @brief ComponentHandle<C>::Borrow 的类型擦除半身（Registry.cc 实现）。
 *
 * 返回的对象地址是 `CoComponent*` 而非 `void*`：CoComponent 可能不是
 * 派生对象的首基类，经 CoComponent* 向 C* 的 static_cast 才由编译器完成
 * 正确的基类→派生指针调整（多继承安全）。
 *
 * @param object 组件对象句柄。
 * @param binding_generation 发放 handle 时记录的绑定代。
 * @param allow_installing_borrow 装配窗口借用标志：当前 HEAD 无调用方置真
 *        （Registry 内已支持 Installed 借用，等待生命周期节点接线）。
 * @return 成功返回 (binding lease, 对象地址)；lease 归还计数由 BindingLease 析构驱动。
 */
result<std::pair<std::shared_ptr<void>, CoComponent*>> BorrowImpl(
    const ObjectHandle& object, std::uint64_t binding_generation,
    bool allow_installing_borrow);

/**
 * @brief ComponentHandle/ComponentBorrow 的类型化构造点。
 *
 * Registry/CoApp 经本函数供给（业务不经由此路径构造句柄）。
 * @tparam C 组件类型。
 * @param lease 类型擦除的 BindingLease。
 * @param obj 组件对象指针（非拥有）。
 * @return 成功返回 ComponentBorrow<C>；lease 或 obj 为空返回 InternalError。
 */
template <class C>
result<ComponentBorrow<C>> MakeComponentBorrow(std::shared_ptr<void> lease,
                                               C* obj);
/**
 * @brief 构造携带绑定代的类型化组件句柄（Registry/CoApp 专用）。
 * @tparam C 组件类型。
 * @param object 组件对象句柄。
 * @param binding_generation 绑定代。
 * @return ComponentHandle<C> 值。
 */
template <class C>
ComponentHandle<C> MakeComponentHandle(ObjectHandle object,
                                       std::uint64_t binding_generation);
} // namespace detail

/**
 * @brief 最后一次错误的稳定摘要（不含原始异常文本）。
 */
struct ObjectError {
    /**
     * 稳定错误码。
     */
    ErrorCode code = ErrorCode::InternalError;
    /**
     * install/start/stop/uninstall/observe 等相位。
     */
    std::string phase;
    /**
     * 已脱敏说明。
     */
    std::string message;
};

/**
 * @brief 对象身份与状态的 owning 值快照。
 *
 * ObjectHandle::Info() 的唯一返回面，不携带对象指针、引用或 shared_ptr。
 */
struct ObjectInfo {
    /**
     * 稳定逻辑 id。
     */
    ObjectId id;
    /**
     * 对象代；句柄失配即失效。
     */
    std::uint64_t generation = 0;
    /**
     * 对象类别。
     */
    ObjectKind kind = ObjectKind::App;
    /**
     * 静态类型名（服务名/actor type/组件 typeid 名）。
     */
    std::string type_name;
    /**
     * 逻辑名（service/component name；actor 为 key）。
     */
    std::string name;
    /**
     * 组件归属范围。
     */
    ComponentScope scope = ComponentScope::App;
    /**
     * Service/Actor scope 组件的宿主 id；其余为空。
     */
    ObjectId owner_id;
    /**
     * 观察时刻状态。
     */
    ObjectState state = ObjectState::Unknown;
    /**
     * 最近一次错误摘要。
     */
    std::optional<ObjectError> last_error;
    /**
     * 快照时间。
     */
    std::chrono::system_clock::time_point observed_at;
};

/**
 * @brief Actor 扩展快照：在 ObjectInfo 上附带 actor_id 与 mailbox 值。
 */
struct ActorInfo : ObjectInfo {
    /**
     * Actor 实例标识。
     */
    ActorId actor_id;
    /**
     * MailBox 为地址值（非对象引用）；生成期填充。
     */
    MailBox mailbox;
};

/**
 * @brief 对象状态事件（owning 值），Observe 回调的唯一载荷。
 */
struct ObjectStateEvent {
    /**
     * 事件对象 id。
     */
    ObjectId id;
    /**
     * 对象代。
     */
    std::uint64_t generation = 0;
    /**
     * 新状态。
     */
    ObjectState state = ObjectState::Unknown;
    /**
     * 每对象单调递增事件序号。
     */
    std::uint64_t revision = 0;
    /**
     * 事件时间。
     */
    std::chrono::system_clock::time_point observed_at;
    /**
     * 失败事件的错误摘要。
     */
    std::optional<ObjectError> error;
    /**
     * 仅 Actor 终态附带。
     */
    std::optional<ActorInfo> actor;
};

/**
 * @brief 状态观察者回调签名；在锁外执行，抛异常被框架计数。
 */
using ObjectObserver = std::function<void(const ObjectStateEvent&)>;

/**
 * @brief 非拥有对象句柄。
 *
 * @par 职责
 * 携带「Registry 弱引用 + id + generation + 失效位」，供观察与寻址。
 *
 * @par 不负责
 * 不持有对象所有权；对象由 Registry 强持有并最终归 CoApp。
 * IsValid 只表示「对象仍登记且 generation 未变」，是观察值，不能据此随后
 * 解引用——Service/Actor 根本不解引用（无 borrow），Component 使用方必须经
 * ComponentHandle::Borrow() 拿实际 lease。
 */
class ObjectHandle {
public:
    ObjectHandle() = default;

    /**
     * @brief 对象稳定逻辑 id。
     */
    const ObjectId& Id() const noexcept { return m_id; }
    /**
     * @brief 对象代。
     */
    std::uint64_t   Generation() const noexcept { return m_generation; }
    /**
     * @brief 对象是否仍登记且 generation 未变（观察值，不代表可解引用）。
     * @return 有效返回 true；Registry 已关闭或对象失效返回 false。
     */
    bool            IsValid() const noexcept;
    /**
     * @brief 取对象身份与状态快照；Registry 关闭返回 Closed。
     */
    result<ObjectInfo> Info() const;
    /**
     * @brief 取 Service/Actor 地址值；Component/App 或 Registry 关闭返回错误。
     */
    result<MailBox> GetMailBox() const;
    /**
     * @brief 对象类别。
     */
    ObjectKind      Kind() const noexcept { return m_kind; }

private:
    ObjectHandle(std::weak_ptr<detail::Registry> reg, ObjectId id,
                 std::uint64_t generation, ObjectKind kind)
        : m_reg(std::move(reg)), m_id(std::move(id)),
          m_generation(generation), m_kind(kind) {}

    std::weak_ptr<detail::Registry> m_reg;
    ObjectId                        m_id;
    std::uint64_t                   m_generation = 0;
    ObjectKind                      m_kind = ObjectKind::App;

    friend class detail::Registry;
    friend class CoApp;
    friend class CoApp;
    friend result<std::pair<std::shared_ptr<void>, CoComponent*>>
        detail::BorrowImpl(const ObjectHandle&, std::uint64_t, bool);
};

/**
 * @brief Component 实际借用 lease。
 *
 * 移动语义，析构归还 borrow 计数。Get() 的引用只在本 borrow 生存期内有效；
 * 不得保存到别处、不得跨任务交出。
 * @tparam C 组件类型。
 */
template <class C>
class ComponentBorrow {
public:
    ComponentBorrow(ComponentBorrow&& o) noexcept;
    ComponentBorrow& operator=(ComponentBorrow&& o) noexcept;
    ~ComponentBorrow() noexcept { _Release(); }

    ComponentBorrow(const ComponentBorrow&) = delete;
    ComponentBorrow& operator=(const ComponentBorrow&) = delete;

    /**
     * @brief 被借用的组件引用；只在本 borrow 生存期内有效。
     */
    C& Get() const noexcept { return *m_obj; }

private:
    ComponentBorrow(std::shared_ptr<void> binding_lease, C* obj)
        : m_binding(std::move(binding_lease)), m_obj(obj) {}

    void _Release() noexcept;

    /**
     * 保持 binding lease（内部为 detail::BindingLease，析构把 borrow 计数
     * 归还 Registry）；对象本体由 Registry 持有，这里只放非拥有指针。
     */
    std::shared_ptr<void> m_binding;
    C*                    m_obj = nullptr;

    template <class T>
    friend result<ComponentBorrow<T>> detail::MakeComponentBorrow(
        std::shared_ptr<void>, T*);
};

namespace detail {
template <class C>
result<ComponentBorrow<C>> MakeComponentBorrow(std::shared_ptr<void> lease,
                                               C* obj) {
    if (!lease || obj == nullptr)
        return result<ComponentBorrow<C>>::err(MakeError(
            ErrorCode::InternalError, "MakeComponentBorrow: null lease/obj"));
    return result<ComponentBorrow<C>>::ok(
        ComponentBorrow<C>(std::move(lease), obj));
}
} // namespace detail

/**
 * @brief 类型化组件句柄。
 *
 * @par 职责
 * registry 身份 + generation + binding generation + 失效位。
 *
 * @par 不负责
 * 不拥有 C；Borrow() 在 Registry 同一临界区内校验对象/绑定状态后才发 lease。
 * @tparam C 组件类型。
 */
template <class C>
class ComponentHandle {
public:
    ComponentHandle() = default;

    /**
     * @brief 组件对象的非拥有句柄。
     */
    ObjectHandle Object() const { return m_object; }
    /**
     * @brief 对象是否仍登记且 generation 未变。
     */
    bool         IsValid() const noexcept { return m_object.IsValid(); }
    /**
     * @brief 借取组件实际引用 lease。
     * @return 成功返回 ComponentBorrow<C>；对象销毁/绑定失效/状态门拒/封门
     *         时返回 Closed 或 Overloaded。
     */
    result<ComponentBorrow<C>> Borrow() const;

private:
    ComponentHandle(ObjectHandle object, std::uint64_t binding_generation)
        : m_object(std::move(object)), m_binding_gen(binding_generation) {}

    result<ComponentBorrow<C>> _Borrow(bool allow_installing_borrow) const;

    ObjectHandle   m_object;
    std::uint64_t  m_binding_gen = 0;

    template <class T>
    friend ComponentHandle<T> detail::MakeComponentHandle(
        ObjectHandle, std::uint64_t);
    /**
     * 装配窗口借 Installed 依赖
     */
    friend class CoApp;
};

namespace detail {
/**
 * ComponentHandle<C> 的唯一构造点：Registry/CoApp 在绑定成功后经本函数
 * 供给（object handle + binding generation）。
 */
template <class C>
ComponentHandle<C> MakeComponentHandle(ObjectHandle object,
                                       std::uint64_t binding_generation) {
    return ComponentHandle<C>(std::move(object), binding_generation);
}
} // namespace detail

/**
 * @brief 观察订阅 token。
 *
 * 析构即逻辑 Revoke；不强杀在途回调（回调 lease 由 Registry 排空）。
 * Revoke 之后回调不再被新派发。移动语义。
 */
class ObserverToken {
public:
    ObserverToken() = default;
    ObserverToken(ObserverToken&& o) noexcept
        : m_reg(std::move(o.m_reg)), m_id(o.m_id) { o.m_id = 0; }
    ObserverToken& operator=(ObserverToken&& o) noexcept {
        if (this != &o) { Revoke(); m_reg = std::move(o.m_reg);
            m_id = o.m_id; o.m_id = 0; }
        return *this;
    }
    ~ObserverToken() noexcept { Revoke(); }

    ObserverToken(const ObserverToken&) = delete;
    ObserverToken& operator=(const ObserverToken&) = delete;

    /**
     * @brief 逻辑撤销订阅；在途回调不被强杀，Revoke 后不再新派发。
     */
    void Revoke() noexcept;
    /**
     * @brief 订阅是否仍生效（Revoke 或 observer/target 卸载后为 false）。
     */
    bool IsActive() const noexcept;

private:
    ObserverToken(std::weak_ptr<detail::Registry> reg, std::uint64_t id)
        : m_reg(std::move(reg)), m_id(id) {}

    std::weak_ptr<detail::Registry> m_reg;
    std::uint64_t                   m_id = 0;

    friend class detail::Registry;
};

/**
 * ---- Options（契约 §4.1 的最小落地：只有本切片用到的 setter）----
 */

/**
 * @brief 生命周期操作的时限选项。
 *
 * 未显式设置 deadline 时由 App 的 lifecycle budget 回填。
 */
class LifecycleOptions {
public:
    /**
     * @brief 设置操作期限。
     */
    LifecycleOptions& SetDeadline(bbt::coroutine::Deadline d) {
        m_deadline = d; return *this;
    }
    /**
     * @brief 已设 deadline 或 Deadline::max()。
     *
     * 调用方需要区分「未设」时用 RawDeadline()（Registry/CoApp 据此回填
     * App lifecycle budget）。
     */
    bbt::coroutine::Deadline Deadline() const noexcept {
        return m_deadline.value_or(bbt::coroutine::Deadline::max());
    }
    /**
     * @brief 未显式设置时返回空。
     */
    std::optional<bbt::coroutine::Deadline> RawDeadline() const noexcept {
        return m_deadline;
    }
private:
    std::optional<bbt::coroutine::Deadline> m_deadline;
};

/**
 * @brief 组件归属选项：scope 与（Service/Actor scope 必需的）owner。
 */
class ComponentOptions {
public:
    /**
     * @brief 设置归属范围（默认 App）。
     */
    ComponentOptions& SetScope(ComponentScope s) { m_scope = s; return *this; }
    /**
     * @brief 设置宿主对象（Service/Actor scope 必须；App scope 禁止）。
     */
    ComponentOptions& SetOwner(ObjectHandle owner) {
        m_owner = std::move(owner); return *this;
    }
    /**
     * @brief 归属范围。
     */
    ComponentScope Scope() const noexcept { return m_scope; }
    /**
     * @brief 宿主句柄（未设为空）。
     */
    const std::optional<ObjectHandle>& Owner() const noexcept { return m_owner; }
private:
    ComponentScope            m_scope = ComponentScope::App;
    std::optional<ObjectHandle> m_owner;
};

/**
 * @brief 服务级组件绑定声明（类型擦除值）。
 *
 * ServiceOptions::BindComponent<C>(name) 只声明「允许+指向已安装项」；
 * InstallService 展开时按声明做 BindComponent。
 */
namespace detail {
struct ComponentBindingDecl {
    /**
     * &typeid(C)
     */
    const std::type_info* component_type;
    /**
     * 组件逻辑名
     */
    std::string name;
};
} // namespace detail

/**
 * @brief 接收端有序流授权的服务级上限（OrderedIngress 的装配来源）。
 *
 * enabled=false 时三项上限必须为 0；enabled=true 时三项必须为有限正值，
 * 且只允许 ActorSerial 服务（执行策略见 ServiceOptions::SetExecution）。
 */
struct OrderedIngressOptions {
    bool enabled = false;
    /**
     * 最大授权流数。
     */
    std::size_t max_ordered_streams = 0;
    /**
     * 终态缓存条目上限。
     */
    std::size_t max_cached_results = 0;
    /**
     * 终态缓存字节上限。
     */
    std::size_t max_cached_result_bytes = 0;
};

/**
 * @brief Service 安装选项。
 *
 * Validate 落实执行策略矩阵：Concurrent 要求 max_inflight > 0 且不创建
 * 串行队列；ActorSerial 要求 max_inflight > 0、MaxSerialKeys > 0 且
 * SerialQueueCapacity > 0；有序入口只允许 ActorSerial，启用时三项上限
 * 均为有限正值，禁用时均为 0。
 */
class ServiceOptions {
public:
    /**
     * @brief 本服务安装/启动等生命周期步骤的时限选项。
     */
    ServiceOptions& SetLifecycleOptions(LifecycleOptions o) {
        m_life = o; return *this;
    }
    /**
     * @brief 执行策略（默认 Concurrent）。
     */
    ServiceOptions& SetExecution(ExecutionPolicy p) { m_execution = p; return *this; }
    /**
     * @brief 最大在途请求数（必须 >0，Install 校验）。
     */
    ServiceOptions& SetMaxInflight(std::size_t n) { m_max_inflight = n; return *this; }
    /**
     * @brief 按 key 串行的最大 key 数（ActorSerial 必须为有限正值）。
     */
    ServiceOptions& SetMaxSerialKeys(std::size_t n) { m_max_serial_keys = n; return *this; }
    /**
     * @brief 串行队列容量（ActorSerial 必须为有限正值）。
     */
    ServiceOptions& SetSerialQueueCapacity(std::size_t n) {
        m_serial_queue_capacity = n; return *this;
    }
    /**
     * @brief 接收端有序流授权上限（只允许 ActorSerial）。
     */
    ServiceOptions& SetOrderedIngress(OrderedIngressOptions o) {
        m_ordered_ingress = o; return *this;
    }

    /**
     * @brief 声明本服务允许绑定组件 C（名 name）。
     * @tparam C 组件类型，应同时出现在 Service<Components...> 声明集合内。
     * @param name 目标组件逻辑名（指向已安装项）。
     */
    template <class C>
    ServiceOptions& BindComponent(std::string name);

    /**
     * @brief 生命周期选项。
     */
    const LifecycleOptions& Life() const noexcept { return m_life; }
    /**
     * @brief 执行策略。
     */
    ExecutionPolicy Execution() const noexcept { return m_execution; }
    /**
     * @brief 最大在途请求数。
     */
    std::size_t MaxInflight() const noexcept { return m_max_inflight; }
    /**
     * @brief 最大串行 key 数。
     */
    std::size_t MaxSerialKeys() const noexcept { return m_max_serial_keys; }
    /**
     * @brief 串行队列容量。
     */
    std::size_t SerialQueueCapacity() const noexcept {
        return m_serial_queue_capacity;
    }
    /**
     * @brief 有序入口授权上限。
     */
    const OrderedIngressOptions& OrderedIngress() const noexcept {
        return m_ordered_ingress;
    }
    /**
     * @brief 已声明的组件绑定列表。
     */
    const std::vector<detail::ComponentBindingDecl>& ComponentBindings()
        const noexcept { return m_component_bindings; }

    /**
     * @brief 校验选项（执行策略矩阵，见类注释）。
     */
    result<void> Validate() const;
private:
    LifecycleOptions m_life;
    ExecutionPolicy  m_execution = ExecutionPolicy::Concurrent;
    /**
     * 0 = 未设；Install 校验 >0
     */
    std::size_t m_max_inflight = 0;
    std::size_t      m_max_serial_keys = 0;
    std::size_t      m_serial_queue_capacity = 0;
    OrderedIngressOptions m_ordered_ingress;
    std::vector<detail::ComponentBindingDecl> m_component_bindings;
};

/**
 * @brief Actor 类型登记选项。
 *
 * Validate 要求 max_actors/mailbox_capacity/aggregate_queue_capacity 均为有限正值。
 */
class ActorOptions {
public:
    /**
     * @brief 生命周期选项。
     */
    ActorOptions& SetLifecycleOptions(LifecycleOptions o) {
        m_life = o; return *this;
    }
    /**
     * @brief 本类型最大实例数（必须 >0）。
     */
    ActorOptions& SetMaxActors(std::size_t n) { m_max_actors = n; return *this; }
    /**
     * @brief 每实例邮箱容量（必须 >0）。
     */
    ActorOptions& SetMailboxCapacity(std::size_t n) {
        m_mailbox_capacity = n; return *this;
    }
    /**
     * @brief 聚合队列容量（必须 >0）。
     */
    ActorOptions& SetAggregateQueueCapacity(std::size_t n) {
        m_aggregate_queue_capacity = n; return *this;
    }
    /**
     * @brief 声明本 Actor 类型允许绑定组件 C。
     * @tparam C 组件类型。
     * @param name 目标组件逻辑名。
     */
    template <class C>
    ActorOptions& BindComponent(std::string name);

    /**
     * @brief 生命周期选项。
     */
    const LifecycleOptions& Life() const noexcept { return m_life; }
    /**
     * @brief 最大实例数。
     */
    std::size_t MaxActors() const noexcept { return m_max_actors; }
    /**
     * @brief 每实例邮箱容量。
     */
    std::size_t MailboxCapacity() const noexcept { return m_mailbox_capacity; }
    /**
     * @brief 聚合队列容量。
     */
    std::size_t AggregateQueueCapacity() const noexcept {
        return m_aggregate_queue_capacity;
    }
    /**
     * @brief 已声明的组件绑定列表。
     */
    const std::vector<detail::ComponentBindingDecl>& ComponentBindings()
        const noexcept { return m_component_bindings; }

    /**
     * @brief 校验选项（三项额度必须为有限正值）。
     */
    result<void> Validate() const;
private:
    LifecycleOptions m_life;
    std::size_t      m_max_actors = 0;
    std::size_t      m_mailbox_capacity = 0;
    std::size_t      m_aggregate_queue_capacity = 0;
    std::vector<detail::ComponentBindingDecl> m_component_bindings;
};

/**
 * @brief 实例创建选项（本切片最小形态：仅 deadline）。
 */
class CreateOptions {
public:
    /**
     * @brief 设置创建期限。
     */
    CreateOptions& SetDeadline(bbt::coroutine::Deadline d) {
        m_deadline = d; return *this;
    }
    /**
     * @brief 已设期限；未设为空。
     */
    std::optional<bbt::coroutine::Deadline> Deadline() const noexcept {
        return m_deadline;
    }
private:
    std::optional<bbt::coroutine::Deadline> m_deadline;
};

/**
 * @brief 显式配置的静态出站路由（服务名 → "host:port" endpoint）。
 */
struct StaticRoute {
    std::string service_name;
    std::string address;
};

/**
 * @brief CoApp 装配选项。
 *
 * Validate 要求 SetLimits 四项额度为有限正值、shutdown/lifecycle budget
 * 为正。网络面字段（监听/路由/身份/策略/限额）在 CoStart 时校验：
 * 未配置监听则本 App 不装配网络宿主（纯受管对象面）；配置了监听则
 * listen host、local_identity、静态路由与 network_limits 全部校验。
 */
class AppOptions {
public:
    /**
     * @brief 配置监听地址："host:port"（port 0 = 随机端口）。格式非法时
     *        登记为空 host，CoStart 校验拒绝；未配置则不装配网络宿主。
     */
    AppOptions& Listen(std::string uri) {
        m_listen = std::move(uri);
        m_listen_addr = bbt::infra::ListenAddress{};
        if (!m_listen) return *this;
        const auto colon = m_listen->rfind(':');
        if (colon != std::string::npos) {
            const std::string host = m_listen->substr(0, colon);
            const std::string port = m_listen->substr(colon + 1);
            bool numeric = !port.empty();
            for (char c : port) numeric = numeric && (c >= '0' && c <= '9');
            unsigned value = 0;
            if (numeric) {
                const unsigned long parsed = std::stoul(port);
                value = static_cast<unsigned>(parsed);
                numeric = parsed <= 0xFFFFu;
            }
            if (!host.empty() && numeric) {
                m_listen_addr.host = host;
                m_listen_addr.port = static_cast<std::uint16_t>(value);
            }
        }
        return *this;
    }
    /**
     * @brief 设置受管对象/绑定/观察者/事件队列额度（四项均须 >0）。
     * @param max_objects 最大受管对象数。
     * @param max_bindings 最大绑定边数。
     * @param max_observers 最大观察订阅数。
     * @param observer_queue_capacity 事件队列容量。
     */
    AppOptions& SetLimits(std::size_t max_objects, std::size_t max_bindings,
                          std::size_t max_observers,
                          std::size_t observer_queue_capacity) {
        m_max_objects = max_objects; m_max_bindings = max_bindings;
        m_max_observers = max_observers;
        m_observer_queue_capacity = observer_queue_capacity;
        return *this;
    }
    /**
     * @brief 关闭收口总预算（须为正）。
     */
    AppOptions& SetShutdownBudget(std::chrono::milliseconds budget) {
        m_shutdown_budget = budget; return *this;
    }
    /**
     * @brief 各生命周期步骤默认预算（须为正；未显式 SetDeadline 时使用）。
     */
    AppOptions& SetLifecycleBudget(std::chrono::milliseconds budget) {
        m_lifecycle_budget = budget; return *this;
    }
    /**
     * @brief 配置静态出站路由（重复服务名以首次为准，CoStart 校验拒绝重复）。
     */
    AppOptions& AddRoute(std::string service, std::string endpoint) {
        m_static_routes.push_back(StaticRoute{std::move(service),
                                              std::move(endpoint)});
        return *this;
    }
    /**
     * @brief 会话身份（必填当配置了监听；trusted-network identity 不是认证凭据）。
     */
    AppOptions& SetLocalIdentity(std::string identity) {
        m_local_identity = std::move(identity); return *this;
    }
    /**
     * @brief 出站策略；nullptr 使用默认策略（App 持有至析构）。
     */
    AppOptions& SetRpcPolicy(std::shared_ptr<RpcPolicy> policy) {
        m_rpc_policy = std::move(policy); return *this;
    }
    /**
     * @brief 可选动态路由表：发现提供方发布快照，出站按逻辑服务名解析；
     *        已配置的服务名优先于静态路由且不回退。
     */
    AppOptions& SetDynamicRoutes(std::shared_ptr<DynamicRouteTable> table) {
        m_dynamic_routes = std::move(table); return *this;
    }
    /**
     * @brief 网络限额（帧上限/正文上限等；CoStart 校验）。
     */
    AppOptions& SetNetworkLimits(bbt::infra::NetworkLimits limits) {
        m_network_limits = std::move(limits); return *this;
    }

    /**
     * @brief 监听地址串（未设为空）。
     */
    const std::optional<std::string>& ListenUri() const noexcept { return m_listen; }
    /**
     * @brief 解析后的监听地址（host 非空即合法；port 0 = 随机端口）。
     */
    const bbt::infra::ListenAddress& ListenAddr() const noexcept {
        return m_listen_addr;
    }
    /**
     * @brief 最大受管对象数。
     */
    std::size_t MaxObjects() const noexcept { return m_max_objects; }
    /**
     * @brief 最大绑定边数。
     */
    std::size_t MaxBindings() const noexcept { return m_max_bindings; }
    /**
     * @brief 最大观察订阅数。
     */
    std::size_t MaxObservers() const noexcept { return m_max_observers; }
    /**
     * @brief 事件队列容量。
     */
    std::size_t ObserverQueueCapacity() const noexcept {
        return m_observer_queue_capacity;
    }
    /**
     * @brief 关闭收口预算。
     */
    std::chrono::milliseconds ShutdownBudget() const noexcept {
        return m_shutdown_budget;
    }
    /**
     * @brief 生命周期默认预算。
     */
    std::chrono::milliseconds LifecycleBudget() const noexcept {
        return m_lifecycle_budget;
    }
    /**
     * @brief 静态出站路由。
     */
    const std::vector<StaticRoute>& StaticRoutes() const noexcept {
        return m_static_routes;
    }
    /**
     * @brief 会话身份（未设为空）。
     */
    const std::string& LocalIdentity() const noexcept { return m_local_identity; }
    /**
     * @brief 出站策略（未设为空）。
     */
    const std::shared_ptr<RpcPolicy>& RpcPolicyPtr() const noexcept { return m_rpc_policy; }
    /**
     * @brief 动态路由表（未设为空）。
     */
    const std::shared_ptr<DynamicRouteTable>& DynamicRoutes() const noexcept {
        return m_dynamic_routes;
    }
    /**
     * @brief 网络限额。
     */
    const bbt::infra::NetworkLimits& NetworkLimits() const noexcept {
        return m_network_limits;
    }

    /**
     * @brief 校验选项（额度与预算必须为有限正值）。
     */
    result<void> Validate() const;
private:
    std::optional<std::string> m_listen;
    bbt::infra::ListenAddress  m_listen_addr;
    std::vector<StaticRoute>   m_static_routes;
    std::string                m_local_identity;
    std::shared_ptr<RpcPolicy> m_rpc_policy;
    std::shared_ptr<DynamicRouteTable> m_dynamic_routes;
    bbt::infra::NetworkLimits  m_network_limits;
    /**
     * 有限正值才放行；0 表示未设（Validate 拒绝）。
     */
    std::size_t m_max_objects = 0;
    std::size_t m_max_bindings = 0;
    std::size_t m_max_observers = 0;
    std::size_t m_observer_queue_capacity = 0;
    std::chrono::milliseconds m_shutdown_budget{0};
    /**
     * 未设 LifecycleOptions::SetDeadline 时各 lifecycle 步使用的总预算。
     */
    std::chrono::milliseconds m_lifecycle_budget{0};
};

/**
 * ---- 收尾/失败诊断（值快照，bounded ring 由 Registry 维持）----
 * @brief 收口未完成项快照（PendingCleanup 的元素）。
 */
struct CleanupInfo {
    /**
     * 对象 id。
     */
    ObjectId object_id;
    /**
     * 对象代。
     */
    std::uint64_t generation = 0;
    /**
     * 收口相位。
     */
    std::string phase;
    /**
     * 未归还的 borrow 数。
     */
    std::size_t pending_borrows = 0;
    /**
     * 未完成任务数（本切片恒 0）。
     */
    std::size_t pending_tasks = 0;
    /**
     * 未派发回调数。
     */
    std::size_t pending_callbacks = 0;
    /**
     * 稳定错误码。
     */
    ErrorCode code = ErrorCode::InternalError;
};

/**
 * @brief 生命周期失败记录（LifecycleFailures 的元素；bounded ring）。
 */
struct LifecycleFailure {
    /**
     * 对象 id。
     */
    ObjectId object_id;
    /**
     * 对象代。
     */
    std::uint64_t generation = 0;
    /**
     * 操作名（install/start/stop 等）。
     */
    std::string operation;
    /**
     * 失败相位。
     */
    std::string phase;
    /**
     * 稳定错误码。
     */
    ErrorCode code = ErrorCode::InternalError;
    /**
     * 发生时间。
     */
    std::chrono::system_clock::time_point at;
    /**
     * 本地脱敏说明。
     */
    std::string message;
};

/**
 * @brief 绑定/解绑事件值快照（OnBindingChanged 的载荷）。
 */
struct BindingEvent {
    /**
     * 绑定发起方 id。
     */
    ObjectId subject_id;
    /**
     * 目标组件 id。
     */
    ObjectId target_id;
    /**
     * 目标对象代。
     */
    std::uint64_t generation = 0;
    /**
     * true=绑定发布，false=解绑完成。
     */
    bool bound = false;
    /**
     * 目标对象事件序号。
     */
    std::uint64_t revision = 0;
    /**
     * 事件时间。
     */
    std::chrono::system_clock::time_point observed_at;
};

template <class C>
ComponentBorrow<C>::ComponentBorrow(ComponentBorrow&& o) noexcept
    : m_binding(std::move(o.m_binding)), m_obj(o.m_obj) {
    o.m_obj = nullptr;
}

template <class C>
ComponentBorrow<C>& ComponentBorrow<C>::operator=(ComponentBorrow&& o) noexcept {
    if (this != &o) {
        _Release();
        m_binding = std::move(o.m_binding);
        m_obj = o.m_obj;
        o.m_obj = nullptr;
    }
    return *this;
}

template <class C>
void ComponentBorrow<C>::_Release() noexcept {
    m_binding.reset();
    m_obj = nullptr;
}

template <class C>
result<ComponentBorrow<C>> ComponentHandle<C>::Borrow() const {
    if (!bbt::coroutine::IsInCoroutineContext())
        return result<ComponentBorrow<C>>::err(MakeError(
            ErrorCode::InvalidContext, "Borrow requires coroutine context"));
    return _Borrow(false);
}

template <class C>
result<ComponentBorrow<C>> ComponentHandle<C>::_Borrow(bool allow_installing_borrow) const {
    auto r = detail::BorrowImpl(m_object, m_binding_gen,
                                allow_installing_borrow);
    if (!r)
        return result<ComponentBorrow<C>>::err(std::move(r.error()));
    return detail::MakeComponentBorrow<C>(
        std::move(r.value().first), static_cast<C*>(r.value().second));
}

template <class C>
ServiceOptions& ServiceOptions::BindComponent(std::string name) {
    m_component_bindings.push_back(
        detail::ComponentBindingDecl{&typeid(C), std::move(name)});
    return *this;
}

template <class C>
ActorOptions& ActorOptions::BindComponent(std::string name) {
    m_component_bindings.push_back(
        detail::ComponentBindingDecl{&typeid(C), std::move(name)});
    return *this;
}

} // namespace bbt::framework
