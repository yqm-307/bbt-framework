#pragma once
#include <bbt/coroutine/object/CoRuntimeOnly.hpp>
// framework v2 受管对象公共值层：id/generation/状态/info/handle/borrow/token。
//
// 本头只承载契约 §3 的值类型与句柄面；Registry/生命周期实现见
// internal/Registry.hpp，CoApp 装配面见 CoApp.hpp（v2 面经 Framework.hpp
// 透出）。所有 handle 只携带「Registry 弱引用 + id + generation + 失效位」，
// 不持有对象所有权；对象由对应 Registry 强持有并最终归 CoApp。

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

#include <bbt/framework/Address.hpp>
#include <bbt/framework/Result.hpp>

namespace bbt::framework {
namespace v2 { class CoApp; class CoComponent; }

/// @brief 受管对象的稳定逻辑标识；不是指针、fd 或协程 id。
using ObjectId = std::string;
/// @brief Actor 实例标识：(actor type, key)。
using ActorId = std::pair<std::string, std::string>;

/**
 * @brief 受管对象生命周期状态。
 *
 * 状态推进由 Registry 在锁内完成并向观察者派发 ObjectStateEvent；
 * InstallFailed/Failed 是可 Find 的终前状态，不是自动回滚。
 */
enum class ObjectState {
    Created,      ///< 已构造登记、未开始安装 hook。
    Installing,   ///< 登记完成、OnInstall/绑定装配进行中。
    Installed,    ///< OnInstall 成功；App 未 Running 时停在此态。
    Starting,     ///< OnStart 进行中。
    Running,      ///< 已启动，可派发/借用。
    Paused,       ///< OnPause 成功，拒新。
    Stopping,     ///< 收口进行中。
    Stopped,      ///< 收口完成。
    InstallFailed,///< OnInstall 或声明绑定失败；仍可 Find/卸载。
    Failed,       ///< OnStart/Pause/Resume 等运行期 hook 失败。
    StopIncomplete,   ///< Stop 超时或有 pending borrow；允许再次 Stop 推进。
    Uninstalling,     ///< 卸载进行中。
    UninstallIncomplete, ///< 卸载超时（borrow 未归零等）。
    Destroyed,    ///< 已从 Registry 移除并销毁。
    Invalidated,  ///< 观察端视角：句柄 generation 已失配。
    Unknown       ///< 无证据状态。
};

/// @brief 组件归属范围：App 共享、Service 私有或 Actor 实例私有。
enum class ComponentScope { App, Service, Actor };

/// @brief v2::CoApp 自身生命周期状态。
enum class AppState {
    Created,        ///< 已构造未启动。
    Starting,       ///< Start 进行中。
    Running,        ///< 已启动，接纳安装/绑定操作。
    Paused,         ///< Pause 成功，拒新。
    Stopping,       ///< Stop 收口进行中。
    StopIncomplete, ///< 收口未竟（pending cleanup），可再次 Stop 推进。
    Stopped         ///< 收口完成；Stop 后不能 Start。
};

/// @brief 受管对象类别。
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
 * 只有 Registry 与装配根 v2::CoApp 能构造/销毁受管对象；测试要构造受管类型，
 * 同样必须经 CoApp/Registry 走正式安装路径。私有构造显式提供（非
 * `= default`），使本类不是 aggregate，业务不能以
 * `detail::ConstructKey{}` 聚合初始化绕过受控创建。
 */
class ConstructKey {
private:
    ConstructKey() {}
    friend class Registry;
    friend class ::bbt::framework::v2::CoApp;
};

/**
 * @brief ComponentHandle<C>::Borrow 的类型擦除半身（Registry.cc 实现）。
 *
 * 返回的对象地址是 `v2::CoComponent*` 而非 `void*`：CoComponent 可能不是
 * 派生对象的首基类，经 CoComponent* 向 C* 的 static_cast 才由编译器完成
 * 正确的基类→派生指针调整（多继承安全）。
 *
 * @param object 组件对象句柄。
 * @param binding_generation 发放 handle 时记录的绑定代。
 * @param allow_installing_borrow 装配窗口借用标志：当前 HEAD 无调用方置真
 *        （Registry 内已支持 Installed 借用，等待生命周期节点接线）。
 * @return 成功返回 (binding lease, 对象地址)；lease 归还计数由 BindingLease 析构驱动。
 */
result<std::pair<std::shared_ptr<void>, v2::CoComponent*>> BorrowImpl(
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

/// @brief 最后一次错误的稳定摘要（不含原始异常文本）。
struct ObjectError {
    ErrorCode   code = ErrorCode::InternalError; ///< 稳定错误码。
    std::string phase;      ///< install/start/stop/uninstall/observe 等相位。
    std::string message;    ///< 已脱敏说明。
};

/**
 * @brief 对象身份与状态的 owning 值快照。
 *
 * ObjectHandle::Info() 的唯一返回面，不携带对象指针、引用或 shared_ptr。
 */
struct ObjectInfo {
    ObjectId        id;             ///< 稳定逻辑 id。
    std::uint64_t   generation = 0; ///< 对象代；句柄失配即失效。
    ObjectKind      kind = ObjectKind::App; ///< 对象类别。
    std::string     type_name;      ///< 静态类型名（服务名/actor type/组件 typeid 名）。
    std::string     name;           ///< 逻辑名（service/component name；actor 为 key）。
    ComponentScope  scope = ComponentScope::App; ///< 组件归属范围。
    ObjectId        owner_id;       ///< Service/Actor scope 组件的宿主 id；其余为空。
    ObjectState     state = ObjectState::Unknown; ///< 观察时刻状态。
    std::optional<ObjectError> last_error;    ///< 最近一次错误摘要。
    std::chrono::system_clock::time_point observed_at; ///< 快照时间。
};

/// @brief Actor 扩展快照：在 ObjectInfo 上附带 actor_id 与 mailbox 值。
struct ActorInfo : ObjectInfo {
    ActorId actor_id;   ///< Actor 实例标识。
    /// MailBox 为地址值（非对象引用）；生成期填充。
    MailBox mailbox;
};

/**
 * @brief 对象状态事件（owning 值），Observe 回调的唯一载荷。
 */
struct ObjectStateEvent {
    ObjectId        id;             ///< 事件对象 id。
    std::uint64_t   generation = 0; ///< 对象代。
    ObjectState     state = ObjectState::Unknown; ///< 新状态。
    std::uint64_t   revision = 0;   ///< 每对象单调递增事件序号。
    std::chrono::system_clock::time_point observed_at; ///< 事件时间。
    std::optional<ObjectError> error;   ///< 失败事件的错误摘要。
    std::optional<ActorInfo> actor;     ///< 仅 Actor 终态附带。
};

/// @brief 状态观察者回调签名；在锁外执行，抛异常被框架计数。
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

    /// @brief 对象稳定逻辑 id。
    const ObjectId& Id() const noexcept { return m_id; }
    /// @brief 对象代。
    std::uint64_t   Generation() const noexcept { return m_generation; }
    /**
     * @brief 对象是否仍登记且 generation 未变（观察值，不代表可解引用）。
     * @return 有效返回 true；Registry 已关闭或对象失效返回 false。
     */
    bool            IsValid() const noexcept;
    /// @brief 取对象身份与状态快照；Registry 关闭返回 Closed。
    result<ObjectInfo> Info() const;
    /// @brief 取 Service/Actor 地址值；Component/App 或 Registry 关闭返回错误。
    result<MailBox> GetMailBox() const;
    /// @brief 对象类别。
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
    friend class ::bbt::framework::v2::CoApp;
    friend result<std::pair<std::shared_ptr<void>, v2::CoComponent*>>
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
    ComponentBorrow(ComponentBorrow&& o) noexcept
        : m_binding(std::move(o.m_binding)), m_obj(o.m_obj) { o.m_obj = nullptr; }
    ComponentBorrow& operator=(ComponentBorrow&& o) noexcept {
        if (this != &o) { _Release(); m_binding = std::move(o.m_binding);
            m_obj = o.m_obj; o.m_obj = nullptr; }
        return *this;
    }
    ~ComponentBorrow() noexcept { _Release(); }

    ComponentBorrow(const ComponentBorrow&) = delete;
    ComponentBorrow& operator=(const ComponentBorrow&) = delete;

    /// @brief 被借用的组件引用；只在本 borrow 生存期内有效。
    C& Get() const noexcept { return *m_obj; }

private:
    ComponentBorrow(std::shared_ptr<void> binding_lease, C* obj)
        : m_binding(std::move(binding_lease)), m_obj(obj) {}

    void _Release() noexcept {
        m_binding.reset();
        m_obj = nullptr;
    }

    // 保持 binding lease（内部为 detail::BindingLease，析构把 borrow 计数
    // 归还 Registry）；对象本体由 Registry 持有，这里只放非拥有指针。
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

    /// @brief 组件对象的非拥有句柄。
    ObjectHandle Object() const { return m_object; }
    /// @brief 对象是否仍登记且 generation 未变。
    bool         IsValid() const noexcept { return m_object.IsValid(); }
    /**
     * @brief 借取组件实际引用 lease。
     * @return 成功返回 ComponentBorrow<C>；对象销毁/绑定失效/状态门拒/封门
     *         时返回 Closed 或 Overloaded。
     */
    result<ComponentBorrow<C>> Borrow() const {
        if (!bbt::coroutine::IsInCoroutineContext())
            return result<ComponentBorrow<C>>::err(MakeError(
                ErrorCode::InvalidContext, "Borrow requires coroutine context"));
        return _Borrow(false);
    }

private:
    ComponentHandle(ObjectHandle object, std::uint64_t binding_generation)
        : m_object(std::move(object)), m_binding_gen(binding_generation) {}

    result<ComponentBorrow<C>> _Borrow(bool allow_installing_borrow) const {
        auto r = detail::BorrowImpl(m_object, m_binding_gen,
                                    allow_installing_borrow);
        if (!r) return result<ComponentBorrow<C>>::err(std::move(r.error()));
        return detail::MakeComponentBorrow<C>(
            std::move(r.value().first), static_cast<C*>(r.value().second));
    }

    ObjectHandle   m_object;
    std::uint64_t  m_binding_gen = 0;

    template <class T>
    friend ComponentHandle<T> detail::MakeComponentHandle(
        ObjectHandle, std::uint64_t);
    friend class ::bbt::framework::v2::CoApp;   // 装配窗口借 Installed 依赖
};

namespace detail {
// ComponentHandle<C> 的唯一构造点：Registry/CoApp 在绑定成功后经本函数
// 供给（object handle + binding generation）。
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

    /// @brief 逻辑撤销订阅；在途回调不被强杀，Revoke 后不再新派发。
    void Revoke() noexcept;
    /// @brief 订阅是否仍生效（Revoke 或 observer/target 卸载后为 false）。
    bool IsActive() const noexcept;

private:
    ObserverToken(std::weak_ptr<detail::Registry> reg, std::uint64_t id)
        : m_reg(std::move(reg)), m_id(id) {}

    std::weak_ptr<detail::Registry> m_reg;
    std::uint64_t                   m_id = 0;

    friend class detail::Registry;
};

// ---- Options（契约 §4.1 的最小落地：只有本切片用到的 setter）----

/**
 * @brief 生命周期操作的时限选项。
 *
 * 未显式设置 deadline 时由 App 的 lifecycle budget 回填。
 */
class LifecycleOptions {
public:
    /// @brief 设置操作期限。
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
    /// @brief 未显式设置时返回空。
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
    /// @brief 设置归属范围（默认 App）。
    ComponentOptions& SetScope(ComponentScope s) { m_scope = s; return *this; }
    /// @brief 设置宿主对象（Service/Actor scope 必须；App scope 禁止）。
    ComponentOptions& SetOwner(ObjectHandle owner) {
        m_owner = std::move(owner); return *this;
    }
    /// @brief 归属范围。
    ComponentScope Scope() const noexcept { return m_scope; }
    /// @brief 宿主句柄（未设为空）。
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
    const std::type_info* component_type;   ///< &typeid(C)
    std::string           name;             ///< 组件逻辑名
};
} // namespace detail

/**
 * @brief Service 安装选项。
 *
 * Validate 要求 max_inflight 为有限正值。
 */
class ServiceOptions {
public:
    /// @brief 本服务安装/启动等生命周期步骤的时限选项。
    ServiceOptions& SetLifecycleOptions(LifecycleOptions o) {
        m_life = o; return *this;
    }
    /// @brief 最大在途请求数（必须 >0，Install 校验）。
    ServiceOptions& SetMaxInflight(std::size_t n) { m_max_inflight = n; return *this; }
    /// @brief 按 key 串行的最大 key 数。
    ServiceOptions& SetMaxSerialKeys(std::size_t n) { m_max_serial_keys = n; return *this; }
    /// @brief 串行队列容量。
    ServiceOptions& SetSerialQueueCapacity(std::size_t n) {
        m_serial_queue_capacity = n; return *this;
    }

    /**
     * @brief 声明本服务允许绑定组件 C（名 name）。
     * @tparam C 组件类型，应同时出现在 Service<Components...> 声明集合内。
     * @param name 目标组件逻辑名（指向已安装项）。
     */
    template <class C>
    ServiceOptions& BindComponent(std::string name) {
        m_component_bindings.push_back(
            detail::ComponentBindingDecl{&typeid(C), std::move(name)});
        return *this;
    }

    /// @brief 生命周期选项。
    const LifecycleOptions& Life() const noexcept { return m_life; }
    /// @brief 最大在途请求数。
    std::size_t MaxInflight() const noexcept { return m_max_inflight; }
    /// @brief 最大串行 key 数。
    std::size_t MaxSerialKeys() const noexcept { return m_max_serial_keys; }
    /// @brief 串行队列容量。
    std::size_t SerialQueueCapacity() const noexcept {
        return m_serial_queue_capacity;
    }
    /// @brief 已声明的组件绑定列表。
    const std::vector<detail::ComponentBindingDecl>& ComponentBindings()
        const noexcept { return m_component_bindings; }

    /// @brief 校验选项（max_inflight 必须为有限正值）。
    result<void> Validate() const;
private:
    LifecycleOptions m_life;
    std::size_t      m_max_inflight = 0;          // 0 = 未设；Install 校验 >0
    std::size_t      m_max_serial_keys = 0;
    std::size_t      m_serial_queue_capacity = 0;
    std::vector<detail::ComponentBindingDecl> m_component_bindings;
};

/**
 * @brief Actor 类型登记选项。
 *
 * Validate 要求 max_actors/mailbox_capacity/aggregate_queue_capacity 均为有限正值。
 */
class ActorOptions {
public:
    /// @brief 生命周期选项。
    ActorOptions& SetLifecycleOptions(LifecycleOptions o) {
        m_life = o; return *this;
    }
    /// @brief 本类型最大实例数（必须 >0）。
    ActorOptions& SetMaxActors(std::size_t n) { m_max_actors = n; return *this; }
    /// @brief 每实例邮箱容量（必须 >0）。
    ActorOptions& SetMailboxCapacity(std::size_t n) {
        m_mailbox_capacity = n; return *this;
    }
    /// @brief 聚合队列容量（必须 >0）。
    ActorOptions& SetAggregateQueueCapacity(std::size_t n) {
        m_aggregate_queue_capacity = n; return *this;
    }
    /**
     * @brief 声明本 Actor 类型允许绑定组件 C。
     * @tparam C 组件类型。
     * @param name 目标组件逻辑名。
     */
    template <class C>
    ActorOptions& BindComponent(std::string name) {
        m_component_bindings.push_back(
            detail::ComponentBindingDecl{&typeid(C), std::move(name)});
        return *this;
    }

    /// @brief 生命周期选项。
    const LifecycleOptions& Life() const noexcept { return m_life; }
    /// @brief 最大实例数。
    std::size_t MaxActors() const noexcept { return m_max_actors; }
    /// @brief 每实例邮箱容量。
    std::size_t MailboxCapacity() const noexcept { return m_mailbox_capacity; }
    /// @brief 聚合队列容量。
    std::size_t AggregateQueueCapacity() const noexcept {
        return m_aggregate_queue_capacity;
    }
    /// @brief 已声明的组件绑定列表。
    const std::vector<detail::ComponentBindingDecl>& ComponentBindings()
        const noexcept { return m_component_bindings; }

    /// @brief 校验选项（三项额度必须为有限正值）。
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
    /// @brief 设置创建期限。
    CreateOptions& SetDeadline(bbt::coroutine::Deadline d) {
        m_deadline = d; return *this;
    }
    /// @brief 已设期限；未设为空。
    std::optional<bbt::coroutine::Deadline> Deadline() const noexcept {
        return m_deadline;
    }
private:
    std::optional<bbt::coroutine::Deadline> m_deadline;
};

/**
 * @brief v2::CoApp 装配选项。
 *
 * Validate 要求 SetLimits 四项额度为有限正值、shutdown/lifecycle budget 为正。
 */
class AppOptions {
public:
    /// @brief 配置监听地址（本切片登记值，未接网络面）。
    AppOptions& Listen(std::string uri) { m_listen = std::move(uri); return *this; }
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
    /// @brief 关闭收口总预算（须为正）。
    AppOptions& SetShutdownBudget(std::chrono::milliseconds budget) {
        m_shutdown_budget = budget; return *this;
    }
    /// @brief 各生命周期步骤默认预算（须为正；未显式 SetDeadline 时使用）。
    AppOptions& SetLifecycleBudget(std::chrono::milliseconds budget) {
        m_lifecycle_budget = budget; return *this;
    }

    /// @brief 监听地址（未设为空）。
    const std::optional<std::string>& ListenUri() const noexcept { return m_listen; }
    /// @brief 最大受管对象数。
    std::size_t MaxObjects() const noexcept { return m_max_objects; }
    /// @brief 最大绑定边数。
    std::size_t MaxBindings() const noexcept { return m_max_bindings; }
    /// @brief 最大观察订阅数。
    std::size_t MaxObservers() const noexcept { return m_max_observers; }
    /// @brief 事件队列容量。
    std::size_t ObserverQueueCapacity() const noexcept {
        return m_observer_queue_capacity;
    }
    /// @brief 关闭收口预算。
    std::chrono::milliseconds ShutdownBudget() const noexcept {
        return m_shutdown_budget;
    }
    /// @brief 生命周期默认预算。
    std::chrono::milliseconds LifecycleBudget() const noexcept {
        return m_lifecycle_budget;
    }

    /// @brief 校验选项（额度与预算必须为有限正值）。
    result<void> Validate() const;
private:
    std::optional<std::string> m_listen;
    // 有限正值才放行；0 表示未设（Validate 拒绝）。
    std::size_t m_max_objects = 0;
    std::size_t m_max_bindings = 0;
    std::size_t m_max_observers = 0;
    std::size_t m_observer_queue_capacity = 0;
    std::chrono::milliseconds m_shutdown_budget{0};
    // 未设 LifecycleOptions::SetDeadline 时各 lifecycle 步使用的总预算。
    std::chrono::milliseconds m_lifecycle_budget{0};
};

// ---- 收尾/失败诊断（值快照，bounded ring 由 Registry 维持）----
/// @brief 收口未完成项快照（PendingCleanup 的元素）。
struct CleanupInfo {
    ObjectId      object_id;            ///< 对象 id。
    std::uint64_t generation = 0;       ///< 对象代。
    std::string   phase;                ///< 收口相位。
    std::size_t   pending_borrows = 0;  ///< 未归还的 borrow 数。
    std::size_t   pending_tasks = 0;    ///< 未完成任务数（本切片恒 0）。
    std::size_t   pending_callbacks = 0;///< 未派发回调数。
    ErrorCode     code = ErrorCode::InternalError; ///< 稳定错误码。
};

/// @brief 生命周期失败记录（LifecycleFailures 的元素；bounded ring）。
struct LifecycleFailure {
    ObjectId      object_id;            ///< 对象 id。
    std::uint64_t generation = 0;       ///< 对象代。
    std::string   operation;            ///< 操作名（install/start/stop 等）。
    std::string   phase;                ///< 失败相位。
    ErrorCode     code = ErrorCode::InternalError; ///< 稳定错误码。
    std::chrono::system_clock::time_point at;      ///< 发生时间。
    std::string   message;  ///< 本地脱敏说明。
};

/// @brief 绑定/解绑事件值快照（OnBindingChanged 的载荷）。
struct BindingEvent {
    ObjectId      subject_id;           ///< 绑定发起方 id。
    ObjectId      target_id;            ///< 目标组件 id。
    std::uint64_t generation = 0;       ///< 目标对象代。
    bool          bound = false;        ///< true=绑定发布，false=解绑完成。
    std::uint64_t revision = 0;         ///< 目标对象事件序号。
    std::chrono::system_clock::time_point observed_at; ///< 事件时间。
};

} // namespace bbt::framework
