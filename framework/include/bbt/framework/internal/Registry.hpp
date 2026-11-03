#pragma once
// framework v2 受管对象注册表（机器面，internal/）。
//
// 单一 Registry 实体统一簿记 Service/Actor/Component 三种受管对象：
// 每条目强持有对象（类型化 deleter）+ generation + 状态 + binding/观察者/
// lease 簿记。业务不可构造、不可 delete——唯一创建/销毁路径是 Registry
// 的类型化入口（ConstructKey + protected/private 析构 + Registry 友元）。
//
// 线程纪律：条目字段全部在 m_mtx 下读写；用户 hook（OnInstall/OnStart/
// OnStop/OnUninstall/OnCreate/OnDestroy/OnBindingChanged）与观察者回调一律
// 在锁外执行——状态推进在锁内完成、快照发布后在锁外调用，锁内不等待业务。
// 同一对象一次只允许一个进行中的控制面操作（op_in_progress 互斥），并发
// Install/Start/Stop/Uninstall/Unbind 冲突返回 Busy。

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <vector>

#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/coroutine/sync/WaitTypes.hpp>

#include <bbt/framework/Address.hpp>
#include <bbt/framework/Object.hpp>
#include <bbt/framework/Result.hpp>

namespace bbt::framework {

namespace v2 {
class CoServiceBase;
class CoComponent;
class CoActor;
class CoApp;
} // namespace v2

namespace detail {

// 对象簿记（Registry 内部）。对象经 shared_ptr<void> + 类型化 deleter
// 强持有；Registry 不向业务交出 shared_ptr<T>。
struct Binding {
    ObjectId      subject_id;
    std::uint64_t generation = 0;
    bool          active = false;      // gate 开（接受新 borrow）
    bool          retiring = false;    // Unbind 已封口、等本 binding borrow 归零
    std::size_t   borrows = 0;         // 本 binding 的在途借用数（Unbind 只等自身）
};

struct RegistryEntry {
    ObjectId     id;
    std::uint64_t generation = 0;
    ObjectKind   kind = ObjectKind::Service;
    std::string  type_name;      // Service：service name；Actor：actor type；Component：静态类型名
    std::string  name;           // 逻辑名（service/component name；actor key）
    ComponentScope scope = ComponentScope::App;
    ObjectId     owner_id;       // Service/Actor scope 组件的宿主 id
    ObjectState  state = ObjectState::Created;
    std::optional<ObjectError> last_error;
    std::uint64_t revision = 0;          // 状态事件序号

    std::shared_ptr<void> object;        // 强持有（类型化 deleter）
    const std::type_info* object_type = nullptr;

    // 组件边簿记：每条 binding 的 generation 单调递增，Unbind 推进使旧
    // ComponentHandle 失效；borrow 计数按组件本体合计（共享组件所有
    // binding 的借用进同一计数）。
    std::vector<Binding> bindings;
    std::size_t   component_borrows = 0;

    // Actor 专属：该实例在邮箱层的执行租约（本切片恒 0，消息分发后续接入）。
    std::size_t   exec_leases = 0;

    // 生命周期 hook 执行簿记：受管协程内仍有 hook 未达真实终态时 >0；
    // 超时返回不预扣——owner 保留至 hook 真实完成，收口前不允许销毁。
    std::size_t   hooks_in_flight = 0;
    // 协程侧的「在途 hook 归零」等待位：宿主路径等 m_cv，Co* 入口不能
    // 在 worker 上 std::cv 阻塞，改在本 waiter 上挂起；任务体结束经
    // NoteHookSettled 在计数归零时 Notify。懒创建（仅协程等待时分配）。
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> hook_settled_waiter;

    bool          op_in_progress = false;  // 控制面互斥
    bool          gate_closed = false;     // 拒新 borrow/新 dispatch
    std::string   destroy_phase;           // 诊断：收口相位
};

struct ObserverRecord {
    std::uint64_t   id = 0;
    ObjectId        observer_id;
    std::uint64_t   observer_generation = 0;
    ObjectId        target_id;
    std::uint64_t   target_generation = 0;
    ObjectObserver  callback;
    bool            active = true;
    // 在途 callback lease：DispatchEvents 在锁内 ++、回调归还后 --。
    // 「查 active + ++in_flight」与 Revoke 的「标 inactive + 等归零 +
    // 抹除」同受 m_mtx 串行化，无锁外窗口；回调执行期记录不可抹除。
    std::size_t     in_flight = 0;
};

// 组件借用 lease：析构把 borrow 计数归还 Registry 并唤醒等待者。
// ComponentBorrow<C> 的 m_binding 持有本对象（shared_ptr<void> 擦除）。
struct BindingLease {
    std::weak_ptr<Registry> reg_w;
    std::shared_ptr<void> owner; // borrow 持有实际对象版本，不保活 Registry。
    ObjectId     component_id;
    ObjectId     subject_id;
    std::uint64_t binding_generation = 0;
    ~BindingLease();
};

class Registry : public std::enable_shared_from_this<Registry> {
public:
    struct Limits {
        std::size_t max_objects = 0;
        std::size_t max_bindings = 0;
        std::size_t max_observers = 0;
        std::size_t observer_queue_capacity = 0;
        std::chrono::milliseconds lifecycle_budget{0};
        std::chrono::milliseconds shutdown_budget{0};
    };

    explicit Registry(Limits limits) : m_limits(limits) {}
    ~Registry() = default;
    Registry(const Registry&) = delete;
    Registry& operator=(const Registry&) = delete;

    const Limits& GetLimits() const noexcept { return m_limits; }
    bbt::coroutine::Deadline DefaultDeadline() const {
        return std::chrono::steady_clock::now() + m_limits.lifecycle_budget;
    }
    bbt::coroutine::Deadline ResolveDeadline(
        const LifecycleOptions& life) const {
        if (auto d = life.RawDeadline()) return *d;
        return DefaultDeadline();
    }
    bbt::coroutine::Deadline DeadlineFromNow() const {
        return std::chrono::steady_clock::now() + m_limits.lifecycle_budget;
    }

    std::mutex& Mutex() noexcept { return m_mtx; }
    // hook 完成通知：hook 任务结束（正常或异常终态）时唤醒所有等待者
    // （borrow 归零通知共用 m_cv）。调用方已持 m_mtx 时经本方法等待：
    // 谓词在锁内复查，超时返回 false。宿主线程专用——协程内调用会阻塞
    // worker（协程路径不得经此等待，用 Busy 语义）。
    template <class Pred>
    bool MutexCvWait(std::unique_lock<std::mutex>& lk,
                     bbt::coroutine::Deadline deadline, Pred&& pred) {
        return m_cv.wait_until(lk, deadline, std::forward<Pred>(pred));
    }

    // 锁内稳定迭代：返回全部条目的 ObjectId（指针/引用会随 erase 失效，
    // 跨锁使用只拿 id）。
    std::vector<ObjectId> ListIdsLocked() const;
    // 按 kind+state 过滤的 id 列表（锁内快照）。
    std::vector<ObjectId> ListIdsLocked(ObjectKind kind,
                                        ObjectState state) const;
    // 按 kind 过滤的 id 列表（不筛选 state）。
    std::vector<ObjectId> ListIdsByKindLocked(ObjectKind kind) const;

    // ---- 条目生命周期（子类模板调用）----
    // 锁内登记新条目（id/generation 由 Registry 分配）。object 必须先构造
    // 完成（含类型化 deleter）；entry 初始状态 Installing（Actor 由调用方
    // 自行覆写语义）。
    result<RegistryEntry*> CreateEntryLocked(
        ObjectKind kind, std::string type_name, std::string name,
        ComponentScope scope, ObjectId owner_id,
        const std::type_info& object_type, const std::shared_ptr<void>& object);

    RegistryEntry* FindEntryLocked(std::string_view id);
    const RegistryEntry* FindEntryLocked(std::string_view id) const;
    // Service/Actor：按 (kind, type_name, name) 查；Component 按
    // (scope, owner_id, type, name) 查。
    RegistryEntry* FindComponentLocked(ComponentScope scope,
                                     const ObjectId& owner_id,
                                     const std::type_info& type,
                                     std::string_view name);
    RegistryEntry* FindActorLocked(const std::string& type,
                                   const std::string& key);

    // 统一状态推进（锁内）：记录 revision/last_error 并收集目标观察者回调
    // 快照；返回待派发事件列表（锁外由 DispatchEvents 执行）。
    std::vector<std::pair<ObserverRecord, ObjectStateEvent>>
        SetStateLocked(RegistryEntry& e, ObjectState s,
                       std::optional<ObjectError> err = std::nullopt);
    // 等价便捷形：按 id 查条目并 SetStateLocked；未找到时返回空。
    std::vector<std::pair<ObserverRecord, ObjectStateEvent>>
        SetStateAndGetEvents(const ObjectId& id, ObjectState s,
                             std::optional<ObjectError> err = std::nullopt);
    void DispatchEvents(
        std::vector<std::pair<ObserverRecord, ObjectStateEvent>> events);

    // 控制面互斥：进入/退出一个对象的进行中的转换；进行中返回 Busy。
    bool BeginOpLocked(RegistryEntry& e);
    void EndOpLocked(RegistryEntry& e);

    // ---- 观察 ----
    result<ObserverToken> Observe(const ObjectHandle& observer,
                                  const ObjectHandle& target,
                                  ObjectObserver callback);
    // 逻辑撤销：标记 active=false，并阻塞至已派发的在途 callback lease 归零
    // 后抹除记录与闭包（ lease 由 DispatchEvents 持锁归还）。
    void RevokeObserver(std::uint64_t token_id) noexcept;
    bool ObserverActive(std::uint64_t token_id) const noexcept;
    // observer 对象卸载时撤销其全部 token。Locked 版供已持锁路径调用：
    // 只标记 inactive 并启动排空；真正抹除须由未持锁方继续
    // DrainInactiveObservers 完成。
    void RevokeObserversOf(std::string_view observer_id);
    void RevokeObserversOfLocked(std::string_view observer_id);
    // 排空并抹除全部 inactive observer 记录（等其 in_flight 归零；
    // 调用方不得持 m_mtx）。observer 对象卸载路径在对象析构前调用，
    // 保证其闭包先于 owner 释放。
    void DrainInactiveObservers() noexcept;

    // ---- handle 观察面 ----
    // 句柄是否属于本 Registry（跨 App/Registry 句柄拒绝，不查对象是否存在）。
    bool OwnsHandle(const ObjectHandle& h) const noexcept;
    result<ObjectInfo> Info(const ObjectHandle& h) const;
    bool HandleValid(const ObjectHandle& h) const noexcept;
    result<MailBox> MailBoxOf(const ObjectHandle& h) const;
    // Service/Actor 地址：Service → AppObjectAddress::Service(name)；
    // Actor → AppObjectAddress::Actor(type, key)。Component/App → InvalidArgument。
    static result<AppObjectAddress> AddressOf(const RegistryEntry& e);

    // ---- 组件 binding / borrow ----
    // subject_id 为绑定发起方（Service/Actor/其他组件的 id；App scope 由
    // CoApp 直接绑定给 service 时 subject 为 service id）。subject_entry
    // 在同一临界区提供 subject gate 判定——已封口/销毁/进行中控制面操作
    // 的 subject 不得再接纳新 binding（销毁只擦除 retiring 边，新增
    // active 边会留下指向已销毁 subject 的悬挂关系）。
    result<void> BindLocked(RegistryEntry& component_entry,
                            const RegistryEntry& subject_entry,
                            const ObjectId& subject_id,
                            std::uint64_t* out_generation);
    // 封该 binding 新 borrow（generation 推进使旧 handle 失效）；
    // 若该 binding 无在途 borrow → 立即移除边，返回 ok。仍有 borrow →
    // 标记 retiring，等本 binding borrow 归零后完成（waiter 驱动）。
    // out_generation 回填（新的）binding generation，供按边等待。
    result<void> BeginUnbindLocked(RegistryEntry& component_entry,
                                   const ObjectId& subject_id,
                                   std::uint64_t* out_generation);
    // BindingLease 析构归还：按 (component, subject, gen) 减计数并在
    // retiring 且归零时移除边；唤醒等待者。
    void ReleaseBorrow(const ObjectId& component_id,
                       const ObjectId& subject_id,
                       std::uint64_t binding_generation) noexcept;

    // 新 borrow：同一临界区校验「对象存在 + generation + 状态 Running 或
    // Installing-hook 借用 + binding active + gate 未封」，成功后
    // component_borrows 与 binding.borrows 各 +1 并返回 BindingLease
    // （由 ComponentBorrow 持有）。
    // allow_installing_borrow：显式装配窗口借用；此外 subject 条目处于
    // Installing（OnInstall 执行中）时自动允许借 Installed 依赖做纯内存装配。
    result<std::shared_ptr<BindingLease>> AcquireBorrowLocked(
        RegistryEntry& component_entry, const ObjectId& subject_id,
        std::uint64_t binding_generation, bool allow_installing_borrow);

    // 等组件全部 borrow 归零（deadline 有限；供 Uninstall/销毁收口）。
    result<void> WaitBorrowsDrained(std::string_view component_id,
                                    bbt::coroutine::Deadline deadline);
    // 等指定 binding（subject + generation）的在途 borrow 归零；
    // binding 边已被移除视为归零。Unbind 只等自身，不看其他 subject。
    result<void> WaitBindingDrained(const ObjectId& component_id,
                                    const ObjectId& subject_id,
                                    std::uint64_t binding_generation,
                                    bbt::coroutine::Deadline deadline);
    // 找指定 subject 的 retiring binding（Unbind 超时后的续等入口）。
    // 返回 true 时经 out_generation 回填该边的代。
    bool FindRetiringBindingLocked(const RegistryEntry& component_entry,
                                   const ObjectId& subject_id,
                                   std::uint64_t* out_generation) const;
    // 排空确认后收口：抹除指定 (subject, generation) 的 retiring 边。
    void EraseBindingLocked(RegistryEntry& component_entry,
                            const ObjectId& subject_id,
                            std::uint64_t generation);
    // 收口指定 subject 的全部 retiring 边（供 subject 销毁路径）。
    void EraseSubjectBindingsLocked(RegistryEntry& component_entry,
                                    const ObjectId& subject_id);

    // ---- hook 执行簿记（锁内）----
    // 有 hook 在受管协程中尚未到达真实终态（超时只结束等待，不预扣）。
    bool HookInFlightLocked(const RegistryEntry& e) const noexcept {
        return e.hooks_in_flight > 0;
    }
    // 协程等待位（懒创建）：Co* 入口遇前序在途 hook 时在其上挂起至归零；
    // 宿主路径仍走 m_cv（MutexCvWait），两等待域共享同一 hooks_in_flight。
    std::shared_ptr<bbt::coroutine::sync::CoWaiter>
        HookSettledWaiterLocked(RegistryEntry& e) {
        if (!e.hook_settled_waiter)
            e.hook_settled_waiter = bbt::coroutine::sync::CoWaiter::Create();
        return e.hook_settled_waiter;
    }
    // hook 协程结束时由任务体归还：--hooks_in_flight 并唤醒等待者。
    void NoteHookSettled(const ObjectId& id) noexcept;

    // ---- 组件依赖拓扑（锁内计算，返回 id 顺序）----
    // 依赖边：binding.subject 是 Component 且 target 是本组件条目
    // （subject → target 读作 subject 依赖 target）。
    // 启动序：依赖先于使用者（DFS 后序，稳定按登记顺序）。
    // 环在 Bind 期拒绝；若仍残留（防御），环内节点按登记顺序附尾并返回
    // had_cycle=true。
    std::vector<ObjectId> ComponentStartOrderLocked(bool* had_cycle) const;

    // ---- Uninstall ----
    // 封 gate → 标 destroying/op → 供调用方随后锁外跑 OnStop/OnUninstall。
    // 仍有 active 或 retiring binding → Busy（组件被其他使用者绑定中）。
    result<RegistryEntry*> BeginDestroyLocked(std::string_view id);
    // 从表中移除并发布 Destroyed；返回 (object, events)：object 由调用方
    // 锁外释放（业务析构不得在锁内执行），events 锁外派发。
    std::pair<std::shared_ptr<void>,
              std::vector<std::pair<ObserverRecord, ObjectStateEvent>>>
        FinishDestroyLocked(RegistryEntry& e);

    // ---- 诊断 ----
    std::vector<CleanupInfo> PendingCleanup() const;
    void RecordFailure(LifecycleFailure f);
    std::vector<LifecycleFailure> Failures() const;

    // ---- 子类使用的工具 ----
    std::size_t CountObjectsLocked() const { return m_entries.size(); }
    std::size_t CountBindingsLocked() const;
    RegistryEntry* FindByTypeNameLocked(ObjectKind kind,
                                        std::string_view type_name);
    ObjectHandle MakeHandleLocked(const RegistryEntry& e);
    // 派发事件快照（按 id 查条目并 SetStateLocked→解锁由调用方
    // DispatchEvents 的二合一便捷形；内部先取快照再立即派发）。
    void SetStateAndDispatch(const ObjectId& id, ObjectState s,
                             std::optional<ObjectError> err = std::nullopt);

    // 类型化构造/销毁：业务受管类（构造/析构 protected/private）把本类
    // 声明为 friend 后，此处成为唯一合法 new/delete 点。CoApp 的
    // Install 模板把原始调用转发到这两个静态成员，不在业务上下文
    // 直接 new/delete。
    // DeleteManaged 接受静态类型指针：受管对象的 owner 是
    // 「静态类型指针 + 本 deleter」的 shared_ptr，从该指针删除派生
    // 对象与业务类型如何布局无关（不经过 void* 反向 static_cast）。
    template <class T>
    static T* NewManaged(ConstructKey k) { return new T(k); }
    template <class T, class Opt>
    static T* NewManaged(ConstructKey k, Opt&& options) {
        return new T(k, std::forward<Opt>(options));
    }
    template <class T>
    static void DeleteManaged(T* p) noexcept { delete p; }

private:
    result<void> _WaitBorrows(const std::function<bool()>& ready,
                               bbt::coroutine::Deadline deadline);
    // observer lease 排空等待（双域）：协程内挂起在 m_observer_waiters
    // 上的 CoWaiter，native 线程走 m_cv；谓词每次均在 m_mtx 内复查。
    // 供 noexcept 撤销路径使用——协程等待无期限（撤销终态唯一），
    // 等待通道异常时降级为让出后轮询，不吞责任也不死等断言。
    void _WaitObserversDone(const std::function<bool()>& done) noexcept;
    std::vector<std::shared_ptr<bbt::coroutine::sync::CoWaiter>> m_borrow_waiters;
    std::vector<std::shared_ptr<bbt::coroutine::sync::CoWaiter>> m_observer_waiters;
    ObjectId NextIdLocked(ObjectKind kind);
    static const char* KindPrefix(ObjectKind kind) noexcept;

    Limits                      m_limits;
    mutable std::mutex          m_mtx;
    std::condition_variable     m_cv;   // borrow/lease 归零唤醒
    std::deque<RegistryEntry>   m_entries;
    std::unordered_map<std::string, std::size_t> m_index; // id → deque 下标
    std::map<std::uint64_t, ObserverRecord> m_observers;
    std::uint64_t               m_next_observer_id = 1;
    std::uint64_t               m_next_generation = 1;
    std::uint64_t               m_next_id_seq = 0;
    std::deque<LifecycleFailure> m_failures;         // bounded ring
    std::size_t                 m_failures_dropped = 0;

    friend struct BindingLease;
};

// 契约命名面：三个 Registry 是同一注册表的类型化视图别名（业务
// `friend class fw::detail::ServiceRegistry` 即 friend 本 Registry）。
using ServiceRegistry   = Registry;
using ActorRegistry     = Registry;
using ComponentRegistry = Registry;

} // namespace detail
} // namespace bbt::framework
