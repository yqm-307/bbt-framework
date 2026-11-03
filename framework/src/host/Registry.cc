// framework v2 受管对象注册表实现（见 internal/Registry.hpp 头部纪律）。

#include <algorithm>
#include <mutex>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/framework/Component.hpp>
#include <bbt/framework/Service.hpp>
#include <bbt/framework/internal/Registry.hpp>

namespace bbt::framework {
namespace detail {

// ---- BindingLease ----

BindingLease::~BindingLease() {
    auto reg = reg_w.lock();
    if (!reg) return;
    reg->ReleaseBorrow(component_id, subject_id, binding_generation);
}

// ---- 工具 ----

const char* Registry::KindPrefix(ObjectKind kind) noexcept {
    switch (kind) {
    case ObjectKind::App:       return "app";
    case ObjectKind::Service:   return "service";
    case ObjectKind::Actor:     return "actor";
    case ObjectKind::Component: return "component";
    }
    return "object";
}

ObjectId Registry::NextIdLocked(ObjectKind kind) {
    return std::string(KindPrefix(kind)) + ":" +
        std::to_string(m_next_id_seq++);
}

// ---- 条目生命周期 ----

result<RegistryEntry*> Registry::CreateEntryLocked(
    ObjectKind kind, std::string type_name, std::string name,
    ComponentScope scope, ObjectId owner_id,
    const std::type_info& object_type, const std::shared_ptr<void>& object) {
    if (CountObjectsLocked() >= m_limits.max_objects)
        return result<RegistryEntry*>::err(MakeError(ErrorCode::Overloaded,
            "registry: object limit reached"));

    RegistryEntry e;
    e.id         = NextIdLocked(kind);
    e.generation = m_next_generation++;
    e.kind       = kind;
    e.type_name  = std::move(type_name);
    e.name       = std::move(name);
    e.scope      = scope;
    e.owner_id   = std::move(owner_id);
    e.state      = ObjectState::Created;
    e.object     = object;
    e.object_type = &object_type;

    m_entries.push_back(std::move(e));
    const std::size_t idx = m_entries.size() - 1;
    try {
        m_index.emplace(m_entries[idx].id, idx);
    } catch (...) {
        m_entries.pop_back(); // caller retains object owner until this lock is released.
        throw;
    }
    return result<RegistryEntry*>::ok(&m_entries[idx]);
}

RegistryEntry* Registry::FindEntryLocked(std::string_view id) {
    auto it = m_index.find(std::string(id));
    if (it == m_index.end()) return nullptr;
    return &m_entries[it->second];
}

const RegistryEntry* Registry::FindEntryLocked(std::string_view id) const {
    auto it = m_index.find(std::string(id));
    if (it == m_index.end()) return nullptr;
    return &m_entries[it->second];
}

RegistryEntry* Registry::FindComponentLocked(
    ComponentScope scope, const ObjectId& owner_id,
    const std::type_info& type, std::string_view name) {
    for (auto& e : m_entries) {
        if (e.kind != ObjectKind::Component) continue;
        if (e.scope != scope) continue;
        if (e.scope != ComponentScope::App && e.owner_id != owner_id) continue;
        if (*e.object_type != type) continue;
        if (e.name != name) continue;
        return &e;
    }
    return nullptr;
}

RegistryEntry* Registry::FindActorLocked(const std::string& type,
                                         const std::string& key) {
    for (auto& e : m_entries) {
        if (e.kind != ObjectKind::Actor) continue;
        if (e.type_name == type && e.name == key) return &e;
    }
    return nullptr;
}

RegistryEntry* Registry::FindByTypeNameLocked(ObjectKind kind,
                                              std::string_view type_name) {
    for (auto& e : m_entries) {
        if (e.kind != kind) continue;
        if (e.type_name == type_name) return &e;
    }
    return nullptr;
}

std::size_t Registry::CountBindingsLocked() const {
    std::size_t n = 0;
    for (const auto& e : m_entries) n += e.bindings.size();
    return n;
}

ObjectHandle Registry::MakeHandleLocked(const RegistryEntry& e) {
    return ObjectHandle(weak_from_this(), e.id, e.generation, e.kind);
}

// ---- 稳定 id 快照 ----

std::vector<ObjectId> Registry::ListIdsLocked() const {
    std::vector<ObjectId> out;
    out.reserve(m_entries.size());
    for (const auto& e : m_entries) out.push_back(e.id);
    return out;
}

std::vector<ObjectId> Registry::ListIdsLocked(ObjectKind kind,
                                              ObjectState state) const {
    std::vector<ObjectId> out;
    for (const auto& e : m_entries)
        if (e.kind == kind && e.state == state) out.push_back(e.id);
    return out;
}

std::vector<ObjectId> Registry::ListIdsByKindLocked(ObjectKind kind) const {
    std::vector<ObjectId> out;
    for (const auto& e : m_entries)
        if (e.kind == kind) out.push_back(e.id);
    return out;
}

// ---- 状态推进与事件 ----
//
// SetStateLocked 只标记 active token 的待派发意图；DispatchEvents 在锁外
// 逐个获取 callback lease（在途回调计数）后执行回调——已撤销的 token 其
// 已复制意图被丢弃，不执行业务代码；仍 active 的回调执行期间 Revoke 只
// 等待 lease 归零，不撤销在途调用。

std::vector<std::pair<ObserverRecord, ObjectStateEvent>>
Registry::SetStateLocked(RegistryEntry& e, ObjectState s,
                         std::optional<ObjectError> err) {
    e.state = s;
    if (err) e.last_error = std::move(err);
    ++e.revision;

    ObjectStateEvent ev;
    ev.id = e.id;
    ev.generation = e.generation;
    ev.state = s;
    ev.revision = e.revision;
    ev.observed_at = std::chrono::system_clock::now();
    ev.error = e.last_error;
    if (e.kind == ObjectKind::Actor) {
        ActorInfo ai;
        static_cast<ObjectInfo&>(ai) = ObjectInfo{
            e.id, e.generation, e.kind, e.type_name, e.name, e.scope,
            e.owner_id, e.state, e.last_error, ev.observed_at};
        ai.actor_id = ActorId{e.type_name, e.name};
        auto addr = AddressOf(e);
        if (addr) ai.mailbox = MailBox(addr.value());
        ev.actor = std::move(ai);
    }

    std::vector<std::pair<ObserverRecord, ObjectStateEvent>> out;
    for (const auto& [tid, rec] : m_observers) {
        if (!rec.active) continue;
        if (rec.target_id != e.id || rec.target_generation != e.generation)
            continue;
        out.emplace_back(rec, ev);
    }
    return out;
}

std::vector<std::pair<ObserverRecord, ObjectStateEvent>>
Registry::SetStateAndGetEvents(const ObjectId& id, ObjectState s,
                               std::optional<ObjectError> err) {
    auto* e = FindEntryLocked(id);
    if (!e) return {};   // vector 空
    return SetStateLocked(*e, s, std::move(err));
}

void Registry::DispatchEvents(
    std::vector<std::pair<ObserverRecord, ObjectStateEvent>> events) {
    // 每条意图锁外逐个取 lease 后调用：回调执行期记录不可抹除；
    // 已撤销的意图跳过（Revoke 排空语义），回调抛异常计数不传播。
    // 事件向量本身携带 callback 副本（rec 拷贝）——回调结束即释放该
    // 副本，不让已撤销/已派发闭包多活一轮。
    std::vector<std::shared_ptr<bbt::coroutine::sync::CoWaiter>> waiters;
    for (auto& item : events) {
        auto& ev = item.second;
        ObjectObserver callback;
        {
            std::unique_lock<std::mutex> lk(m_mtx);
            auto it = m_observers.find(item.first.id);
            if (it == m_observers.end() || !it->second.active)
                continue;
            ++it->second.in_flight;
            callback = it->second.callback;
        }
        try {
            callback(ev);
        } catch (...) {
            std::lock_guard<std::mutex> lk(m_mtx);
            RecordFailure(LifecycleFailure{
                item.first.target_id, item.first.target_generation,
                "observe", "dispatch", ErrorCode::InternalError,
                std::chrono::system_clock::now(),
                "observer callback threw"});
        }
        callback = nullptr;   // 立即归还闭包副本
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            auto it = m_observers.find(item.first.id);
            if (it != m_observers.end() && it->second.in_flight > 0)
                --it->second.in_flight;
            waiters.swap(m_observer_waiters);
        }
        for (const auto& w : waiters) w->Notify();
        m_cv.notify_all();
    }
}

void Registry::SetStateAndDispatch(const ObjectId& id, ObjectState s,
                                   std::optional<ObjectError> err) {
    std::vector<std::pair<ObserverRecord, ObjectStateEvent>> events;
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        auto* e = FindEntryLocked(id);
        if (!e) return;
        events = SetStateLocked(*e, s, std::move(err));
    }
    DispatchEvents(std::move(events));
}

// ---- 控制面互斥 ----

bool Registry::BeginOpLocked(RegistryEntry& e) {
    if (e.op_in_progress) return false;
    e.op_in_progress = true;
    return true;
}

void Registry::EndOpLocked(RegistryEntry& e) {
    e.op_in_progress = false;
}

// ---- 观察 ----
//
// ObserverRecord 的 callback lease：DispatchEvents 在锁外取 lease（++
// in_flight）再执行业务回调；Revoke 先标 active=false 阻塞新派发，
// 并等已派发 lease 归零后抹除记录与闭包——callback 捕获对象不会在
// 回调仍在途时被销毁。

result<ObserverToken> Registry::Observe(const ObjectHandle& observer,
                                        const ObjectHandle& target,
                                        ObjectObserver callback) {
    if (!callback)
        return result<ObserverToken>::err(MakeError(ErrorCode::InvalidArgument,
            "Observe: null callback"));
    if (observer.m_reg.lock().get() != this ||
        target.m_reg.lock().get() != this)
        return result<ObserverToken>::err(MakeError(ErrorCode::InvalidArgument,
            "Observe: handle belongs to another registry"));

    std::lock_guard<std::mutex> lk(m_mtx);
    // 额度只计活跃订阅：撤销后记录已抹除，不留占位。
    std::size_t active_count = 0;
    for (const auto& [tid, rec] : m_observers)
        if (rec.active) ++active_count;
    if (active_count >= m_limits.max_observers)
        return result<ObserverToken>::err(MakeError(ErrorCode::Overloaded,
            "Observe: observer limit reached"));
    const RegistryEntry* obs = FindEntryLocked(observer.m_id);
    const RegistryEntry* tgt = FindEntryLocked(target.m_id);
    if (!obs || obs->generation != observer.m_generation)
        return result<ObserverToken>::err(MakeError(ErrorCode::NotFound,
            "Observe: observer not found or generation stale"));
    if (!tgt || tgt->generation != target.m_generation)
        return result<ObserverToken>::err(MakeError(ErrorCode::NotFound,
            "Observe: target not found or generation stale"));

    const std::uint64_t tid = m_next_observer_id++;
    ObserverRecord rec;
    rec.id = tid;
    rec.observer_id = observer.m_id;
    rec.observer_generation = observer.m_generation;
    rec.target_id = target.m_id;
    rec.target_generation = target.m_generation;
    rec.callback = std::move(callback);
    m_observers.emplace(tid, std::move(rec));
    return result<ObserverToken>::ok(
        ObserverToken(weak_from_this(), tid));
}

void Registry::RevokeObserver(std::uint64_t token_id) noexcept {
    // 锁内撤销 + 等在途 lease 归零 + 抹除记录（闭包随之销毁）。
    // 双域等待：协程内不阻塞 worker（挂起在 m_observer_waiters）。
    {
        std::unique_lock<std::mutex> lk(m_mtx);
        auto it = m_observers.find(token_id);
        if (it == m_observers.end()) return;
        it->second.active = false;
    }
    _WaitObserversDone([this, token_id] {
        auto it = m_observers.find(token_id);
        return it == m_observers.end() || it->second.in_flight == 0;
    });
    std::lock_guard<std::mutex> lk(m_mtx);
    m_observers.erase(token_id);
}

void Registry::_WaitObserversDone(
    const std::function<bool()>& done) noexcept {
    if (bbt::coroutine::IsInCoroutineContext()) {
        // 协程域：CoWaiter 挂起不阻塞 worker；谓词在锁内复查，等待异常
        // （AlreadyWaiting/Cancelled 等非 Completed 状态）时降级为
        // 让出后轮询——撤销语义必须到达，不能把错误透成 noexcept 失败。
        auto waiter = bbt::coroutine::sync::CoWaiter::Create();
        for (;;) {
            {
                std::lock_guard<std::mutex> lk(m_mtx);
                if (done()) return;
            }
            bbt::coroutine::WaitOptions wait;   // 无期限：lease 归零是唯一终态
            const auto status = waiter->WaitWithCallback(wait, [&] {
                bool ready;
                {
                    std::lock_guard<std::mutex> lk(m_mtx);
                    m_observer_waiters.push_back(waiter);
                    ready = done();
                }
                if (ready) waiter->Notify();
                return true;
            });
            {
                std::lock_guard<std::mutex> lk(m_mtx);
                m_observer_waiters.erase(
                    std::remove(m_observer_waiters.begin(),
                        m_observer_waiters.end(), waiter),
                    m_observer_waiters.end());
            }
            if (status == bbt::coroutine::WaitStatus::Completed) continue;
            // 异常等待通道：让出当前协程并复查，不静默退出。
            bbtco_sleep(1);
        }
    } else {
        // native 域：m_cv 谓词等待（DispatchEvents 归还 lease 后唤醒）。
        std::unique_lock<std::mutex> lk(m_mtx);
        m_cv.wait(lk, done);
    }
}

bool Registry::ObserverActive(std::uint64_t token_id) const noexcept {
    std::lock_guard<std::mutex> lk(m_mtx);
    auto it = m_observers.find(token_id);
    return it != m_observers.end() && it->second.active;
}

void Registry::RevokeObserversOf(std::string_view observer_id) {
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        RevokeObserversOfLocked(observer_id);
    }
    DrainInactiveObservers();
}

void Registry::RevokeObserversOfLocked(std::string_view observer_id) {
    for (auto& [tid, rec] : m_observers)
        if (rec.observer_id == observer_id) rec.active = false;
}

void Registry::DrainInactiveObservers() noexcept {
    // 等全部 inactive 记录的在途 lease 归零后抹除（闭包销毁在锁内完成，
    // 其捕获对象的析构不得晚于本函数返回——卸载路径据此保证 owner 顺序）。
    {
        std::unique_lock<std::mutex> lk(m_mtx);
        for (auto it = m_observers.begin(); it != m_observers.end();) {
            if (!it->second.active && it->second.in_flight == 0)
                it = m_observers.erase(it);
            else
                ++it;
        }
    }
    _WaitObserversDone([this] {
        // 在途 callback 归零后 DispatchEvents 只唤醒不抹除（记录所有权归
        // 排空方）——谓词必须按「inactive 且 in_flight>0 的记录不存在」
        // 判定，否则一旦首次扫描留下在途记录将永久等待。
        for (const auto& [tid, rec] : m_observers)
            if (!rec.active && rec.in_flight > 0) return false;
        return true;
    });
    std::unique_lock<std::mutex> lk(m_mtx);
    for (auto it = m_observers.begin(); it != m_observers.end();) {
        if (!it->second.active)
            it = m_observers.erase(it);
        else
            ++it;
    }
}

// ---- handle 观察面 ----

bool Registry::OwnsHandle(const ObjectHandle& h) const noexcept {
    return h.m_reg.lock().get() == this;
}

result<ObjectInfo> Registry::Info(const ObjectHandle& h) const {
    std::lock_guard<std::mutex> lk(m_mtx);
    const RegistryEntry* e = FindEntryLocked(h.m_id);
    if (!e)
        return result<ObjectInfo>::err(MakeError(ErrorCode::NotFound,
            "ObjectHandle: object destroyed"));
    if (e->generation != h.m_generation)
        return result<ObjectInfo>::err(MakeError(ErrorCode::InvalidArgument,
            "ObjectHandle: generation stale"));
    return result<ObjectInfo>::ok(ObjectInfo{
        e->id, e->generation, e->kind, e->type_name, e->name, e->scope,
        e->owner_id, e->state, e->last_error,
        std::chrono::system_clock::now()});
}

bool Registry::HandleValid(const ObjectHandle& h) const noexcept {
    std::lock_guard<std::mutex> lk(m_mtx);
    const RegistryEntry* e = FindEntryLocked(h.m_id);
    return e != nullptr && e->generation == h.m_generation &&
        e->state != ObjectState::Destroyed &&
        e->state != ObjectState::Invalidated;
}

result<AppObjectAddress> Registry::AddressOf(const RegistryEntry& e) {
    switch (e.kind) {
    case ObjectKind::Service:
        return result<AppObjectAddress>::ok(
            AppObjectAddress::Service(e.type_name));
    case ObjectKind::Actor:
        return result<AppObjectAddress>::ok(
            AppObjectAddress::Actor(e.type_name, e.name));
    default:
        return result<AppObjectAddress>::err(MakeError(
            ErrorCode::InvalidArgument,
            "GetMailBox: 仅 Service/Actor 有地址"));
    }
}

result<MailBox> Registry::MailBoxOf(const ObjectHandle& h) const {
    std::lock_guard<std::mutex> lk(m_mtx);
    const RegistryEntry* e = FindEntryLocked(h.m_id);
    if (!e)
        return result<MailBox>::err(MakeError(ErrorCode::NotFound,
            "GetMailBox: object destroyed"));
    if (e->generation != h.m_generation)
        return result<MailBox>::err(MakeError(ErrorCode::InvalidArgument,
            "GetMailBox: generation stale"));
    auto addr = AddressOf(*e);
    if (!addr) return result<MailBox>::err(std::move(addr.error()));
    return result<MailBox>::ok(MailBox(addr.value()));
}

// ---- binding / borrow ----

result<void> Registry::BindLocked(RegistryEntry& component_entry,
                                  const RegistryEntry& subject_entry,
                                  const ObjectId& subject_id,
                                  std::uint64_t* out_generation) {
    if (component_entry.kind != ObjectKind::Component)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "BindComponent: target is not a component"));
    if (component_entry.gate_closed)
        return result<void>::err(MakeError(ErrorCode::Closed,
            "BindComponent: component gate closed"));
    // subject 同临界区 gate：销毁/收口路径先封 subject（gate_closed /
    // op_in_progress / 关闭语义状态）再擦除其 retiring 边——此后新增的
    // active 边不会被收口看到，必须以 Closed 拒绝（P1-3）。
    if (subject_entry.gate_closed || subject_entry.op_in_progress ||
        subject_entry.state == ObjectState::Stopping ||
        subject_entry.state == ObjectState::Stopped ||
        subject_entry.state == ObjectState::Uninstalling ||
        subject_entry.state == ObjectState::Destroyed ||
        subject_entry.state == ObjectState::Invalidated ||
        subject_entry.state == ObjectState::StopIncomplete ||
        subject_entry.state == ObjectState::UninstallIncomplete)
        return result<void>::err(MakeError(ErrorCode::Closed,
            "BindComponent: subject is closing"));
    if (CountBindingsLocked() >= m_limits.max_bindings)
        return result<void>::err(MakeError(ErrorCode::Overloaded,
            "BindComponent: binding limit reached"));
    // 同 (subject, component) 已有 active binding → InvalidArgument。
    for (const auto& b : component_entry.bindings)
        if (b.subject_id == subject_id && !b.retiring)
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                "BindComponent: duplicate binding"));

    Binding b;
    b.subject_id = subject_id;
    b.generation = m_next_generation++;
    b.active = true;
    component_entry.bindings.push_back(std::move(b));
    *out_generation = component_entry.bindings.back().generation;
    return result<void>::ok();
}

result<void> Registry::BeginUnbindLocked(RegistryEntry& component_entry,
                                       const ObjectId& subject_id,
                                       std::uint64_t* out_generation) {
    for (auto it = component_entry.bindings.begin();
         it != component_entry.bindings.end(); ++it) {
        if (it->subject_id != subject_id || it->retiring) continue;
        it->active = false;
        it->retiring = true;
        // generation 不推进：retiring 自身即让旧 handle 失效（Borrow 先看
        // !active||retiring → Closed）；保持原代让在途 BindingLease 的
        // ReleaseBorrow 仍能按 (subject, gen) 归还计数。
        if (out_generation) *out_generation = it->generation;
        // 边保留至 Unbind 等待方确认排空后 EraseBindingLocked 收口。
        return result<void>::ok();
    }
    return result<void>::err(MakeError(ErrorCode::NotFound,
        "UnbindComponent: binding not found"));
}

void Registry::ReleaseBorrow(const ObjectId& component_id,
                             const ObjectId& subject_id,
                             std::uint64_t binding_generation) noexcept {
    std::vector<std::shared_ptr<bbt::coroutine::sync::CoWaiter>> waiters;
    std::unique_lock<std::mutex> lk(m_mtx);
    RegistryEntry* e = FindEntryLocked(component_id);
    if (!e) return;
    if (e->component_borrows > 0) --e->component_borrows;

    // 本 binding 计数归还。retiring 边在归零后不自动抹除——「已排空的
    // retiring 记录」由 Unbind 等待方在收口确认后显式 EraseBindingLocked
    // 关闭；提前抹除会让重试的 Unbind 失去可续等的对象。
    for (auto& b : e->bindings) {
        if (b.subject_id == subject_id &&
            b.generation == binding_generation) {
            if (b.borrows > 0) --b.borrows;
            break;
        }
    }
    waiters.swap(m_borrow_waiters);
    lk.unlock();
    for (const auto& waiter : waiters) waiter->Notify();
    m_cv.notify_all();
}

result<std::shared_ptr<BindingLease>> Registry::AcquireBorrowLocked(
    RegistryEntry& component_entry, const ObjectId& subject_id,
    std::uint64_t binding_generation, bool allow_installing_borrow) {
    if (component_entry.gate_closed)
        return result<std::shared_ptr<BindingLease>>::err(MakeError(
            ErrorCode::Closed, "Borrow: component gate closed"));

    // 装配窗口：subject（绑定发起方）正处于 Installing（OnInstall 执行中）
    // 时允许借 Installed 依赖做纯内存装配——窗口由 subject 状态推导，
    // 不靠调用方传 flag 保证（显式 allow_installing_borrow 等价放行）。
    bool installing_borrow = allow_installing_borrow;
    if (!installing_borrow) {
        const RegistryEntry* subj = FindEntryLocked(subject_id);
        installing_borrow =
            (subj != nullptr && subj->state == ObjectState::Installing);
    }

    // 对象状态门：Running 可借；OnInstall 装配窗口内 Installed 可借。
    const ObjectState s = component_entry.state;
    const bool state_ok =
        (s == ObjectState::Running) ||
        (installing_borrow && s == ObjectState::Installed);
    if (!state_ok) {
        ErrorCode code = ErrorCode::Closed;
        if (s == ObjectState::Installing || s == ObjectState::Starting ||
            s == ObjectState::Paused)
            code = ErrorCode::Overloaded;
        else if (s == ObjectState::Destroyed || s == ObjectState::Invalidated)
            code = ErrorCode::Closed;
        else if (s == ObjectState::InstallFailed || s == ObjectState::Failed)
            code = ErrorCode::Closed;
        else if (s == ObjectState::Created || s == ObjectState::Installed)
            code = ErrorCode::Overloaded;   // 未 Running 且非装配窗口
        return result<std::shared_ptr<BindingLease>>::err(MakeError(code,
            "Borrow: component not borrowable in current state"));
    }

    // binding 边：按 (subject, generation) 唯一匹配。retiring/inactive 或
    // 代失配的旧 handle → Closed；同 subject 的新边不影响。
    Binding* edge = nullptr;
    for (auto& b : component_entry.bindings) {
        if (b.subject_id == subject_id &&
            b.generation == binding_generation) {
            edge = &b;
            break;
        }
    }
    if (edge == nullptr)
        return result<std::shared_ptr<BindingLease>>::err(MakeError(
            ErrorCode::Closed, "Borrow: binding invalidated"));
    if (!edge->active || edge->retiring)
        return result<std::shared_ptr<BindingLease>>::err(MakeError(
            ErrorCode::Closed, "Borrow: binding invalidated"));

    auto lease = std::make_shared<BindingLease>();
    lease->reg_w = weak_from_this();
    lease->owner = component_entry.object;
    lease->component_id = component_entry.id;
    lease->subject_id = subject_id;
    lease->binding_generation = binding_generation;
    ++component_entry.component_borrows;
    ++edge->borrows;
    return result<std::shared_ptr<BindingLease>>::ok(std::move(lease));
}

result<void> Registry::_WaitBorrows(
    const std::function<bool()>& ready, bbt::coroutine::Deadline deadline) {
    if (!bbt::coroutine::IsInCoroutineContext())
        return result<void>::err(MakeError(ErrorCode::InvalidContext,
            "borrow wait requires coroutine context"));
    auto waiter = bbt::coroutine::sync::CoWaiter::Create();
    for (;;) {
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            if (ready()) return result<void>::ok();
        }
        bbt::coroutine::WaitOptions wait;
        wait.deadline = deadline;
        const auto status = waiter->WaitWithCallback(wait, [&] {
            bool done;
            {
                std::lock_guard<std::mutex> lk(m_mtx);
                m_borrow_waiters.push_back(waiter);
                done = ready();
            }
            if (done) waiter->Notify();
            return true;
        });
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            m_borrow_waiters.erase(std::remove(m_borrow_waiters.begin(),
                m_borrow_waiters.end(), waiter), m_borrow_waiters.end());
        }
        if (status == bbt::coroutine::WaitStatus::Completed) continue;
        const auto code = status == bbt::coroutine::WaitStatus::TimedOut
            ? ErrorCode::TimedOut : status == bbt::coroutine::WaitStatus::Cancelled
            ? ErrorCode::Cancelled : ErrorCode::RuntimeUnavailable;
        return result<void>::err(MakeError(code, "borrow wait did not complete"));
    }
}

result<void> Registry::WaitBorrowsDrained(
    std::string_view component_id, bbt::coroutine::Deadline deadline) {
    return _WaitBorrows([&] {
        const auto* e = FindEntryLocked(component_id);
        return !e || e->component_borrows == 0;
    }, deadline);
}

result<void> Registry::WaitBindingDrained(
    const ObjectId& component_id, const ObjectId& subject_id,
    std::uint64_t binding_generation, bbt::coroutine::Deadline deadline) {
    return _WaitBorrows([&] {
        const auto* e = FindEntryLocked(component_id);
        if (!e) return true;
        for (const auto& b : e->bindings) {
            if (b.subject_id == subject_id && b.generation == binding_generation)
                return b.borrows == 0;
        }
        return true;
    }, deadline);
}

bool Registry::FindRetiringBindingLocked(
    const RegistryEntry& component_entry, const ObjectId& subject_id,
    std::uint64_t* out_generation) const {
    for (const auto& b : component_entry.bindings) {
        if (b.subject_id == subject_id && b.retiring) {
            if (out_generation) *out_generation = b.generation;
            return true;
        }
    }
    return false;
}

void Registry::EraseBindingLocked(RegistryEntry& component_entry,
                                  const ObjectId& subject_id,
                                  std::uint64_t generation) {
    for (auto it = component_entry.bindings.begin();
         it != component_entry.bindings.end(); ++it) {
        if (it->subject_id == subject_id &&
            it->generation == generation && it->retiring) {
            component_entry.bindings.erase(it);
            return;
        }
    }
}

void Registry::EraseSubjectBindingsLocked(RegistryEntry& component_entry,
                                          const ObjectId& subject_id) {
    auto& bs = component_entry.bindings;
    bs.erase(std::remove_if(bs.begin(), bs.end(), [&](const Binding& b) {
        return b.subject_id == subject_id && b.retiring;
    }), bs.end());
}

void Registry::NoteHookSettled(const ObjectId& id) noexcept {
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> w;
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        auto* e = FindEntryLocked(id);
        if (e && e->hooks_in_flight > 0) --e->hooks_in_flight;
        if (e && e->hooks_in_flight == 0) w = e->hook_settled_waiter;
    }
    // Notify 在锁外：Notify 链路会触发协程唤醒/驱动回调，不阻塞 Registry
    // 临界区；m_cv 唤醒宿主等待者同样不持锁（m_cv 谓词在锁内复查）。
    m_cv.notify_all();
    if (w) w->Notify();
}

// ---- 组件依赖拓扑 ----

std::vector<ObjectId> Registry::ComponentStartOrderLocked(
    bool* had_cycle) const {
    // 依赖图：X 的依赖 = 所有「subject==X」的 binding 指向的目标组件。
    // DFS 后序：依赖先于使用者。按登记顺序遍历保持稳定。
    std::unordered_map<ObjectId, std::vector<ObjectId>> deps;
    for (const auto& e : m_entries) {
        if (e.kind != ObjectKind::Component) continue;
        auto& out = deps[e.id];
        for (const auto& ce : m_entries) {
            if (ce.kind != ObjectKind::Component) continue;
            for (const auto& b : ce.bindings) {
                if (b.subject_id == e.id) out.push_back(ce.id);
            }
        }
    }

    std::vector<ObjectId> order;
    std::unordered_map<ObjectId, int> mark;   // 0=未访，1=栈中，2=完成
    bool cycle = false;
    std::function<void(const ObjectId&)> dfs = [&](const ObjectId& id) {
        mark[id] = 1;
        for (const auto& d : deps[id]) {
            const auto it = mark.find(d);
            if (it != mark.end() && it->second == 2) continue;
            if (it != mark.end() && it->second == 1) { cycle = true; continue; }
            if (deps.count(d) == 0) continue;   // 目标非组件（防御）
            dfs(d);
        }
        mark[id] = 2;
        order.push_back(id);
    };
    for (const auto& e : m_entries) {
        if (e.kind != ObjectKind::Component) continue;
        if (mark.count(e.id) == 0) dfs(e.id);
    }
    if (had_cycle) *had_cycle = cycle;
    return order;
}

// ---- Uninstall ----

result<RegistryEntry*> Registry::BeginDestroyLocked(std::string_view id) {
    RegistryEntry* e = FindEntryLocked(id);
    if (!e)
        return result<RegistryEntry*>::err(MakeError(ErrorCode::NotFound,
            "BeginDestroy: object not found"));

    // 组件：仍有 binding 边 → Busy（不自动解绑使用者）。retiring 且
    // borrow 归零的边由 ReleaseBorrow 即时抹除，剩下的边都有未定责任。
    if (e->kind == ObjectKind::Component && !e->bindings.empty())
        return result<RegistryEntry*>::err(MakeError(ErrorCode::Overloaded,
            "UninstallComponent: active bindings remain"));
    if (e->op_in_progress)
        return result<RegistryEntry*>::err(MakeError(ErrorCode::Overloaded,
            "BeginDestroy: operation in progress"));

    e->gate_closed = true;
    e->op_in_progress = true;
    return result<RegistryEntry*>::ok(e);
}

std::pair<std::shared_ptr<void>,
          std::vector<std::pair<ObserverRecord, ObjectStateEvent>>>
Registry::FinishDestroyLocked(RegistryEntry& e) {
    auto events = SetStateLocked(e, ObjectState::Destroyed);
    m_index.erase(e.id);
    auto it = m_entries.begin();
    for (; it != m_entries.end(); ++it) {
        if (it->id == e.id) break;
    }
    std::shared_ptr<void> obj;
    if (it != m_entries.end()) {
        obj = std::move(it->object);
        m_entries.erase(it);
    }
    // deque 下标变化，整体重建索引。
    m_index.clear();
    for (std::size_t i = 0; i < m_entries.size(); ++i)
        m_index.emplace(m_entries[i].id, i);
    return {std::move(obj), std::move(events)};
}

// ---- 诊断 ----

std::vector<CleanupInfo> Registry::PendingCleanup() const {
    std::lock_guard<std::mutex> lk(m_mtx);
    std::vector<CleanupInfo> out;
    for (const auto& e : m_entries) {
        const bool pending = e.component_borrows > 0 || e.exec_leases > 0 ||
            e.hooks_in_flight > 0 ||
            !e.destroy_phase.empty() ||
            e.state == ObjectState::StopIncomplete ||
            e.state == ObjectState::UninstallIncomplete;
        if (!pending) continue;
        CleanupInfo ci;
        ci.object_id = e.id;
        ci.generation = e.generation;
        ci.phase = e.destroy_phase;
        ci.pending_borrows = e.component_borrows;
        ci.pending_tasks = e.exec_leases + e.hooks_in_flight;
        ci.code = e.last_error ? e.last_error->code : ErrorCode::TimedOut;
        out.push_back(std::move(ci));
    }
    return out;
}

void Registry::RecordFailure(LifecycleFailure f) {
    // bounded ring：上限取 max_objects（超出丢弃并计 dropped）。
    const std::size_t cap = m_limits.max_objects;
    if (m_failures.size() >= cap) {
        m_failures.pop_front();
        ++m_failures_dropped;
    }
    m_failures.push_back(std::move(f));
}

std::vector<LifecycleFailure> Registry::Failures() const {
    std::lock_guard<std::mutex> lk(m_mtx);
    return std::vector<LifecycleFailure>(m_failures.begin(), m_failures.end());
}

// ---- BorrowImpl（ComponentHandle<C>::Borrow 的类型擦除半身）----
//
// 返回 v2::CoComponent*（不擦为 void*）：派生类型 C 经
// CoComponent*→C* static_cast 完成指针调整，多继承布局下不产生错误地址。

result<std::pair<std::shared_ptr<void>, v2::CoComponent*>> BorrowImpl(
    const ObjectHandle& object, std::uint64_t binding_generation,
    bool allow_installing_borrow) {
    auto reg = object.m_reg.lock();
    if (!reg)
        return result<std::pair<std::shared_ptr<void>, v2::CoComponent*>>::err(
            MakeError(ErrorCode::Closed, "Borrow: registry closed"));
    std::lock_guard<std::mutex> lk(reg->Mutex());
    RegistryEntry* e = reg->FindEntryLocked(object.m_id);
    if (!e)
        return result<std::pair<std::shared_ptr<void>, v2::CoComponent*>>::err(
            MakeError(ErrorCode::Closed, "Borrow: object destroyed"));
    if (e->generation != object.m_generation)
        return result<std::pair<std::shared_ptr<void>, v2::CoComponent*>>::err(
            MakeError(ErrorCode::Closed, "Borrow: generation stale"));
    // subject_id 由调用方（service handle）给出——ComponentHandle 经
    // BindComponent 发放时 subject 已记录在该 binding 内；这里用
    // 「binding_generation 匹配」反查 subject，保证 handle 不可伪造
    // 其他 subject 的借用。
    ObjectId subject;
    bool found = false;
    for (const auto& b : e->bindings) {
        if (b.generation == binding_generation) {
            subject = b.subject_id;
            found = true;
            break;
        }
    }
    if (!found)
        return result<std::pair<std::shared_ptr<void>, v2::CoComponent*>>::err(
            MakeError(ErrorCode::Closed, "Borrow: binding invalidated"));
    auto lease = reg->AcquireBorrowLocked(*e, subject, binding_generation,
                                          allow_installing_borrow);
    if (!lease)
        return result<std::pair<std::shared_ptr<void>, v2::CoComponent*>>::err(
            std::move(lease.error()));
    return result<std::pair<std::shared_ptr<void>, v2::CoComponent*>>::ok(
        std::make_pair(std::shared_ptr<void>(std::move(lease.value())),
                       static_cast<v2::CoComponent*>(e->object.get())));
}

} // namespace detail
} // namespace bbt::framework
