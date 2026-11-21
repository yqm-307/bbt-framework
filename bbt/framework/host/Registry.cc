/**
 * framework 受管对象注册表实现（见 internal/Registry.hpp 头部纪律）。
 */

#include <algorithm>
#include <mutex>
#include <vector>

#include <boost/scope/scope_exit.hpp>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/framework/Component.hpp>
#include <bbt/framework/CoService.hpp>
#include <bbt/framework/internal/Registry.hpp>

namespace bbt::framework {
namespace detail {

BindingLease::~BindingLease() {
    auto reg = reg_w.lock();
    if (!reg)
        return;
    reg->ReleaseBorrow(component_id, subject_id, binding_generation);
}

const char* Registry::KindPrefix(ObjectKind kind) noexcept {
    switch (kind) {
    case ObjectKind::App:
        return "app";
    case ObjectKind::Service:
        return "service";
    case ObjectKind::Actor:
        return "actor";
    case ObjectKind::Component:
        return "component";
    }
    return "object";
}

ObjectId Registry::NextIdLocked(ObjectKind kind) {
    return std::string(KindPrefix(kind)) + ":" +
           std::to_string(m_next_id_seq++);
}

bbt::coroutine::WaitStatus Registry::WaitList::WaitUntilReady(std::mutex& mutex,
                                                              const std::shared_ptr<bbt::coroutine::sync::CoWaiter>& waiter,
                                                              const std::function<bool()>& ready, const bbt::coroutine::WaitOptions& options) {
    for (;;) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (ready())
                return bbt::coroutine::WaitStatus::Completed;
        }
        const auto status = waiter->WaitWithCallback(options, [&] {
            bool done;
            {
                std::lock_guard<std::mutex> lock(mutex);
                m_waiters.push_back(waiter);
                // 状态可能在协程登记前已变化，登记后须在同一临界区复查。
                done = ready();
            }
            if (done)
                waiter->Notify();
            return true;
        });
        {
            std::lock_guard<std::mutex> lock(mutex);
            m_waiters.erase(std::remove(m_waiters.begin(), m_waiters.end(), waiter),
                            m_waiters.end());
        }
        if (status != bbt::coroutine::WaitStatus::Completed)
            return status;
    }
}

void Registry::WaitList::NotifyAll(std::unique_lock<std::mutex> lock,
                                   std::condition_variable& cv) {
    decltype(m_waiters) waiters;
    waiters.swap(m_waiters);
    lock.unlock();
    for (const auto& waiter : waiters)
        waiter->Notify();
    cv.notify_all();
}

result<RegistryEntry*> Registry::CreateEntryLocked(
    ObjectKind kind, std::string type_name, std::string name,
    ComponentScope scope, ObjectId owner_id,
    const std::type_info& object_type, const std::shared_ptr<void>& object) {
    if (CountObjectsLocked() >= m_limits.max_objects)
        return result<RegistryEntry*>::err(MakeError(ErrorCode::Overloaded,
                                                     "registry: object limit reached"));

    RegistryEntry e;
    e.id = NextIdLocked(kind);
    e.generation = m_next_generation++;
    e.kind = kind;
    e.type_name = std::move(type_name);
    e.name = std::move(name);
    e.scope = scope;
    e.owner_id = std::move(owner_id);
    e.state = ObjectState::Created;
    e.object = object;
    e.object_type = &object_type;

    m_entries.push_back(std::move(e));
    const std::size_t idx = m_entries.size() - 1;
    try {
        m_index.emplace(m_entries[idx].id, idx);
    } catch (...) {
        /**
         * caller retains object owner until this lock is released.
         */
        m_entries.pop_back();
        throw;
    }
    return result<RegistryEntry*>::ok(&m_entries[idx]);
}

RegistryEntry* Registry::FindEntryLocked(std::string_view id) {
    auto it = m_index.find(std::string(id));
    if (it == m_index.end())
        return nullptr;
    return &m_entries[it->second];
}

const RegistryEntry* Registry::FindEntryLocked(std::string_view id) const {
    auto it = m_index.find(std::string(id));
    if (it == m_index.end())
        return nullptr;
    return &m_entries[it->second];
}

RegistryEntry* Registry::FindComponentLocked(
    ComponentScope scope, const ObjectId& owner_id,
    const std::type_info& type, std::string_view name) {
    for (auto& e : m_entries) {
        if (e.kind != ObjectKind::Component)
            continue;
        if (e.scope != scope)
            continue;
        if (e.scope != ComponentScope::App && e.owner_id != owner_id)
            continue;
        if (*e.object_type != type)
            continue;
        if (e.name != name)
            continue;
        return &e;
    }
    return nullptr;
}

RegistryEntry* Registry::FindActorLocked(const std::string& type,
                                         const std::string& key) {
    for (auto& e : m_entries) {
        if (e.kind != ObjectKind::Actor)
            continue;
        if (e.type_name == type && e.name == key)
            return &e;
    }
    return nullptr;
}

RegistryEntry* Registry::FindByTypeNameLocked(ObjectKind kind,
                                              std::string_view type_name) {
    for (auto& e : m_entries) {
        if (e.kind != kind)
            continue;
        if (e.type_name == type_name)
            return &e;
    }
    return nullptr;
}

std::size_t Registry::CountBindingsLocked() const {
    std::size_t n = 0;
    for (const auto& e : m_entries)
        n += e.bindings.size();
    return n;
}

ObjectHandle Registry::MakeHandleLocked(const RegistryEntry& e) {
    return ObjectHandle(weak_from_this(), e.id, e.generation, e.kind);
}

std::vector<ObjectId> Registry::ListIdsLocked() const {
    std::vector<ObjectId> out;
    out.reserve(m_entries.size());
    for (const auto& e : m_entries)
        out.push_back(e.id);
    return out;
}

std::vector<ObjectId> Registry::ListIdsLocked(ObjectKind kind,
                                              ObjectState state) const {
    std::vector<ObjectId> out;
    for (const auto& e : m_entries)
        if (e.kind == kind && e.state == state)
            out.push_back(e.id);
    return out;
}

std::vector<ObjectId> Registry::ListIdsByKindLocked(ObjectKind kind) const {
    std::vector<ObjectId> out;
    for (const auto& e : m_entries)
        if (e.kind == kind)
            out.push_back(e.id);
    return out;
}

/**
 * ---- 状态推进与事件 ----
 *
 * SetStateLocked 只标记 active token 的待派发意图；DispatchEvents 在锁外
 * 逐个获取 callback lease（在途回调计数）后执行回调——已撤销的 token 其
 * 已复制意图被丢弃，不执行业务代码；仍 active 的回调执行期间 Revoke 只
 * 等待 lease 归零，不撤销在途调用。
 */

ObserverEvents
Registry::SetStateLocked(RegistryEntry& e, ObjectState s,
                         std::optional<ObjectError> err) {
    e.state = s;
    if (err)
        e.last_error = std::move(err);
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
        if (addr)
            ai.mailbox = MailBox(addr.value());
        ev.actor = std::move(ai);
    }

    ObserverEvents out;
    for (const auto& [tid, rec] : m_observers) {
        if (!rec.active)
            continue;
        if (rec.target_id != e.id || rec.target_generation != e.generation)
            continue;
        out.emplace_back(tid, ev);
    }
    return out;
}

ObserverEvents
Registry::SetStateAndGetEvents(const ObjectId& id, ObjectState s,
                               std::optional<ObjectError> err) {
    auto* e = FindEntryLocked(id);
    /**
     * vector 空
     */
    if (!e)
        return {};
    return SetStateLocked(*e, s, std::move(err));
}

void Registry::DispatchEvents(ObserverEvents events) {
    for (const auto& [token, ev] : events) {
        try {
            ObjectObserver callback;
            {
                std::lock_guard<std::mutex> lk(m_mtx);
                auto it = m_observers.find(token);
                if (it == m_observers.end() || !it->second.active)
                    continue;
                callback = it->second.callback;
                ++it->second.in_flight;
            }
            auto release = boost::scope::make_scope_exit([&] {
                callback = nullptr;
                std::unique_lock<std::mutex> lk(m_mtx);
                --m_observers.find(token)->second.in_flight;
                m_observer_waiters.NotifyAll(std::move(lk), m_cv);
            });
            callback(ev);
        } catch (...) {
            std::lock_guard<std::mutex> lk(m_mtx);
            RecordFailure(LifecycleFailure{
                ev.id, ev.generation, "observe", "dispatch", ErrorCode::InternalError,
                std::chrono::system_clock::now(), "observer callback threw"});
        }
    }
}

void Registry::SetStateAndDispatch(const ObjectId& id, ObjectState s,
                                   std::optional<ObjectError> err) {
    ObserverEvents events;
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        auto* e = FindEntryLocked(id);
        if (!e)
            return;
        events = SetStateLocked(*e, s, std::move(err));
    }
    DispatchEvents(std::move(events));
}

bool Registry::BeginOpLocked(RegistryEntry& e) {
    if (e.op_in_progress)
        return false;
    e.op_in_progress = true;
    return true;
}

void Registry::EndOpLocked(RegistryEntry& e) {
    e.op_in_progress = false;
}

/**
 * ---- 观察 ----
 *
 * ObserverRecord 的 callback lease：DispatchEvents 在锁外取 lease（++
 * in_flight）再执行业务回调；Revoke 先标 active=false 阻塞新派发，
 * 并等已派发 lease 归零后抹除记录与闭包——callback 捕获对象不会在
 * 回调仍在途时被销毁。
 */

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
    /**
     * 额度只计活跃订阅：撤销后记录已抹除，不留占位。
     */
    std::size_t active_count = 0;
    for (const auto& [tid, rec] : m_observers)
        if (rec.active)
            ++active_count;
    if (active_count >= m_limits.max_observers)
        return result<ObserverToken>::err(MakeError(ErrorCode::Overloaded,
                                                    "Observe: observer limit reached"));
    const RegistryEntry* obs = FindEntryLocked(observer.m_id);
    const RegistryEntry* tgt = FindEntryLocked(target.m_id);
    if (!obs || obs->generation != observer.m_generation)
        return result<ObserverToken>::err(MakeError(ErrorCode::NotFound,
                                                    "Observe: observer not found or generation stale"));
    if (obs->gate_closed)
        return result<ObserverToken>::err(MakeError(ErrorCode::Closed, "Observe: observer is closing"));
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
    /**
     * 锁内撤销 + 等在途 lease 归零 + 抹除记录（闭包随之销毁）。
     * 双域等待：协程内不阻塞 worker（挂起在 m_observer_waiters）。
     */
    {
        std::unique_lock<std::mutex> lk(m_mtx);
        auto it = m_observers.find(token_id);
        if (it == m_observers.end())
            return;
        it->second.active = false;
    }
    _WaitObserversDone([this, token_id] {
        auto it = m_observers.find(token_id);
        return it == m_observers.end() || it->second.in_flight == 0;
    });
    decltype(m_observers)::node_type retired;
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        retired = m_observers.extract(token_id);
    }
}

void Registry::_WaitObserversDone(
    const std::function<bool()>& done) noexcept {
    if (bbt::coroutine::IsInCoroutineContext()) {
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            if (done())
                return;
        }
        /**
         * 协程域：CoWaiter 挂起不阻塞 worker；谓词在锁内复查，等待异常
         * （AlreadyWaiting/Cancelled 等非 Completed 状态）时降级为
         * 让出后轮询——撤销语义必须到达，不能把错误透成 noexcept 失败。
         */
        auto waiter = bbt::coroutine::sync::CoWaiter::Create();
        while (m_observer_waiters.WaitUntilReady(m_mtx, waiter, done, {}) !=
               bbt::coroutine::WaitStatus::Completed) {
            bbtco_sleep(1);
        }
    } else {
        /**
         * native 域：m_cv 谓词等待（DispatchEvents 归还 lease 后唤醒）。
         */
        std::unique_lock<std::mutex> lk(m_mtx);
        m_cv.wait(lk, done);
    }
}

bool Registry::ObserverActive(std::uint64_t token_id) const noexcept {
    std::lock_guard<std::mutex> lk(m_mtx);
    auto it = m_observers.find(token_id);
    return it != m_observers.end() && it->second.active;
}

void Registry::RevokeObserversOfLocked(std::string_view observer_id) {
    for (auto& [tid, rec] : m_observers)
        if (rec.observer_id == observer_id)
            rec.active = false;
}

bool Registry::_ObserversDrainedLocked(const ObjectId& observer_id) const {
    return std::none_of(m_observers.begin(), m_observers.end(), [&](const auto& item) {
        return item.second.observer_id == observer_id && !item.second.active && item.second.in_flight != 0;
    });
}

void Registry::_EraseInactiveObserversOf(const ObjectId& observer_id) {
    // 逐条摘出，闭包析构在锁外执行，避免析构回入 Registry 时死锁。
    for (;;) {
        decltype(m_observers)::node_type retired;
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            auto it = std::find_if(m_observers.begin(), m_observers.end(), [&](const auto& item) {
                const auto& rec = item.second;
                return rec.observer_id == observer_id && !rec.active && rec.in_flight == 0;
            });
            if (it == m_observers.end())
                return;
            retired = m_observers.extract(it);
        }
    }
}

result<void> Registry::DrainObserversOf(const ObjectId& observer_id, bbt::coroutine::Deadline deadline) {
    auto drained = _WaitCoDrain(m_observer_waiters, [&] { return _ObserversDrainedLocked(observer_id); }, deadline, "observer drain did not complete");
    if (drained)
        _EraseInactiveObserversOf(observer_id);
    return drained;
}

void Registry::DrainInactiveObserversOf(const ObjectId& observer_id) noexcept {
    _WaitObserversDone([&] { return _ObserversDrainedLocked(observer_id); });
    _EraseInactiveObserversOf(observer_id);
}

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
            "GetMailBox: only Service and Actor objects have addresses"));
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
    if (!addr)
        return result<MailBox>::err(std::move(addr.error()));
    return result<MailBox>::ok(MailBox(addr.value()));
}

std::pair<RegistryEntry*, Binding*> Registry::FindBindingLocked(
    const ObjectId& subject_id, const std::type_info& type,
    std::string_view slot, const ComponentOptions* placement, bool pending_drain) {
    for (auto& entry : m_entries) {
        if (entry.kind != ObjectKind::Component || *entry.object_type != type)
            continue;
        if (placement && (entry.scope != placement->Scope() ||
                          (placement->Owner() && entry.owner_id != placement->Owner()->Id())))
            continue;
        for (auto& binding : entry.bindings) {
            if (binding.subject_id == subject_id && binding.slot == slot &&
                (pending_drain ? binding.drain_requested : !binding.retiring))
                return {&entry, &binding};
        }
    }
    return {};
}

ComponentBindingSnapshot Registry::SnapshotBindingLocked(
    const RegistryEntry& target, std::uint64_t generation) {
    return {MakeHandleLocked(target), std::static_pointer_cast<CoComponent>(target.object),
            generation};
}

result<std::uint64_t> Registry::BindLocked(RegistryEntry& target,
                                           const RegistryEntry& subject,
                                           std::string_view slot, RegistryEntry* previous,
                                           Binding* previous_binding) {
    using BindResult = result<std::uint64_t>;
    if (target.gate_closed)
        return BindResult::err(MakeError(ErrorCode::Closed,
                                         "BindComponent: component gate closed"));
    if (subject.gate_closed ||
        subject.state == ObjectState::Stopping || subject.state == ObjectState::Stopped ||
        subject.state == ObjectState::Uninstalling || subject.state == ObjectState::Destroyed ||
        subject.state == ObjectState::Invalidated || subject.state == ObjectState::StopIncomplete ||
        subject.state == ObjectState::UninstallIncomplete)
        return BindResult::err(MakeError(ErrorCode::Closed,
                                         "BindComponent: subject is closing"));
    // 只有同一临界区内可以立即回收的旧代，才能把配额让给新代。
    if (CountBindingsLocked() >= m_limits.max_bindings &&
        (!previous_binding || previous_binding->borrows != 0))
        return BindResult::err(MakeError(ErrorCode::Overloaded,
                                         "BindComponent: binding limit reached"));

    const auto generation = m_next_generation++;
    target.bindings.push_back(Binding{subject.id, generation, std::string(slot)});
    // 新代插入可能抛异常，必须成功后再摘旧代；换绑的两条记录位于不同目标。
    if (previous)
        RetireBindingLocked(*previous, *previous_binding, false);
    return BindResult::ok(generation);
}

void Registry::RetireBindingLocked(RegistryEntry& target, Binding& binding, bool drain) {
    binding.retiring = true;
    binding.drain_requested = drain;
    if (!drain && binding.borrows == 0)
        EraseBindingLocked(target, binding.subject_id, binding.generation);
}

void Registry::RetireSubjectBindingsLocked(const ObjectId& subject_id) {
    const bool component_subject = FindEntryLocked(subject_id)->kind == ObjectKind::Component;
    for (auto& target : m_entries) {
        auto& bindings = target.bindings;
        for (auto& binding : bindings) {
            if (binding.subject_id == subject_id) {
                if (component_subject && !binding.retiring)
                    binding.cleanup_dependency = true;
                binding.retiring = true;
                binding.drain_requested = false;
            }
        }
        bindings.erase(std::remove_if(bindings.begin(), bindings.end(), [&](const Binding& binding) {
                           return binding.subject_id == subject_id && binding.borrows == 0 && !binding.cleanup_dependency;
                       }),
                       bindings.end());
    }
}

void Registry::RetireTargetBindingsLocked(RegistryEntry& target) {
    auto& bindings = target.bindings;
    for (auto& binding : bindings)
        binding.retiring = true;
    bindings.erase(std::remove_if(bindings.begin(), bindings.end(), [](const Binding& binding) {
                       return !binding.drain_requested && binding.borrows == 0 && !binding.cleanup_dependency;
                   }),
                   bindings.end());
}

result<void> Registry::DrainSubjectBorrows(const ObjectId& subject_id,
                                           bbt::coroutine::Deadline deadline) {
    return _WaitCoDrain(m_borrow_waiters, [&] {
        for (const auto& target : m_entries) {
            for (const auto& binding : target.bindings) {
                if (binding.subject_id == subject_id && binding.borrows != 0)
                    return false;
            }
        }
        return true; }, deadline, "borrow wait did not complete");
}

void Registry::ReleaseBorrow(const ObjectId& component_id,
                             const ObjectId& subject_id,
                             std::uint64_t binding_generation) noexcept {
    std::unique_lock<std::mutex> lk(m_mtx);
    RegistryEntry* e = FindEntryLocked(component_id);
    if (!e)
        return;
    if (e->component_borrows > 0)
        --e->component_borrows;

    for (auto& b : e->bindings) {
        if (b.subject_id == subject_id && b.generation == binding_generation) {
            --b.borrows;
            if (b.retiring && !b.drain_requested && b.borrows == 0 && !b.cleanup_dependency)
                EraseBindingLocked(*e, subject_id, binding_generation);
            break;
        }
    }
    m_borrow_waiters.NotifyAll(std::move(lk), m_cv);
}

result<void> Registry::_WaitCoDrain(WaitList& waiters, const std::function<bool()>& ready,
                                    bbt::coroutine::Deadline deadline, const char* context) {
    if (!bbt::coroutine::IsInCoroutineContext())
        return result<void>::err(MakeError(ErrorCode::InvalidContext,
                                           "drain requires coroutine context"));
    bbt::coroutine::WaitOptions wait;
    wait.deadline = deadline;
    const auto waiter = bbt::coroutine::sync::CoWaiter::Create();
    const auto status = waiters.WaitUntilReady(m_mtx, waiter, ready, wait);
    if (status == bbt::coroutine::WaitStatus::Completed)
        return result<void>::ok();
    const auto code = status == bbt::coroutine::WaitStatus::TimedOut
                          ? ErrorCode::TimedOut
                      : status == bbt::coroutine::WaitStatus::Cancelled
                          ? ErrorCode::Cancelled
                          : ErrorCode::RuntimeUnavailable;
    return result<void>::err(MakeError(code, context));
}

result<void> Registry::WaitBorrowsDrained(
    std::string_view component_id, bbt::coroutine::Deadline deadline) {
    return _WaitCoDrain(m_borrow_waiters, [&] {
        const auto* e = FindEntryLocked(component_id);
        return !e || e->component_borrows == 0; }, deadline, "borrow wait did not complete");
}

result<void> Registry::WaitBindingDrained(
    const ObjectId& component_id, const ObjectId& subject_id,
    std::uint64_t binding_generation, bbt::coroutine::Deadline deadline) {
    return _WaitCoDrain(m_borrow_waiters, [&] {
        const auto* e = FindEntryLocked(component_id);
        if (!e)
            return true;
        for (const auto& b : e->bindings) {
            if (b.subject_id == subject_id && b.generation == binding_generation)
                return b.borrows == 0;
        }
        return true; }, deadline, "borrow wait did not complete");
}

bool Registry::EraseBindingLocked(RegistryEntry& target,
                                  const ObjectId& subject_id, std::uint64_t generation) {
    auto& bindings = target.bindings;
    auto binding = std::find_if(bindings.begin(), bindings.end(), [&](const Binding& b) {
        return b.generation == generation && b.subject_id == subject_id &&
               b.retiring && b.borrows == 0;
    });
    if (binding == bindings.end())
        return false;
    bindings.erase(binding);
    return true;
}

void Registry::AcquireBindingCallbackLocked(RegistryEntry& subject, std::uint64_t coroutine_id) {
    m_binding_callback_coroutines.insert(coroutine_id);
    ++subject.hooks_in_flight;
}

void Registry::ReleaseBindingCallback(const ObjectId& id, std::uint64_t coroutine_id) noexcept {
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        m_binding_callback_coroutines.erase(coroutine_id);
    }
    NoteHookSettled(id);
}

bool Registry::InBindingCallback(std::uint64_t coroutine_id) {
    std::lock_guard<std::mutex> lk(m_mtx);
    return m_binding_callback_coroutines.count(coroutine_id) != 0;
}

void Registry::NoteHookSettled(const ObjectId& id, bool stop_completed) noexcept {
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> w;
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        auto* e = FindEntryLocked(id);
        if (e && stop_completed)
            e->stop_hook_done = true;
        if (e && e->hooks_in_flight > 0)
            --e->hooks_in_flight;
        if (e && e->hooks_in_flight == 0)
            w = e->hook_settled_waiter;
    }
    /**
     * Notify 在锁外：Notify 链路会触发协程唤醒/驱动回调，不阻塞 Registry
     * 临界区；m_cv 唤醒宿主等待者同样不持锁（m_cv 谓词在锁内复查）。
     */
    m_cv.notify_all();
    if (w)
        w->Notify();
}

bool Registry::AcquireDispatchLease(const ObjectId& id) {
    std::lock_guard<std::mutex> lk(m_mtx);
    auto* e = FindEntryLocked(id);
    if (!e || e->gate_closed)
        return false;
    ++e->hooks_in_flight;
    return true;
}

Registry::ComponentDependencies Registry::ComponentDependenciesLocked() const {
    ComponentDependencies deps;
    for (const auto& target : m_entries) {
        if (target.kind != ObjectKind::Component)
            continue;
        deps.try_emplace(target.id);
        for (const auto& binding : target.bindings) {
            if (binding.retiring && !binding.cleanup_dependency)
                continue;
            const auto* subject = FindEntryLocked(binding.subject_id);
            if (subject && subject->kind == ObjectKind::Component)
                deps[subject->id].push_back(target.id);
        }
    }
    return deps;
}

bool Registry::HasComponentDependentsLocked(const RegistryEntry& target) const {
    return std::any_of(target.bindings.begin(), target.bindings.end(), [&](const Binding& binding) {
        if (binding.cleanup_dependency)
            return true;
        const auto* subject = FindEntryLocked(binding.subject_id);
        return !binding.retiring && subject && subject->kind == ObjectKind::Component;
    });
}

std::vector<ObjectId> Registry::ComponentStartOrderLocked(
    const ComponentDependencies& deps, bool* had_cycle) const {
    std::vector<ObjectId> order;
    /**
     * 0=未访，1=栈中，2=完成
     */
    std::unordered_map<ObjectId, int> mark;
    bool cycle = false;
    std::function<void(const ObjectId&)> dfs = [&](const ObjectId& id) {
        mark[id] = 1;
        for (const auto& d : deps.at(id)) {
            const auto it = mark.find(d);
            if (it != mark.end() && it->second == 2)
                continue;
            if (it != mark.end() && it->second == 1) {
                cycle = true;
                continue;
            }
            /**
             * 目标非组件（防御）
             */
            if (deps.count(d) == 0)
                continue;
            dfs(d);
        }
        mark[id] = 2;
        order.push_back(id);
    };
    for (const auto& e : m_entries) {
        if (e.kind != ObjectKind::Component)
            continue;
        if (mark.count(e.id) == 0)
            dfs(e.id);
    }
    if (had_cycle)
        *had_cycle = cycle;
    return order;
}

result<RegistryEntry*> Registry::BeginDestroyLocked(std::string_view id) {
    RegistryEntry* e = FindEntryLocked(id);
    if (!e)
        return result<RegistryEntry*>::err(MakeError(ErrorCode::NotFound,
                                                     "BeginDestroy: object not found"));

    // 已摘除的普通旧代只需排空借用；关闭依赖必须等使用者收口。
    if (e->kind == ObjectKind::Component &&
        std::any_of(e->bindings.begin(), e->bindings.end(),
                    [](const Binding& binding) { return !binding.retiring || binding.cleanup_dependency; }))
        return result<RegistryEntry*>::err(MakeError(ErrorCode::Overloaded,
                                                     "UninstallComponent: active bindings or cleanup dependencies remain"));
    if (e->op_in_progress)
        return result<RegistryEntry*>::err(MakeError(ErrorCode::Overloaded,
                                                     "BeginDestroy: operation in progress"));

    e->gate_closed = true;
    e->op_in_progress = true;
    return result<RegistryEntry*>::ok(e);
}

std::pair<std::shared_ptr<void>,
          ObserverEvents>
Registry::FinishDestroyLocked(RegistryEntry& e) {
    auto events = SetStateLocked(e, ObjectState::Destroyed);
    if (e.kind == ObjectKind::Component) {
        for (auto& target : m_entries) {
            auto& bindings = target.bindings;
            bindings.erase(std::remove_if(bindings.begin(), bindings.end(), [&](const Binding& binding) {
                               return binding.cleanup_dependency && binding.subject_id == e.id;
                           }),
                           bindings.end());
        }
    }
    m_index.erase(e.id);
    auto it = m_entries.begin();
    for (; it != m_entries.end(); ++it) {
        if (it->id == e.id)
            break;
    }
    std::shared_ptr<void> obj;
    if (it != m_entries.end()) {
        obj = std::move(it->object);
        m_entries.erase(it);
    }
    /**
     * deque 下标变化，整体重建索引。
     */
    m_index.clear();
    for (std::size_t i = 0; i < m_entries.size(); ++i)
        m_index.emplace(m_entries[i].id, i);
    return {std::move(obj), std::move(events)};
}

std::vector<CleanupInfo> Registry::PendingCleanup() const {
    std::lock_guard<std::mutex> lk(m_mtx);
    std::vector<CleanupInfo> out;
    for (const auto& e : m_entries) {
        const bool pending = e.component_borrows > 0 || e.exec_leases > 0 ||
                             e.hooks_in_flight > 0 ||
                             !e.destroy_phase.empty() ||
                             e.state == ObjectState::StopIncomplete ||
                             e.state == ObjectState::UninstallIncomplete;
        if (!pending)
            continue;
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
    /**
     * bounded ring：上限取 max_objects（超出丢弃并计 dropped）。
     */
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

/**
 * ---- BorrowImpl（ComponentHandle<C>::Borrow 的类型擦除半身）----
 *
 * 返回 CoComponent*（不擦为 void*）：派生类型 C 经
 * CoComponent*→C* static_cast 完成指针调整，多继承布局下不产生错误地址。
 */

bool IsComponentBound(const ObjectHandle& object, std::uint64_t generation) noexcept {
    auto registry = object.m_reg.lock();
    if (!registry)
        return false;
    std::lock_guard<std::mutex> lk(registry->Mutex());
    const auto* entry = registry->FindEntryLocked(object.Id());
    return entry && entry->generation == object.Generation() &&
           std::any_of(entry->bindings.begin(), entry->bindings.end(), [&](const Binding& b) {
               return b.generation == generation && !b.retiring;
           });
}

result<std::pair<std::shared_ptr<void>, CoComponent*>> BorrowImpl(
    const ObjectHandle& object, std::uint64_t binding_generation) {
    using BorrowResult = result<std::pair<std::shared_ptr<void>, CoComponent*>>;
    auto reg = object.m_reg.lock();
    if (!reg)
        return BorrowResult::err(
            MakeError(ErrorCode::Closed, "Borrow: registry closed"));
    std::lock_guard<std::mutex> lk(reg->Mutex());
    RegistryEntry* e = reg->FindEntryLocked(object.m_id);
    if (!e)
        return BorrowResult::err(
            MakeError(ErrorCode::Closed, "Borrow: object destroyed"));
    if (e->generation != object.m_generation)
        return BorrowResult::err(
            MakeError(ErrorCode::Closed, "Borrow: generation stale"));
    /**
     * subject 取自命中的边，确保借用计数归属于句柄对应的那一代绑定。
     */
    Binding* edge = nullptr;
    for (auto& b : e->bindings) {
        if (b.generation == binding_generation) {
            edge = &b;
            break;
        }
    }
    if (!edge || edge->retiring)
        return BorrowResult::err(
            MakeError(ErrorCode::Closed, "Borrow: binding invalidated"));

    if (e->gate_closed)
        return BorrowResult::err(
            MakeError(ErrorCode::Closed, "Borrow: component gate closed"));

    /**
     * 装配窗口由 subject 状态推导：subject 正处于 Installing（OnInstall
     * 执行中）时允许借 Installed 依赖做纯内存装配。
     */
    const RegistryEntry* subj = reg->FindEntryLocked(edge->subject_id);
    const bool installing_borrow =
        subj != nullptr && subj->state == ObjectState::Installing;

    /**
     * 对象状态门：Running 可借；OnInstall 装配窗口内 Installed 可借。
     */
    const ObjectState s = e->state;
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
            /**
             * 未 Running 且非装配窗口
             */
            code = ErrorCode::Overloaded;
        return BorrowResult::err(MakeError(code,
                                           "Borrow: component not borrowable in current state"));
    }
    auto lease = std::make_shared<BindingLease>();
    lease->reg_w = reg->weak_from_this();
    lease->owner = e->object;
    lease->component_id = e->id;
    lease->subject_id = edge->subject_id;
    lease->binding_generation = binding_generation;
    ++e->component_borrows;
    ++edge->borrows;
    return BorrowResult::ok(
        std::make_pair(std::shared_ptr<void>(std::move(lease)),
                       static_cast<CoComponent*>(e->object.get())));
}

} // namespace detail
} // namespace bbt::framework
