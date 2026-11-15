// framework v2 受管对象基类实现：身份观察面与 subject 绑定表。

#include <mutex>

#include <bbt/framework/Actor.hpp>
#include <bbt/framework/Component.hpp>
#include <bbt/framework/Service.hpp>
#include <bbt/framework/internal/Registry.hpp>
#include <bbt/framework/internal/ServiceBase.hpp>

namespace bbt::framework {

// ---- detail::ObjectBase ----

ObjectInfo detail::ObjectBase::_ObjectInfo() const {
    auto r = m_handle.Info();
    if (r) return r.value();
    // 对象已销毁/失效：返回带原 id/generation 的最小快照。
    ObjectInfo info;
    info.id = m_handle.Id();
    info.generation = m_handle.Generation();
    info.state = ObjectState::Invalidated;
    info.observed_at = std::chrono::system_clock::now();
    return info;
}

void detail::ObjectBase::_BindComponentEntry(const std::type_info& type,
                                             std::string name,
                                             ObjectHandle object,
                                             std::uint64_t gen) {
    std::lock_guard<std::mutex> lk(m_bound_mtx);
    for (auto& b : m_bound_components) {
        if (*b.type == type && b.name == name) {
            b.object = std::move(object);
            b.binding_generation = gen;
            return;
        }
    }
    m_bound_components.push_back(BoundComponent{
        &type, std::move(name), std::move(object), gen});
}

void detail::ObjectBase::_UnbindComponentEntry(const std::type_info& type,
                                               std::string_view name) {
    std::lock_guard<std::mutex> lk(m_bound_mtx);
    for (auto it = m_bound_components.begin();
         it != m_bound_components.end(); ++it) {
        if (*it->type == type && it->name == name) {
            m_bound_components.erase(it);
            return;
        }
    }
}

void detail::ObjectBase::_InvalidateComponentEntries() noexcept {
    std::lock_guard<std::mutex> lk(m_bound_mtx);
    m_bound_components.clear();
}

bool detail::ObjectBase::_FindBoundCopy(const std::type_info& type,
                                        std::string_view name,
                                        BoundComponent* out) const {
    std::lock_guard<std::mutex> lk(m_bound_mtx);
    for (const auto& b : m_bound_components) {
        if (*b.type == type && b.name == name) {
            *out = b;
            return true;
        }
    }
    return false;
}

// ---- CoComponent::Info ----

ObjectInfo v2::CoComponent::Info() const { return _ObjectInfo(); }

// ---- CoServiceBase ----

ObjectInfo v2::CoServiceBase::Info() const { return _ObjectInfo(); }

// ---- CoActor::Info ----

ActorInfo v2::CoActor::Info() const {
    ActorInfo out;
    auto r = _ObjectHandle().Info();
    if (r) {
        static_cast<ObjectInfo&>(out) = r.value();
    } else {
        out.id = _ObjectHandle().Id();
        out.generation = _ObjectHandle().Generation();
        out.state = ObjectState::Invalidated;
        out.observed_at = std::chrono::system_clock::now();
    }
    out.actor_id = m_actor_id;
    out.mailbox = m_mailbox;
    return out;
}

} // namespace bbt::framework
