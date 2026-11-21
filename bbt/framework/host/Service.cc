/**
 * framework 受管对象基类实现：身份观察与当前绑定查询。
 */

#include <mutex>

#include <bbt/framework/Actor.hpp>
#include <bbt/framework/Component.hpp>
#include <bbt/framework/CoService.hpp>
#include <bbt/framework/internal/Registry.hpp>
#include <bbt/framework/internal/ServiceBase.hpp>

namespace bbt::framework {

ObjectInfo detail::ObjectBase::_ObjectInfo() const {
    auto r = m_handle.Info();
    if (r)
        return r.value();
    /**
     * 对象已销毁/失效：返回带原 id/generation 的最小快照。
     */
    ObjectInfo info;
    info.id = m_handle.Id();
    info.generation = m_handle.Generation();
    info.state = ObjectState::Invalidated;
    info.observed_at = std::chrono::system_clock::now();
    return info;
}

result<void> detail::ObjectBase::_CheckGetComponentContext() {
    if (!bbt::coroutine::IsInCoroutineContext())
        return result<void>::err(MakeError(
            ErrorCode::InvalidContext, "GetComponent requires coroutine context"));
    return result<void>::ok();
}

result<detail::ComponentBindingSnapshot>
detail::ObjectBase::_GetComponentLookup(const std::type_info& type,
                                        std::string_view name) const {
    if (auto registry = m_handle.m_reg.lock()) {
        std::lock_guard<std::mutex> lk(registry->Mutex());
        auto [component, binding] = registry->FindBindingLocked(m_handle.Id(), type, name);
        if (component)
            return result<ComponentBindingSnapshot>::ok(
                registry->SnapshotBindingLocked(*component, binding->generation));
    }
    return result<ComponentBindingSnapshot>::err(MakeError(
        ErrorCode::NotFound,
        "GetComponent: component '" + std::string(name) + "' is not bound"));
}

ObjectInfo CoComponent::Info() const {
    return _ObjectInfo();
}

ObjectInfo CoServiceBase::Info() const {
    return _ObjectInfo();
}

ActorInfo CoActor::Info() const {
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
