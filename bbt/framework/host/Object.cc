// framework v2 公共值层实现：Options 校验、ObjectHandle/ObserverToken 的
// Registry 委托、AppObjectAddress 校验、ComponentBorrow 释放。

#include <bbt/framework/Address.hpp>
#include <bbt/framework/Object.hpp>
#include <bbt/framework/Service.hpp>
#include <bbt/framework/internal/Registry.hpp>

namespace bbt::framework {

// ---- Options::Validate ----

result<void> ServiceOptions::Validate() const {
    if (m_max_inflight == 0)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "ServiceOptions: max_inflight must be a finite positive value"));
    return result<void>::ok();
}

result<void> ActorOptions::Validate() const {
    if (m_max_actors == 0)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "ActorOptions: max_actors must be a finite positive value"));
    if (m_mailbox_capacity == 0)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "ActorOptions: mailbox_capacity must be a finite positive value"));
    if (m_aggregate_queue_capacity == 0)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "ActorOptions: aggregate_queue_capacity must be a finite positive value"));
    return result<void>::ok();
}

result<void> AppOptions::Validate() const {
    if (m_max_objects == 0 || m_max_bindings == 0 || m_max_observers == 0 ||
        m_observer_queue_capacity == 0)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "AppOptions: SetLimits requires all four limits to be positive"));
    if (m_shutdown_budget.count() <= 0)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "AppOptions: SetShutdownBudget must be a finite positive value"));
    if (m_lifecycle_budget.count() <= 0)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "AppOptions: SetLifecycleBudget must be a finite positive value"));
    return result<void>::ok();
}

// ---- ObjectHandle ----

bool ObjectHandle::IsValid() const noexcept {
    auto reg = m_reg.lock();
    if (!reg) return false;
    return reg->HandleValid(*this);
}

result<ObjectInfo> ObjectHandle::Info() const {
    auto reg = m_reg.lock();
    if (!reg)
        return result<ObjectInfo>::err(MakeError(ErrorCode::Closed,
            "ObjectHandle: registry closed"));
    return reg->Info(*this);
}

result<MailBox> ObjectHandle::GetMailBox() const {
    auto reg = m_reg.lock();
    if (!reg)
        return result<MailBox>::err(MakeError(ErrorCode::Closed,
            "ObjectHandle: registry closed"));
    return reg->MailBoxOf(*this);
}

// ---- ObserverToken ----

void ObserverToken::Revoke() noexcept {
    if (m_id == 0) return;
    auto reg = m_reg.lock();
    const std::uint64_t id = m_id;
    m_id = 0;
    if (reg) reg->RevokeObserver(id);
}

bool ObserverToken::IsActive() const noexcept {
    if (m_id == 0) return false;
    auto reg = m_reg.lock();
    if (!reg) return false;
    return reg->ObserverActive(m_id);
}

// ---- ComponentBorrow ----

// _Release 为内联（见 Object.hpp）。

// ---- ComponentHandle ----
// IsValid/Borrow 需要 Registry 内部簿记；实现放在 Registry.cc
// （模板成员函数的显式实例化也在那里做——业务只经 GetComponent/BindComponent
// 获得实例化类型）。

// ---- AppObjectAddress ----

result<void> AppObjectAddress::Validate() const {
    switch (m_kind) {
    case Kind::Service:
        if (m_type.empty())
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                "AppObjectAddress: service name empty"));
        break;
    case Kind::Actor:
        if (m_type.empty() || m_key.empty())
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                "AppObjectAddress: actor type/key empty"));
        break;
    }
    return result<void>::ok();
}

} // namespace bbt::framework
