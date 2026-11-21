#pragma once

namespace bbt::framework {

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
template <class C>
ComponentHandle<C> MakeComponentHandle(ComponentBindingSnapshot binding) {
    return ComponentHandle<C>(std::move(binding));
}
} // namespace detail

template <class C>
ComponentBorrow<C>::~ComponentBorrow() noexcept {
    _Release();
}

template <class C>
C& ComponentBorrow<C>::Get() const noexcept {
    return *m_obj;
}

template <class C>
ComponentBorrow<C>::ComponentBorrow(std::shared_ptr<void> binding_lease, C* obj)
    : m_binding(std::move(binding_lease)), m_obj(obj) {}

template <class C>
ObjectHandle ComponentHandle<C>::Object() const {
    return m_object;
}

template <class C>
bool ComponentHandle<C>::IsValid() const noexcept {
    return m_object.IsValid();
}

template <class C>
bool ComponentHandle<C>::IsBound() const noexcept {
    return detail::IsComponentBound(m_object, m_binding_gen);
}

template <class C>
std::shared_ptr<C> ComponentHandle<C>::lock() const noexcept {
    return std::static_pointer_cast<C>(m_instance.lock());
}

template <class C>
ComponentHandle<C>::ComponentHandle(detail::ComponentBindingSnapshot binding)
    : m_object(std::move(binding.object)),
      m_instance(std::move(binding.instance)),
      m_binding_gen(binding.binding_generation) {}

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
    auto r = detail::BorrowImpl(m_object, m_binding_gen);
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
