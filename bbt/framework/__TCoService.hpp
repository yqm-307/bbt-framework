#pragma once

namespace bbt::framework {

template <class... Components>
CoService<Components...>::CoService(detail::ConstructKey key) : CoServiceBase(key) {}

template <class... Components>
bool CoService<Components...>::_AllowsComponent(const std::type_info& t) const noexcept {
    return ((t == typeid(Components)) || ...);
}

template <class... Components>
template <class C>
result<ComponentHandle<C>> CoService<Components...>::GetComponent(
    std::string_view name) const {
    if (auto ctx = _CheckGetComponentContext(); !ctx)
        return result<ComponentHandle<C>>::err(std::move(ctx.error()));
    if (!_AllowsComponent(typeid(C)))
        return result<ComponentHandle<C>>::err(MakeError(
            ErrorCode::InvalidArgument,
            "GetComponent: component type is not declared in this Service’s allowed set"));
    auto b = _GetComponentLookup(typeid(C), name);
    if (!b)
        return result<ComponentHandle<C>>::err(std::move(b.error()));
    const auto& bound = b.value();
    return result<ComponentHandle<C>>::ok(
        detail::MakeComponentHandle<C>(bound.object, bound.binding_generation));
}

template <class... Components>
template <class C>
result<ComponentBorrow<C>> CoService<Components...>::BorrowComponent(
    std::string_view name) const {
    auto handle = GetComponent<C>(name);
    if (!handle)
        return result<ComponentBorrow<C>>::err(std::move(handle.error()));
    return handle.value().Borrow();
}

template <class S, class Enable>
std::string CoServiceStaticName<S, Enable>::Get() {
    return {};
}

template <class S>
std::string CoServiceStaticName<S, std::void_t<decltype(S::kServiceName)>>::Get() {
    return std::string(S::kServiceName);
}

} // namespace bbt::framework
