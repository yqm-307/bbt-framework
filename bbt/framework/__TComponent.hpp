#pragma once

namespace bbt::framework {

template <class C>
result<ComponentHandle<C>> CoComponent::GetComponent(
    std::string_view name) const {
    static_assert(std::is_base_of_v<CoComponent, C>,
                  "GetComponent: C must derive from CoComponent");
    if (auto ctx = _CheckGetComponentContext(); !ctx)
        return result<ComponentHandle<C>>::err(std::move(ctx.error()));
    auto b = _GetComponentLookup(typeid(C), name);
    if (!b)
        return result<ComponentHandle<C>>::err(std::move(b.error()));
    const auto& bound = b.value();
    return result<ComponentHandle<C>>::ok(
        detail::MakeComponentHandle<C>(bound.object, bound.binding_generation));
}

template <class C>
result<ComponentBorrow<C>> CoComponent::BorrowComponent(std::string_view name) const {
    auto handle = GetComponent<C>(name);
    if (!handle)
        return result<ComponentBorrow<C>>::err(std::move(handle.error()));
    return handle.value().Borrow();
}

} // namespace bbt::framework
