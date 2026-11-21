#pragma once

namespace bbt::framework {

template <class C>
result<ComponentHandle<C>> CoActor::GetComponent(std::string_view name) const {
    static_assert(std::is_base_of_v<CoComponent, C>,
                  "GetComponent: C must derive from CoComponent");
    if (auto ctx = _CheckGetComponentContext(); !ctx)
        return result<ComponentHandle<C>>::err(std::move(ctx.error()));
    auto b = _GetComponentLookup(typeid(C), name);
    if (!b)
        return result<ComponentHandle<C>>::err(std::move(b.error()));
    return result<ComponentHandle<C>>::ok(
        detail::MakeComponentHandle<C>(std::move(b.value())));
}

template <class C>
result<ComponentBorrow<C>> CoActor::BorrowComponent(std::string_view name) const {
    auto handle = GetComponent<C>(name);
    if (!handle)
        return result<ComponentBorrow<C>>::err(std::move(handle.error()));
    return handle.value().Borrow();
}

template <class A, class Enable>
std::string CoActorStaticName<A, Enable>::Get() {
    return {};
}

template <class A>
std::string CoActorStaticName<A, std::void_t<decltype(A::kActorType)>>::Get() {
    return std::string(A::kActorType);
}

} // namespace bbt::framework
