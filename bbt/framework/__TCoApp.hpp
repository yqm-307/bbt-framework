#pragma once

namespace bbt::framework {

template <class C>
bbt_co_async(result<void>) CoApp::CoInstallComponent(
    std::string name, typename C::Options options,
    const ComponentOptions& placement, const LifecycleOptions& life) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<void>::err(std::move(ctx.error()));
    static_assert(std::is_base_of_v<CoComponent, C>,
                  "CoInstallComponent: C must derive from CoComponent");
    static_assert(!std::is_default_constructible_v<C>,
                  "CoInstallComponent: C constructor must accept only ConstructKey");
    static_assert(!std::is_destructible_v<C>,
                  "CoInstallComponent: C destructor must be protected or private");
    return _InstallComponentImpl(
        typeid(C), std::move(name), placement, life,
        [opts = std::move(options)](detail::ConstructKey k) mutable
            -> CoComponent* {
            return detail::Registry::NewManaged<C>(k, std::move(opts));
        },
        [](CoComponent* p) { detail::Registry::DeleteManaged<C>(
                                 static_cast<C*>(p)); });
}

template <class C>
bbt_co_async(result<void>) CoApp::CoUninstallComponent(
    std::string_view name, const ComponentOptions& placement,
    const LifecycleOptions& life) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<void>::err(std::move(ctx.error()));
    static_assert(std::is_base_of_v<CoComponent, C>,
                  "CoUninstallComponent: C must derive from CoComponent");
    return _UninstallComponentImpl(typeid(C), name, placement, life);
}

template <class S>
bbt_co_async(result<void>) CoApp::CoInstallService(ServiceOptions options) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<void>::err(std::move(ctx.error()));
    static_assert(std::is_base_of_v<CoServiceBase, S>,
                  "CoInstallService: S must derive from CoService<...>");
    static_assert(!std::is_default_constructible_v<S>,
                  "CoInstallService: S constructor must accept only ConstructKey");
    static_assert(!std::is_destructible_v<S>,
                  "CoInstallService: S destructor must be protected or private");

    if (auto r = options.Validate(); !r)
        return r;

    return _InstallService(
        typeid(S), CoServiceStaticName<S>::Get(), std::move(options),
        [] { return BuildMethodTable<S>(); },
        [](detail::ConstructKey key) -> CoServiceBase* {
            return detail::Registry::NewManaged<S>(key);
        },
        [](CoServiceBase* service) {
            detail::Registry::DeleteManaged<S>(static_cast<S*>(service));
        });
}

template <class S>
bbt_co_async(result<void>) CoApp::CoUninstallService(const LifecycleOptions& life) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<void>::err(std::move(ctx.error()));
    static_assert(std::is_base_of_v<CoServiceBase, S>,
                  "CoUninstallService: S must derive from CoService<...>");
    std::string name = CoServiceStaticName<S>::Get();
    if (name.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                           "CoUninstallService: S must declare kServiceName"));
    return _UninstallServiceImpl(name, life);
}

template <class A>
bbt_co_async(result<void>) CoApp::CoRegisterActorType(ActorOptions options) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<void>::err(std::move(ctx.error()));
    static_assert(std::is_base_of_v<CoActor, A>,
                  "CoRegisterActorType: A must derive from CoActor");
    static_assert(!std::is_default_constructible_v<A>,
                  "CoRegisterActorType: A constructor must accept only ConstructKey");
    static_assert(!std::is_destructible_v<A>,
                  "CoRegisterActorType: A destructor must be protected or private");
    if (auto r = options.Validate(); !r)
        return r;

    std::string type = CoActorStaticName<A>::Get();
    if (type.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                           "CoInstallActor: A must declare a non-empty kActorType"));

    // 在有访问权的 Registry 中实例化构造与删除，保留原有编译期约束。
    [[maybe_unused]] constexpr auto make =
        static_cast<A* (*)(detail::ConstructKey)>(&detail::Registry::NewManaged<A>);
    [[maybe_unused]] constexpr auto destroy = &detail::Registry::DeleteManaged<A>;
    using ActorBase = decltype(static_cast<CoActor*>(std::declval<A*>()));
    static_assert(std::is_same_v<decltype(static_cast<A*>(std::declval<ActorBase>())), A*>);
    return _RegisterActorType(typeid(A), std::move(type));
}

template <class A>
bbt_co_async(result<void>) CoApp::CoInstallActor(ActorOptions options) {
    return CoRegisterActorType<A>(std::move(options));
}

template <class A>
bbt_co_async(result<void>) CoApp::CoUninstallActor(const LifecycleOptions& life) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<void>::err(std::move(ctx.error()));
    static_assert(std::is_base_of_v<CoActor, A>,
                  "CoUninstallActor: A must derive from CoActor");
    std::string type = CoActorStaticName<A>::Get();
    if (type.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                           "CoUninstallActor: A must declare kActorType"));
    return _UninstallActorTypeImpl(type, life);
}

template <class C>
result<ObjectHandle> CoApp::FindComponent(
    std::string_view name, const ComponentOptions& placement) const {
    static_assert(std::is_base_of_v<CoComponent, C>,
                  "FindComponent: C must derive from CoComponent");
    return _FindComponentImpl(typeid(C), name, placement);
}

template <class C>
bbt_co_async(result<ComponentHandle<C>>) CoApp::CoBindComponent(
    const ObjectHandle& subject, std::string_view name,
    const ComponentOptions& placement, const LifecycleOptions& life) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<ComponentHandle<C>>::err(std::move(ctx.error()));
    static_assert(std::is_base_of_v<CoComponent, C>,
                  "CoBindComponent: C must derive from CoComponent");
    (void)life;
    auto r = _BindComponentImpl(typeid(C), subject, name, name, placement, false);
    if (!r)
        return result<ComponentHandle<C>>::err(std::move(r.error()));
    return result<ComponentHandle<C>>::ok(
        detail::MakeComponentHandle<C>(std::move(r.value())));
}

template <class C>
bbt_co_async(result<ComponentHandle<C>>) CoApp::CoRebindComponent(
    const ObjectHandle& subject, std::string_view slot, std::string_view target_name,
    const ComponentOptions& placement) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<ComponentHandle<C>>::err(std::move(ctx.error()));
    static_assert(std::is_base_of_v<CoComponent, C>,
                  "CoRebindComponent: C must derive from CoComponent");
    auto r = _BindComponentImpl(typeid(C), subject, slot, target_name, placement, true);
    if (!r)
        return result<ComponentHandle<C>>::err(std::move(r.error()));
    return result<ComponentHandle<C>>::ok(
        detail::MakeComponentHandle<C>(std::move(r.value())));
}

template <class C>
bbt_co_async(result<void>) CoApp::CoUnbindComponent(
    const ObjectHandle& subject, std::string_view slot,
    const ComponentOptions& placement) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<void>::err(std::move(ctx.error()));
    static_assert(std::is_base_of_v<CoComponent, C>,
                  "CoUnbindComponent: C must derive from CoComponent");
    return _UnbindComponentImpl(typeid(C), subject, slot, placement, nullptr);
}

template <class C>
bbt_co_async(result<void>) CoApp::CoUnbindComponent(
    const ObjectHandle& subject, std::string_view name,
    const ComponentOptions& placement, const LifecycleOptions& life) {
    if (auto ctx = _CheckCoContext(); !ctx)
        return result<void>::err(std::move(ctx.error()));
    static_assert(std::is_base_of_v<CoComponent, C>,
                  "CoUnbindComponent: C must derive from CoComponent");
    return _UnbindComponentImpl(typeid(C), subject, name, placement, &life);
}

} // namespace bbt::framework
