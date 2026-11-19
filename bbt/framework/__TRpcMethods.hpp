#pragma once

namespace bbt::framework {

template <auto Ptr>
constexpr detail::MethodDecl<Ptr> Method(const char* name) {
    return detail::MethodDecl<Ptr>{name};
}

template <auto Ptr, class Key, std::size_t Index>
constexpr detail::PositionalActorMethodDecl<Ptr, Key, Index>
ActorMethodAt(const char* name) {
    return detail::PositionalActorMethodDecl<Ptr, Key, Index>{name};
}

template <auto Ptr, class Req, class Resp>
constexpr detail::ProtoMethodDecl<Ptr, Req, Resp>
ProtoMethod(const char* name) {
    return detail::ProtoMethodDecl<Ptr, Req, Resp>{name};
}

template <class... D>
constexpr detail::MethodList<D...> RpcMethods(D... decls) {
    return detail::MethodList<D...>{std::tuple<D...>(decls...)};
}

} // namespace bbt::framework
