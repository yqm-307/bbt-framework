#pragma once

namespace bbt::framework {

template <class T>
result<CoRpcReq> CoRpcReq::From(const T& value) {
    if constexpr (rpc_detail::is_proto_codec_v<T>) {
        auto encoded = rpc_detail::ProtoCodec<T>::Encode(value);
        if (!encoded)
            return result<CoRpcReq>::err(std::move(encoded.error()));
        return result<CoRpcReq>::ok(CoRpcReq(std::move(encoded.value())));
    } else {
        auto encoded = bbt::infra::rpc::EncodeTuple(std::tuple<T>{value},
                                                    std::index_sequence<0>{});
        if (!encoded)
            return result<CoRpcReq>::err(std::move(encoded.error()));
        return result<CoRpcReq>::ok(CoRpcReq(std::move(encoded.value())));
    }
}

template <class... Ts>
result<CoRpcReq> CoRpcReq::From(const std::tuple<Ts...>& values) {
    auto encoded = bbt::infra::rpc::EncodeTuple(values,
                                                std::index_sequence_for<Ts...>{});
    if (!encoded)
        return result<CoRpcReq>::err(std::move(encoded.error()));
    return result<CoRpcReq>::ok(CoRpcReq(std::move(encoded.value())));
}

template <class... Ts,
          class>
result<CoRpcReq> CoRpcReq::From(Ts&&... values) {
    return From(std::tuple<std::decay_t<Ts>...>{
        std::forward<Ts>(values)...});
}

template <class Proto>
result<CoRpcReq> CoRpcReq::FromProto(const Proto& value) {
    static_assert(rpc_detail::is_proto_codec_v<Proto>,
                  "specialize ProtoCodec<T> before FromProto<T>");
    auto encoded = rpc_detail::ProtoCodec<Proto>::Encode(value);
    if (!encoded)
        return result<CoRpcReq>::err(std::move(encoded.error()));
    return result<CoRpcReq>::ok(CoRpcReq(std::move(encoded.value())));
}

template <class... Ts>
auto CoRpcReq::Parse() const {
    static_assert(sizeof...(Ts) > 0, "CoRpcReq::Parse requires a type");
    if constexpr (sizeof...(Ts) == 1) {
        using T = std::tuple_element_t<0, std::tuple<Ts...>>;
        if constexpr (rpc_detail::is_proto_codec_v<T>)
            return rpc_detail::ProtoCodec<T>::Decode(m_payload);
        else {
            auto tup = bbt::infra::rpc::DecodeTuple<Ts...>(m_payload,
                                                           std::index_sequence_for<Ts...>{});
            if (!tup)
                return result<T>::err(std::move(tup.error()));
            return result<T>::ok(std::move(std::get<0>(tup.value())));
        }
    } else {
        return bbt::infra::rpc::DecodeTuple<Ts...>(m_payload,
                                                   std::index_sequence_for<Ts...>{});
    }
}

template <class Proto>
result<Proto> CoRpcReq::ParseProto() const {
    static_assert(rpc_detail::is_proto_codec_v<Proto>,
                  "specialize ProtoCodec<T> before ParseProto<T>");
    return rpc_detail::ProtoCodec<Proto>::Decode(m_payload);
}

template <class T>
CoRpcResp CoRpcResp::From(const T& value) {
    if constexpr (rpc_detail::is_proto_codec_v<T>) {
        auto encoded = rpc_detail::ProtoCodec<T>::Encode(value);
        if (!encoded)
            return Error(std::move(encoded.error()));
        return FromPayload(std::move(encoded.value()));
    } else {
        auto encoded = bbt::infra::rpc::EncodeTuple(std::tuple<T>{value},
                                                    std::index_sequence<0>{});
        if (!encoded)
            return Error(std::move(encoded.error()));
        return FromPayload(std::move(encoded.value()));
    }
}

template <class... Ts>
CoRpcResp CoRpcResp::From(const std::tuple<Ts...>& values) {
    auto encoded = bbt::infra::rpc::EncodeTuple(values,
                                                std::index_sequence_for<Ts...>{});
    if (!encoded)
        return Error(std::move(encoded.error()));
    return FromPayload(std::move(encoded.value()));
}

template <class... Ts,
          class>
CoRpcResp CoRpcResp::From(Ts&&... values) {
    return From(std::tuple<std::decay_t<Ts>...>{
        std::forward<Ts>(values)...});
}

template <class Proto>
CoRpcResp CoRpcResp::FromProto(const Proto& value) {
    static_assert(rpc_detail::is_proto_codec_v<Proto>,
                  "specialize ProtoCodec<T> before FromProto<T>");
    auto encoded = rpc_detail::ProtoCodec<Proto>::Encode(value);
    if (!encoded)
        return Error(std::move(encoded.error()));
    return FromPayload(std::move(encoded.value()));
}

} // namespace bbt::framework
