#pragma once
/**
 * 位置参数 RPC 公共面（业务面）。
 *
 * CoRpcReq/CoRpcResp 只拥有 payload 与错误结果；线格式不暴露给业务。
 * 通用位置参数 codec（字节规则/schema/错误码）的唯一实现在
 * bbt::infra::rpc（bbt/infra/rpc/codec/PositionalCodec.hpp），framework 侧
 * 直接消费。rpc_detail 只保留 protobuf codec trait seam：ProtoCodec<T>
 * 继承 infra 默认 seam，业务对 `bbt::framework::rpc_detail::ProtoCodec<X>`
 * 的显式特化继续生效，仍不引入 protobuf 头。
 */

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <bbt/framework/Result.hpp>
#include <bbt/infra/rpc/codec/PositionalCodec.hpp>

namespace bbt::framework {

namespace rpc_detail {

/**
 * 通用位置参数 codec 的唯一实现在 bbt::infra::rpc::*（EncodeTuple/
 * DecodeTuple/DecodeAt 等）；本头不再转发名字，调用方直接消费 infra
 * 公开实现。result/Error 与 infra 同一类型（framework/Result.hpp
 * 即 infra 的别名）。这里只保留 protobuf codec trait seam：
 */

/**
 * protobuf codec trait seam：继承 infra 默认 seam，令既有显式特化
 * `template\<\> struct bbt::framework::rpc_detail::ProtoCodec<X> { ... }` 继续
 * 生效——用户特化仍是 framework 层的自由度，infra 不认识业务 proto 类型，
 * 也不新增第二 registry。
 */
template <class T> struct ProtoCodec : bbt::infra::rpc::ProtoCodec<T> {};

template <class T> inline constexpr bool is_proto_codec_v = ProtoCodec<T>::kDefined;

} // namespace rpc_detail

/**
 * schema 常量从 infra 唯一来源映射，避免出现第二个字面量副本。
 */
inline constexpr std::string_view kCoRpcPositionalSchema = bbt::infra::rpc::kRpcPositionalSchema;

class CoRpcReq {
public:
    CoRpcReq() = default;
    explicit CoRpcReq(std::vector<std::uint8_t> payload)
        : m_payload(std::move(payload)) {}

    const std::vector<std::uint8_t>& payload() const noexcept { return m_payload; }

    /**
     * 对称编码工厂：caller 不经 CoRpcResp 借道构造请求。
     * 编码失败以 result<CoRpcReq> 返回，与 Parse 侧同一错误体系。
     */
    template <class T>
    static result<CoRpcReq> From(const T& value) {
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
    static result<CoRpcReq> From(const std::tuple<Ts...>& values) {
        auto encoded = bbt::infra::rpc::EncodeTuple(values,
                                               std::index_sequence_for<Ts...>{});
        if (!encoded)
            return result<CoRpcReq>::err(std::move(encoded.error()));
        return result<CoRpcReq>::ok(CoRpcReq(std::move(encoded.value())));
    }

    /**
     * 可变参数便捷形：From(v0, v1, ...) ≡ From(std::tuple{v0, v1, ...})，
     * 业务不必手写 std::tuple。仅在 ≥2 个参数时启用——单参 From(v)
     * 走上面的 T 版本（保留 proto 类型分流）。
     */
    template <class... Ts,
              class = std::enable_if_t<(sizeof...(Ts) >= 2)>>
    static result<CoRpcReq> From(Ts&&... values) {
        return From(std::tuple<std::decay_t<Ts>...>{
            std::forward<Ts>(values)...});
    }

    template <class Proto>
    static result<CoRpcReq> FromProto(const Proto& value) {
        static_assert(rpc_detail::is_proto_codec_v<Proto>,
                      "specialize ProtoCodec<T> before FromProto<T>");
        auto encoded = rpc_detail::ProtoCodec<Proto>::Encode(value);
        if (!encoded)
            return result<CoRpcReq>::err(std::move(encoded.error()));
        return result<CoRpcReq>::ok(CoRpcReq(std::move(encoded.value())));
    }

    template <class... Ts>
    auto Parse() const {
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
    result<Proto> ParseProto() const {
        static_assert(rpc_detail::is_proto_codec_v<Proto>,
                      "specialize ProtoCodec<T> before ParseProto<T>");
        return rpc_detail::ProtoCodec<Proto>::Decode(m_payload);
    }

private:
    std::vector<std::uint8_t> m_payload;
};

class CoRpcResp {
public:
    using ErrorType = bbt::infra::Error;
    static CoRpcResp FromPayload(std::vector<std::uint8_t> payload) {
        CoRpcResp out;
        out.m_payload = std::move(payload);
        return out;
    }

    template <class T>
    static CoRpcResp From(const T& value) {
        if constexpr (rpc_detail::is_proto_codec_v<T>) {
            auto encoded = rpc_detail::ProtoCodec<T>::Encode(value);
            if (!encoded) return Error(std::move(encoded.error()));
            return FromPayload(std::move(encoded.value()));
        } else {
            auto encoded = bbt::infra::rpc::EncodeTuple(std::tuple<T>{value},
                                                    std::index_sequence<0>{});
            if (!encoded) return Error(std::move(encoded.error()));
            return FromPayload(std::move(encoded.value()));
        }
    }

    template <class... Ts>
    static CoRpcResp From(const std::tuple<Ts...>& values) {
        auto encoded = bbt::infra::rpc::EncodeTuple(values,
                                               std::index_sequence_for<Ts...>{});
        if (!encoded) return Error(std::move(encoded.error()));
        return FromPayload(std::move(encoded.value()));
    }

    /**
     * 可变参数便捷形：From(v0, v1, ...) ≡ From(std::tuple{v0, v1, ...})。
     * 仅在 ≥2 个参数时启用——单参 From(v) 走上面的 T 版本（proto 分流）。
     */
    template <class... Ts,
              class = std::enable_if_t<(sizeof...(Ts) >= 2)>>
    static CoRpcResp From(Ts&&... values) {
        return From(std::tuple<std::decay_t<Ts>...>{
            std::forward<Ts>(values)...});
    }

    template <class Proto>
    static CoRpcResp FromProto(const Proto& value) {
        static_assert(rpc_detail::is_proto_codec_v<Proto>,
                      "specialize ProtoCodec<T> before FromProto<T>");
        auto encoded = rpc_detail::ProtoCodec<Proto>::Encode(value);
        if (!encoded) return Error(std::move(encoded.error()));
        return FromPayload(std::move(encoded.value()));
    }

    static CoRpcResp Error(ErrorType error) {
        CoRpcResp out;
        out.m_error = std::move(error);
        return out;
    }

    bool ok() const noexcept { return !m_error.has_value(); }
    const ErrorType& error() const& { return *m_error; }
    const std::vector<std::uint8_t>& payload() const noexcept { return m_payload; }

private:
    std::vector<std::uint8_t> m_payload;
    std::optional<ErrorType> m_error;
};

} // namespace bbt::framework
