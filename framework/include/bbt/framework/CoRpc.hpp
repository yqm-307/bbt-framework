#pragma once
// service-actor/v3：位置参数 RPC 公共面。
//
// CoRpcReq/CoRpcResp 只拥有 payload 与错误结果；线格式不暴露给业务。
// 默认位置参数 codec 对应逻辑 field 1..N；ProtoCodec<T> 是未来
// protobuf Message/MessageLite 的独立适配 seam，不引入 protobuf 头。

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <bbt/framework/Result.hpp>

namespace bbt::framework {

namespace rpc_detail {

enum class WireType : std::uint8_t {
    Bool = 1, Int = 2, Uint = 3, Float = 4, String = 5, Bytes = 6,
};

template <class T>
struct ProtoCodec {
    static constexpr bool kDefined = false;
};

template <class T>
inline constexpr bool is_proto_codec_v = ProtoCodec<T>::kDefined;

template <class T>
using Decay = std::decay_t<T>;

template <class T>
inline constexpr bool is_bytes_v =
    std::is_same_v<Decay<T>, std::vector<std::uint8_t>>;

template <class T>
inline constexpr WireType wire_type_v = [] {
    using D = Decay<T>;
    if constexpr (std::is_same_v<D, bool>) return WireType::Bool;
    else if constexpr (std::is_integral_v<D> && std::is_signed_v<D>)
        return WireType::Int;
    else if constexpr (std::is_integral_v<D> && std::is_unsigned_v<D>)
        return WireType::Uint;
    else if constexpr (std::is_floating_point_v<D>) return WireType::Float;
    else if constexpr (std::is_same_v<D, std::string>) return WireType::String;
    else if constexpr (is_bytes_v<D>) return WireType::Bytes;
    else return WireType::String;
}();

template <class T>
inline constexpr bool scalar_supported_v =
    std::is_same_v<Decay<T>, bool> ||
    (std::is_integral_v<Decay<T>> && !std::is_same_v<Decay<T>, bool>) ||
    std::is_floating_point_v<Decay<T>> ||
    std::is_same_v<Decay<T>, std::string> || is_bytes_v<T>;

inline void PutU32(std::vector<std::uint8_t>& out, std::uint32_t v) {
    for (int i = 0; i < 4; ++i)
        out.push_back(static_cast<std::uint8_t>(v >> (8 * i)));
}

inline bool TakeU32(const std::vector<std::uint8_t>& in, std::size_t& pos,
                   std::uint32_t& v) {
    if (pos > in.size() || in.size() - pos < 4) return false;
    v = static_cast<std::uint32_t>(in[pos]) |
        (static_cast<std::uint32_t>(in[pos + 1]) << 8) |
        (static_cast<std::uint32_t>(in[pos + 2]) << 16) |
        (static_cast<std::uint32_t>(in[pos + 3]) << 24);
    pos += 4;
    return true;
}

inline Error Bad(const char* what) {
    return MakeError(ErrorCode::ProtocolError,
                     std::string("rpc payload: ") + what);
}

template <class T>
result<std::vector<std::uint8_t>> EncodeOne(const T& value) {
    using D = Decay<T>;
    static_assert(scalar_supported_v<D>,
                  "CoRpc positional argument has no scalar codec");
    std::vector<std::uint8_t> out;
    constexpr auto wt = wire_type_v<D>;
    if constexpr (wt == WireType::Bool) {
        out.push_back(value ? 1 : 0);
    } else if constexpr (wt == WireType::Int || wt == WireType::Uint) {
        const std::uint64_t u = static_cast<std::uint64_t>(value);
        for (std::size_t i = 0; i < sizeof(D); ++i)
            out.push_back(static_cast<std::uint8_t>(u >> (8 * i)));
    } else if constexpr (wt == WireType::Float) {
        if constexpr (sizeof(D) == 4) {
            std::uint32_t u = 0;
            std::memcpy(&u, &value, sizeof(u));
            for (int i = 0; i < 4; ++i)
                out.push_back(static_cast<std::uint8_t>(u >> (8 * i)));
        } else {
            std::uint64_t u = 0;
            std::memcpy(&u, &value, sizeof(u));
            for (int i = 0; i < 8; ++i)
                out.push_back(static_cast<std::uint8_t>(u >> (8 * i)));
        }
    } else if constexpr (wt == WireType::String) {
        out.insert(out.end(), value.begin(), value.end());
    } else {
        out.insert(out.end(), value.begin(), value.end());
    }
    return result<std::vector<std::uint8_t>>::ok(std::move(out));
}

template <class T>
result<T> DecodeOne(std::uint8_t type, const std::uint8_t* data,
                   std::size_t len) {
    using D = Decay<T>;
    static_assert(scalar_supported_v<D>,
                  "CoRpc positional argument has no scalar codec");
    if (type != static_cast<std::uint8_t>(wire_type_v<D>))
        return result<T>::err(MakeError(ErrorCode::TypeMismatch,
                                        "rpc payload argument type mismatch"));
    if constexpr (std::is_same_v<D, bool>) {
        if (len != 1 || data[0] > 1)
            return result<T>::err(Bad("invalid bool"));
        return result<T>::ok(data[0] != 0);
    } else if constexpr (std::is_integral_v<D> && !std::is_same_v<D, bool>) {
        if (len != sizeof(D)) return result<T>::err(Bad("invalid integer length"));
        std::uint64_t u = 0;
        for (std::size_t i = 0; i < len; ++i)
            u |= static_cast<std::uint64_t>(data[i]) << (8 * i);
        if constexpr (std::is_signed_v<D>) {
            if (len < 8 && (u & (std::uint64_t{1} << (len * 8 - 1))))
                u |= ~std::uint64_t{0} << (len * 8);
            return result<T>::ok(static_cast<D>(static_cast<std::int64_t>(u)));
        } else {
            return result<T>::ok(static_cast<D>(u));
        }
    } else if constexpr (std::is_floating_point_v<D>) {
        if (len != sizeof(D)) return result<T>::err(Bad("invalid float length"));
        D out{};
        if constexpr (sizeof(D) == 4) {
            std::uint32_t u = 0;
            for (int i = 0; i < 4; ++i) u |= static_cast<std::uint32_t>(data[i]) << (8 * i);
            std::memcpy(&out, &u, sizeof(out));
        } else {
            std::uint64_t u = 0;
            for (int i = 0; i < 8; ++i) u |= static_cast<std::uint64_t>(data[i]) << (8 * i);
            std::memcpy(&out, &u, sizeof(out));
        }
        return result<T>::ok(out);
    } else if constexpr (std::is_same_v<D, std::string>) {
        return result<T>::ok(std::string(reinterpret_cast<const char*>(data), len));
    } else {
        return result<T>::ok(std::vector<std::uint8_t>(data, data + len));
    }
}

template <class T>
result<T> DecodeAt(const std::vector<std::uint8_t>& in, std::size_t wanted) {
    std::size_t pos = 0;
    std::uint32_t count = 0;
    if (!TakeU32(in, pos, count))
        return result<T>::err(Bad("header truncated"));
    if (wanted >= count)
        return result<T>::err(MakeError(
            ErrorCode::InvalidArgument, "rpc payload argument index out of range"));

    std::optional<T> value;
    for (std::size_t index = 0; index < count; ++index) {
        if (pos >= in.size())
            return result<T>::err(Bad("missing argument"));
        const std::uint8_t type = in[pos++];
        std::uint32_t len = 0;
        if (!TakeU32(in, pos, len) || len > in.size() - pos)
            return result<T>::err(Bad("truncated argument"));
        if (index == wanted) {
            auto decoded = DecodeOne<T>(type, in.data() + pos, len);
            if (!decoded)
                return result<T>::err(std::move(decoded.error()));
            value = std::move(decoded.value());
        }
        pos += len;
    }
    if (pos != in.size())
        return result<T>::err(Bad("trailing garbage"));
    if (!value.has_value())
        return result<T>::err(Bad("missing argument"));
    return result<T>::ok(std::move(*value));
}

template <class... Ts, std::size_t... I>
result<std::tuple<Ts...>> DecodeTuple(const std::vector<std::uint8_t>& in,
                                      std::index_sequence<I...>) {
    std::size_t pos = 0;
    std::uint32_t count = 0;
    if (!TakeU32(in, pos, count))
        return result<std::tuple<Ts...>>::err(Bad("header truncated"));
    if (count != sizeof...(Ts))
        return result<std::tuple<Ts...>>::err(MakeError(
            ErrorCode::InvalidArgument, "rpc payload argument count mismatch"));
    std::tuple<Ts...> out;
    result<void> status = result<void>::ok();
    auto one = [&](auto index) {
        constexpr std::size_t N = decltype(index)::value;
        using T = std::tuple_element_t<N, std::tuple<Ts...>>;
        if (!status) return;
        if (pos >= in.size()) { status = result<void>::err(Bad("missing argument")); return; }
        const std::uint8_t type = in[pos++];
        std::uint32_t len = 0;
        if (!TakeU32(in, pos, len) || len > in.size() - pos) {
            status = result<void>::err(Bad("truncated argument"));
            return;
        }
        auto v = DecodeOne<T>(type, in.data() + pos, len);
        pos += len;
        if (!v) { status = result<void>::err(std::move(v.error())); return; }
        std::get<N>(out) = std::move(v.value());
    };
    (one(std::integral_constant<std::size_t, I>{}), ...);
    if (!status) return result<std::tuple<Ts...>>::err(std::move(status.error()));
    if (pos != in.size()) return result<std::tuple<Ts...>>::err(Bad("trailing garbage"));
    return result<std::tuple<Ts...>>::ok(std::move(out));
}

template <class Tuple, std::size_t... I>
result<std::vector<std::uint8_t>> EncodeTuple(const Tuple& values,
                                              std::index_sequence<I...>) {
    std::vector<std::uint8_t> out;
    PutU32(out, sizeof...(I));
    result<void> status = result<void>::ok();
    auto one = [&](auto index) {
        constexpr std::size_t N = decltype(index)::value;
        if (!status) return;
        auto encoded = EncodeOne(std::get<N>(values));
        if (!encoded) { status = result<void>::err(std::move(encoded.error())); return; }
        out.push_back(static_cast<std::uint8_t>(wire_type_v<std::tuple_element_t<N, Tuple>>));
        PutU32(out, static_cast<std::uint32_t>(encoded.value().size()));
        out.insert(out.end(), encoded.value().begin(), encoded.value().end());
    };
    (one(std::integral_constant<std::size_t, I>{}), ...);
    if (!status) return result<std::vector<std::uint8_t>>::err(std::move(status.error()));
    return result<std::vector<std::uint8_t>>::ok(std::move(out));
}

} // namespace rpc_detail

inline constexpr std::string_view kCoRpcPositionalSchema = "bbt.rpc.positional/v1";

class CoRpcReq {
public:
    CoRpcReq() = default;
    explicit CoRpcReq(std::vector<std::uint8_t> payload)
        : m_payload(std::move(payload)) {}

    const std::vector<std::uint8_t>& payload() const noexcept { return m_payload; }

    // 对称编码工厂：caller 不经 CoRpcResp 借道构造请求。
    // 编码失败以 result<CoRpcReq> 返回，与 Parse 侧同一错误体系。
    template <class T>
    static result<CoRpcReq> From(const T& value) {
        if constexpr (rpc_detail::is_proto_codec_v<T>) {
            auto encoded = rpc_detail::ProtoCodec<T>::Encode(value);
            if (!encoded)
                return result<CoRpcReq>::err(std::move(encoded.error()));
            return result<CoRpcReq>::ok(CoRpcReq(std::move(encoded.value())));
        } else {
            auto encoded = rpc_detail::EncodeTuple(std::tuple<T>{value},
                                                   std::index_sequence<0>{});
            if (!encoded)
                return result<CoRpcReq>::err(std::move(encoded.error()));
            return result<CoRpcReq>::ok(CoRpcReq(std::move(encoded.value())));
        }
    }

    template <class... Ts>
    static result<CoRpcReq> From(const std::tuple<Ts...>& values) {
        auto encoded = rpc_detail::EncodeTuple(values,
                                               std::index_sequence_for<Ts...>{});
        if (!encoded)
            return result<CoRpcReq>::err(std::move(encoded.error()));
        return result<CoRpcReq>::ok(CoRpcReq(std::move(encoded.value())));
    }

    // 可变参数便捷形：From(v0, v1, ...) ≡ From(std::tuple{v0, v1, ...})，
    // 业务不必手写 std::tuple。仅在 ≥2 个参数时启用——单参 From(v)
    // 走上面的 T 版本（保留 proto 类型分流）。
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
                auto tup = rpc_detail::DecodeTuple<Ts...>(m_payload,
                    std::index_sequence_for<Ts...>{});
                if (!tup)
                    return result<T>::err(std::move(tup.error()));
                return result<T>::ok(std::move(std::get<0>(tup.value())));
            }
        } else {
            return rpc_detail::DecodeTuple<Ts...>(m_payload,
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
            auto encoded = rpc_detail::EncodeTuple(std::tuple<T>{value},
                                                    std::index_sequence<0>{});
            if (!encoded) return Error(std::move(encoded.error()));
            return FromPayload(std::move(encoded.value()));
        }
    }

    template <class... Ts>
    static CoRpcResp From(const std::tuple<Ts...>& values) {
        auto encoded = rpc_detail::EncodeTuple(values,
                                               std::index_sequence_for<Ts...>{});
        if (!encoded) return Error(std::move(encoded.error()));
        return FromPayload(std::move(encoded.value()));
    }

    // 可变参数便捷形：From(v0, v1, ...) ≡ From(std::tuple{v0, v1, ...})。
    // 仅在 ≥2 个参数时启用——单参 From(v) 走上面的 T 版本（proto 分流）。
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
