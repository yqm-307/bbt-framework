#pragma once
/**
 * @brief 构造 RPC 请求和回复，并按位置参数或 protobuf 类型读写内容。
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
 * @brief 默认使用 infra 的编解码器。业务可为自己的 protobuf 类型特化 ProtoCodec。
 */
template <class T> struct ProtoCodec : bbt::infra::rpc::ProtoCodec<T> {};

template <class T> inline constexpr bool is_proto_codec_v = ProtoCodec<T>::kDefined;

} // namespace rpc_detail

/**
 * 位置参数请求和回复使用同一个 schema 标识。
 */
inline constexpr std::string_view kCoRpcPositionalSchema = bbt::infra::rpc::kRpcPositionalSchema;

class CoRpcReq {
public:
    CoRpcReq() = default;
    explicit CoRpcReq(std::vector<std::uint8_t> payload)
        : m_payload(std::move(payload)) {}

    const std::vector<std::uint8_t>& payload() const noexcept { return m_payload; }

    /**
     * @brief 把值编码为请求；编码失败时返回错误。
     */
    template <class T>
    static result<CoRpcReq> From(const T& value);

    template <class... Ts>
    static result<CoRpcReq> From(const std::tuple<Ts...>& values);

    /**
     * @brief 将两个或更多参数按顺序编码，等同于传入一个元组。
     */
    template <class... Ts,
              class = std::enable_if_t<(sizeof...(Ts) >= 2)>>
    static result<CoRpcReq> From(Ts&&... values);

    template <class Proto>
    static result<CoRpcReq> FromProto(const Proto& value);

    template <class... Ts>
    auto Parse() const;

    template <class Proto>
    result<Proto> ParseProto() const;

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
    static CoRpcResp From(const T& value);

    template <class... Ts>
    static CoRpcResp From(const std::tuple<Ts...>& values);

    /**
     * @brief 将两个或更多返回值按顺序编码，等同于传入一个元组。
     */
    template <class... Ts,
              class = std::enable_if_t<(sizeof...(Ts) >= 2)>>
    static CoRpcResp From(Ts&&... values);

    template <class Proto>
    static CoRpcResp FromProto(const Proto& value);

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

#include <bbt/framework/__TCoRpc.hpp>
