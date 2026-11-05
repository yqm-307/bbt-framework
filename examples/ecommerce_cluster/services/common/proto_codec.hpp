#pragma once
// 业务 payload codec：wire 字节即 protobuf 序列化。
//
// 三个服务共用同一口径：schema 由生成物的 descriptor 推导，代码里不手写
// schema 字符串，也不复制第二套字节规则。特化必须留在 framework 的
// rpc_detail 命名空间（那是框架公开的 trait seam），所以本头是唯一允许
// 触碰 rpc_detail 的业务文件。

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include <bbt/example/ecommerce/v1/ecommerce.pb.h>

#include <bbt/framework/CoRpc.hpp>
#include <bbt/framework/Result.hpp>

namespace bbt::framework::rpc_detail {

namespace ecommerce_codec {

// 每个消息的编解码只有一份实现；具体消息用下面的显式特化接进 ProtoCodec。
template <class T>
struct MessageCodec {
    static constexpr bool kDefined = true;

    static result<std::vector<std::uint8_t>> Encode(const T& msg) {
        std::string bytes;
        if (!msg.SerializeToString(&bytes))
            return result<std::vector<std::uint8_t>>::err(
                MakeError(ErrorCode::ProtocolError,
                          "ecommerce: serialize " +
                              std::string(T::descriptor()->full_name())));
        return result<std::vector<std::uint8_t>>::ok(
            std::vector<std::uint8_t>(bytes.begin(), bytes.end()));
    }

    static result<T> Decode(const std::vector<std::uint8_t>& bytes) {
        T msg;
        if (!msg.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())))
            return result<T>::err(MakeError(
                ErrorCode::ProtocolError,
                "ecommerce: malformed " +
                    std::string(T::descriptor()->full_name())));
        return result<T>::ok(std::move(msg));
    }
};

} // namespace ecommerce_codec

template <>
struct ProtoCodec<bbt::example::ecommerce::v1::PriceRequest>
    : ecommerce_codec::MessageCodec<bbt::example::ecommerce::v1::PriceRequest> {};

template <>
struct ProtoCodec<bbt::example::ecommerce::v1::PriceResponse>
    : ecommerce_codec::MessageCodec<bbt::example::ecommerce::v1::PriceResponse> {};

template <>
struct ProtoCodec<bbt::example::ecommerce::v1::QuoteRequest>
    : ecommerce_codec::MessageCodec<bbt::example::ecommerce::v1::QuoteRequest> {};

template <>
struct ProtoCodec<bbt::example::ecommerce::v1::QuoteResponse>
    : ecommerce_codec::MessageCodec<bbt::example::ecommerce::v1::QuoteResponse> {};

template <>
struct ProtoCodec<bbt::example::ecommerce::v1::CheckoutRequest>
    : ecommerce_codec::MessageCodec<bbt::example::ecommerce::v1::CheckoutRequest> {};

template <>
struct ProtoCodec<bbt::example::ecommerce::v1::CheckoutResponse>
    : ecommerce_codec::MessageCodec<bbt::example::ecommerce::v1::CheckoutResponse> {};

template <>
struct ProtoCodec<bbt::example::ecommerce::v1::OrderRequest>
    : ecommerce_codec::MessageCodec<bbt::example::ecommerce::v1::OrderRequest> {};

template <>
struct ProtoCodec<bbt::example::ecommerce::v1::LookupRequest>
    : ecommerce_codec::MessageCodec<bbt::example::ecommerce::v1::LookupRequest> {};

template <>
struct ProtoCodec<bbt::example::ecommerce::v1::BillRequest>
    : ecommerce_codec::MessageCodec<bbt::example::ecommerce::v1::BillRequest> {};

template <>
struct ProtoCodec<bbt::example::ecommerce::v1::Record>
    : ecommerce_codec::MessageCodec<bbt::example::ecommerce::v1::Record> {};

} // namespace bbt::framework::rpc_detail
