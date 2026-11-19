#pragma once

namespace bbt::framework {

template <class Req, class Resp>
bbt_co_async(result<Resp>) CoServiceBase::call(
    std::string_view service_name,
    std::string_view method_name,
    const Req& request,
    const RouteFields& custom,
    const CallOptions& options) {
    static_assert(rpc_detail::is_proto_codec_v<Req> &&
                      rpc_detail::is_proto_codec_v<Resp>,
                  "typed call requires ProtoCodec<Req>/ProtoCodec<Resp> specializations");
    auto encoded = rpc_detail::ProtoCodec<Req>::Encode(request);
    if (!encoded)
        return result<Resp>::err(std::move(encoded.error()));
    auto reply = bbt_co_await _CallSend(
        service_name, method_name,
        Req::descriptor()->full_name(), Resp::descriptor()->full_name(),
        std::move(encoded.value()), custom, options);
    if (!reply)
        return result<Resp>::err(std::move(reply.error()));
    return rpc_detail::ProtoCodec<Resp>::Decode(reply.value());
}

} // namespace bbt::framework
