#pragma once
// bbt-framework Issue #4（P0-A）：GetValue 示例业务。
//
// 这是应用层最小 typed codec + 服务实现：
//   - ProtoCodec<GetValueRequest/Response> 特化：只经公开 protobuf
//     Serialize/Parse 做 payload 编解码，业务用公开 CoRpcReq::ParseProto /
//     CoRpcResp::FromProto；
//   - GetValueService：in-memory 只读存储，无外部后端、无写副作用；
//   - 方法表经 fw::ProtoMethod 声明，schema 由生成物 descriptor full_name 推导。
//
// 生成头 get_value.pb.h 只落构建目录（见 CMakeLists），仓库内不提交。

#include <functional>
#include <limits>
#include <map>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <bbt/example/v1/get_value.pb.h>

#include <bbt/framework/CoRpc.hpp>
#include <bbt/framework/CoService.hpp>
#include <bbt/framework/RequestContext.hpp>
#include <bbt/framework/Result.hpp>
#include <bbt/framework/RpcMethods.hpp>

namespace bbt::framework::rpc_detail {

// GetValueRequest payload codec：wire 字节即 protobuf message 序列化。
template <>
struct ProtoCodec<bbt::example::v1::GetValueRequest> {
    static constexpr bool kDefined = true;
    static result<std::vector<std::uint8_t>> Encode(
        const bbt::example::v1::GetValueRequest& msg) {
        std::string bytes;
        if (!msg.SerializeToString(&bytes))
            return result<std::vector<std::uint8_t>>::err(MakeError(
                ErrorCode::ProtocolError,
                "getvalue: serialize GetValueRequest failed"));
        return result<std::vector<std::uint8_t>>::ok(
            std::vector<std::uint8_t>(bytes.begin(), bytes.end()));
    }
    static result<bbt::example::v1::GetValueRequest> Decode(
        const std::vector<std::uint8_t>& bytes) {
        bbt::example::v1::GetValueRequest msg;
        if (bytes.size() > static_cast<std::size_t>(
                                std::numeric_limits<int>::max()))
            return result<bbt::example::v1::GetValueRequest>::err(MakeError(
                ErrorCode::ProtocolError,
                "getvalue: GetValueRequest payload exceeds int range"));
        if (!msg.ParseFromArray(bytes.data(),
                                static_cast<int>(bytes.size())))
            return result<bbt::example::v1::GetValueRequest>::err(MakeError(
                ErrorCode::ProtocolError,
                "getvalue: malformed GetValueRequest payload"));
        return result<bbt::example::v1::GetValueRequest>::ok(std::move(msg));
    }
};

template <>
struct ProtoCodec<bbt::example::v1::GetValueResponse> {
    static constexpr bool kDefined = true;
    static result<std::vector<std::uint8_t>> Encode(
        const bbt::example::v1::GetValueResponse& msg) {
        std::string bytes;
        if (!msg.SerializeToString(&bytes))
            return result<std::vector<std::uint8_t>>::err(MakeError(
                ErrorCode::ProtocolError,
                "getvalue: serialize GetValueResponse failed"));
        return result<std::vector<std::uint8_t>>::ok(
            std::vector<std::uint8_t>(bytes.begin(), bytes.end()));
    }
    static result<bbt::example::v1::GetValueResponse> Decode(
        const std::vector<std::uint8_t>& bytes) {
        bbt::example::v1::GetValueResponse msg;
        if (bytes.size() > static_cast<std::size_t>(
                                std::numeric_limits<int>::max()))
            return result<bbt::example::v1::GetValueResponse>::err(MakeError(
                ErrorCode::ProtocolError,
                "getvalue: GetValueResponse payload exceeds int range"));
        if (!msg.ParseFromArray(bytes.data(),
                                static_cast<int>(bytes.size())))
            return result<bbt::example::v1::GetValueResponse>::err(MakeError(
                ErrorCode::ProtocolError,
                "getvalue: malformed GetValueResponse payload"));
        return result<bbt::example::v1::GetValueResponse>::ok(std::move(msg));
    }
};

} // namespace bbt::framework::rpc_detail

namespace getvalue {

// 可选观测钩子：handler 被真正调用时回调（request_id, peer_principal）。示例
// server 用它写 journal，证明「预算过期/协议错误的请求未进 handler」；未设置时
// 为 no-op，业务语义不依赖它。
using ServedHook =
    std::function<void(const std::string&, const std::string&)>;

inline ServedHook& ServedHookRef() {
    static ServedHook hook;
    return hook;
}
inline void SetServedHook(ServedHook hook) {
    ServedHookRef() = std::move(hook);
}

// in-memory 只读存储：构造期固定，运行期无写副作用。
class InMemoryStore {
public:
    InMemoryStore() {
        entries_ = {
            {"alpha", "value-alpha"},
            {"beta", "value-beta"},
            {"hello", "world"},
        };
    }
    const std::string* Find(const std::string& key) const {
        const auto it = entries_.find(key);
        return it == entries_.end() ? nullptr : &it->second;
    }

private:
    std::map<std::string, std::string, std::less<>> entries_;
};

// 业务服务：GetValue(key) -> {found, value}。空 key 返回 InvalidArgument；
// 负载非法/类型错误由 ProtoCodec::Decode 返回 ProtocolError，原样经错误信封
// 回传；miss 返回 found=false。
class GetValueService final : public bbt::framework::CoService<GetValueService> {
public:
    static constexpr std::string_view kServiceName =
        "bbt.example.v1.GetValueService";

    bbt::framework::CoRpcResp GetValue(bbt::framework::CoRpcReq req) {
        using bbt::framework::CoRpcResp;
        using bbt::framework::ErrorCode;
        using bbt::framework::MakeError;

        // 观测钩子：handler 被真正调用（=请求已进业务处理，预算未过期、路由/
        // schema 已匹配）时回调。request_id/peer_principal 取自框架受管上下文，
        // 不读 metadata。示例 server 用它写 journal，证明预算过期/协议被拒的
        // 请求未进 handler；业务语义不依赖它。
        if (ServedHookRef()) {
            std::string request_id;
            std::string peer_principal;
            if (auto ctx = bbt::framework::CurrentRequestContext()) {
                request_id     = ctx.value()->request_id;
                peer_principal = ctx.value()->peer_principal;
            }
            ServedHookRef()(request_id, peer_principal);
        }

        auto parsed = req.ParseProto<bbt::example::v1::GetValueRequest>();
        if (!parsed)
            return CoRpcResp::Error(parsed.error());

        const std::string& key = parsed.value().key();
        if (key.empty())
            return CoRpcResp::Error(MakeError(
                ErrorCode::InvalidArgument, "getvalue: key must be non-empty"));

        bbt::example::v1::GetValueResponse out;
        if (const std::string* v = store_.Find(key)) {
            out.set_found(true);
            out.set_value(*v);
        } else {
            out.set_found(false);
        }
        return CoRpcResp::FromProto(out);
    }

    static constexpr auto kRpcMethods = bbt::framework::RpcMethods(
        bbt::framework::ProtoMethod<&GetValueService::GetValue,
                                    bbt::example::v1::GetValueRequest,
                                    bbt::example::v1::GetValueResponse>(
            "GetValue"));

private:
    InMemoryStore store_;
};

} // namespace getvalue
