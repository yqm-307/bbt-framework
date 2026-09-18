#pragma once
// co-service-actor/v1 F2-b1：有序入口发送侧共享类型。
//  - OrderedGrant：授权流标识 + 授权身份，字段与契约 §F2
//    （decisions/0001，第 184-191 行）逐字一致；operator== 是「同一完整
//    grant」的判定基础，用于幂等重开与冲突判定。
//  - OrderedTicket：OrderedSession::prepare 签发的票据状态——已分配但
//    尚未发送的序号与 request_id，以及首次发送时绑定的请求内容。
//  - ordered_domain_code::* 与 kOrderedErrorDomain：契约第 213 行规定
//    的 domain / 六个 domain_code，逐字集中定义。

#include <cstdint>
#include <mutex>
#include <string>
#include <utility>

namespace bbt::framework {

struct OrderedGrant {
    std::string service;
    std::string actor_key;
    std::string producer_id;
    std::string producer_epoch;
    std::string receiver_epoch;
    std::string peer_principal;
};

inline bool operator==(const OrderedGrant& a, const OrderedGrant& b) {
    return a.service == b.service &&
           a.actor_key == b.actor_key &&
           a.producer_id == b.producer_id &&
           a.producer_epoch == b.producer_epoch &&
           a.receiver_epoch == b.receiver_epoch &&
           a.peer_principal == b.peer_principal;
}
inline bool operator!=(const OrderedGrant& a, const OrderedGrant& b) {
    return !(a == b);
}

// 契约第 205 行：接收方匹配 service/actor/producer/epoch/receiver_epoch，
// 这五个字段即流标识；流标识相同但完整 grant 不同（如 peer_principal
// 不同）属冲突 grant → StreamRejected。
inline bool SameOrderedStreamId(const OrderedGrant& a, const OrderedGrant& b) {
    return a.service == b.service &&
           a.actor_key == b.actor_key &&
           a.producer_id == b.producer_id &&
           a.producer_epoch == b.producer_epoch &&
           a.receiver_epoch == b.receiver_epoch;
}

// 契约第 213 行：六个错误统一 ErrorCode::RemoteError，
// domain="framework.actor"，domain_code 取下列常量之一。
inline constexpr const char* kOrderedErrorDomain = "framework.actor";

namespace ordered_domain_code {
inline constexpr const char* kSequenceGap          = "SequenceGap";
inline constexpr const char* kInProgress           = "InProgress";
inline constexpr const char* kSequenceConflict     = "SequenceConflict";
inline constexpr const char* kResultExpired        = "ResultExpired";
inline constexpr const char* kOrderedStampRequired = "OrderedStampRequired";
inline constexpr const char* kStreamRejected       = "StreamRejected";
} // namespace ordered_domain_code

// 票据：承载已准备但尚未发送的序号、request_id 与绑定内容。
// 由 OrderedSession 签发并共享给 OrderedStamp::State；自同步。
// 生命周期：已签发未发送（IsSent()==false）→ 首次发送绑定内容后
// IsSent()==true，之后只允许以同一内容重发。
class OrderedTicket {
public:
    // 首次发送时绑定的请求内容（契约第 207 行）：
    // service/method、actor-key、custom 与编码后的请求字节。
    // custom 以编码后字节承载；字段级一致性即「同一请求」的判定。
    struct BoundContent {
        std::string service;
        std::string method;
        std::string actor_key;
        std::string custom;
        std::string request_bytes;
    };

    OrderedTicket(std::uint64_t sequence, std::string request_id)
        : sequence_(sequence), request_id_(std::move(request_id)) {}

    OrderedTicket(const OrderedTicket&) = delete;
    OrderedTicket& operator=(const OrderedTicket&) = delete;

    std::uint64_t sequence() const noexcept { return sequence_; }
    const std::string& request_id() const noexcept { return request_id_; }

    // 「该票据是否已发送」。false 即「已准备但尚未发送」——发送端丢弃
    // 它会制造序号缺口，必须保留重试或停止整流（契约第 209 行）。
    bool IsSent() const {
        std::lock_guard<std::mutex> lk(mtx_);
        return bound_;
    }

    // 发送路径调用：未绑定 → 记录内容并标记已发送，返回 true；
    // 已绑定 → 内容一致返回 true（允许重发同一请求），不一致返回
    // false（调用方本地拒绝为 SequenceConflict，不发出）。
    bool BindOrCheck(const BoundContent& content) {
        std::lock_guard<std::mutex> lk(mtx_);
        if (!bound_) {
            bound_content_ = content;
            bound_ = true;
            return true;
        }
        return bound_content_.service == content.service &&
               bound_content_.method == content.method &&
               bound_content_.actor_key == content.actor_key &&
               bound_content_.custom == content.custom &&
               bound_content_.request_bytes == content.request_bytes;
    }

    // 拷贝当前绑定内容；未绑定（未发送）返回 false。
    bool BoundContentOf(BoundContent& out) const {
        std::lock_guard<std::mutex> lk(mtx_);
        if (!bound_) return false;
        out = bound_content_;
        return true;
    }

private:
    const std::uint64_t sequence_;
    const std::string   request_id_;
    mutable std::mutex  mtx_;
    BoundContent        bound_content_{};
    bool                bound_ = false;
};

} // namespace bbt::framework
