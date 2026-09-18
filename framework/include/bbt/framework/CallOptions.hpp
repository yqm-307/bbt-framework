#pragma once
// co-service-actor/v1 F0：OrderedStamp 完整拥有型声明（仅内部 State 不完整）
// 与 CallOptions。票据由 OrderedSession（F2）签发，ICoService 在调用时读取
// 票据字段写入 fw.* 系统元数据；F0 不提供票据的创建入口。
// F2-b1：State 持有 OrderedTicket（序号、request_id、发送绑定内容）。

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include <bbt/infra/ICoObject.hpp>
#include <bbt/framework/OrderedTypes.hpp>

namespace bbt::framework {

class ICoService;
class OrderedSession;

class OrderedStamp {
public:
    OrderedStamp(const OrderedStamp&) noexcept = default;
    OrderedStamp& operator=(const OrderedStamp&) noexcept = default;
    std::string request_id() const;
    std::uint64_t sequence() const;
private:
    struct State;
    std::shared_ptr<State> state_;
    explicit OrderedStamp(std::shared_ptr<State> state) noexcept;
    friend class OrderedSession;
    friend class ICoService;
};

struct CallOptions {
    std::optional<bbt::coroutine::Deadline> deadline;
    bbt::coroutine::CancellationToken       cancel;
    std::optional<OrderedStamp>             ordered;
};

// —— 以下为 F0 内部实现细节；State 是 OrderedStamp 的私有成员类型，
//    外部代码无法命名或构造，票据字段不可由业务修改。
//    F2-b1：State 持有 OrderedTicket，sequence、绑定请求内容经
//    OrderedSession/ICoService 友元读写；拷贝 OrderedStamp 共享同一
//    票据，保证重发语义作用于同一状态。
struct OrderedStamp::State {
    std::shared_ptr<OrderedTicket> ticket;
    // F1-b2：签发会话弱指针，co_rpc_call 经它执行 BindForSend
    // （首次发送绑定/重放校验/序号水位检查）；会话已销毁的票据
    // 如实失败，不静默直发。
    std::weak_ptr<OrderedSession>  session;
};

inline OrderedStamp::OrderedStamp(std::shared_ptr<State> state) noexcept
    : state_(std::move(state)) {}

inline std::string OrderedStamp::request_id() const {
    return (state_ && state_->ticket) ? state_->ticket->request_id()
                                    : std::string{};
}

inline std::uint64_t OrderedStamp::sequence() const {
    return (state_ && state_->ticket) ? state_->ticket->sequence() : 0;
}

} // namespace bbt::framework
