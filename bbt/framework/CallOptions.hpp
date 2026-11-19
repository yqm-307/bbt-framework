#pragma once
/**
 * @brief 单次出站调用的期限和可选的有序票据。
 * 票据由 OrderedSession 签发。副本共享重试内容，最后一份副本销毁时释放。
 */

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include <bbt/infra/ICoObject.hpp>
#include <bbt/coroutine/sync/WaitTypes.hpp>
#include <bbt/framework/OrderedTypes.hpp>

namespace bbt::framework {

class CoServiceBase;
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
    friend class CoServiceBase;
};

struct CallOptions {
    std::optional<bbt::coroutine::Deadline> deadline;
    std::optional<OrderedStamp> ordered;
};

/**
 * —— 以下为内部实现细节；State 是 OrderedStamp 的私有成员类型，
 * 外部代码无法命名或构造，票据字段不可由业务修改。
 * 拷贝 OrderedStamp 共享同一票据，保证重发校验使用首次绑定的内容。
 */
struct OrderedStamp::State {
    State(std::uint64_t sequence, std::string request_id,
          std::weak_ptr<OrderedSession> issuer)
        : ticket(sequence, std::move(request_id)), session(std::move(issuer)) {}

    OrderedTicket ticket;

    /* 核对签发关系，同时允许会话析构释放 grant 占位。 */
    const std::weak_ptr<OrderedSession> session;
};

inline OrderedStamp::OrderedStamp(std::shared_ptr<State> state) noexcept
    : state_(std::move(state)) {}

inline std::string OrderedStamp::request_id() const {
    return state_ ? state_->ticket.request_id() : std::string{};
}

inline std::uint64_t OrderedStamp::sequence() const {
    return state_ ? state_->ticket.sequence() : 0;
}

} // namespace bbt::framework
