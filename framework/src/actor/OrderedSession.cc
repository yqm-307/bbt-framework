// co-service-actor/v1 F2-b1：OrderedSession 发送侧实现。语义见头注释。

#include <bbt/framework/OrderedSession.hpp>

#include <atomic>
#include <limits>
#include <utility>

namespace bbt::framework {

std::mutex OrderedSession::s_registry_mtx;
std::vector<OrderedSession::RegistryEntry> OrderedSession::s_registry;

namespace {

// 契约第 213 行：六个错误统一 RemoteError + domain="framework.actor"。
Error OrderedDomainError(const char* domain_code, std::string message) {
    Error e = MakeError(ErrorCode::RemoteError, std::move(message));
    e.domain = kOrderedErrorDomain;
    e.domain_code = domain_code;
    return e;
}

// 会话实例序号：request_id 唯一性分量之一，不依赖墙钟。
std::atomic<std::uint64_t> g_session_instance{0};

} // namespace

OrderedSession::OrderedSession(OrderedGrant grant, std::uint64_t first_sequence)
    : grant_(std::move(grant)),
      instance_id_(g_session_instance.fetch_add(1, std::memory_order_relaxed) + 1),
      next_sequence_(first_sequence) {}

OrderedSession::~OrderedSession() {
    // 会话析构即释放 grant 占位；本对象 shared_ptr 归零时其 weak_ptr
    // 已 expired，顺带清扫其他失效项。
    std::lock_guard<std::mutex> lk(s_registry_mtx);
    for (auto it = s_registry.begin(); it != s_registry.end();) {
        it = it->session.expired() ? s_registry.erase(it) : std::next(it);
    }
}

result<OrderedSession::SPtr> OrderedSession::Open(const OrderedGrant& grant) {
    return OpenImpl(grant, 1);
}

result<OrderedSession::SPtr> OrderedSession::OpenForTest(
    const OrderedGrant& grant, std::uint64_t first_sequence) {
    if (first_sequence == 0) {
        return result<SPtr>::err(
            MakeError(ErrorCode::InvalidArgument,
                      "ordered first_sequence must be >= 1"));
    }
    return OpenImpl(grant, first_sequence);
}

result<OrderedSession::SPtr> OrderedSession::OpenImpl(
    const OrderedGrant& grant, std::uint64_t first_sequence) {
    std::lock_guard<std::mutex> lk(s_registry_mtx);
    for (auto it = s_registry.begin(); it != s_registry.end();) {
        if (auto live = it->session.lock()) {
            if (SameOrderedStreamId(it->grant, grant)) {
                if (it->grant == grant) {
                    return result<SPtr>::ok(live);  // 同一完整 grant → 共享会话
                }
                return result<SPtr>::err(OrderedDomainError(
                    ordered_domain_code::kStreamRejected,
                    "ordered stream grant conflicts with a live session"));
            }
            ++it;
        } else {
            it = s_registry.erase(it);  // 清扫已销毁会话占位
        }
    }
    auto session = SPtr(new OrderedSession(grant, first_sequence));
    s_registry.push_back(RegistryEntry{grant, session});
    return result<SPtr>::ok(std::move(session));
}

std::string OrderedSession::MakeRequestId(std::uint64_t sequence) const {
    // 唯一性由 (grant 流标识, 会话实例序号, sequence) 保证；不依赖墙钟、
    // 不依赖客户端自增。
    return grant_.producer_id + "/" + grant_.producer_epoch + "/" +
           grant_.receiver_epoch + "/" + std::to_string(instance_id_) + "/" +
           std::to_string(sequence);
}

result<OrderedStamp> OrderedSession::prepare() {
    std::lock_guard<std::mutex> lk(mtx_);
    if (exhausted_) {
        return result<OrderedStamp>::err(MakeError(
            ErrorCode::InternalError,
            "ordered sequence space exhausted; refusing to wrap"));
    }
    const std::uint64_t seq = next_sequence_;
    if (next_sequence_ == std::numeric_limits<std::uint64_t>::max()) {
        exhausted_ = true;  // 分配最后一个序号后关闭，不回绕到 0/1
    } else {
        ++next_sequence_;
    }
    auto ticket = std::make_shared<OrderedTicket>(seq, MakeRequestId(seq));
    tickets_.emplace(ticket->request_id(), ticket);
    auto state = std::make_shared<OrderedStamp::State>();
    state->ticket  = std::move(ticket);
    // F1-b2：票据回指签发会话，co_rpc_call 经它执行 BindForSend；
    // 弱指针不影响会话生命周期语义（析构即释放 grant 占位）。
    state->session = shared_from_this();
    return result<OrderedStamp>::ok(OrderedStamp(std::move(state)));
}

std::shared_ptr<OrderedTicket> OrderedSession::TicketOf(
    const OrderedStamp& stamp) {
    if (!stamp.state_ || !stamp.state_->ticket) return nullptr;
    return stamp.state_->ticket;
}

result<void> OrderedSession::BindForSend(
    const OrderedStamp& stamp, const OrderedTicket::BoundContent& content) {
    auto ticket = TicketOf(stamp);
    if (!ticket) {
        return result<void>::err(MakeError(
            ErrorCode::InvalidArgument, "empty ordered stamp"));
    }
    std::lock_guard<std::mutex> lk(mtx_);
    auto it = tickets_.find(ticket->request_id());
    if (it == tickets_.end() || it->second.get() != ticket.get()) {
        return result<void>::err(MakeError(
            ErrorCode::InvalidArgument,
            "ordered stamp not issued by this session"));
    }
    if (ticket->IsSent()) {
        // 复用票据只能重发同一请求；内容不一致本地拒绝，不发出。
        if (ticket->BindOrCheck(content)) return result<void>::ok();
        return result<void>::err(OrderedDomainError(
            ordered_domain_code::kSequenceConflict,
            "ordered ticket already bound to a different request"));
    }
    // 首次发送不得越过仍未发送的更小序号票据（契约第 209 行）。
    if (ticket->sequence() != bind_watermark_ + 1) {
        Error e = OrderedDomainError(
            ordered_domain_code::kSequenceGap,
            "first send would skip an unsent ticket");
        e.details.emplace_back("expected_sequence",
                               std::to_string(bind_watermark_ + 1));
        return result<void>::err(std::move(e));
    }
    ticket->BindOrCheck(content);  // 未绑定，必然成功
    ++bind_watermark_;
    return result<void>::ok();
}

bool OrderedSession::IsTicketSent(const OrderedStamp& stamp) const {
    auto ticket = TicketOf(stamp);
    if (!ticket) return false;
    std::lock_guard<std::mutex> lk(mtx_);
    auto it = tickets_.find(ticket->request_id());
    if (it == tickets_.end() || it->second.get() != ticket.get()) return false;
    return ticket->IsSent();
}

} // namespace bbt::framework
