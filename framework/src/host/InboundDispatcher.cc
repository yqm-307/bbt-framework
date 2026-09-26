#include <bbt/framework/internal/InboundDispatcher.hpp>

#include <chrono>
#include <cstdint>
#include <limits>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include <bbt/coroutine/sync/CompletionSignal.hpp>

#include <bbt/framework/internal/OrderedIngress.hpp>

#include <bbt/framework/internal/ErrorDomainRule.hpp>
#include <bbt/framework/internal/RequestScope.hpp>
#include <bbt/framework/Route.hpp>

namespace bbt::framework {

namespace {

// custom 路由字段的有界接收上限：契约要求「有上限」但未冻结数值
// （见本切片报告待澄清项），取与 Error.details 同构的保守界。
constexpr std::size_t kMaxRouteFieldCount  = 16;
constexpr std::size_t kMaxRouteKeyBytes    = 64;
constexpr std::size_t kMaxRouteValueBytes  = 256;

constexpr std::string_view kRoutePrefix = "route.";
constexpr std::string_view kFwTraceId   = "fw.trace_id";

// IncomingCallContext.deadline 是整个在途交换的硬看门：读、排队、处理
// 直到回包写出共用同一时刻，infra 到点即取消并切断回复路径。若
// handler 可见期限与之同点，恰在到期时刻产出的结果必然无法送达——
// 调用方只能看到无信息的传输断开而非真实的 TimedOut/Cancelled。
// handler 可用预算预留一小段回包送达窗口，使到期类结果仍可经正常
// 回复路径交付；对远大于该值的预算此预留不可感知，对不足该值的
// 紧预算 handler 看到的是已过期 ctx，同样如实走 TimedOut 路径。
constexpr std::chrono::milliseconds kReplyDeliveryReserve{20};

bool IsOrderedFwKey(std::string_view k) {
    return k == "fw.producer_id" || k == "fw.producer_epoch" ||
           k == "fw.sequence"    || k == "fw.receiver_epoch";
}

bool IsUnsignedDecimal(std::string_view s) {
    if (s.empty()) return false;
    for (char c : s)
        if (c < '0' || c > '9') return false;
    return true;
}

bool ParseUnsignedDecimal(std::string_view s, std::uint64_t& out) {
    if (!IsUnsignedDecimal(s)) return false;
    std::uint64_t value = 0;
    for (char c : s) {
        const auto digit = static_cast<std::uint64_t>(c - '0');
        if (value > (std::numeric_limits<std::uint64_t>::max() - digit) / 10)
            return false;
        value = value * 10 + digit;
    }
    out = value;
    return true;
}

std::string OrderedRequestDigest(std::string_view method,
                                 std::string_view actor_key,
                                 const std::vector<std::uint8_t>& payload) {
    // 长度前缀避免分隔符碰撞；这是协议内一致性标识，不宣称密码学哈希。
    std::string digest;
    digest.reserve(method.size() + actor_key.size() + payload.size() + 32);
    const auto append = [&digest](const void* data, std::size_t size) {
        digest += std::to_string(size);
        digest.push_back(':');
        digest.append(static_cast<const char*>(data), size);
        digest.push_back('|');
    };
    append(method.data(), method.size());
    append(actor_key.data(), actor_key.size());
    if (!payload.empty())
        append(payload.data(), payload.size());
    else
        append("", 0);
    return digest;
}

Error ErrInvalid(std::string what) {
    return MakeError(ErrorCode::InvalidArgument, std::move(what));
}

} // namespace

InboundDispatcher::InboundDispatcher(
    std::map<std::string, ServiceEntry> services) {
    for (auto& [name, e] : services) {
        auto ps = std::make_unique<PerService>();
        ps->entry = std::move(e);
        m_services.emplace(name, std::move(ps));
    }
}

result<bbt::infra::RpcEnvelope> InboundDispatcher::Dispatch(
    const bbt::infra::IncomingCallContext& incoming,
    bbt::infra::RpcEnvelope request) {
    using bbt::infra::RpcEnvelope;
    using bbt::coroutine::WaitOptions;
    using bbt::coroutine::WaitStatus;

    // 1. envelope 基本字段：service/method/request_id 为分发的最小事实。
    if (request.service.empty() || request.method.empty())
        return result<RpcEnvelope>::err(
            ErrInvalid("inbound envelope missing service/method"));
    if (request.request_id.empty())
        return result<RpcEnvelope>::err(
            ErrInvalid("inbound envelope missing request_id"));

    // 2. metadata：route.* 之外的键一律不接受；fw.* 只收白名单。
    //    custom 字段只来自 route.*，系统字段只来自 fw.*——不互相伪造。
    RouteFields custom;
    std::optional<std::string> trace_id;
    std::string ordered_producer_id;
    std::string ordered_producer_epoch;
    std::string ordered_receiver_epoch;
    std::uint64_t ordered_sequence = 0;
    bool has_sequence_field = false;
    for (const auto& [k, v] : request.metadata) {
        if (k.compare(0, kRoutePrefix.size(), kRoutePrefix) == 0) {
            const std::string name = k.substr(kRoutePrefix.size());
            if (name.empty() || name.size() > kMaxRouteKeyBytes ||
                v.size() > kMaxRouteValueBytes)
                return result<RpcEnvelope>::err(
                    ErrInvalid("route.* field out of bounds"));
            if (custom.values.size() >= kMaxRouteFieldCount)
                return result<RpcEnvelope>::err(
                    ErrInvalid("too many route.* fields"));
            custom.values.emplace_back(name, v);
        } else if (k == kFwTraceId) {
            trace_id = v;
        } else if (IsOrderedFwKey(k)) {
            // 受限系统字段：格式校验（有序流语义 F2-b3 才消费）。
            if (v.empty())
                return result<RpcEnvelope>::err(
                    ErrInvalid("malformed " + k));
            if (k == "fw.sequence") {
                if (!IsUnsignedDecimal(v))
                    return result<RpcEnvelope>::err(
                        ErrInvalid("fw.sequence must be unsigned decimal"));
                if (!ParseUnsignedDecimal(v, ordered_sequence))
                    return result<RpcEnvelope>::err(
                        ErrInvalid("fw.sequence exceeds uint64 range"));
                has_sequence_field = true;
            } else if (k == "fw.producer_id") {
                ordered_producer_id = v;
            } else if (k == "fw.producer_epoch") {
                ordered_producer_epoch = v;
            } else if (k == "fw.receiver_epoch") {
                ordered_receiver_epoch = v;
            }
        } else {
            return result<RpcEnvelope>::err(
                ErrInvalid("metadata key outside route.*: " + k));
        }
    }
    (void)custom;   // 本切片不驱动路由策略；custom 只验签不收进上下文。

    // 3. 服务与方法查找；schema 不符显式失败，不等 handler 随机失败。
    auto svc_it = m_services.find(request.service);
    if (svc_it == m_services.end())
        return result<RpcEnvelope>::err(MakeError(ErrorCode::NotFound,
            "inbound dispatch: unregistered service '" + request.service + "'"));
    PerService& svc = *svc_it->second;
    const RpcMethod* method = svc.entry.table->find(request.method);
    if (method == nullptr)
        return result<RpcEnvelope>::err(MakeError(ErrorCode::NotFound,
            "inbound dispatch: unknown method '" + request.method +
                "' on service '" + request.service + "'"));
    if (request.request_schema != method->request_schema)
        return result<RpcEnvelope>::err(MakeError(ErrorCode::TypeMismatch,
            "inbound dispatch: request schema mismatch on '" +
                request.service + "." + request.method + "'"));

    // 4. 服务级接纳上限：容量耗尽拒绝，不无限排队。
    if (svc.inflight.fetch_add(1, std::memory_order_acq_rel) >=
        svc.entry.options.max_inflight) {
        svc.inflight.fetch_sub(1, std::memory_order_acq_rel);
        return result<RpcEnvelope>::err(MakeError(ErrorCode::Overloaded,
            "inbound dispatch: service '" + request.service +
                "' reached max_inflight"));
    }
    InflightGuard guard(svc.inflight);

    // 5. 请求上下文：字段只取已验证来源（infra 换算的 deadline/cancel、
    //    已验证 peer_principal、envelope 顶层 request_id、fw.trace_id）。
    auto ctx = std::make_shared<RequestContext>();
    ctx->request_id     = request.request_id;
    ctx->deadline       = incoming.deadline - kReplyDeliveryReserve;
    ctx->cancel         = incoming.cancel;
    ctx->peer_principal = incoming.peer_principal;
    ctx->trace_id       = std::move(trace_id);

    std::vector<std::uint8_t> reply_payload;

    if (method->actor_keyed) {
        // ActorSerial：key 由解码请求的提取器得出（不从 metadata 伪造）。
        auto key = method->extract_key(request.payload);
        if (!key)
            return result<RpcEnvelope>::err(std::move(key.error()));
        ctx->actor_key = key.value();

        std::shared_ptr<OrderedIngress> ordered_ingress;
        std::optional<OrderedIngress::Admission> ordered_admission;
        if (svc.entry.options.ordered_ingress) {
            ordered_ingress = svc.entry.ordered_ingress;
            if (!ordered_ingress)
                return result<RpcEnvelope>::err(MakeError(
                    ErrorCode::RuntimeUnavailable,
                    "ordered ingress is not bound for service '" +
                        request.service + "'"));

            OrderedIngressRequest ordered_request;
            ordered_request.method            = request.method;
            ordered_request.actor_key         = key.value();
            ordered_request.peer_principal    = incoming.peer_principal;
            ordered_request.has_ordered_stamp = has_sequence_field;
            ordered_request.producer_id       = ordered_producer_id;
            ordered_request.producer_epoch    = ordered_producer_epoch;
            ordered_request.receiver_epoch    = ordered_receiver_epoch;
            ordered_request.sequence          = ordered_sequence;
            ordered_request.request_id        = request.request_id;
            ordered_request.request_digest = OrderedRequestDigest(
                request.method, key.value(), request.payload);

            auto admitted = ordered_ingress->Admit(ordered_request);
            if (!admitted)
                return result<RpcEnvelope>::err(std::move(admitted.error()));
            if (admitted.value().kind == OrderedIngress::DecisionKind::Replay) {
                const auto& replay = admitted.value().replay;
                if (!replay.is_ok) {
                    // infra #39 收口：Replay 是缓存终态离开本组件的边界。
                    // 尽管 Complete 入缓存前已规范化，这里仍对出缓存的
                    // 错误做同一边界校验作防御——任何经其他路径进入缓存的
                    // 非法错误不会经非 HTTP Dispatch 原样重放。
                    const Error* rep = &replay.error;
                    Error safe;
                    if (auto v = ValidateErrorAtBoundary(*rep); !v) {
                        safe = v.error();
                        rep = &safe;
                    }
                    return result<RpcEnvelope>::err(*rep);
                }
                RpcEnvelope reply;
                reply.service         = request.service;
                reply.method          = request.method;
                reply.request_id      = request.request_id;
                reply.response_schema = method->response_schema;
                reply.payload.assign(replay.payload.begin(), replay.payload.end());
                return result<RpcEnvelope>::ok(std::move(reply));
            }
            ordered_admission = std::move(admitted.value());
        }

        auto finish_ordered_error = [&](Error error)
            -> result<RpcEnvelope> {
            // 首发结果与 Complete 缓存的终态须使用同一安全错误；
            // 不能只规范化缓存，让非 HTTP 首发路径泄露非法 details。
            if (auto valid = ValidateErrorAtBoundary(error); !valid)
                error = std::move(valid.error());
            if (!ordered_admission)
                return result<RpcEnvelope>::err(std::move(error));
            OrderedTerminalReply terminal;
            terminal.is_ok = false;
            terminal.error = error;
            auto completed = ordered_ingress->Complete(
                *ordered_admission, std::move(terminal));
            if (!completed)
                return result<RpcEnvelope>::err(std::move(completed.error()));
            return result<RpcEnvelope>::err(std::move(error));
        };

        auto inst = svc.entry.registry->GetOrCreate(request.service,
                                                   key.value());
        if (!inst)
            return finish_ordered_error(std::move(inst.error()));
        auto mailbox = _MailboxFor(svc, key.value());
        if (!mailbox)
            return finish_ordered_error(MakeError(
                ErrorCode::InternalError, "mailbox create failed"));

        // 每请求一个完成信号；业务结果经 slot 回传。完成先于 Wait
        // 可见（先写结果再 Complete，acquire/release 语义由信号保证）。
        struct Slot {
            result<std::vector<std::uint8_t>> r =
                result<std::vector<std::uint8_t>>::err(MakeError(
                    ErrorCode::InternalError, "actor task produced no result"));
        };
        auto slot = std::make_shared<Slot>();
        std::shared_ptr<bbt::coroutine::CompletionSignal> sig;
        try {
            sig = std::make_shared<bbt::coroutine::CompletionSignal>();
        } catch (...) {
            return finish_ordered_error(MakeError(
                ErrorCode::RuntimeUnavailable,
                "completion signal unavailable"));
        }
        auto enq = mailbox->TryEnqueue(
            [ctx, method, inst,
             payload = std::move(request.payload), slot, sig]() mutable {
                RequestScope scope(ctx);
                slot->r = method->invoke(*inst.value(), payload);
                sig->Complete();
            });
        if (!enq)
            return finish_ordered_error(std::move(enq.error()));

        WaitOptions wo;
        wo.deadline = ctx->deadline;
        wo.cancel   = ctx->cancel;
        const WaitStatus st = sig->Wait(wo);
        switch (st) {
        case WaitStatus::Completed:
            break;
        case WaitStatus::TimedOut:
            return finish_ordered_error(MakeError(ErrorCode::TimedOut,
                "actor handler did not finish within request budget"));
        case WaitStatus::Cancelled:
            return finish_ordered_error(MakeError(ErrorCode::Cancelled,
                "inbound request cancelled while queued/running"));
        case WaitStatus::RuntimeUnavailable:
            return finish_ordered_error(MakeError(
                ErrorCode::RuntimeUnavailable,
                "runtime unavailable while waiting actor result"));
        default:
            // InvalidContext/AlreadyWaiting：派发约定内不应出现，
            // 不静默吞掉，如实上报内部错误。
            return finish_ordered_error(MakeError(
                ErrorCode::InternalError,
                "unexpected wait status " +
                    std::to_string(static_cast<int>(st))));
        }
        if (!slot->r)
            return finish_ordered_error(std::move(slot->r.error()));
        reply_payload = std::move(slot->r.value());
        if (ordered_admission) {
            OrderedTerminalReply terminal;
            terminal.is_ok = true;
            terminal.payload.assign(reply_payload.begin(), reply_payload.end());
            auto completed = ordered_ingress->Complete(
                *ordered_admission, std::move(terminal));
            if (!completed)
                return result<RpcEnvelope>::err(std::move(completed.error()));
        }
    } else {
        // Concurrent：派发协程内联执行，实例并发安全由业务负责。
        auto& inst = svc.entry.instance;
        if (!inst)
            return result<RpcEnvelope>::err(MakeError(
                ErrorCode::RuntimeUnavailable,
                "service instance not bound"));
        RequestScope scope(ctx);
        auto r = method->invoke(*inst, request.payload);
        if (!r)
            return result<RpcEnvelope>::err(std::move(r.error()));
        reply_payload = std::move(r.value());
    }

    RpcEnvelope reply;
    reply.service         = request.service;
    reply.method          = request.method;
    reply.request_id      = request.request_id;
    reply.response_schema = method->response_schema;
    reply.payload         = std::move(reply_payload);
    return result<RpcEnvelope>::ok(std::move(reply));
}

ActorMailbox::SPtr InboundDispatcher::_MailboxFor(
    PerService& svc, const std::string& actor_key) {
    std::lock_guard<std::mutex> lk(svc.mb_mtx);
    auto it = svc.mailboxes.find(actor_key);
    if (it != svc.mailboxes.end())
        return it->second;
    auto mb = ActorMailbox::Create(svc.entry.options.mailbox_capacity);
    svc.mailboxes.emplace(actor_key, mb);
    return mb;
}

} // namespace bbt::framework
