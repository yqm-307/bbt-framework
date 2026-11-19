#include <bbt/framework/internal/InboundDispatcher.hpp>

#include <chrono>
#include <boost/scope/scope_exit.hpp>
#include <cstdint>
#include <shared_mutex>
#include <limits>
#include <optional>
#include <string_view>
#include <utility>
#include <unordered_set>
#include <vector>

#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/framework/internal/OrderedIngress.hpp>

#include <bbt/framework/internal/ErrorDomainRule.hpp>
#include <bbt/framework/internal/RequestScope.hpp>
#include <bbt/framework/Route.hpp>

namespace bbt::framework {

namespace {

/**
 * 限制每个请求可携带的路由字段数量及每个字段的大小。
 */
constexpr std::size_t kMaxRouteFieldCount = 16;
constexpr std::size_t kMaxRouteKeyBytes = 64;
constexpr std::size_t kMaxRouteValueBytes = 256;

constexpr std::string_view kRoutePrefix = "route.";
constexpr std::string_view kFwTraceId = "fw.trace_id";

/**
 * Request::Context().deadline 是整个在途交换的硬看门：读、排队、处理
 * 直到回包写出共用同一时刻，infra 到点即取消并切断回复路径。若
 * handler 可见期限与之同点，恰在到期时刻产出的结果必然无法送达——
 * 调用方只能看到无信息的传输断开而非真实的 TimedOut/Cancelled。
 * handler 可用预算预留一小段回包送达窗口，使到期类结果仍可经正常
 * 回复路径交付；对远大于该值的预算此预留不可感知，对不足该值的
 * 紧预算 handler 看到的是已过期 ctx，同样如实走 TimedOut 路径。
 */
constexpr std::chrono::milliseconds kReplyDeliveryReserve{20};

bool IsOrderedFwKey(std::string_view k) {
    return k == "fw.producer_id" || k == "fw.producer_epoch" ||
           k == "fw.sequence" || k == "fw.receiver_epoch";
}

bool IsUnsignedDecimal(std::string_view s) {
    if (s.empty())
        return false;
    for (char c : s)
        if (c < '0' || c > '9')
            return false;
    return true;
}

bool ParseUnsignedDecimal(std::string_view s, std::uint64_t& out) {
    if (!IsUnsignedDecimal(s))
        return false;
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
    /**
     * 长度前缀避免分隔符碰撞；这是协议内一致性标识，不宣称密码学哈希。
     */
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
    std::map<std::string, ServiceEntry> services)
    : InboundDispatcher([&services] {
          std::map<std::string, ServiceBinding> bindings;
          for (auto& [name, entry] : services) {
              ServiceDescription& description = entry;
              ServiceTarget& target = entry;
              bindings.emplace(name, ServiceBinding{
                                         std::make_shared<const ServiceDescription>(std::move(description)),
                                         std::move(target)});
          }
          return bindings;
      }()) {}

InboundDispatcher::InboundDispatcher(
    std::map<std::string, ServiceBinding> services) {
    for (auto& [name, binding] : services)
        m_services.emplace(name, std::make_shared<PerService>(std::move(binding)));
}

void InboundDispatcher::RemoveService(const std::string& service) {
    std::shared_ptr<PerService> removed;
    {
        std::unique_lock<std::shared_mutex> lk(m_services_mtx);
        auto it = m_services.find(service);
        if (it == m_services.end())
            return;
        removed = std::move(it->second);
        m_services.erase(it);
    }
    /**
     * 移除登记后，新请求找不到该服务。已取得记录的请求仍可能来查邮箱，
     * 所以必须在创建邮箱所用的同一把锁下标记关闭。已入队的任务仍会被取出，执行前再检查服务是否允许进入。
     */
    std::lock_guard<std::mutex> lk(removed->mb_mtx);
    removed->closed = true;
    for (auto& [_, mb] : removed->mailboxes)
        mb->Close();
}

result<bbt::infra::rpc::Response> InboundDispatcher::Dispatch(
    const bbt::infra::rpc::Request& request, const std::function<void()>& on_dispatched) {
    using bbt::coroutine::WaitOptions;
    using bbt::coroutine::WaitStatus;
    using bbt::infra::rpc::Response;

    /**
     * 调度门只覆盖同步校验和 Actor 入队；业务挂起之前必须放行后续请求。
     */
    bool dispatched = false;
    const auto ready = [&] {
        if (!dispatched) {
            dispatched = true;
            if (on_dispatched)
                on_dispatched();
        }
    };
    auto dispatch_guard = boost::scope::make_scope_exit(ready);
    const auto& incoming = request.Context();

    /**
     * 1. envelope 基本字段：service/method/request_id 为分发的最小事实。
     */
    if (request.Service().empty() || request.Method().empty())
        return result<Response>::err(
            ErrInvalid("inbound envelope missing service/method"));
    if (request.RequestId().empty() &&
        (request.Reply() == bbt::infra::rpc::ReplyMode::Required || request.MessageId().empty()))
        return result<Response>::err(
            ErrInvalid("inbound envelope missing request_id"));

    /**
     * 2. metadata：route.* 之外的键一律不接受；fw.* 只收白名单。
     * custom 字段只来自 route.*，系统字段只来自 fw.*——不互相伪造。
     */
    RouteFields custom;
    std::optional<std::string> trace_id;
    std::string ordered_producer_id;
    std::string ordered_producer_epoch;
    std::string ordered_receiver_epoch;
    std::uint64_t ordered_sequence = 0;
    bool has_sequence_field = false;
    std::unordered_set<std::string> metadata_keys;
    if (request.Metadata().size() > 32)
        return result<Response>::err(ErrInvalid("too many metadata entries"));
    for (const auto& [k, v] : request.Metadata()) {
        if (k.empty() || k.size() > 64 || v.size() > 256 || !metadata_keys.insert(k).second)
            return result<Response>::err(ErrInvalid("invalid or duplicate metadata entry"));
        if (k.compare(0, kRoutePrefix.size(), kRoutePrefix) == 0) {
            const std::string name = k.substr(kRoutePrefix.size());
            if (name.empty() || name.size() > kMaxRouteKeyBytes ||
                v.size() > kMaxRouteValueBytes)
                return result<Response>::err(
                    ErrInvalid("route.* field out of bounds"));
            if (custom.values.size() >= kMaxRouteFieldCount)
                return result<Response>::err(
                    ErrInvalid("too many route.* fields"));
            custom.values.emplace_back(name, v);
        } else if (k == kFwTraceId) {
            trace_id = v;
        } else if (IsOrderedFwKey(k)) {
            /**
             * 受限系统字段：格式校验（有序流语义由 OrderedIngress 消费）。
             */
            if (v.empty())
                return result<Response>::err(
                    ErrInvalid("malformed " + k));
            if (k == "fw.sequence") {
                if (!IsUnsignedDecimal(v))
                    return result<Response>::err(
                        ErrInvalid("fw.sequence must be unsigned decimal"));
                if (!ParseUnsignedDecimal(v, ordered_sequence))
                    return result<Response>::err(
                        ErrInvalid("fw.sequence exceeds uint64 range"));
                has_sequence_field = true;
            } else if (k == "fw.producer_id") {
                ordered_producer_id = v;
            } else if (k == "fw.producer_epoch") {
                ordered_producer_epoch = v;
            } else if (k == "fw.receiver_epoch") {
                ordered_receiver_epoch = v;
            }
        } else if (k.compare(0, 6, "trace.") != 0) {
            return result<Response>::err(
                ErrInvalid("metadata key outside framework namespaces: " + k));
        }
    }
    /**
     * 不驱动路由策略；custom 只验签不收进上下文。
     */
    (void)custom;

    /**
     * 3. 服务与方法查找；schema 不符显式失败，不等 handler 随机失败。
     * 表锁内复制在途记录 shared_ptr：摘表后记录随请求存活，锁外全部
     * 经该记录使用安装描述与邮箱表。
     */
    std::shared_ptr<PerService> svc;
    {
        std::shared_lock<std::shared_mutex> lk(m_services_mtx);
        auto svc_it = m_services.find(request.Service());
        if (svc_it == m_services.end())
            return result<Response>::err(MakeError(ErrorCode::NotFound,
                                                   "inbound dispatch: unregistered service '" + request.Service() + "'"));
        svc = svc_it->second;
    }
    const auto& description = *svc->binding.description;
    const auto& target = svc->binding.target;
    const RpcMethod* method = description.table->find(request.Method());
    if (method == nullptr)
        return result<Response>::err(MakeError(ErrorCode::NotFound,
                                               "inbound dispatch: unknown method '" + request.Method() +
                                                   "' on service '" + request.Service() + "'"));
    if (request.RequestSchema() != method->request_schema ||
        request.ResponseSchema() != method->response_schema)
        return result<Response>::err(MakeError(ErrorCode::TypeMismatch,
                                               "inbound dispatch: request schema mismatch on '" +
                                                   request.Service() + "." + request.Method() + "'"));

    /**
     * 4. 服务级接纳上限：容量耗尽拒绝，不无限排队。
     */
    if (svc->inflight.fetch_add(1, std::memory_order_acq_rel) >=
        description.options.MaxInflight()) {
        svc->inflight.fetch_sub(1, std::memory_order_acq_rel);
        return result<Response>::err(MakeError(ErrorCode::Overloaded,
                                               "inbound dispatch: service '" + request.Service() +
                                                   "' reached max_inflight"));
    }
    InflightGuard guard(svc->inflight);

    /**
     * 5. 请求上下文：字段只取已验证来源（infra 换算的 deadline、已验证
     * peer_principal、envelope 顶层 request_id、fw.trace_id）。
     */
    auto ctx = std::make_shared<RequestContext>();
    ctx->request_id = request.RequestId().empty() ? request.MessageId() : request.RequestId();
    ctx->deadline = incoming.deadline - kReplyDeliveryReserve;
    ctx->peer_principal = incoming.peer_principal;
    ctx->trace_id = std::move(trace_id);

    /**
     * 入站「已过期不进 handler」在共享入口统一拒绝，actor 与
     * Concurrent 两条策略一致——不再进入等待/内联执行，也不扩大 actor
     * 路径职责。此处尚未做有序流接纳（Admit），故过期的请求不产生
     * ordered 终态副作用；0 预算已在 wire 解码期拒绝，本门承载「可解码
     * 但 handler 可见期限已过」。
     */
    if (ctx->deadline <= std::chrono::steady_clock::now())
        return result<Response>::err(MakeError(ErrorCode::TimedOut,
                                               "inbound request budget already expired before handler entry"));

    std::vector<std::uint8_t> reply_payload;

    if (method->extract_key != nullptr) {
        /**
         * ActorSerial：key 由解码请求的提取器得出（不从 metadata 伪造）。
         */
        auto key = method->extract_key(request.Payload());
        if (!key)
            return result<Response>::err(std::move(key.error()));
        ctx->actor_key = key.value();

        std::shared_ptr<OrderedIngress> ordered_ingress;
        std::optional<OrderedIngress::Admission> ordered_admission;
        if (description.options.OrderedIngress().enabled) {
            ordered_ingress = description.ordered_ingress;
            if (!ordered_ingress)
                return result<Response>::err(MakeError(
                    ErrorCode::RuntimeUnavailable,
                    "ordered ingress is not bound for service '" +
                        request.Service() + "'"));

            OrderedIngressRequest ordered_request;
            ordered_request.method = request.Method();
            ordered_request.actor_key = key.value();
            ordered_request.peer_principal = incoming.peer_principal;
            ordered_request.has_ordered_stamp = has_sequence_field;
            ordered_request.producer_id = ordered_producer_id;
            ordered_request.producer_epoch = ordered_producer_epoch;
            ordered_request.receiver_epoch = ordered_receiver_epoch;
            ordered_request.sequence = ordered_sequence;
            ordered_request.request_id = request.RequestId();
            ordered_request.request_digest = OrderedRequestDigest(
                request.Method(), key.value(), request.Payload());

            auto admitted = ordered_ingress->Admit(ordered_request);
            if (!admitted)
                return result<Response>::err(std::move(admitted.error()));
            if (admitted.value().kind == OrderedIngress::DecisionKind::Replay) {
                const auto& replay = admitted.value().replay;
                if (!replay.is_ok) {
                    /**
                     * Replay 是缓存终态离开本组件的边界。
                     * 尽管 Complete 入缓存前已规范化，这里仍对出缓存的
                     * 错误做同一边界校验作防御——任何经其他路径进入缓存的
                     * 非法错误不会经非 HTTP Dispatch 原样重放。
                     */
                    const Error* rep = &replay.error;
                    Error safe;
                    if (auto v = ValidateErrorAtBoundary(*rep); !v) {
                        safe = v.error();
                        rep = &safe;
                    }
                    return result<Response>::err(*rep);
                }
                /**
                 * 重放仍从本次请求复制五个关联字段，不沿用缓存的请求身份。
                 */
                auto reply = Response::FromRequest(request);
                reply.SetPayload(std::vector<std::uint8_t>(replay.payload.begin(), replay.payload.end()));
                return result<Response>::ok(std::move(reply));
            }
            ordered_admission = std::move(admitted.value());
        }

        auto finish_ordered_error = [&](Error error)
            -> result<Response> {
            /**
             * 首发结果与 Complete 缓存的终态须使用同一安全错误；
             * 不能只规范化缓存，让非 HTTP 首发路径泄露非法 details。
             */
            if (auto valid = ValidateErrorAtBoundary(error); !valid)
                error = std::move(valid.error());
            if (!ordered_admission)
                return result<Response>::err(std::move(error));
            OrderedTerminalReply terminal;
            terminal.is_ok = false;
            terminal.error = error;
            auto completed = ordered_ingress->Complete(
                *ordered_admission, std::move(terminal));
            if (!completed)
                return result<Response>::err(std::move(completed.error()));
            return result<Response>::err(std::move(error));
        };

        CoServiceBase* instance = target.instance;
        if (!instance)
            return finish_ordered_error(MakeError(
                ErrorCode::RuntimeUnavailable,
                "service instance not bound"));
        auto mailbox = _MailboxFor(*svc, key.value());
        if (!mailbox)
            return finish_ordered_error(std::move(mailbox.error()));
        auto mb = std::move(mailbox.value());

        /**
         * 每请求一个 CoWaiter：登记成功后（on_registered 内）再
         * TryEnqueue——早到/入队失败不丢通知。业务结果或入队失败经 slot
         * 回传；handler 完成即 Notify（登记后同步 Notify 走 PENDING 早到
         * 路径），入队失败则记入 slot 并 Notify 自唤醒。
         */
        struct Slot {
            result<std::vector<std::uint8_t>> r =
                result<std::vector<std::uint8_t>>::err(MakeError(
                    ErrorCode::InternalError, "actor task produced no result"));
        };
        auto slot = std::make_shared<Slot>();
        bbt::coroutine::sync::CoWaiter::SPtr waiter;
        try {
            waiter = bbt::coroutine::sync::CoWaiter::Create();
        } catch (...) {
            return finish_ordered_error(MakeError(
                ErrorCode::RuntimeUnavailable,
                "co waiter unavailable"));
        }
        WaitOptions wo;
        wo.deadline = ctx->deadline;
        /**
         * 邮箱任务可能比等待结果的 Dispatch 活得更久，因此按值持有方法和
         * 租约回调。实例裸指针只在成功取得 Registry 租约后才解引用。
         */
        RpcMethod method_copy = *method;
        const WaitStatus st = waiter->WaitWithCallback(wo, [&]() -> bool {
            const auto lease_acquire = target.lease_acquire;
            const auto lease_release = target.lease_release;
            auto enq = mb->TryEnqueue(
                [ctx, method = std::move(method_copy), instance, slot, waiter,
                 lease_acquire, lease_release,
                 payload = request.Payload()]() mutable {
                    /**
                     * 入队不代表一定执行。轮到该请求时若服务已开始拒绝执行，则返回 Closed，不调用业务方法。
                     */
                    if (lease_acquire && !lease_acquire()) {
                        slot->r = result<std::vector<std::uint8_t>>::err(
                            MakeError(ErrorCode::Closed,
                                      "actor handler refused: service closing"));
                        waiter->Notify();
                        return;
                    }
                    struct LeaseGuard {
                        const std::function<void()>& release;
                        ~LeaseGuard() {
                            if (release)
                                release();
                        }
                    } lease_guard{lease_release};
                    RequestScope scope(ctx);
                    slot->r = method.invoke(*instance, payload);
                    waiter->Notify();
                });
            if (!enq) {
                slot->r = result<std::vector<std::uint8_t>>::err(
                    std::move(enq.error()));
                waiter->Notify();
            }
            ready();
            return true;
        });
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
        case WaitStatus::InvalidContext:
            /**
             * 非协程派发调用：不做等待、未入队，如实返回而非未定义行为。
             */
            return finish_ordered_error(MakeError(
                ErrorCode::RuntimeUnavailable,
                "inbound dispatch requires a coroutine context"));
        case WaitStatus::AlreadyWaiting:
        default:
            /**
             * AlreadyWaiting 等在派发约定内不应出现，不静默吞掉。
             */
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
                return result<Response>::err(std::move(completed.error()));
        }
    } else {
        /**
         * Concurrent：派发协程内联执行，实例并发安全由业务负责。
         */
        auto* instance = target.instance;
        if (!instance)
            return result<Response>::err(MakeError(
                ErrorCode::RuntimeUnavailable,
                "service instance not bound"));
        /**
         * 取得执行租约后才能进入业务方法；服务已拒绝新执行时返回 Closed。
         */
        if (target.lease_acquire && !target.lease_acquire())
            return result<Response>::err(MakeError(
                ErrorCode::Closed,
                "inbound dispatch: service '" + request.Service() +
                    "' is closing"));
        struct LeaseGuard {
            const std::function<void()>& release;
            ~LeaseGuard() {
                if (release)
                    release();
            }
        } lease_guard{target.lease_release};
        RequestScope scope(ctx);
        ready();
        auto r = method->invoke(*instance, request.Payload());
        if (!r) {
            if (auto valid = ValidateErrorAtBoundary(r.error()); !valid)
                return result<Response>::err(std::move(valid.error()));
            return result<Response>::err(std::move(r.error()));
        }
        reply_payload = std::move(r.value());
    }

    /**
     * 两个 schema 已与方法表对齐，成功回复复制请求的五个关联字段。
     */
    auto reply = Response::FromRequest(request);
    reply.SetPayload(std::move(reply_payload));
    return result<Response>::ok(std::move(reply));
}

result<ActorMailbox::SPtr> InboundDispatcher::_MailboxFor(
    PerService& svc, const std::string& actor_key) {
    std::lock_guard<std::mutex> lk(svc.mb_mtx);
    /**
     * 摘除后拒绝新建：closed 由 RemoveService 在同一锁内置位，二者互斥，
     * 已闭服务不再产生可接纳任务的邮箱（原请求仍可用已建邮箱收尾）。
     */
    if (svc.closed)
        return result<ActorMailbox::SPtr>::err(MakeError(
            ErrorCode::Closed,
            "inbound dispatch: service removed, serial key refused"));
    auto it = svc.mailboxes.find(actor_key);
    if (it != svc.mailboxes.end())
        return result<ActorMailbox::SPtr>::ok(it->second);
    /**
     * 串行 key 数封顶（ServiceOptions::MaxSerialKeys）：超过即拒绝新 key，
     * 不静默扩容；已建立的 key 继续服务。
     */
    const auto& options = svc.binding.description->options;
    if (options.MaxSerialKeys() != 0 &&
        svc.mailboxes.size() >= options.MaxSerialKeys())
        return result<ActorMailbox::SPtr>::err(MakeError(
            ErrorCode::Overloaded,
            "inbound dispatch: reached max serial keys"));
    auto mb = ActorMailbox::Create(options.SerialQueueCapacity());
    svc.mailboxes.emplace(actor_key, mb);
    return result<ActorMailbox::SPtr>::ok(std::move(mb));
}

} // namespace bbt::framework
