// co-service-actor/v1 F2-b2：有序入口接收侧实现。语义与实现自由度见头注释。

#include <bbt/framework/internal/OrderedIngress.hpp>

#include <utility>

#include <bbt/framework/internal/ErrorDomainRule.hpp>

namespace bbt::framework {

namespace {

// 契约第 213 行：六个有序错误统一 RemoteError + domain="framework.actor"。
Error OrderedIngestError(const char* domain_code, std::string message) {
    Error e = MakeError(ErrorCode::RemoteError, std::move(message));
    e.domain = kOrderedErrorDomain;
    e.domain_code = domain_code;
    return e;
}

Error OrderedIngestInvalid(std::string message) {
    return MakeError(ErrorCode::InvalidArgument, std::move(message));
}

} // namespace

OrderedIngress::OrderedIngress(OrderedIngressConfig config)
    : config_(std::move(config)), service_(config_.service) {}

result<OrderedIngress::SPtr> OrderedIngress::Create(
    OrderedIngressConfig config) {
    if (config.service.empty()) {
        return result<SPtr>::err(
            OrderedIngestInvalid("ordered ingress: empty service name"));
    }
    // 与 ValidateServiceOptions 对 ordered_ingress 启用分支同一约束：
    // 三项有序上限必须 > 0（契约第 207 行）。
    if (config.max_ordered_streams == 0 || config.max_cached_results == 0 ||
        config.max_cached_result_bytes == 0) {
        return result<SPtr>::err(OrderedIngestInvalid(
            "ordered ingress: max_ordered_streams, max_cached_results and "
            "max_cached_result_bytes must all be > 0"));
    }
    return result<SPtr>::ok(SPtr(new OrderedIngress(std::move(config))));
}

std::size_t OrderedIngress::StreamKeyHash::operator()(
    const StreamKey& k) const {
    std::size_t h = std::hash<std::string>{}(k.actor_key);
    const auto mix = [&h](const std::string& s) {
        h ^= std::hash<std::string>{}(s) + 0x9e3779b97f4a7c15ULL + (h << 6) +
             (h >> 2);
    };
    mix(k.producer_id);
    mix(k.producer_epoch);
    return h;
}

bool OrderedIngress::SameProducerStream(const OrderedGrant& a,
                                        const OrderedGrant& b) {
    return a.actor_key == b.actor_key && a.producer_id == b.producer_id &&
           a.producer_epoch == b.producer_epoch;
}

std::size_t OrderedIngress::ReplyBytes(const OrderedTerminalReply& reply) {
    // 只计被保留的文本字节：成功计回复体；失败计错误域/码/文本与 details。
    std::size_t n = reply.payload.size();
    if (reply.is_ok) return n;
    n += reply.error.domain.size();
    n += reply.error.domain_code.size();
    n += reply.error.message.size();
    for (const auto& kv : reply.error.details) {
        n += kv.first.size() + kv.second.size();
    }
    return n;
}

Error OrderedIngress::_GapError(std::uint64_t next_sequence) const {
    // infra #39：带 details 的 actor 域错误在构造边界过
    // ValidateErrorAtBoundary；非法形态整体降级为 ProtocolError。
    Error e = OrderedIngestError(ordered_domain_code::kSequenceGap,
        "ordered sequence gap: future sequence not buffered");
    e.details.emplace_back(std::string(kErrorDetailExpectedSequence),
                           std::to_string(next_sequence));
    auto v = ValidateErrorAtBoundary(e);
    if (!v) return v.error();
    return e;
}

result<void> OrderedIngress::GrantStream(const OrderedGrant& grant) {
    if (grant.service != service_) {
        return result<void>::err(OrderedIngestInvalid(
            "ordered ingress: grant service does not match this ingress"));
    }
    if (grant.service.empty() || grant.actor_key.empty() ||
        grant.producer_id.empty() || grant.producer_epoch.empty() ||
        grant.receiver_epoch.empty()) {
        return result<void>::err(OrderedIngestInvalid(
            "ordered ingress: grant has empty field; refusing to install"));
    }

    std::lock_guard<std::mutex> lk(m_mtx);

    for (const auto& entry : grants_) {
        if (!SameOrderedStreamId(entry.grant, grant)) continue;
        if (entry.grant == grant) return result<void>::ok();  // 幂等重装
        return result<void>::err(OrderedIngestError(
            ordered_domain_code::kStreamRejected,
            "ordered stream grant conflicts with the installed grant"));
    }

    // 同一 producer 流的再次授权即会话切换：撤销旧 receiver_epoch 授权并
    // 丢弃其序号状态，此后旧 epoch 请求一律 StreamRejected（旧会话拒绝）。
    for (auto it = grants_.begin(); it != grants_.end();) {
        if (SameProducerStream(it->grant, grant) &&
            it->grant.receiver_epoch != grant.receiver_epoch) {
            const StreamKey key{it->grant.actor_key, it->grant.producer_id,
                                it->grant.producer_epoch};
            _DropStreamCache(key);   // 先清它在服务级缓存里的终态
            streams_.erase(key);
            it = grants_.erase(it);
        } else {
            ++it;
        }
    }

    if (grants_.size() >= config_.max_ordered_streams) {
        return result<void>::err(MakeError(
            ErrorCode::Overloaded,
            "ordered ingress: max_ordered_streams reached"));
    }

    grants_.push_back(GrantEntry{grant});
    auto& state = streams_[StreamKey{grant.actor_key, grant.producer_id,
                                     grant.producer_epoch}];
    state.next_sequence = 1;  // 新授权流从 1 起（契约第 249 行）
    return result<void>::ok();
}

result<OrderedIngress::Admission> OrderedIngress::Admit(
    const OrderedIngressRequest& request) {
    // 无票据调用启用了 ordered_ingress 的目标：不消费序号，不执行
    // （契约第 249 行）。
    if (!request.has_ordered_stamp) {
        return result<Admission>::err(OrderedIngestError(
            ordered_domain_code::kOrderedStampRequired,
            "ordered ingress requires an ordered stamp on this target"));
    }
    if (request.producer_id.empty() || request.producer_epoch.empty() ||
        request.receiver_epoch.empty()) {
        return result<Admission>::err(OrderedIngestError(
            ordered_domain_code::kStreamRejected,
            "ordered stamp fields incomplete"));
    }

    std::lock_guard<std::mutex> lk(m_mtx);

    const bool authorized = [&] {
        for (const auto& entry : grants_) {
            if (entry.grant.actor_key != request.actor_key ||
                entry.grant.producer_id != request.producer_id) {
                continue;
            }
            if (entry.grant.producer_epoch != request.producer_epoch) continue;
            if (entry.grant.receiver_epoch != request.receiver_epoch) {
                return false;  // 旧 receiver_epoch 会话：拒绝
            }
            return entry.grant.peer_principal == request.peer_principal;
        }
        return false;
    }();
    if (!authorized) {
        return result<Admission>::err(OrderedIngestError(
            ordered_domain_code::kStreamRejected,
            "ordered stream not authorized for this "
            "actor/producer/epoch/peer"));
    }

    StreamState& state = streams_[StreamKey{request.actor_key,
                                            request.producer_id,
                                            request.producer_epoch}];

    // sequence == 0 不是发送侧会分配的值（序号从 1 起）：按缺口拒绝，
    // 不消耗序号、不执行。
    if (request.sequence == 0) {
        return result<Admission>::err(_GapError(state.next_sequence));
    }
    if (request.sequence > state.next_sequence) {
        return result<Admission>::err(_GapError(state.next_sequence));
    }

    if (request.sequence < state.next_sequence) {
        // 已消耗序号：在途 → InProgress；已完成 → 重放原终态；
        // 已淘汰 → ResultExpired。均不重新执行（契约第 255 行）。
        auto in_flight = state.in_flight.find(request.sequence);
        if (in_flight != state.in_flight.end()) {
            if (in_flight->second.request_id == request.request_id &&
                in_flight->second.digest == request.request_digest) {
                return result<Admission>::err(OrderedIngestError(
                    ordered_domain_code::kInProgress,
                    "ordered request of this sequence is still in progress"));
            }
            return result<Admission>::err(OrderedIngestError(
                ordered_domain_code::kSequenceConflict,
                "ordered sequence already bound to a different request"));
        }
        const CachedKey ckey{StreamKey{request.actor_key, request.producer_id,
                                       request.producer_epoch},
                             request.sequence};
        auto cached = cached_.find(ckey);
        if (cached != cached_.end()) {
            if (cached->second.request_id != request.request_id ||
                cached->second.digest != request.request_digest) {
                return result<Admission>::err(OrderedIngestError(
                    ordered_domain_code::kSequenceConflict,
                    "ordered sequence already bound to a different request"));
            }
            Admission out;
            out.kind           = DecisionKind::Replay;
            out.sequence       = request.sequence;
            out.actor_key      = request.actor_key;
            out.producer_id    = request.producer_id;
            out.producer_epoch = request.producer_epoch;
            out.replay         = cached->second.reply;
            return result<Admission>::ok(std::move(out));
        }
        return result<Admission>::err(OrderedIngestError(
            ordered_domain_code::kResultExpired,
            "ordered result of this sequence was evicted from the cache"));
    }

    // 接纳同步边界：在此一次性完成序号消耗与在途登记（契约第 257 行）。
    state.in_flight.emplace(request.sequence,
                            InFlight{request.request_id, request.request_digest});
    ++state.next_sequence;

    Admission out;
    out.kind           = DecisionKind::Execute;
    out.sequence       = request.sequence;
    out.actor_key      = request.actor_key;
    out.producer_id    = request.producer_id;
    out.producer_epoch = request.producer_epoch;
    return result<Admission>::ok(std::move(out));
}

void OrderedIngress::_RemoveCached(const CachedKey& key) {
    auto it = cached_.find(key);
    if (it == cached_.end()) return;
    cached_bytes_ -= it->second.bytes;
    cached_.erase(it);
    // cached_order_ 里同键只出现一次；线性移除（完成序保序由剩余元素维持）。
    for (auto o = cached_order_.begin(); o != cached_order_.end(); ++o) {
        if (*o == key) { cached_order_.erase(o); break; }
    }
    auto stream = streams_.find(key.stream);
    if (stream != streams_.end()) stream->second.cached_seqs.erase(key.sequence);
}

void OrderedIngress::_DropStreamCache(const StreamKey& key) {
    auto stream = streams_.find(key);
    if (stream == streams_.end()) return;
    // 先收集序号再逐个移除：_RemoveCached 会同步 cached_seqs.erase，
    // 不能在遍历 cached_seqs 的同时删它的元素（迭代器失效）。
    std::vector<std::uint64_t> seqs;
    seqs.reserve(stream->second.cached_seqs.size());
    for (const auto& kv : stream->second.cached_seqs) seqs.push_back(kv.first);
    for (const std::uint64_t s : seqs) _RemoveCached(CachedKey{key, s});
}

void OrderedIngress::_EvictToLimits() {
    // 服务级条数 + 字节双限 FIFO：cached_order_ 是全局完成顺序，跨流淘汰
    // 最旧终态；同步清所属流的 cached_seqs，绝不回退 next_sequence
    // （契约第 207/255 行）。
    while (!cached_order_.empty() &&
           (cached_.size() > config_.max_cached_results ||
            cached_bytes_ > config_.max_cached_result_bytes)) {
        _RemoveCached(cached_order_.front());
    }
}

result<void> OrderedIngress::Complete(const Admission& admission,
                                      OrderedTerminalReply reply) {
    if (admission.kind != DecisionKind::Execute) {
        return result<void>::err(OrderedIngestInvalid(
            "ordered ingress: complete requires an execute admission"));
    }

    std::lock_guard<std::mutex> lk(m_mtx);
    auto stream = streams_.find(StreamKey{admission.actor_key,
                                          admission.producer_id,
                                          admission.producer_epoch});
    if (stream == streams_.end()) {
        return result<void>::err(OrderedIngestInvalid(
            "ordered ingress: admission does not reference a live stream"));
    }
    StreamState& state = stream->second;
    auto in_flight = state.in_flight.find(admission.sequence);
    if (in_flight == state.in_flight.end()) {
        return result<void>::err(OrderedIngestInvalid(
            "ordered ingress: admission is not in flight (already completed "
            "or not issued by this ingress)"));
    }

    const std::string request_id = in_flight->second.request_id;
    const std::string digest     = in_flight->second.digest;
    state.in_flight.erase(in_flight);

    // infra #39 收口：缓存终态前过 framework.actor 域边界校验。违规错误
    // （如非本域写入 expected_sequence 保留键或值格式非法）不进入缓存——
    // 规范化为无 details 的 ProtocolError 终态，保证后续非 HTTP Replay
    // 不会把非法 actor details 重放到框架内部消费路径。序号终态语义不变：
    // 已消耗序号仍保留终态，重复请求仍重放（只是内容已是安全错误）。
    if (!reply.is_ok) {
        if (auto v = ValidateErrorAtBoundary(reply.error); !v) {
            Error safe = v.error();          // 边界校验自身返回的 ProtocolError
            reply.error      = std::move(safe);
            reply.error.details.clear();     // 防御：确保无非法 details 入缓存
        }
    }

    // 接纳后取消、排队到期、业务失败都保留终态：后续重复请求重放该终态，
    // 不重新执行（契约第 257 行）。
    const std::size_t bytes = ReplyBytes(reply);
    if (bytes > config_.max_cached_result_bytes) {
        // 单条终态本身超字节上限：无法保留（下一次重复请求 → ResultExpired）。
        return result<void>::ok();
    }
    const StreamKey skey{admission.actor_key, admission.producer_id,
                         admission.producer_epoch};
    const CachedKey ckey{skey, admission.sequence};
    cached_.emplace(ckey, CachedResult{skey, admission.sequence, request_id,
                                     digest, std::move(reply), bytes});
    cached_order_.push_back(ckey);
    cached_bytes_ += bytes;
    state.cached_seqs.emplace(admission.sequence, '\0');
    _EvictToLimits();
    return result<void>::ok();
}

std::size_t OrderedIngress::StreamCount() const {
    std::lock_guard<std::mutex> lk(m_mtx);
    return grants_.size();
}

std::size_t OrderedIngress::CachedResultCount() const {
    std::lock_guard<std::mutex> lk(m_mtx);
    return cached_.size();
}

std::size_t OrderedIngress::CachedResultBytes() const {
    std::lock_guard<std::mutex> lk(m_mtx);
    return cached_bytes_;
}

result<OrderedStreamObservation> OrderedIngress::ObserveStream(
    const std::string& actor_key,
    const std::string& producer_id,
    const std::string& producer_epoch) const {
    std::lock_guard<std::mutex> lk(m_mtx);
    auto it = streams_.find(StreamKey{actor_key, producer_id, producer_epoch});
    if (it == streams_.end()) {
        return result<OrderedStreamObservation>::err(
            MakeError(ErrorCode::NotFound, "ordered stream not installed"));
    }
    OrderedStreamObservation out;
    out.next_sequence = it->second.next_sequence;
    out.in_flight     = it->second.in_flight.size();
    out.cached        = it->second.cached_seqs.size();
    return result<OrderedStreamObservation>::ok(out);
}

} // namespace bbt::framework
