#pragma once
// co-service-actor/v1 F2-b2：有序入口接收侧——授权流校验 + 每流序号状态机
// + 去重终态缓存（契约 §F2「有序入口与重试」，Issue #2 第 247-259 行）。
//
// 本切片只做接收侧语义核，不做入站接线：从 RpcEnvelope 顶层字段与 fw.*
// 系统元数据提取「有序请求事实」是入站适配器的职责，把本组件接进
// InboundDispatcher 的执行路径属 F2-b3。因此本组件不持有传输类型，
// 只消费调用方给出的事实，不伪造字段来源。
//
// 语义要点（逐条对应契约）：
//  - 未启用有序入口的目标不构造本组件；一旦请求到达本组件而没带票据
//    （has_ordered_stamp == false）→ err(RemoteError, OrderedStampRequired)，
//    不消费序号（契约第 249 行）。
//  - 授权流由装配者安装（对应 CoApp::grant_ordered_stream）：只接受匹配
//    service/actor/producer_epoch/receiver_epoch 且 peer_principal 获授权的
//    流；loopback 的空 peer_principal 表示匿名本地身份，仍要求请求精确
//    匹配；任何其他字段校验失败一律拒绝，不自动建流（契约第 247 行）。
//  - 序号状态按 (actor-key, producer_id, producer_epoch) 维护，从 1 起、
//    只在「接纳同步边界」自增，永不回退（契约第 253/257 行）。
//  - 未来序号 → err(SequenceGap) + Error.details[expected_sequence]，
//    不缓冲缺口、不执行 handler、不消费序号（契约第 253 行）。
//  - 已消耗序号：在途 → InProgress；已完成落缓存 → 直接重放原终态结果，
//    不重新执行；缓存已淘汰 → ResultExpired，不重新执行（契约第 255 行）。
//  - 同序号但 request_id 或请求摘要不同 → SequenceConflict（契约第 255 行）。
//  - 接纳后取消/排队到期/业务失败都消耗序号并保留终态（契约第 257 行）。
//  - 六个错误统一 infra ErrorCode::RemoteError、domain="framework.actor"、
//    domain_code 取 OrderedTypes.hpp 的常量（契约第 213 行）。
//
// 实现策略（契约留有的自由度，须显式记录，不作为契约承诺）：
//  - 同一 producer/epoch/actor-key 安装新的 receiver_epoch 授权时，旧
//    receiver_epoch 授权被撤销并丢弃其序号状态：旧会话请求此后一律
//    StreamRejected。契约只要求「旧会话拒绝」，未规定撤销的触发方式；
//    此处以「同一 producer 流的再次授权」为触发点（契约第 251 行）。
//  - 结果缓存按条数 + 字节双限、服务级 FIFO 淘汰（契约第 207 行把三项
//    有序上限并列在 ServiceOptions，与 max_ordered_streams 同为服务级）。
//    cached_order_ 记录全局完成顺序，跨流按完成先后淘汰，绝不回退某条流的
//    next_sequence；如果单条终态本身超过字节上限，它无法被保留（立即淘汰），
//    该序号此后一律 ResultExpired。这是「有限保留」内的实现选择。
//  - sequence == 0 不是有效分配值（发送侧从 1 起），按 SequenceGap 拒绝
//    并给出期望值 1，不消费序号。

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <bbt/framework/OrderedTypes.hpp>
#include <bbt/framework/Result.hpp>

namespace bbt::framework {

// 有序请求的终态回复：成功回复体或终态错误。去重缓存保留的就是它。
//  - is_ok == true：payload 为编码后的回复字节，error 无意义；
//  - is_ok == false：error 为终态错误（业务失败、取消、排队到期……）。
struct OrderedTerminalReply {
    bool        is_ok = false;
    std::string payload;
    Error       error;
};

// 入站有序请求事实。字段由入站适配器从信封顶层字段与 fw.* 系统元数据
// 提取后填入；本组件只读不改，不从 user custom 推导这些值。
struct OrderedIngressRequest {
    std::string   method;
    std::string   actor_key;              // 目标 Actor key（单 Actor 服务为固定 key）
    std::string   peer_principal;         // infra 已验证的调用方身份
    bool          has_ordered_stamp = false;  // 是否携带有序票据（fw.* 四元组 + sequence）
    std::string   producer_id;
    std::string   producer_epoch;
    std::string   receiver_epoch;
    std::uint64_t sequence = 0;
    std::string   request_id;             // RpcEnvelope 顶层字段，不写 metadata
    std::string   request_digest;         // 请求摘要（内容一致性判定）
};

// 接收侧上限：三项必须 > 0，与 ServiceOptions 中 ordered_ingress 启用时
// 的约束一致（契约第 207 行）。三项均为服务级：max_ordered_streams 上限
// 授权流条数；max_cached_results / max_cached_result_bytes 上限本组件
// （本服务）跨流保留终态的条数 / 总字节。service 非空且为已注册的有序目标服务名。
struct OrderedIngressConfig {
    std::string service;
    std::size_t max_ordered_streams      = 0;
    std::size_t max_cached_results       = 0;
    std::size_t max_cached_result_bytes  = 0;
};

// 单条流观察口（测试/诊断用）。
struct OrderedStreamObservation {
    std::uint64_t next_sequence = 0;  // 下一个待接纳序号
    std::size_t   in_flight     = 0;  // 已接纳未完成
    std::size_t   cached        = 0;  // 本流已保留终态的条数（服务级缓存的子集）
};

class OrderedIngress {
public:
    using SPtr = std::shared_ptr<OrderedIngress>;

    enum class DecisionKind {
        Execute,  // 新接纳：序号已消耗，调用方必须执行并随后 Complete
        Replay,   // 已完成重复请求：直接回复 replay 原终态，不执行、不 Complete
    };

    struct Admission {
        DecisionKind          kind     = DecisionKind::Execute;
        std::uint64_t         sequence = 0;
        // 本票据所属流（Complete 据此定位序号状态；由 Admit 填充）。
        std::string           actor_key;
        std::string           producer_id;
        std::string           producer_epoch;
        OrderedTerminalReply  replay;   // 仅 kind == Replay 有效
    };

    // 装配期校验：service 非空、三项上限均 > 0；违规 err(InvalidArgument)。
    static result<SPtr> Create(OrderedIngressConfig config);

    // 安装一条授权流（接收端会话权威，对应 CoApp::grant_ordered_stream）。
    //  - service 与本组件服务不符、或除 peer_principal 外任一字段为空
    //    → err(InvalidArgument)；peer_principal 为空仅用于 loopback 匿名身份；
    //  - 同一流标识且完整 grant 相同 → ok（幂等，不重建、不重置序号）；
    //  - 同一流标识但完整 grant 冲突 → err(StreamRejected)；
    //  - 同一 (actor-key, producer_id, producer_epoch) 的新 receiver_epoch
    //    授权会撤销旧 receiver_epoch 授权及其序号状态（旧会话拒绝）；
    //  - 授权流条数已达 max_ordered_streams → err(Overloaded)，不淘汰活动流。
    result<void> GrantStream(const OrderedGrant& grant);

    // 接纳判定。返回 Decision::Execute 时序号已在本调用内消耗；其余情况
    // 返回错误且不消费序号。
    //  - 无票据 → err(RemoteError, OrderedStampRequired)；
    //  - 授权流不匹配（service/actor/producer/epoch/receiver_epoch/
    //    peer_principal）→ err(RemoteError, StreamRejected)；
    //  - 未来序号 → err(RemoteError, SequenceGap + expected_sequence)；
    //  - 已消耗序号在途 → err(RemoteError, InProgress)；内容不同 →
    //    SequenceConflict；已淘汰 → ResultExpired。
    result<Admission> Admit(const OrderedIngressRequest& request);

    // 记录终态：从在途移入服务级去重缓存，按条数 + 字节双限、全局完成序
    // FIFO 淘汰。只接受本组件签发、kind == Execute 且仍在途的 Admission。
    result<void> Complete(const Admission& admission,
                          OrderedTerminalReply reply);

    // 观察口。
    std::size_t StreamCount() const;
    // 服务级缓存的聚合量（跨流求和）：CachedResultCount <= max_cached_results、
    // CachedResultBytes <= max_cached_result_bytes 恒成立。
    std::size_t CachedResultCount() const;
    std::size_t CachedResultBytes() const;
    // 无该流 → err(NotFound)。
    result<OrderedStreamObservation> ObserveStream(
        const std::string& actor_key,
        const std::string& producer_id,
        const std::string& producer_epoch) const;

    const std::string& service() const noexcept { return service_; }

private:
    explicit OrderedIngress(OrderedIngressConfig config);

    // 序号状态按 (actor-key, producer_id, producer_epoch) 分桶：契约只承诺
    // 单 producer 流内的顺序，多个 producer 以接纳顺序合并、无天然全序。
    struct StreamKey {
        std::string actor_key;
        std::string producer_id;
        std::string producer_epoch;
        bool operator==(const StreamKey& o) const {
            return actor_key == o.actor_key && producer_id == o.producer_id &&
                   producer_epoch == o.producer_epoch;
        }
    };
    struct StreamKeyHash {
        std::size_t operator()(const StreamKey& k) const;
    };

    struct InFlight {
        std::string request_id;
        std::string digest;
    };
    // 服务级终态缓存条目：sequence 为所属流内序号（键已含流标识，不再单列）。
    struct CachedResult {
        StreamKey            stream;
        std::uint64_t        sequence = 0;
        std::string          request_id;
        std::string          digest;
        OrderedTerminalReply reply;
        std::size_t          bytes = 0;
    };
    struct StreamState {
        std::uint64_t next_sequence = 1;   // 只增不减
        std::unordered_map<std::uint64_t, InFlight> in_flight;
        // 本流当前仍被服务级缓存保留的序号集合（cached_seqs ⊆ cached_ 键）。
        // 与 cached_/cached_order_/cached_bytes_ 同步增删。
        std::unordered_map<std::uint64_t, char> cached_seqs;
    };

    struct GrantEntry {
        OrderedGrant grant;
    };

    // 服务级缓存键 = (流标识, 流内序号)：sequence 只在单流内唯一，
    // 跨流可重复，必须连同 StreamKey 一起作主键。
    struct CachedKey {
        StreamKey     stream;
        std::uint64_t sequence = 0;
        bool operator==(const CachedKey& o) const {
            return sequence == o.sequence && stream == o.stream;
        }
    };
    struct CachedKeyHash {
        std::size_t operator()(const CachedKey& k) const {
            std::size_t h = StreamKeyHash{}(k.stream);
            h ^= std::hash<std::uint64_t>{}(k.sequence) +
                 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
            return h;
        }
    };

    static bool SameProducerStream(const OrderedGrant& a, const OrderedGrant& b);
    static std::size_t ReplyBytes(const OrderedTerminalReply& reply);
    // 服务级双限 FIFO 淘汰：聚合 cached_.size() > max_cached_results 或
    // cached_bytes_ > max_cached_result_bytes 时按全局完成序淘汰最旧条目，
    // 同步从所属流的 cached_seqs 移除，绝不回退任何流的 next_sequence。
    void _EvictToLimits();
    // 从服务级缓存移除一条终态（同步 cached_order_/cached_bytes_/流 cached_seqs）。
    void _RemoveCached(const CachedKey& key);
    // 撤销一条流的缓存终态（流撤销时调用）。
    void _DropStreamCache(const StreamKey& key);
    Error _GapError(std::uint64_t next_sequence) const;

    const OrderedIngressConfig config_;
    const std::string         service_;

    mutable std::mutex        m_mtx;
    std::vector<GrantEntry>   grants_;   // 已授权流
    std::unordered_map<StreamKey, StreamState, StreamKeyHash> streams_;

    // 服务级去重终态缓存（跨流共享，按全局完成序 FIFO 淘汰）。
    std::unordered_map<CachedKey, CachedResult, CachedKeyHash> cached_;
    std::deque<CachedKey>                                      cached_order_;
    std::size_t                                                cached_bytes_ = 0;
};

} // namespace bbt::framework
