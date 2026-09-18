#pragma once
// co-service-actor/v1 F2-b1：OrderedSession——同一授权流的唯一发送序号
// 所有者（契约 §F2，第 192-207 行）。本切片只覆盖发送侧：prepare 发号
// 与票据首次发送绑定/重放校验；接收侧与 CoApp 控制面在 F2-b2/b3。
//
// 语义要点：
//  - 序号由本类分配：从 1 开始、线程安全、不回绕（空间耗尽明确失败），
//    不依赖墙钟、不依赖客户端自增。
//  - 同一授权流（service/actor/producer/epoch/receiver_epoch 五字段流
//    标识）在进程内至多一个存活发送实例：同一完整 grant 再次 Open 返回
//    同一个共享会话（契约第 207 行 open_ordered_session 语义）；流标识
//    相同但完整 grant 冲突 → StreamRejected。跨进程 fencing 契约明确
//    不承诺，唯一部署由启动装配者保证。
//  - 票据首次发送绑定 service/method、actor-key、custom 与编码请求
//    字节；已绑定票据只能重发同一请求，内容不一致 → 本地拒绝
//    SequenceConflict，不发出。
//  - 发送端不得跳过未发送票据制造序号缺口（契约第 209 行）：首次发送
//    必须按序号顺序进行；越序首次发送 → 本地拒绝 SequenceGap，并在
//    Error.details 的 expected_sequence 给出应先发送的序号。已绑定
//    票据的重发不受水位限制。契约未规定 prepare 是否应因未结算票据
//    阻塞——本实现选择不阻塞 prepare、在发送侧强制顺序，属实现策略
//    而非契约要求（待澄清）。

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <bbt/framework/CallOptions.hpp>
#include <bbt/framework/OrderedTypes.hpp>
#include <bbt/framework/Result.hpp>

namespace bbt::framework {

class OrderedSession : public std::enable_shared_from_this<OrderedSession> {
public:
    using SPtr = std::shared_ptr<OrderedSession>;

    ~OrderedSession();
    OrderedSession(const OrderedSession&) = delete;
    OrderedSession& operator=(const OrderedSession&) = delete;

    // 打开授权流的发送实例：同一完整 grant 已有存活会话时返回该共享
    // 会话；流标识相同但 grant 冲突 → err(RemoteError, domain_code=
    // StreamRejected)。
    static result<SPtr> Open(const OrderedGrant& grant);

    // 测试注入点：显式指定首个待分配序号（须 >= 1），用于验证序号
    // 空间上界行为；其余语义与 Open 相同。
    static result<SPtr> OpenForTest(const OrderedGrant& grant,
                                    std::uint64_t first_sequence);

    // 线程安全分配从 1 开始的 uint64 序号与唯一 request_id；不回绕：
    // 序号空间耗尽返回明确错误（err(InternalError)，契约未定耗尽错误
    // 码——待澄清），绝不静默回到小序号。
    result<OrderedStamp> prepare();

    // 发送路径入口（F2-b2 起由 co_rpc_call 调用；本切片直接公开）。
    //  - 未发送票据：须恰为最小未发送序号，绑定 content 并标记已发送；
    //    越序 → err(domain_code=SequenceGap, expected_sequence=<应先发的>)。
    //  - 已发送票据：content 一致 → ok（重发同一请求）；不一致 →
    //    err(domain_code=SequenceConflict)。
    //  - 票据非本会话签发 → err(InvalidArgument)。
    result<void> BindForSend(const OrderedStamp& stamp,
                             const OrderedTicket::BoundContent& content);

    // 观测：该票据是否已发送；非本会话票据返回 false。
    bool IsTicketSent(const OrderedStamp& stamp) const;

    const OrderedGrant& grant() const noexcept { return grant_; }

private:
    OrderedSession(OrderedGrant grant, std::uint64_t first_sequence);

    static result<SPtr> OpenImpl(const OrderedGrant& grant,
                                 std::uint64_t first_sequence);
    static std::shared_ptr<OrderedTicket> TicketOf(const OrderedStamp& stamp);
    std::string MakeRequestId(std::uint64_t sequence) const;

    struct RegistryEntry {
        OrderedGrant grant;
        std::weak_ptr<OrderedSession> session;
    };
    static std::mutex s_registry_mtx;
    static std::vector<RegistryEntry> s_registry;  // 存活会话占用的 grant

    mutable std::mutex mtx_;
    const OrderedGrant grant_;
    const std::uint64_t instance_id_;   // 进程内会话实例序号，入 request_id
    std::uint64_t next_sequence_;       // 下一个待分配序号
    bool exhausted_ = false;            // 已分配到 UINT64_MAX，禁止回绕
    std::uint64_t bind_watermark_ = 0;  // 已连续完成首次发送的最大序号
    // 本会话签发的全部票据（按 request_id 索引），兼作归属判定。
    std::unordered_map<std::string, std::shared_ptr<OrderedTicket>> tickets_;
};

} // namespace bbt::framework
