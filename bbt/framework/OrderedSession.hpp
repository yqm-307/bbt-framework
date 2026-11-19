#pragma once
/**
 * @brief 为同一条有序请求流分配序号和重试票据。
 * 每个进程内同一条流只能有一个存活会话；跨进程的唯一性由部署方保证。
 * 可以预先领多张票，但首次使用必须按序号。丢弃尚未绑定的票会留下缺号，后面的票不能跳过它。
 */

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

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

    /**
     * 打开授权流的发送实例：同一完整 grant 已有存活会话时返回该共享
     * 会话；流标识相同但 grant 冲突 → err(RemoteError, domain_code=
     * StreamRejected)。
     */
    static result<SPtr> Open(const OrderedGrant& grant);

    /**
     * 兼容既有测试。新测试通过 internal/CoAppSeam.hpp 指定首个序号。
     */
    static result<SPtr> OpenForTest(const OrderedGrant& grant,
                                    std::uint64_t first_sequence);

    /**
     * @brief 线程安全地分配序号和唯一请求编号。Open 创建的会话从 1 开始发号。
     * @return 序号用尽时返回 InternalError，不会从头发号。
     */
    result<OrderedStamp> prepare();

    /**
     * @brief 首次发送时，把票据与服务、方法、Actor key、路由字段及请求字节绑定。
     * 同一张票可重试同一请求。
     * @return 跳号返回 SequenceGap，expected_sequence 给出下一张应使用的票号；
     * 重试时内容不同返回 SequenceConflict；不是此会话签发的票返回 InvalidArgument。
     */
    result<void> BindForSend(const OrderedStamp& stamp,
                             const OrderedTicket::BoundContent& content);

    /**
     * @brief 检查本会话的票是否已经绑定请求；返回 true 不表示对端已收到。其他会话的票返回 false。
     */
    bool IsTicketSent(const OrderedStamp& stamp) const;

    const OrderedGrant& grant() const noexcept { return grant_; }

private:
    OrderedSession(OrderedGrant grant, std::uint64_t first_sequence);

    static result<SPtr> OpenImpl(const OrderedGrant& grant,
                                 std::uint64_t first_sequence);

    std::string MakeRequestId(std::uint64_t sequence) const;

    struct RegistryEntry {
        OrderedGrant grant;
        std::weak_ptr<OrderedSession> session;
    };
    static std::mutex s_registry_mtx;
    /**
     * 存活会话占用的 grant
     */
    static std::vector<RegistryEntry> s_registry;

    mutable std::mutex mtx_;
    const OrderedGrant grant_;
    /**
     * 进程内会话实例序号，入 request_id
     */
    const std::uint64_t instance_id_;
    /**
     * 下一个待分配序号
     */
    std::uint64_t next_sequence_;
    /**
     * 已分配到 UINT64_MAX，禁止回绕
     */
    bool exhausted_ = false;
    /* 已连续完成首次绑定的最大序号。 */
    std::uint64_t bind_watermark_ = 0;
};

} // namespace bbt::framework
