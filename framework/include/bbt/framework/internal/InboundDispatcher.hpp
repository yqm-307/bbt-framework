#pragma once
// co-service-actor/v1 F1-b2：入站适配器——把 infra 的入站调用事实
// （IncomingCallContext + RpcEnvelope）落成受管请求上下文并分发到
// 已托管 Service/Actor 实例（契约 F1 第 139 行）。
//
// 字段来源纪律（不得伪造）：
//  - deadline/cancel/peer_principal 只取 infra 已换算/已验证的
//    IncomingCallContext；request_id 与受限系统字段（fw.*）只取
//    RpcEnvelope；业务 custom 只允许出现在 route.* 键下。
//  - metadata 中 route.* 与受限 fw.* 白名单之外的键一律拒绝
//    （err(InvalidArgument)），不静默忽略、不转写进上下文。
//  - 白名单 fw.*：fw.trace_id 落入上下文 trace_id；有序流四元组
//    （fw.producer_id/producer_epoch/sequence/receiver_epoch）本切片
//    只做存在性与格式校验（非空、sequence 为无符号十进制）——有序
//    入口的执行语义属 F2-b3，不提前消费。
//
// 绑定语义：RequestContext 随逻辑请求/协程绑定（RequestScope），
// 不用 thread_local 跨挂起缓存；Service 并发处理多请求互不串扰——
// Concurrent 在派发协程内联执行，ActorSerial 经注册表取实例后交由
// 该 (service,actor-key) 的 ActorMailbox 串行执行，派发协程以
// 每请求一个 CompletionSignal 等待业务结果（等待期间让出 worker，
// 执行资格仍由邮箱持有，不重入）。

#include <atomic>
#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <bbt/infra/NetworkTypes.hpp>

#include <bbt/framework/internal/ActorMailbox.hpp>
#include <bbt/framework/internal/ActorRegistry.hpp>
#include <bbt/framework/ExecutionPolicy.hpp>
#include <bbt/framework/ICoService.hpp>
#include <bbt/framework/Result.hpp>
#include <bbt/framework/internal/MethodTable.hpp>

namespace bbt::framework {

class InboundDispatcher;
class OrderedIngress;

class InboundDispatcher {
public:
    // 每个已注册服务的分发表项：Concurrent 给启动期单实例；
    // ActorSerial 给激活注册表（实例按 key 经 GetOrCreate 激活）。
    struct ServiceEntry {
        ServiceOptions          options;
        const RpcMethodTable*   table;
        std::shared_ptr<ICoService> instance;      // 仅 Concurrent
        ActorRegistry*              registry = nullptr; // 仅 ActorSerial
        std::shared_ptr<OrderedIngress> ordered_ingress; // 仅 ordered ActorSerial
    };

    explicit InboundDispatcher(
        std::map<std::string, ServiceEntry> services);

    // 在受管协程内调用（infra 派发的 handler 上下文）。完成 envelope
    // 校验 → 上下文落地 → 执行策略分发 → 回复封包。ActorSerial 路径
    // 的 CompletionSignal::Wait 要求协程上下文；非协程调用在 actor
    // 路径会以错误返回而不是未定义行为。
    result<bbt::infra::RpcEnvelope> Dispatch(
        const bbt::infra::IncomingCallContext& incoming,
        bbt::infra::RpcEnvelope request);

private:
    struct PerService {
        ServiceEntry        entry;
        std::atomic_size_t  inflight{0};
        std::mutex          mb_mtx;
        std::unordered_map<std::string, ActorMailbox::SPtr> mailboxes;
    };

    // 服务级 max_inflight 接纳计数（RAII 递减）。
    struct InflightGuard {
        explicit InflightGuard(std::atomic_size_t& c) : c_(c) {}
        ~InflightGuard() { c_.fetch_sub(1, std::memory_order_acq_rel); }
        std::atomic_size_t& c_;
    };

    ActorMailbox::SPtr _MailboxFor(PerService& svc,
                                   const std::string& actor_key);

    std::map<std::string, std::unique_ptr<PerService>> m_services;
};

} // namespace bbt::framework
