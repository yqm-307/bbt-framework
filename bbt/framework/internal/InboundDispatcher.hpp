#pragma once
/**
 * 入站适配器（机器面，internal/）：把 infra 的入站调用事实
 * （rpc::Request + Request::Context()）落成受管请求上下文并分发到
 * 已托管 Service/Actor 实例。
 *
 * 字段来源纪律（不得伪造）：
 * - deadline/peer_principal 只取 infra 已换算/已验证的
 * Request::Context()；request_id 与受限系统字段（fw.*）只取
 * rpc::Request；业务 custom 只允许出现在 route.* 键下。
 * - metadata 中 route.* 与受限 fw.* 白名单之外的键一律拒绝
 * （err(InvalidArgument)），不静默忽略、不转写进上下文。
 * - 白名单 fw.*：fw.trace_id 落入上下文 trace_id；有序流四元组
 * （fw.producer_id/producer_epoch/sequence/receiver_epoch）此处
 * 只做存在性与格式校验（非空、sequence 为无符号十进制）——有序
 * 入口的执行语义在 OrderedIngress，不提前消费。
 *
 * 绑定语义：RequestContext 随逻辑请求/协程绑定（RequestScope），
 * 不用 thread_local 跨挂起缓存；Service 并发处理多请求互不串扰——
 * Concurrent 在派发协程内联执行；ActorSerial 把任务交由该
 * (service,actor-key) 的 ActorMailbox 串行执行（服务对象是 Registry
 * 受管的同一个实例，串行性由邮箱保证），派发协程以每请求
 * 一个 CoWaiter 等待业务结果（登记成功后再入队；等待期间让出 worker，
 * 执行资格仍由邮箱持有，不重入）。
 */

#include <atomic>
#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/rpc/Request.hpp>
#include <bbt/infra/rpc/Response.hpp>

#include <bbt/framework/ExecutionPolicy.hpp>
#include <bbt/framework/Result.hpp>
#include <bbt/framework/internal/ActorMailbox.hpp>
#include <bbt/framework/internal/MethodTable.hpp>
#include <bbt/framework/internal/ServiceBase.hpp>

namespace bbt::framework {

class InboundDispatcher;
class OrderedIngress;

class InboundDispatcher {
public:
    // 安装簿与各次发布共享。有序入口随安装存活，保留流授权与序号状态。
    struct ServiceDescription {
        /**
         * 安装期一次写入：options/table 来自 CoInstallService，
         * ordered_ingress 由首次网络装配在 m_net_mtx 下挂上，此后只读。
         */
        ServiceOptions options;
        std::shared_ptr<const RpcMethodTable> table;
        std::shared_ptr<OrderedIngress> ordered_ingress;
    };

    // 租约保护非拥有的 Registry 实例。测试直接构造时可省略租约。
    struct ServiceTarget {
        CoServiceBase* instance = nullptr;
        std::function<bool()> lease_acquire;
        std::function<void()> lease_release;
    };

    struct ServiceBinding {
        std::shared_ptr<const ServiceDescription> description;
        ServiceTarget target;
    };

    // 兼容直接构造分发表的测试。
    struct ServiceEntry : ServiceDescription, ServiceTarget {};

    explicit InboundDispatcher(
        std::map<std::string, ServiceEntry> services);
    explicit InboundDispatcher(
        std::map<std::string, ServiceBinding> services);

    /**
     * 移除服务后，新查找返回 NotFound。关闭邮箱不丢弃已入队任务；
     * 已经进入处理函数的请求由服务卸载流程等待，此处不等待。
     */
    void RemoveService(const std::string& service);

    /**
     * 在受管协程内调用（infra 派发的 handler 上下文）。完成 envelope
     * 校验 → 上下文落地 → 执行策略分发 → 回复封包。ActorSerial 路径
     * 的 CoWaiter::WaitWithCallback 要求协程上下文；非协程调用在 actor
     * 路径返回 InvalidContext 错误而不是未定义行为。
     *
     * 执行租约：handler 执行前对服务对象取 Registry 分发租约
     *（与 hook 收口共用计数），gate 已封（对象收口中）→ Closed 拒绝，
     * 不在收口窗口内进入业务。
     */
    result<bbt::infra::rpc::Response> Dispatch(
        const bbt::infra::rpc::Request& request,
        const std::function<void()>& on_dispatched = {});

private:
    /**
     * 在途运行记录。请求在表锁内取得 shared_ptr，跨锁持有本次发布的
     * 绑定与运行状态，因此重新发布或摘表都不会提前销毁它们。
     */
    struct PerService {
        explicit PerService(ServiceBinding service) : binding(std::move(service)) {}

        const ServiceBinding binding;
        std::atomic_size_t inflight{0};
        /**
         * 摘除后拒绝新建邮箱（mb_mtx 保护）：保证摘除后不存在「新建
         * 可接纳任务的邮箱」的窗口。已建邮箱由 RemoveService 统一 Close。
         */
        bool closed{false};
        std::mutex mb_mtx;
        std::unordered_map<std::string, ActorMailbox::SPtr> mailboxes;
    };

    /**
     * 服务级 max_inflight 接纳计数（RAII 递减）。
     */
    struct InflightGuard {
        explicit InflightGuard(std::atomic_size_t& c) : c_(c) {}
        ~InflightGuard() { c_.fetch_sub(1, std::memory_order_acq_rel); }
        std::atomic_size_t& c_;
    };

    result<ActorMailbox::SPtr> _MailboxFor(PerService& svc,
                                           const std::string& actor_key);

    /**
     * 分发表在构建后主要只读；RemoveService 运行期抽表项。shared_mutex：
     * Dispatch 取共享锁（锁内复制在途记录 shared_ptr，锁外持记录使用），
     * RemoveService 取独占锁抽出记录（分发与邮箱操作在锁外）。
     */
    mutable std::shared_mutex m_services_mtx;
    std::map<std::string, std::shared_ptr<PerService>> m_services;
};

} // namespace bbt::framework
