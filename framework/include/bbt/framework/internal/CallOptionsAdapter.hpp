#pragma once
// co-service-actor/v1 F1-a：framework::CallOptions → infra::CallOptions
// 出站适配（契约第 141-143 行）。两者是分层类型，不可直接互换。
//
// 规则：
//  - 缺省 deadline 继承父请求剩余期限；显式期限不得延长父预算（取最小值）。
//  - 顶层入口必须配置有限期限：无有限预算 → 配置失败（明确错误），
//    不默认无限。
//  - 已过期 → 在发起任何 I/O 之前返回 infra::ErrorCode::TimedOut。
//  - infra::CallOptions 只携带单调时钟绝对期限；不携带业务取消令牌。
//    协程等待结果按 coroutine 自身契约处理，业务级取消由业务流程按 CoXxx
//    协程返回值自行决定（if/return/继续下一调用），框架不提供单操作业务取消。
//  - 「发起 I/O」与「消耗序号」经注入点执行，测试可断言动作未发生。
//    ordered 不属于 infra::CallOptions；票据字段只由可信适配器填入
//    RpcEnvelope 的 fw.* 系统元数据（真实接线属 F1-b）。

#include <functional>

#include <bbt/infra/NetworkTypes.hpp>

#include <bbt/framework/CallOptions.hpp>
#include <bbt/framework/RequestContext.hpp>
#include <bbt/framework/Result.hpp>

namespace bbt::framework {

// 出站调用装配的注入点：每个「对外可见动作」都可替换。
struct CallEgressHooks {
    // 消耗接收端序号（ordered 票据首次发送绑定）。仅当 options.ordered
    // 存在时调用；options.ordered 存在但本注入点为空 → InvalidArgument
    // （有序调用必须有序号消耗者，不静默直发）。
    std::function<result<void>(const OrderedStamp& stamp)> consume_sequence;
    // 发起 I/O：把装配好的 infra::CallOptions 交给下游发送；
    // 空 → RuntimeUnavailable（本切片无真实出站接线）。
    std::function<result<void>(const bbt::infra::CallOptions&)> initiate_io;
};

// parent 为空 = 顶层入口。成功路径：装配 infra::CallOptions →（若
// ordered）消耗序号 → 发起 I/O，返回实际传给下游的 infra::CallOptions。
// 任何失败路径都不发起 I/O、不消耗序号。
result<bbt::infra::CallOptions> AdaptCallOptions(
    const RequestContext* parent,
    const CallOptions&    options,
    const CallEgressHooks& hooks);

} // namespace bbt::framework
