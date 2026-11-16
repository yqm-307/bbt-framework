#pragma once
/**
 * 服务执行策略（业务面）。
 * Concurrent：派发协程内联执行，服务级 max_inflight 接纳计数封顶。
 * ActorSerial：按方法声明的 key 提取器取 actor key，每个 key 一条
 * 串行邮箱队列（容量 ServiceOptions::SerialQueueCapacity），同一 key
 * 至多一条 handler 在执行；key 数受 ServiceOptions::MaxSerialKeys 封顶。
 *
 * 「容量耗尽返回 Overloaded」属运行期行为，装配期不在此检查；
 * 装配期规则由 ServiceOptions::Validate 落实。
 */

namespace bbt::framework {

enum class ExecutionPolicy { Concurrent, ActorSerial };

} // namespace bbt::framework
