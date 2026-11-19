#pragma once
/**
 * @brief Concurrent 允许多个请求同时执行；SerialByKey 按方法取出的 key 分队列。
 * 同一 key 每次只执行一个请求，不同 key 可并行。SerialQueueCapacity 限制每个队列等待的请求数，
 * MaxSerialKeys 限制 key 的数量；运行时超出容量返回 Overloaded。Concurrent 的并发数受 max_inflight 限制。
 */

namespace bbt::framework {

enum class ExecutionPolicy {
    Concurrent,
    SerialByKey,
    ActorSerial = SerialByKey, ///< 兼容旧拼写。
};

} // namespace bbt::framework
