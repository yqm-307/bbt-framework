---
schema: wfer-execution/v1
plan_id: infra-rpc-outcome-unknown
revision: 1
artifact: design
---
# 出站请求阶段与未知结果设计

## 实现方向

1. 以当前 `HttpClientImpl::ClientOp` 为唯一 operation owner，先定位 resolve/connect/write/read 的线性化点和现有 Finish/Abort/Unregister 关系。
2. 在 operation 内维护最小阶段状态，并在“请求字节全部交给 socket/发送泵确认完成”的唯一位置发布 `RequestCommitted`；不得在 connect 成功或第一次 send 前发布。
3. 将阶段事实与最终 `Error` 绑定。若新增字段，应保持向后兼容、默认安全；若现有公开 `Error` 不适合扩展，可提供同层公开结果结构，但不能让消费者读取内部 socket 状态。
4. 保留 HTTP 成功响应、明确 HTTP 错误和未提交传输失败的现有映射；仅在已提交且回复未知的路径映射 `OutcomeUnknown`。RpcWire 复用现有 enum/编码，不改 envelope schema，除非证据证明当前 schema 无法承载且另行记录。
5. 用动态 loopback server 控制“完整收到请求后不回复/主动断开”，并用 server-side commit marker 与 client-side phase 交叉断言；另设连接拒绝、写中断、正常响应对照。

## 关键取舍

- 不把所有 EOF/reset/timed-out 统一转为未知：这会把未发送请求误报为可能生效。
- 不只在框架层猜测：框架看不到 HTTP client 的完整写入边界，阶段语义必须在 infra owner 内产生。
- 不引入重试或幂等：本票只提供事实分类，业务如何恢复属于上层语义；当前 GetValue 无副作用也不授权通用 retry。
- 不依赖内部 socket 状态向下游泄漏：公开结果应表达“是否已提交/是否未知”的事实，不表达 Beast/Asio 对象。

## 变更面

预期范围：`bbtools-infra` HTTP client operation/result 公开头与实现、必要的 RpcWire 映射/文档、HTTP loopback 回归和直接耦合测试、最小消费示例或契约文档。

不改：framework 源码、#8 envelope 字段、#37 配额设计、#62 session teardown 的既有修复、coroutine、TLS、认证、部署配置。

## 风险与恢复

- 若当前公开 `Error` 无法安全表达阶段，先形成最小兼容接口并让 framework 消费；不强行改 enum 数值顺序或破坏 ABI。
- 若写泵无法提供可靠的完整写出线性化点，保持 `OutcomeUnknown` 未实现并在 issue 标为阻塞，不用时间/日志猜测。
- 若测试暴露 close/teardown 或配额回归，停止本票扩展，保留失败证据，按直接根因回到对应既有 issue；不把关联问题塞入本票。
- 所有失败路径必须清理自有进程和动态端口；不终止无关进程、不做全局清理。
