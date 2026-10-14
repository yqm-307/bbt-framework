---
schema: wfer-execution/v1
plan_id: infra-rpc-outcome-unknown
revision: 1
artifact: spec
---
# 出站请求阶段与未知结果规格

## 要求与场景

- **R1 阶段可判定**：每次 HTTP/RPC 出站操作必须在不依赖错误文本的情况下区分至少 `NotStarted/Connecting`、`Writing`、`RequestCommitted`、`ReplyTerminal`；阶段事实由 operation owner 维护，晚到回调不能覆盖已发布终态。
- **R2 安全错误分界**：未建立连接或未完整写出请求时返回既有确定失败（通常为 `TransportError`、`TimedOut` 或 `Cancelled`）；请求已完整写出且未获得可信响应终态时返回 `OutcomeUnknown` 或等价、稳定、可序列化的未知结果。不能因为“连接断了”就默认未知。
- **R3 一次终态**：完成、超时、取消、Close、对端断连竞争时只发布一个逻辑终态；物理 socket/buffer 清理继续由 infra owner 完成，不以调用方返回冒充物理释放。
- **R4 保持现有消费者**：不破坏现有 `ErrorCode`、RpcWire 编解码、HTTP 成功/4xx/5xx 语义；未提交失败的既有调用继续得到确定错误；不新增默认重试。
- **R5 可消费接口**：framework 或其他消费者能通过公开稳定结果读取该分界，不接触 Beast/Asio/socket/第三方类型，不解析错误消息猜测阶段。
- **R6 可复验记录**：测试和文档明确记录请求阶段、最终错误码、是否已完整写出、物理清理完成；禁止用日志顺序、退出码或单次成功代替机器断言。

### 场景

- **S1 未建立连接**：动态 loopback 目标拒绝/不可达；结果为确定传输失败，不能是 `OutcomeUnknown`。
- **S2 写入前/写入中失败**：服务端确认未收到完整请求；结果为确定失败，未发生未知结果升级。
- **S3 完整写出后丢回复**：自有 loopback server 记录完整请求后关闭连接且不发可信响应；结果为 `OutcomeUnknown`，一次终态，无自动重试。
- **S4 正常回复**：完整成功响应或明确 HTTP 错误响应；结果保持现有成功/远端错误语义，不被改成未知。
- **S5 deadline/cancel/Close 竞态**：分别覆盖请求尚未提交与已提交两个阶段；前者确定失败，后者未知；物理清理完成后配额/operation 恰好归还。
- **S6 多次读写/keep-alive**：同一 client 的多次请求、连接复用或重建不得串用阶段状态；每个 operation 独立计账。
- **S7 兼容消费**：现有 HTTP loopback、配额和 close/teardown 回归继续通过；不把 #37/#62 的既有修复覆盖掉。

## 约束与不变项

- C++17；标准库/现有 Boost/仓内机制优先，不新增依赖。
- 公开面不泄漏第三方类型；阶段值/结果字段稳定、可序列化、可文档化。
- 不用 errno/message 字符串/固定 sleep 推导提交状态；测试使用自有动态 loopback、事件闸门或明确协议信号。
- 不自动重试、不声称 exactly-once、不提供业务补偿或取消远端操作的保证。
- 不降低既有安全校验、错误脱敏、连接/在途配额和同步物理关闭约束。
- 所有 P0-A 下游结果仍标记 `auth_state=unauthenticated_loopback`；本票不提供认证。
