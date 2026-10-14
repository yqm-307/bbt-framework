---
schema: wfer-execution/v1
plan_id: infra-rpc-outcome-unknown
revision: 1
artifact: proposal
status: issued
---
# infra 出站请求阶段与 OutcomeUnknown

## 目的
补齐 `bbtools-infra` HTTP/RPC 出站结果语义：让调用者可机械区分请求尚未完整发出时的确定失败，与请求已完整发出但没有可信回复时的 `OutcomeUnknown`。这是 framework #4 R5/V05 的真实前置，避免将所有断连猜成未知，也避免把已可能生效的请求伪装成未发送。

## 范围
做：核对当前 HTTP client 的 resolve/connect/write/read 生命周期；定义最小请求阶段事实；实现安全的终态映射；保留既有 `TransportError`、`TimedOut`、`Cancelled` 等适用语义；补单元/loopback/异常时序验证和公开文档。
不做：不改 framework 业务协议、不重做 #8 wire profile、不改配额 #37、不重做 session teardown #62、不增加自动重试/幂等/查询补偿，不引入 TLS/认证或新依赖。

## 影响与依据
- 下游阻塞：`bbt-framework#4` 当前已合并 GetValue/P0-A，但 R5 不能用当前 infra 语义安全闭合。
- 固定消费基线：infra `0a40b702935bd6a640c836d4aadefc9b87d974df`；framework 当前 main `6b1f130a7427cce932f66a6db45557dcc95a6b54`。
- 当前源码事实：HTTP client connect/send/read 失败均经过统一分类；`ErrorCode::OutcomeUnknown` 已存在且 wire 可编码，但缺少请求阶段事实。
- 这些是已核对事实，不表示当前实现已满足本方案。

## 交付约定
交付目标仓 `bbtools-infra` 的自包含执行 issue；计划出口为独立 review、当前 head required CI 全绿、真实请求阶段矩阵通过后形成可合并 PR。创建 issue 和派遣不授予用户未明确的合并、部署或生产操作授权。
