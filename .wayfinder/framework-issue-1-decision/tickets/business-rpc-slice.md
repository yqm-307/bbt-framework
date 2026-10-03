---
label: wayfinder:research
blocked_by: []
assignee:
claim_session:
status: open
outcome:
archive:
---
# 首个业务 RPC 最小切片与跨语言互通边界

## Question
在 infra wire profile 已完成、而 framework Issue #4 仍未完成的前提下，首个真实业务 `.proto`/IDL、deadline → remaining budget、接收端 local-min、错误传播和非 C++ consumer 应如何取最小范围，才能证明业务互通而不把 framework 策略下沉到 infra 或预建通用 codegen 平台？

## Done when
- 选定首个业务切片的范围与 `.proto` 唯一真源，明确生成/校验入口和兼容升级样例。
- 明确发送端 deadline 到 remaining budget、接收端 local-min、取消/关闭、`OutcomeUnknown` 与错误传播的责任层和观察断言。
- 明确 C++ server + 一个非 C++ client 的真实跨进程验收矩阵，覆盖未知字段、类型错误、业务错误、超时/预算和 schema 兼容升级。
- 核对现有 framework/infra 公共 API 和 wire profile 的边界，列出不引入 gRPC、不建设通用 codegen 平台、不修改 infra wire contract 的限制。

## Checkpoint
- 已确认：ProtoCodec seam 不等于正式业务 wire 接线完成；默认 x-bbt header bridge 与正式 protobuf body profile 尚未对齐，Echo 不单独满足真实业务验收。
- 证据与执行接续真源：[Issue #4 研究检查点](https://github.com/yqm-307/bbt-framework/issues/4#issuecomment-5951076514)；方案重规划、候选与工具链前提留在执行 issue，不在 map 另记执行进度。
- 决策下一步：确认首个真实业务、schema 真源、预算/未知结果与正式 bridge 映射范围；不把候选建议当已定业务决定。
