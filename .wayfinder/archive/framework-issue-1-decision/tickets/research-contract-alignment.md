---
label: wayfinder:research
blocked_by: []
assignee:
claim_session:
status: closed
outcome: resolved
archive:
---
# 旧分布式研究与新生命周期契约对齐

## Question
固定版本的分布式研究中，哪些语义仍适合首个业务 RPC 切片，哪些已被 process-lifetime runtime、同步 `Close()` 和新等待机制取代？如何保留预算、未知结果、资源寿命和故障证据要求，而不重新引入 `Scheduler::Stop()`、`WaitClosed`、旧取消 token 或运行时代际？

## Done when
- 固定实际阅读的 framework/infra/coroutine 完整 SHA、规范版本及未提交规范的来源；区分已确认设计、实际实现和未验证建议，不以本机安装版本代替消费版本。
- 对照[研究输入摘录](../other/evidence/research-input-context.md)、本仓架构基调/验收矩阵和归档生命周期工单，形成“原命题 → 当前契约 → 保留/替代/失效/待证 → owning layer → 来源”的逐项对齐表。
- 至少覆盖 runtime stop、`RequestClose`/`WaitClosed`、completion/cancel/deadline、资源物理释放、标识/代际五类风险；区分已删除的运行时代际与业务实例 incarnation、协议会话或 owner/fencing 标识，不因名字相近全部删除。
- 区分 framework 拒新/业务收口、manager 主动同步 `Close()`、coroutine 进程生命周期与 supervisor 硬终止；静态路由无 draining/注销通道时，明确 N/A、本地拒新及不能宣称远端已摘除的边界。
- 给出对预算、`OutcomeUnknown`、幂等前提、默认不重试、admission 和 M/I/D 证据要求的条件性保留建议；研究建议或测试设计不升级为已实现/已通过。
- 列出本仓规范需要复核的准确位置与原因，仅建议后续授权修订；不改归档决定、不修改三仓源码，不恢复兼容壳。需要改变已确认决定时停在用户确认边界。

## Evidence
- [研究输入摘录与版本指纹](../other/evidence/research-input-context.md)：足够定位本问题的背景，不代替完整报告或当前源码。
- [架构基调](../../../../docs/architecture/distributed-framework-baseline.md)、[验收矩阵](../../../../docs/architecture/distributed-framework-validation-matrix.md)、[证据索引](../../../../docs/architecture/distributed-framework-evidence.md)。规范内的旧措辞也须审视，不能因处于规范目录就忽略较新决定。
- [归档生命周期地图](../../coroutine-tick-lifecycle/MAP.md)、[Stop/旧等待机制删除决定](../../coroutine-tick-lifecycle/tickets/remove-scheduler-stop.md)、[资源关闭边界](../../coroutine-tick-lifecycle/tickets/resource-close-boundary.md)。引用决定，不重开。
- [跨仓消费检查点](p0-consumption-matrix.md)、[资源契约检查点](resource-registry-contract.md)、[业务 RPC 检查点](business-rpc-slice.md)。它们是关联输入，不是当前 research 的阻塞前置；本工单不接管其执行 issue。

## Resolution

以本地图已归档的消费闭包为输入，完成了旧研究与当前三仓契约的逐项对齐。固定核对版本为 framework `b1620aa555ce0d91818cbe1e69bbf31c6d51407d`、infra `0a40b702935bd6a640c836d4aadefc9b87d974df`、coroutine `7bcda3b078f975ff2978424be7f6ba38e04fb7f6`；本地其他检出、未提交规范和旧执行候选不作为当前消费证据。

| 原命题/风险 | 当前契约 | 固定版本实际状态 | 责任层与结论 |
| --- | --- | --- | --- |
| runtime stop / restart / generation | coroutine runtime 为进程寿命；无公共 `Scheduler::Stop()`、restart 或运行时代际；业务关闭不由 runtime 承载 | coroutine `agent-docs/2026-09-07-core-runtime-contract.md:94-105` 明确该边界；framework `framework/src/host/HostLifecycle.cc:35,96-103` 不再停止 runtime，infra `include/bbt/infra/NetworkRuntime.hpp` 以 `Start → 使用 → Close` 为生命周期 | coroutine 负责进程寿命；framework/infra 负责各自 owner 关闭。旧研究中“停止 runtime”仅作 superseded 历史输入，不进入验收契约 |
| `RequestClose` / `WaitClosed` / `CloseStatus` | `ICoCloseable::Close()` 幂等同步返回；返回即 owner 自有物理资源已释放；`IsClosed()` 只读；不保留旧兼容壳 | infra `include/bbt/infra/ICoCloseable.hpp` 删除旧面；framework `framework/include/bbt/framework/internal/ResourceClose.hpp` 直接调用 `Close()`，`framework/src/host/HostLifecycle.cc:96-103` 按停止接纳→排空→Close 顺序执行 | infra 定义资源物理释放，framework manager 主动调用；旧研究/旧工作树中看到的两阶段关闭只能标为未迁移候选或历史证据，不能据此放宽当前契约 |
| completion / cancel / deadline | deadline 继续是绝对截止；等待取消是 coroutine 协作式取消；framework 不提供单操作业务取消句柄；业务按 `CoXxx` 返回结果决定后续顺序 | coroutine `bbt/coroutine/sync/WaitTypes.hpp` 仅有 deadline，`CoWaiter` 支持 `WaitWithCallback`/带载荷 `Notify`；framework `CallOptions`/`RequestContext` 只保留 deadline，`InboundDispatcher` 使用 `CoWaiter`；#32 最新用户确认废止 `CancelSource/CancelHandle` 方案 | coroutine 负责等待/唤醒与自身取消；framework 负责业务结果/顺序。不能把协程取消、业务提前 return 或 `Close()` 表述为远端 I/O 已回滚 |
| 资源物理释放与 owner 边界 | framework manager 停止接纳并排空 handler，主动调用 infra `Close()`；infra 收口自有 FD、buffer、driver/worker；coroutine 不替代资源 owner | framework `CoApp::_CloseResources()` 直接收集 `ICoCloseable` 并调用 `Close()`；infra `NetworkRuntime`/`HttpClient` 公共面均声明同步 Close；infra AGENTS 与 `docs/decisions/0002` 将后端线程/资源收口留在 infra | owner/fencing、资源账本和真实后端排空属于 framework/infra 的后续实现验收；本研究只锁定分层，不宣称 Redis/Mongo live 已通过 |
| 标识/代际 | 删除 runtime generation；业务实例 incarnation、协议会话、owner/fencing 若确有需要，必须由所属业务/资源契约单独定义，不能因名字相近一并删除 | coroutine 固定契约不再提供 runtime generation；当前 framework/infra 固定树没有把 runtime generation 作为新生命周期入口；本图不新增业务身份字段 | runtime 身份与业务/资源身份分层；若后续需要 owner/fencing，作为具体 Resource Registry/RPC 决策输入，不回引旧 runtime generation |

### 条件性保留与明确排除

- **保留**：deadline 取父/本次调用可用范围的最小值；过期在发起 I/O 前返回 `TimedOut`；已发起但结果不可确认时按 `OutcomeUnknown` 处理；默认不重试，幂等与 retry 边界由业务/调用层另行确认；`WaitWithCallback` 登记—投递握手、在途账本、真实 owner 排空和 M/I/D 证据要求继续有效。
- **保留但分层**：framework 可表达业务结果、拒新、handler 排空和资源 manager 顺序；infra 只提供 transport/adapter/资源 Close 与错误映射；coroutine 只提供 process-lifetime runtime、等待/唤醒和协程自身取消，不承载业务事务、发现或 owner/fencing。
- **排除**：不恢复 `Scheduler::Stop()`、`RequestClose`、`WaitClosed`、`CompletionSignal`、coroutine `CancellationToken`，不新增 framework-owned 单操作取消句柄，不把断连自动解释为业务取消，不以固定 sleep、逻辑 Closed 或 mock 代替物理资源收口。
- **未宣称**：当前 Resolution 不等于 #4/#8/#5/#9 的实现或真实集成完成；也不等于 live Redis/Mongo、跨语言业务 `.proto`、Compose 短压或 #1 最终 fork 验收通过。

### 证据与边界

- 研究输入摘录 `evidence/research-input-context.md:23-32` 仅作为旧命题和未运行等级的定位材料；其中“停止 runtime”与当前契约冲突，按本 Resolution 标记为 superseded，不复制为当前要求。
- 归档生命周期决定 `../../coroutine-tick-lifecycle/tickets/remove-scheduler-stop.md#resolution` 与 `resource-close-boundary.md#resolution` 是用户确认的决策真源；本票引用，不重开。
- 当前实现核对使用上述三仓完整 SHA；主工作树未提交 `AGENTS.md`、未跟踪 `.wayfinder` 草稿及其他检出不作为证据。旧执行 issue 中的历史评论保留其历史性质；#32 最新用户确认覆盖此前“framework-owned 取消句柄”草案。
- 本票没有运行 framework 实现、Redis/Mongo live、故障注入或全量测试；这些是执行/验收票的后续工作，不因研究完成而伪称通过。

## Checkpoint
- 已确认：固定三仓消费闭包上的 runtime、关闭、等待/取消、资源 owner 和身份/代际五类口径已完成对齐；当前唯一关闭契约是同步 `ICoCloseable::Close()`，framework 不提供单操作业务取消。
- 证据：framework `b1620aa555ce0d91818cbe1e69bbf31c6d51407d` 的 HostLifecycle/CoApp/InboundDispatcher/ResourceClose；infra `0a40b702935bd6a640c836d4aadefc9b87d974df` 的 `ICoCloseable`/NetworkTypes/NetworkRuntime；coroutine `7bcda3b078f975ff2978424be7f6ba38e04fb7f6` 的 runtime contract/WaitTypes/CoWaiter；对应路径与边界见上方 Resolution。
- 已知缺口：旧工作树或历史文档仍可能出现旧名称，但不改变固定消费契约；#4/#8/#5/#9/#33 的实现与真实验收仍未完成，live 后端和全量链路未运行。
- 认领/在途：本次只读核对已结束，无子代理、构建、探针或外部写入在途；释放本票认领。
- 下一步：由 `business-rpc-slice`、`resource-registry-contract`、`service-example-boundary` 各自继续形成具体决策；`p0-scope-evidence-gate` 待这些前置 Resolution 后再进行 HITL 确认，不在本票内替用户作业务承诺。
