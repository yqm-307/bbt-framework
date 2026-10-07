---
label: wayfinder:research
blocked_by: []
assignee:
claim_session:
status: open
outcome:
archive:
---
# 旧分布式研究与新生命周期契约对齐

## Question
固定版本的分布式研究中，哪些语义仍适合首个业务 RPC 切片，哪些已被 process-lifetime runtime、同步 `Close()` 和新等待机制取代？如何保留预算、未知结果、资源寿命和故障证据要求，而不重新引入 `Scheduler::Stop()`、`WaitClosed`、旧取消 token 或运行时代际？

## Done when
- 固定实际阅读的 framework/infra/coroutine 完整 SHA、规范版本及未提交规范的来源；区分已确认设计、实际实现和未验证建议，不以本机安装版本代替消费版本。
- 对照[研究输入摘录](../evidence/research-input-context.md)、本仓架构基调/验收矩阵和归档生命周期工单，形成“原命题 → 当前契约 → 保留/替代/失效/待证 → owning layer → 来源”的逐项对齐表。
- 至少覆盖 runtime stop、`RequestClose`/`WaitClosed`、completion/cancel/deadline、资源物理释放、标识/代际五类风险；区分已删除的运行时代际与业务实例 incarnation、协议会话或 owner/fencing 标识，不因名字相近全部删除。
- 区分 framework 拒新/业务收口、manager 主动同步 `Close()`、coroutine 进程生命周期与 supervisor 硬终止；静态路由无 draining/注销通道时，明确 N/A、本地拒新及不能宣称远端已摘除的边界。
- 给出对预算、`OutcomeUnknown`、幂等前提、默认不重试、admission 和 M/I/D 证据要求的条件性保留建议；研究建议或测试设计不升级为已实现/已通过。
- 列出本仓规范需要复核的准确位置与原因，仅建议后续授权修订；不改归档决定、不修改三仓源码，不恢复兼容壳。需要改变已确认决定时停在用户确认边界。

## Evidence
- [研究输入摘录与版本指纹](../evidence/research-input-context.md)：足够定位本问题的背景，不代替完整报告或当前源码。
- [架构基调](../../../docs/architecture/distributed-framework-baseline.md)、[验收矩阵](../../../docs/architecture/distributed-framework-validation-matrix.md)、[证据索引](../../../docs/architecture/distributed-framework-evidence.md)。规范内的旧措辞也须审视，不能因处于规范目录就忽略较新决定。
- [归档生命周期地图](../../coroutine-tick-lifecycle/MAP.md)、[Stop/旧等待机制删除决定](../../coroutine-tick-lifecycle/tickets/remove-scheduler-stop.md)、[资源关闭边界](../../coroutine-tick-lifecycle/tickets/resource-close-boundary.md)。引用决定，不重开。
- [跨仓消费检查点](p0-consumption-matrix.md)、[资源契约检查点](resource-registry-contract.md)、[业务 RPC 检查点](business-rpc-slice.md)。它们是关联输入，不是当前 research 的阻塞前置；本工单不接管其执行 issue。

## Checkpoint
- 已确认：用户选择扩展现有 Issue #1 地图，以首个可验收业务 RPC 切片为终点，不启动实现、不重开旧决定。
- 已核对：本轮研究使用旧 framework/infra SHA，综合稿仍含“停止 runtime”；归档决定删除公共 Stop/WaitClosed/旧 token，存在需要逐项对齐的真实差异。
- 待决/阻塞：上述差异如何映射到实际消费版本和本次切片，尚未研究、未形成 Resolution；未认领，无本工单在途调查。
- 下一步：由后续一次 `mwc-wfer-continue` 只读推进本工单，先核对输入版本，再给条件性对齐表；不运行框架实现或故障注入。
