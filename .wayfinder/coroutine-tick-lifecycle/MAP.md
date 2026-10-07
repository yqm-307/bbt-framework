---
label: wayfinder:map
status: archived
---
# Coroutine Tick 生命周期重构

## Destination
明确 `bbt-coroutine` 的 process-lifetime runtime 与统一 Tick 驱动模型，决定是否移除公共 `Scheduler::Stop()`，并给出 coroutine、infra、framework 三仓可执行但不混责的迁移边界。完成判据：生命周期、三种驱动模式、资源关闭、测试隔离和兼容迁移均有用户确认的决定；未决定项不得进入实现。

## Notes
- 项目真源：`bbt-framework/docs/architecture/`；关联仓：`bbtools-coroutine`、`bbtools-infra`。
- 用户已确认方向：主线程自动 tick、主线程主动单次 tick、后台线程自动 tick；统一现有 Tick 语义，不新增 `ONCE/NONBLOCK` 参数化复杂度。
- 当前实现事实：`LoopOnce()` → `_OnUpdate()` → `CoPoller::PollOnce()`；poller 当前使用非阻塞单次推进；自动模式反复执行同一推进路径。
- 本地图只做决策探索，不修改三仓代码、不创建实现任务、不提交 PR。

## Decisions so far
- [Runtime process-lifetime 决策](tickets/runtime-lifetime.md): 只初始化一次、不承诺 restart/动态卸载，测试按进程隔离；不影响 framework 业务关闭和 infra 资源关闭。
- [统一 Tick 驱动模式](tickets/tick-driving-modes.md): 三种现有 Start 模式冻结；自动模式按配置间隔推进，单次 LoopOnce 立即推进，不增加等待策略或单线程分支。
- [Scheduler Stop 公共语义](tickets/remove-scheduler-stop.md): 彻底删除公共 `Stop()` 与运行时代际及旧代际守卫；infra 关闭语义收敛为同步 `Close()`（`WaitClosed` 删除）；`CompletionSignal`/`CancellationToken` 整体删除，替代为 `CoWaiter` + 自定义事件 + `CoEventValue`。
- [网络层三层职责与关闭边界](tickets/resource-close-boundary.md): `Scheduler → Connection handler → infra callback`；三层关闭职责（framework 编排 / manager 拥有并主动 Close / infra 同步释放）；回调不承诺可挂起、建议只做一次投递；`Close()` 丢弃未发送数据并同步释放 buffer。
- [Adapter 请求完成与错误边界](tickets/adapter-request-completion.md): 业务调用内部以 `CoWaiter::WaitWithCallback` 完成等待登记/一次投递/挂起；Connection 统一管理网络，Redis/HTTP/Mongo 各自实现协议与适配；pending/已发送/关闭的终态、迟到响应和基础错误语义统一，不在 infra 重试。
- [测试隔离与三仓迁移](tickets/test-and-migration.md): 每个测试可执行文件一次初始化 runtime、进程边界隔离；按 coroutine → infra → framework → 文档迁移；不保留 Stop/RequestClose/WaitClosed/CompletionSignal/CancellationToken/代际兼容壳；各层改测自身资源/服务契约，不以 Scheduler Stop 验收。
- [契约与历史决策污染清理](tickets/contract-and-docs.md): 三仓按层确定规范真源与影响文件；历史 Issue/PR/报告不改写而标注 superseded；公共破坏性迁移不留兼容壳，按 SHA、self-check、真实集成、diff/check、warning 和独立 review/CI 门禁实施。

## Not yet specified
- Connection handler 的物理执行布局（长期协程 vs 按需 handler、worker 绑定、事件预算）已明确划入 Issue #1 实施阶段，按真实连接规模和基准验证；不作为本决策地图的未决项。

## Out of scope
- 本地图已完成决策收口；Issue #1 的执行排程与验收交接见 [`issue-1-delivery-plan.md`](../issue-1-delivery-plan.md)。该计划是实施承接产物，不代表代码已实现。
- Connection handler 的长期协程/按需 handler、worker 绑定、事件预算和性能阈值：留待实现阶段基准验证，不在本地图冻结。
- 本阶段不实现新的调度器、Poller、协程 API 或资源关闭协议。
- 不把资源物理关闭本身视为 Scheduler 生命周期语义；infra 关闭协议形态已由 `remove-scheduler-stop` 与 `resource-close-boundary` 决定。
- 不在本地图内决定业务服务重启、进程 supervisor 或部署编排方案；只定义 runtime 对这些机制的边界。

## Archive
- status: `complete`
- scope: 整张 `coroutine-tick-lifecycle` 决策地图
- user confirmation: 用户确认“正常归档这张决策地图”，并确认 Issue #1 计划继续承接实施，不关闭 Issue #1。
- evidence: 7/7 工单均为 `closed / resolved`；地图预览结构校验通过；执行计划为 `.wayfinder/issue-1-delivery-plan.md`。
- decision boundary: 本地图完成生命周期、Tick、Stop 删除、网络/adapter/资源关闭、测试迁移和契约文档影响面的决策；Connection handler 物理布局不在本图冻结，转入实现阶段基准验证。
- follow-up: 按 Issue #1 计划先做 P0 基线与跨仓消费矩阵；需要继续本地图时，必须明确重开，不因实施推进自动恢复。
