---
label: wayfinder:research
blocked_by: [remove-scheduler-stop, resource-close-boundary]
assignee:
claim_session:
status: closed
outcome: resolved
archive:
---
# 测试隔离与三仓迁移

## Question
移除或降级 Scheduler Stop 后，如何迁移 coroutine 单测、infra/framework 集成测试、fixture、示例、文档和依赖版本，避免通过全局 singleton restart 做测试隔离。

## Done when
盘点完成，给出不改变运行时目标语义的测试策略、迁移顺序、兼容窗口和验证边界；不进入代码实现。


## Resolution
测试隔离与三仓迁移按以下策略冻结（2026-09-30）：

### 1. 隔离原则：每个测试进程只初始化一次 runtime
- CMake 中每个 `add_test` 对应独立测试可执行文件；将“进程边界”作为 runtime 隔离边界。
- 一个测试可执行文件内的多个 Boost.Test case 共享同一个 process-lifetime runtime：fixture 只负责一次初始化，不在 case teardown 调用 `Scheduler::Stop()`，也不通过 Stop/Start 恢复 singleton。
- 需要不同静态 worker 数、stack 配置或 Tick 启动模式的测试，拆成独立测试目标/进程；不能在同一 executable 内通过 Stop/Start 改配置。
- 测试结束由进程退出回收 runtime；测试必须在退出前显式验证自身业务任务、FD、adapter 和 manager 资源已经按各自契约收口。OS 回收 runtime 不替代 infra/framework 资源 Close 验证。

### 2. coroutine 仓迁移
- **删除或改写旧契约测试**：`Test_scheduler_stop`（Stop 唤醒/重复 Stop/停机后拒绝任务）、`Test_scheduler_api` 中 Start→Stop→Start 重启用例、`Test_completion_signal`、`Test_cancellation_token`、`Test_co_object_identity` 中运行时代际断言，以及只验证 Stop 不展开栈的 `Test_stop_no_unwind`，不再作为新契约测试；删除对应测试目标或改为新等待/进程生命周期测试，不能保留旧断言伪装兼容。
- **保留并迁移通用行为测试**：协程切换、`CoWaiter`、`CoPollEvent` 的 PENDING/早到事件、deadline、跨线程 Notify、Tick 三种驱动、FD/定时器/自定义事件和异常行为仍需保留；只移除其 teardown 中的 Stop，并保证每个 executable 的初始化配置固定且只启动一次。
- **补充替代验收**：验证 `WaitWithCallback` 的“登记 → callback 投递 → 挂起”时序、事件先到时 PENDING 不丢、超时/取消结果、单次 runtime 初始化；不再验证 runtime 可停止、重启或动态卸载。
- examples/benchmark/debug 中的 Stop 只作为迁移消费者处理：示例改为进程结束退出，基准按一次进程运行；不把 benchmark 的 Stop 调用当作 runtime 正确性证据。

### 3. infra 仓迁移
- 先固定 coroutine 新契约，再迁移 infra；不在旧 coroutine API 与新 infra API 之间做双轨兼容。
- `RequestClose/WaitClosed` 测试改为 manager/资源 owner 主动 `Close()` 的同步契约：返回即物理资源释放；未发送数据丢弃，不 flush；pending 请求不发送；已发送请求超时/取消后业务立即返回，迟到响应继续被消费但丢弃；连接关闭对未完成请求只交付一次终态。
- CoTCP/CoUDP/HTTP/Redis/Mongo 各自保留真实协议/driver 测试和模块特有错误映射，不把它们改造成统一万能请求测试；共用的是关闭、等待/唤醒、终态一次性交付和基础错误分类。
- infra 测试不得以 `Scheduler::Stop()` 验证资源释放；每个测试目标启动一次 runtime，测试结束前显式调用并验证对应资源 `Close()`，进程退出只负责回收 coroutine runtime。

### 4. framework 仓迁移
- `HostLifecycle` 的正常路径保留：停止接收 → handler 自然返回/协作式取消 → framework manager 主动 Close infra 资源 → 释放对象 → run 返回。
- 删除依赖 `RequestClose/WaitClosed/ReleaseClosed` 和 `GetRunGeneration()==0` 的旧关闭链测试；framework 不再通过 coroutine Stop 完成资源收口。
- F1/F2 中只为 CompletionSignal 或 Stop 造的测试夹具，改用 `CoWaiter::WaitWithCallback`/新的请求完成机制；F3 保留正常关闭、在途请求终态和资源 owner 顺序的真实 loopback 验证。
- handler 卡死、进程强杀、资源泄露兜底不作为本地图的新框架契约；预算耗尽/迟到收尾类测试如需保留，只能作为 supervisor/部署层或诊断覆盖，不能要求 framework/infra/coroutine 提供 Stop 兜底。

### 5. 迁移顺序与兼容窗口
1. **coroutine**：先搬迁等待类型与 `WaitWithCallback` 所需契约，删除 Stop/代际/CompletionSignal/CancellationToken 公共面，完成自身单测和 header self-check。
2. **infra**：更新 Connection/adapter 及其测试到 `Close()` + `CoWaiter`，完成 TCP/UDP/HTTP/Redis/Mongo 的模块级契约和真实 loopback/live 验证。
3. **framework**：更新 HostLifecycle、CallOptions、入站分发、测试夹具和依赖锁，验证服务级优雅关闭与请求完成。
4. **文档/示例**：最后清理三仓 README、API reference、ADR、examples 和旧 Issue/PR 引用，避免新旧契约并存。

公共 API 是一次有意的破坏性迁移：**不保留 `Stop()`、`RequestClose()`、`WaitClosed()`、CompletionSignal、CancellationToken 或运行时代际的兼容壳**。但每一步代码实施仍须按仓库规则由用户确认具体公共 diff，并通过独立 review/CI；本工单不授权实现。

### 6. 验收矩阵
- coroutine：smoke + CoWaiter/PENDING/timeout/cancel + 三种 Tick 模式 + header self-check；进程内无 Stop/Start 重置。
- infra：各模块 unit + loopback/live（可用时）+ Close 丢弃/唤醒/迟到响应/错误映射；无资源测试以 scheduler Stop 作为通过条件。
- framework：服务正常关闭顺序、外部 shutdown、在途请求终态、framework 主动 Close；不再断言 generation 归零或 Scheduler Stop。
- 三仓共同：构建日志无新增 warning；公共头不泄露第三方依赖；依赖 SHA 固定；本地按冒烟/本次功能单测/直接耦合单测验证，完整回归走 PR CI。

**未覆盖**：Connection handler 的长期协程/按需 handler、worker 绑定、事件预算和性能阈值尚未冻结；这些属于实现阶段基准，不阻塞本迁移策略。
