---
label: wayfinder:grilling
blocked_by: [runtime-lifetime, tick-driving-modes]
assignee:
claim_session:
status: closed
outcome: resolved
archive:
---
# Scheduler Stop 公共语义

## Question
在统一 Tick 与 process-lifetime runtime 确认后，是否删除公共 `Scheduler::Stop()`，以及如何处理当前 `IsRunning`、restart generation、任务拒绝和后台线程控制等伴随语义。

## Done when
公共 API 终态、兼容策略、删除顺序和不再承诺的行为有用户确认；不以实现成本倒推决定。

## Checkpoint
- 已确认：彻底删除公共 `Scheduler::Stop()`，不保留兼容壳。
- 已确认：连带清理 Stop 引入的负担（`IsRunning`、restart generation、任务拒绝、worker/DNS 收口、parked/队列清理、测试 singleton reset），并盘点 infra/framework 的 Stop 依赖。
- 已确认 infra Close 语义：`RequestClose` 与 `Close` 合并，`WaitClosed` 删除；由 owner/上层决定关闭时机，`Close()` 同步物理释放并返回，`IsClosed/Status` 只读查询；scheduler 不提供 Pause/暂停语义。
- 已确认：不把 infra 资源绑死为单 owner；并发由协程挂起、`CoWaiter`、自定义事件、Chain/连接池等按效率选择。
- 盘点证据（2026-09-29）：`CompletionSignal`、`CancellationToken/CancellationSource`、窄接口 `CoWaiter::Wait(WaitOptions)`/`WaitStatus`/`WaitOptions`、`RuntimeGeneration`/`CurrentRuntimeGeneration`/`ICoObject` 均由 `0210f45`（2026-09-18，#351 Service/Actor 运行时底座）一次性引入，约 11 天前；`generation` 概念更早来自 `0c0e54a`（2026-09-14，#341 停机生命周期）。`CoWaiter` 本体是 2024-09-03 起的既有通用等待原语，不属新引入。
- 依赖差异：`CancellationToken`/`CancellationSource` **无 generation 依赖**（纯共享取消标志 + 回调表，唯一耦合点是向 `CoWaiter` 登记唤醒回调），但它是 infra/framework 公开签名中的参数类型（`ICoCloseable::WaitClosed`、`CoTCP.hpp:54/123`、`CoUDP.hpp:60`、`NetworkTypes.hpp:20/46`、`HostLifecycle.hpp`、`CallOptionsAdapter.hpp`），删除即两仓公共接口破坏。`CompletionSignal` **有硬性停机耦合**（构造要求有效代际、`Complete`/`Wait` 比对 generation、`RuntimeUnavailable` 仅为停机/旧代际存在），但它是 infra 生产路径 14+ 处「第三方回调线程 → 协程等待」的桥（`IoSupport.hpp`、`MongoSupport.hpp`、`MongoRuntime.cc`、`CoRedisCliImpl.cc`、`HttpClientImpl.cc`、`NetworkRuntimeImpl.cc`、`HttpServerImpl.cc`），framework `InboundDispatcher.cc:297` 亦用于每请求业务结果等待。
- 用户决定（选项 1）：**整体删除** `CompletionSignal` 与 `CancellationToken/CancellationSource`；外部线程完成桥与外部取消改用 `CoWaiter` 组合等待 / 自定义事件重写；接受 infra/framework 公共签名变更与重写量。依据：等待机制本身由 `CoWaiter`/`CoPollEvent` 承载，这两个类型是叠加语义，用户认为同步模式应能覆盖所有设计。
- 随之失效：`WaitStatus::RuntimeUnavailable`（停机/旧代际专用）、`CompletionSignal` 的 generation 门控、`ICoObject` 的运行时代际身份若仅为停机服务则一并处置。
- 用户方向：取消/完成语义不必用类型表达，可用「自定义事件 + 唤醒参数」组合出丰富语义；若 `CoWaiter` 不支持带参唤醒，则扩展它。
- 核实证据（2026-09-29）：`CoWaiter` 已能经 custom event 被唤醒（`RegistCustom(POLL_EVENT_CUSTOM_COND)` + `NotifyCustomEvent`），但**不带参数**：`Notify()` 无参；`CoPollEvent::InitCustomEvent(int key, void* args)` 的 `args` 在 `CoPollEvent.cc:259` 被显式丢弃（`BBTATTR_COMM_UNUSED void* unused_args = args;`），仅存 `m_custom_key`；`Wait`/`Wait(CombinedWaitOptions)` 只返回状态枚举，`key`/`args` 无法交付给等待者。
- 用户决定：带参唤醒的载荷用 **coroutine 自定义结果类型**（小结构体/变体），不使用裸 `void*`；载荷类型与所有权在接口层写死。底层 `InitCustomEvent(key, args)` 的口径不作为对外契约。
- 用户决定：结果类型 = **类型擦除小缓冲**（如 `CoEventValue`），固定容量、不分配；支持整数、指针、小 POD 载荷；调用方按已知类型取回。
- 用户决定：`CoEventValue` 取回口径 = 内联按值拷贝 + 类型标识校验（标识不符则取失败）；指针类载荷为借用，须在等待者恢复前有效。
- 核实证据（2026-09-29）：运行时代际不止服务 `CompletionSignal`。infra `CoTCP.cc:197/217/342/380/430` 用 `CurrentRuntimeGeneration()` 作旧代际对象拒绝守卫；framework `HostLifecycle.cc:35` 用 `CurrentRuntimeGeneration() != 0` 判断运行时是否在跑；framework `CoApp.cc:278/295`、infra `MongoSupport.hpp:168/186`、`MongoRuntime.cc:42/551`、`CoTCP.cc:184` 用它构造对象身份。
- 用户决定：运行时代际（`RuntimeGeneration`/`CurrentRuntimeGeneration`/`GetRunGeneration`）及基于它的旧代际拒绝守卫一并删除；「运行时是否在跑」改用初始化状态标志；对象身份不再携带代际。
- 先前「删 Stop 专用机制还是全部等待机制」的问题已由上述决定回答：保留 `CoWaiter` 本体与既有 Wait 族，删除 `CompletionSignal`/`CancellationToken`/运行时代际。已无待决项。

## Resolution
公共 `Scheduler::Stop()` 彻底删除，不保留兼容壳；scheduler 不再承载 Stop/Pause/Close 语义。infra 关闭语义收敛为 `Close()`（`RequestClose` 与 `Close` 合一），`WaitClosed` 删除，`IsClosed`/`Status` 只读；关闭时机由 owner/上层决定，`Close()` 同步完成物理释放并返回。

`CompletionSignal`、`CancellationToken`/`CancellationSource`、运行时代际（`RuntimeGeneration`/`CurrentRuntimeGeneration`/`GetRunGeneration`）及其旧代际拒绝守卫整体删除；「运行时是否在跑」改用初始化状态标志，对象身份不再携带代际。替代机制为 `CoWaiter` + 自定义事件 + `CoEventValue`（类型擦除小缓冲：固定容量不分配、内联按值拷贝、类型标识校验、指针载荷为借用且须在等待者恢复前有效）；`CoWaiter` 本体与既有 Wait 族保留。

不再承诺的行为：runtime 停止后重启、以 Stop/Start 重置单例、旧运行时代际对象的拒绝语义、`WaitClosed` 式等待关闭完成。

约束与后续：删除触达 infra/framework 公开签名（`ICoCloseable`、`CoTCP`/`CoUDP`、`NetworkTypes`、`HostLifecycle`、`CallOptionsAdapter`），属公共接口破坏，实施前须按 `AGENTS.md` 获取确认；删除顺序、跨仓迁移与验收交由下游 `test-and-migration` 与 `contract-and-docs` 工单。
