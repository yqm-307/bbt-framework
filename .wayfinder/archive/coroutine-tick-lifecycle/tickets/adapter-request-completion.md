---
label: wayfinder:grilling
blocked_by: [resource-close-boundary]
assignee:
claim_session:
status: closed
outcome: resolved
archive:
---
# Adapter 请求与业务协程完成关系

## Question
网络层三层模型确定后，协议 adapter 如何接收业务协程发起的请求、匹配响应并交付结果？如何用既有 CoWaiter / 自定义事件实现等待与唤醒，而不恢复 CompletionSignal、CancellationToken 或 runtime Stop 耦合？

## Done when
用户确认 adapter 与上层 manager 的职责、请求投递与结果交付方式、等待登记与响应竞态的处理边界，以及错误/关闭时请求如何收口；形成按模块适用的契约，不进入实现。

## Inputs
- 用户已选择冻结网络层三层：Scheduler → Connection handler → infra callback。
- Transport 不理解 Redis SET 等协议请求，不持有业务协程身份或替上层编排请求完成。
- Redis SET 场景说明：业务协程发起请求后可能投递任务并挂起，等待的是请求结果而不一定是 FD 就绪。该场景是设计输入，不是所有 adapter 的已冻结实现。
- 关联：[资源关闭与网络执行边界](resource-close-boundary.md)、[Stop 及叠加机制删除决定](remove-scheduler-stop.md)。

## Checkpoint
- 用户明确要求另开工单，未认领、未推进。
- 待决：请求队列/关联方式、等待登记先后与早到响应、完成通知与结果所有权、错误/连接关闭的请求交付边界。
- 必须分别核对 Redis、HTTP、Mongo 的协议与 driver 模型，不默认三者共享每 FD 协程或相同完成机制。
- 非目标：不决定业务重试/事务、部署强杀、不恢复已删除的 token/signal 抽象，不实现或改写生产 adapter。

## 现状事实（代码级，已核实）
业务协程发起一次请求的当前链路（以 `redis.Set` 为例，`src/redis/CoRedisCliImpl.cc:91-131`）：

1. `Set()` → `Submit()`：`PreCheck()`（要求协程上下文 + client 处于 kRunning）；
2. `detail::NewCompletionSignal()` 创建完成信号；构造 `shared_ptr<RedisOp>`，把信号挂在 `op->sig`；
3. `RegisterOp(op)` 登记到 client 的 op 列表（与 teardown 竞态的提交会被明确拒绝）；
4. `m_engine.TryPost([self, op]{ ...AdmitOnIoDomain(op); })` 把命令投递到 io 域（strand）；
5. 业务协程调用 `op->sig->Wait(wait)` 挂起，`wait.deadline = options.deadline`、`wait.cancel = options.cancel`；
6. io 域执行 hiredis 打包/写入；hiredis 回调在 strand 上拿到 reply，写 `op->outcome`，再 `Complete()`；
7. 业务协程被唤醒，读 `op->outcome`，`WaitStatus::Completed` 则返回结果，否则返回映射后的错误。

`CompletionSignal` 的真实构成（`sync/CompletionSignal.hpp`）：**它已经内部委托给 `CoWaiter`**（成员 `m_waiter`），自身只额外提供三件事：
- `m_completed` 粘滞完成位 + 入口决议顺序（已完成 > 取消 > 超时 > 占用），保证「完成先于 Wait 不丢通知，晚到 Wait 立即返回 Completed」；
- `m_waiting` 计数，供 `Complete()` 决定是否需要 `Notify`；
- `m_generation` 运行时代际校验（已决定删除）。
另外注释明确：**只携带通知、不携带业务 payload**——上层必须先写结果再 Complete。

`CoWaiter` 的能力边界（`sync/CoWaiter.hpp`、`CoWaiter.cc:422-433`）：
- `Notify()` 走 `m_notify_mutex` + `g_bbt_poller->NotifyCustomEvent()`，**跨线程调用是安全的**；无等待者时返回 -1，通知即丢失；
- `Wait(WaitOptions)` 支持 `Deadline` 与取消，返回可区分的 `WaitStatus`；
- **只有唯一等待位**：`m_co_event != nullptr` 时第二次 Wait 直接返回 `AlreadyWaiting`；
- 没有「先完成、后等待」的粘滞位。

`WaitOptions`/`WaitStatus`/`Deadline` 目前**物理定义在 `CompletionSignal.hpp`**，且 `CoWaiter.hpp` 反向 include 它。删除该头文件必须先搬家。

## Resolution
请求完成契约按以下边界冻结（2026-09-30，用户逐题确认）：

1. **职责分层**：Connection 层统一管理网络连接、FD 事件、非阻塞读写、连接 buffer、关闭和连接级回调；Redis、HTTP、Mongo 各自实现协议、第三方 driver 适配及协议错误映射。Connection 不理解 Redis/HTTP/Mongo 业务请求，也不直接编排业务协程完成。
2. **一次业务调用**：业务只看到一个完整调用（例如 `rediscli.Get()`），不暴露“投递”和“Wait”两个阶段。调用内部使用 `CoWaiter::WaitWithCallback` 范式：先登记当前协程的等待事件，再执行一次投递回调，随后挂起；响应到达后由 Connection/adapter 路径通知该等待者，原业务协程恢复并读取请求状态中的结果。
3. **早到响应竞态**：登记等待与执行投递回调必须属于同一内部等待流程；不得恢复为先独立 `post`、再独立 `Wait` 的公开或内部时序。响应在协程真正 park 前到达时，复用 `CoPollEvent::PENDING` 状态机完成，不新增通用粘滞完成语义到 `CoWaiter`。
4. **结果与通知分离**：`CoWaiter` 只负责等待/唤醒，不承载协议 payload；adapter 的请求状态保存协议结果、终态和迟到响应处理。每个请求只向业务交付一次终态。
5. **pending 请求**：请求尚未发送到后端时发生 timeout/cancel，可以从 adapter 的 pending 队列移除，保证该请求未被发送；不在 infra 内重试。
6. **已发送请求**：请求已经发送后发生 timeout/cancel，业务协程立即返回对应错误；后端可能已经执行，结果可能未知。adapter 继续消费协议流中的迟到响应以保持连接对齐，但不再交付给已返回的业务调用；是否重试、补偿或接受未知结果由上层决定。
7. **连接关闭**：连接/上下文关闭时，所有尚未完成请求都只交付一次关闭或 transport 终态并唤醒对应等待者；已发送请求的后端真实结果可能未知。关闭路径不迁移请求、不由 infra 重试。
8. **错误处理**：统一错误处理方式和基础错误语义（如 `Cancelled`、`TimedOut`、`Closed`、`TransportError`、`OutcomeUnknown`）；具体 Redis reply、HTTP status、Mongo driver/server 错误由各模块分别映射为 `ProtocolError`、`RemoteError` 或附带 backend details，不新增万能请求接口。

**范围与实现边界**：本工单只冻结 Connection/adapter/业务等待的职责和可观察契约，不修改生产 adapter；`WaitOptions`/`WaitStatus`/`Deadline` 从 `CompletionSignal.hpp` 搬迁到独立 coroutine 等待契约、以及 `WaitWithCallback` 的 deadline/cancel 现代化，是后续实现迁移项。`CompletionSignal`、`CancellationToken`、runtime Stop/代际删除仍以 [remove-scheduler-stop](remove-scheduler-stop.md) 的决定为准。

**依据**：用户确认的 Connection 统一网络管理与“协议各自实现、错误处理方式统一”边界；当前 `CoRedisCliImpl::Submit`、`RedisOp::Finish`、pending/inflight 与关闭收口代码；`CoWaiter::WaitWithCallback` 与 `CoPollEvent::PENDING` 事件状态机；`bbt::infra::ErrorCode` 现有基础错误表面。