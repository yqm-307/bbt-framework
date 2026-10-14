---
label: wayfinder:grilling
blocked_by: [remove-scheduler-stop]
assignee:
claim_session:
status: closed
outcome: resolved
archive:
---
# 外部资源关闭与 Scheduler 解耦

## Question
如何明确 framework 优雅关闭、infra `RequestClose/WaitClosed` 与 coroutine Tick 的边界，确保不再用 Scheduler Stop 代替 FD、第三方 context、连接和 worker bridge 的物理收口。

## Done when
三层关闭职责、超时/进程退出取舍、`WaitClosed` 后是否还需要任何 scheduler API 均有决定。

- 用户确认：按正常优雅路径冻结：framework 停止接收 → handler 结束/协作式取消 → framework 管理者主动调用 infra `Close()` → 进程退出。
- 用户确认：handler 卡死、进程强杀、资源泄露等异常不纳入本地图；交由 supervisor/部署层处理。framework 不为用户行为兜底。
- 分层原则：`RedisMgr`、`MongoMgr` 等上层封装负责资源所有权、在途操作收口和优雅关闭；infra 提供稳定、可靠、简单的资源服务与同步 `Close()`，不由 coroutine runtime Stop 或复杂完成机制兜底。
## Research: Go gnet v2
- 官方定位：TCP/UDP/Unix transport 之上的轻量 event-driven 层；HTTP、RPC、WebSocket、Redis 等协议在其上组合实现，不把应用协议塞进 transport。
- `Conn` 明确区分并发边界：Reader 与普通 Writer/缓冲操作要求在所属 event-loop 内；Socket 操作并发安全；`Wake`、`AsyncWrite` 可跨 goroutine 投递到 event-loop。
- `Conn.Close()` 文档虽标为 concurrency-safe，但源码实现是向所属 poller 投递 `loop.close`；调用返回不等于 FD 已物理释放。`EventLoop.Close(conn)` 才是 owner 域内的关闭操作。故 gnet 的跨线程 `Close()` 不能直接替换本项目已冻结的“同步 `Close()` 返回即资源关闭”语义。
- `Wake`/`AsyncWrite` 同样是跨域投递；它们适合表达“请 owner 域执行动作”，不等同于同步资源释放。
- engine graceful `Stop` 等待连接和 event-loop 关闭，文档描述为无限等待；这属于 engine/service 层语义，不应复制为 coroutine runtime 的公共 Stop。
- 可借鉴原则：不统一规定“所有资源都单 owner”或“所有 Close 都任意线程安全”，而是为每个接口显式声明 owner 域、并发安全性和跨域投递方式；TCP 与 UDP 的读写/发送语义可分别定义。
- 参考：<https://pkg.go.dev/github.com/panjf2000/gnet/v2>、<https://gnet.host/blog/announcing-gnet-v2-8-0/>、<https://github.com/panjf2000/gnet/blob/v2.10.0/gnet.go>。

## Architecture alignment: Scheduler / FD / Connection / business
- 当前 runtime 实际链路：Scheduler 线程 `PollOnce()` 只检测 FD 事件；`CoPollEvent::Trigger()` 回调 `Coroutine::OnCoPollEvent()`，随后 `OnActiveCoroutine()` 把等待协程投入 Processer；真正的 syscall 与 buffer 处理在被恢复的协程中执行。当前不存在独立的 infra per-FD 处理协程。
- 用户提出的目标边界：infra 不负责唤醒业务协程，也不理解业务请求/事务；infra 只负责 Connection 级 FD 事件、读写 buffer、关闭事件，并向上层提供 `OnRead`/`OnWrite`/`OnClose` 等连接事件回调。业务侧如何把连接事件映射为业务协程、队列或 manager 语义，由上层决定。
- 候选时序：`Scheduler.PollOnce → 唤醒 FD 对应 Connection 处理协程 → 处理 syscall/buffer/关闭 → 调用上层连接回调`。需要区分：若唤醒的正是当前等待 `ReadSome/WriteSome` 的协程，则无需额外 per-FD 协程；若固定绑定独立 Connection 协程，则会增加一次“Connection 协程 → 业务协程”的切换，但能把 buffer 与连接事件集中在 infra。
- 设计原则：对齐 gnet 的是“连接归属 event-loop、事件由 loop 检出、跨域通过投递/唤醒、协议逻辑在上层组合”，不是照搬 gnet 的异步 `Close()` 或 engine `Stop()`。

## Checkpoint
- 用户已确认网络层三层职责：`Scheduler → Connection handler → infra callback`。Scheduler 驱动 FD 就绪与通用唤醒；Connection handler 处理本连接的非阻塞读写、buffer 与关闭；通过 `OnRead/OnWrite/OnClose` 等连接级回调交付事件。
- Transport 不持有业务协程身份、不处理协议请求结果或业务等待；上层 adapter 可在回调中自行决定结果交付与唤醒。用户未冻结四层端到端模型。
- adapter 与业务协程的请求完成关系已拆到 [adapter-request-completion](adapter-request-completion.md)，新工单仅建档，未认领或推进。
- 本次冻结的是职责链，不是物理线程/协程布局：一个 handler 是否长期占有 coroutine stack、恢复是否固定在同一 worker、回调执行域、buffer 所有权与事件合并/公平预算尚未决定；不能把先前“倾向方案一”扩写为已冻结这些细节。
- 关闭语义仍沿用已确认决定：manager 决定关闭时机，正确执行域内的 Close 同步物理释放；没有重新引入 scheduler Stop/Pause 或 RequestClose/WaitClosed。
- 用户决定：不承诺 Connection 回调具备挂起能力；建议回调不做阻塞操作，只做一次异步唤醒或投递。Connection handler 不在回调内同步等业务结果；该建议写入契约口径，不作为可挂起能力的承诺。
- 用户决定：`Close()` 直接丢弃未发送数据并同步释放 buffer，不做 flush；需要保序投递时由上层在 `Close()` 前自行确认已写完。至此本工单 Done when 三项（三层关闭职责、异常/超时取舍、`WaitClosed` 后是否还需要 scheduler API）均已决定，收口为 resolved；物理执行布局留待实现阶段验证。

## Decision scope correction
- 前述“业务协程 → handler → 业务协程”为请求场景中的投递/交付边界，不代表每次 I/O 必须发生两次恢复，也不代表两条边界只能由不同协程实现；额外切换与资源成本取决于执行布局和批处理，需要真实验证。
- 单 poller 驱动线程不等于所有工作都只检测事件：当前 GetExecutor 可在同一循环执行已投递的 handler。以上冻结描述目标网络层职责，不声称现有全部 adapter 已按此实现。

## Resolution
关闭边界与网络层职责按以下结论冻结（2026-09-30，用户逐题确认）：

1. **三层关闭职责**：`bbt-framework` 编排服务优雅关闭（停止接收新请求 → handler 自然返回/协作式取消）；资源所有权在 framework 层 manager（`RedisMgr`/`MongoMgr` 等），由其决定时机并**主动调用** infra 资源的同步 `Close()`；`bbtools-infra` 只提供稳定可靠的资源服务与同步物理释放；`bbt-coroutine` 不参与业务关闭，也不承载 Stop/Pause/Close 语义。
2. **异常与超时**：handler 卡死、进程强杀、资源泄露不由下层兜底；交由 supervisor/部署层。framework 不为用户行为提供兜底。
3. **不再需要任何 scheduler API**：`Close()` 同步释放后无需 `WaitClosed` 完成等待，也不依赖 scheduler Stop/Pause；`RequestClose`/`WaitClosed` 随之删除（见 [remove-scheduler-stop](remove-scheduler-stop.md)）。
4. **网络层三层职责**：`Scheduler → Connection handler → infra callback`。Scheduler 只驱动 FD 就绪与通用唤醒；Connection handler 处理本连接的非阻塞读写、连接 buffer 与关闭；通过 `OnRead`/`OnWrite`/`OnClose` 等连接级回调交付连接事件。Transport 不持有业务协程身份，不编排协议请求完成。
5. **回调执行域**：不承诺回调具备挂起能力；建议回调不做阻塞操作，只做一次异步唤醒或投递。Connection handler 不在回调内同步等业务结果。
6. **Close 与 buffer**：`Close()` 直接丢弃未发送数据并同步释放 buffer，不做 flush；需要保序投递时由上层在 `Close()` 前自行确认已写完。

**范围声明**：本次冻结的是职责链与关闭语义，不是物理执行布局。一个 handler 是否长期占用 coroutine stack、恢复是否绑定固定 worker、buffer 所有权细节与事件预算均未冻结，属实现阶段按连接规模与基准验证。Redis/Mongo/HTTP adapter 的请求完成与各自关闭边界移交 [adapter-request-completion](adapter-request-completion.md)。

**依据**：用户对 Close 语义与分层原则的确认；gnet v2 调研（`Conn.Close()` 为跨域投递、`EventLoop.Close` 才在 owner 域收口，其 engine graceful `Stop` 属上层语义，不复制回 coroutine runtime）；当前 runtime 实际链路（`CoPoller::PollOnce → CoPollEvent::Trigger → Coroutine::OnCoPollEvent → OnActiveCoroutine → Processer`）。

**参考**：<https://pkg.go.dev/github.com/panjf2000/gnet/v2>、<https://gnet.host/blog/announcing-gnet-v2-8-0/>、<https://github.com/panjf2000/gnet/blob/v2.10.0/gnet.go>
