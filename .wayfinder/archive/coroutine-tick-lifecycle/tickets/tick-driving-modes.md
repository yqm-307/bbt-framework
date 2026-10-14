---
label: wayfinder:grilling
blocked_by: []
assignee:
claim_session:
status: closed
outcome: resolved
archive:
---
# 统一 Tick 驱动模式

## Question
如何用同一套现有 Tick 语义表达三种模式：主线程自动 tick、主线程主动单次 tick、后台线程自动 tick；配置节拍只决定自动循环何时再次尝试，不增加额外 Tick 类型或参数化等待策略。

## Done when
三种模式、Tick 是否阻塞、节拍配置、事件活跃时连续推进和空闲时行为均有明确契约。

## Checkpoint
- 已确认：`SCHE_START_OPT_SCHE_NO_LOOP` 下主线程只负责主动驱动事件循环；现有 worker/Processer 执行模型保持不变，不新增单线程协程执行模式。
- 已冻结：三种模式沿用现有 API；自动模式按 `m_cfg_scan_interval_ms` 控制下一轮 `_OnUpdate()` 的最早时间；一轮内部活跃事件连续推进；`LoopOnce()` 每次调用立即执行现有 `_OnUpdate()`；不新增 Tick 参数或等待策略。

## Resolution
Tick 驱动模型冻结为三种现有模式：后台线程自动 tick 使用 `SCHE_START_OPT_SCHE_THREAD`，主线程自动 tick 使用 `SCHE_START_OPT_SCHE_LOOP`，主线程主动单次 tick 使用 `SCHE_START_OPT_SCHE_NO_LOOP` + `LoopOnce()`。自动模式只用 `m_cfg_scan_interval_ms` 控制下一轮 `_OnUpdate()` 的最早时间；一次 `_OnUpdate()` 内若有活跃事件则继续推进；主动 `LoopOnce()` 每次调用立即执行现有 `_OnUpdate()`。`SCHE_START_OPT_SCHE_NO_LOOP` 不改变 worker/Processer 执行模型，不新增单线程模式、Tick 参数或等待策略。
