---
label: wayfinder:grilling
blocked_by: []
assignee:
claim_session:
status: closed
outcome: resolved
archive:
---
# Runtime 是否只随进程存在

## Question
`bbt-coroutine` 是否改为 process-lifetime runtime：只初始化一次、不支持 Start/Stop 重启、不提供动态卸载；进程退出由操作系统回收，后台线程不再作为业务可关闭资源。

## Done when
用户确认 runtime lifetime、重启和动态卸载结论，并记录接受的测试/库析构取舍。

- 用户已确认：runtime 只初始化一次，随进程生命周期；不承诺 restart 或动态卸载；后台 runtime 随进程退出由 OS 回收；测试按进程隔离，不依靠 Stop/Start 重置单例。
- 取舍：这是运行时所有权边界，不影响服务自身按 framework 生命周期启动/停止，也不取消 infra 的资源关闭协议。
- 依据：当前核心契约中 `Scheduler::Stop()` 明确不保证业务完成；当前实现 Stop 同时做 scheduler、worker、DNS、队列和 parked coroutine 清理，公开 Stop 已成为跨层重启/测试重置耦合点。

## Resolution
`bbt-coroutine` runtime 采用 process-lifetime：单次初始化，不承诺停止后重启或动态卸载；后台运行线程随进程终止由 OS 回收。测试通过进程隔离，不再把公开 `Scheduler::Stop()` 当 singleton reset 工具。该决定不改变 framework 级业务关闭，也不改变 infra 级 RequestClose/WaitClosed。
