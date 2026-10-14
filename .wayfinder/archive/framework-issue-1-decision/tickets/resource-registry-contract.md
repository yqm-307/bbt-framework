---
label: wayfinder:research
blocked_by: []
assignee: ""
claim_session: ""
status: closed
outcome: resolved
archive:
---
# Resource Registry 公共契约与生命周期边界

## Question
Issue #8 的 Resource Registry 首个公共切片应如何定义 `(资源类型, 逻辑名称)` 注册/查找、Create/Start、失败回滚与关闭顺序、同名共享/不同名隔离和 manager 主动同步 Close，才能不泄漏 hiredis/mongocxx/worker/连接池类型，也不恢复已删除的 Stop/WaitClosed/旧 token 协议？

## Done when
- 回读现有 framework Resource Registry、App/Service、Redis/Mongo consumer 与已合入 infra Close 契约，列出真实调用关系和缺口。
- 明确公共 API 的最小签名/错误语义、资源 owner 归属、重复登记、启动失败回滚和关闭顺序；区分逻辑状态与物理释放。
- 明确两个 Service 共享同名资源、不同名资源隔离、关闭中请求和资源未就绪时的行为矩阵。
- 确认真实 Redis/Mongo consumer 验收所需的最小 fixture、后端前提和不可用边界；不实现、不修改默认生产路径。

## Checkpoint
- 固定消费输入已按原 SHA 复核：framework `b1620aa555ce0d91818cbe1e69bbf31c6d51407d`、infra `0a40b702935bd6a640c836d4aadefc9b87d974df`、coroutine `7bcda3b078f975ff2978424be7f6ba38e04fb7f6`。
- 已完成 framework Registry/App/Service、infra Redis/Mongo consumer、同步 Close 和桩/live 测试边界核对；未改三仓源码、未运行 Redis/Mongo live、未读取凭据、未修改 GitHub。
- 用户已裁定两个冲突：资源相互独立且不承诺顺序；预创建指针资源表示已 ready，框架不调用 `Start()`。

## Resolution

### 用户裁定与公共契约

- Resource Registry 归 `CoApp`，键为 `(资源类型 R, 逻辑名称 name)`。命名入口拒绝空名；空指针、重复键、`run()` 开始后的登记分别返回 `InvalidArgument`、`InvalidArgument`、`Closed`。旧无名 `add_resource<R>(ptr)` 保留为默认名称槽位。
- `add_resource<R>(name, factory)` 的工厂返回 `result<std::shared_ptr<R>>`。工厂只在 coroutine Scheduler 已初始化后、网络 host `Create()` 前调用；若 `R` 暴露精确的 `result<void> Start()`，框架在实例进入 Service 可见表前调用一次。工厂返回错误、空实例、`Start()` 返回错误或抛异常都使启动失败；已创建的可关闭实例仍由回退路径同步收口，半装配实例不进入 Service 资源视图。
- `add_resource<R>(name, shared_ptr<R>)` 是高级装配/测试入口：指针表示调用方已完成外部装配且实例 ready，框架不调用 `Start()`，只负责持有、向 Service 提供共享引用并在收束时统一 Close。需要框架控制 `Create→Start` 的 Redis/Mongo 资源必须使用工厂入口；这避免重复 Start，且不放宽资源实现的单次启动语义。
- Service 只能通过 `ServiceContext::resource<R>()` 或 `resource<R>(name)` 取资源；同一 `(R,name)` 对多个 Service 返回同一托管实例，不同名称隔离，缺失资源返回 `nullptr`。Service 不拥有资源，不接触 driver、worker、连接池、`Create/Start/Close` 或旧等待 API。非 `ICoCloseable` 资源不进入关闭调用，但由 Registry 的共享持有关系在释放时归还；实现 `ICoCloseable` 的资源由 CoApp 调用同步 `Close()`。

### 生命周期与失败行为矩阵

| 场景 | 框架行为 | 对业务可见结果 |
| --- | --- | --- |
| 工厂 Create 成功、Start 成功 | 登记实例视图，再绑定 Service，之后 host 开始接纳 | Service 可按 `(R,name)` 共享消费 |
| 工厂返回错误、空指针、Start 返回错误或抛异常 | 启动失败；host 尚未 Create；已取得的可关闭资源同步 Close；不绑定半装配 Service | `run()` 为启动失败；错误进入生命周期失败清单 |
| 预创建指针 | 登记时持有；启动阶段不调用 factory/Start | 进入正常 Service 视图；前提是调用方已保证 ready |
| 重复键、空名、空指针、run 后登记 | 不登记半成品，返回 `InvalidArgument` 或 `Closed` | 原有 Registry 不受影响 |
| 资源缺失或未就绪 | 查找未登记名返回 `nullptr`；工厂创建/启动失败则应用启动失败，Service 绑定与 host 接纳均不会发生 | Service 只能在资源装配成功后接收请求；业务需显式处理空引用 |
| 关闭期间的新入站请求 | `StopAccepting` 后不再接纳新 handler；已接纳 handler 不取消，等待完成后才 Close 资源 | 不新增单操作取消；handler 超预算时保留资源并继续等待 |
| 正常关闭 | `StopAccepting → WaitHandlersDone → 资源同步 Close → 网络 Close → Service release` | Close 返回即资源物理收束；不恢复 `RequestClose/WaitClosed` |
| 资源 owner 已 Close 后的新操作 | Redis/Mongo 的运行态门禁拒绝新操作并返回 `Closed`；已经接纳的 handler 在 CoApp 正常关闭路径中先排空 | 资源 Close 不是单操作取消 API；底层物理清理遵循 infra owner 契约 |
| handler 等待超预算 | 进入 `ShutdownIncomplete`，继续等待迟到排空，不提前释放资源，最终以迟到关闭码返回 | 资源在 handler 可能仍引用期间保持强持有 |

资源之间**没有隐含依赖、没有注册顺序保证、没有逆序 Close/回滚保证**；当前 `unordered_map` 遍历不是公共语义。若将来需要资源依赖，另建显式依赖/有序生命周期决策，不能依赖容器偶然顺序或本票契约。

用户已裁定 framework 不提供单操作业务取消；#8 旧正文中的 `RequestClose/WaitClosed`、`Scheduler::Stop` 和旧 token 只作为 superseded 历史输入。请求 deadline 可继续沿资源调用链传递，但 deadline/业务返回不等于强杀底层 I/O。

### 证据与验收边界

- framework 固定快照的证据为 `CoApp.hpp`、`CoApp.cc`、`ServiceContext.hpp`、`HostLifecycle.cc`、`ResourceClose.cc` 与 `Test_framework_resource.cc`；它们支持上述命名 Registry、工厂相位、共享/隔离、失败回退和同步关闭结论。
- infra 固定快照 `ICoCloseable.hpp`、`CoRedisCliImpl.cc`、`MongoRuntime.cc`、`CoMongoCli.hpp` 表明 Redis/Mongo 的 `Create` 不等于 `Start`，owner 实现提供同步 `Close()`；公共 framework 面不暴露 hiredis/mongocxx/bsoncxx。
- 当前桩测试覆盖工厂在 Scheduler 后 Create/Start、同名共享/不同名隔离、Create/Start/异常失败回退、登记校验、handler 排空后 Close 和 Close 幂等；它不证明真实后端或真实 HTTP。
- 当前 live fixture 用真实 Redis/Mongo 命令并经 framework 测试分发入口验证资源消费与 `IsClosed`；现有覆盖仍不足以声称完成 [#8 正文](https://github.com/yqm-307/bbt-framework/issues/8) 要求的 Redis+Mongo 组合 cache hit/miss、写后失效、not-found、后端失败、deadline 与关闭中请求矩阵。环境变量缺失时显式返回 77；CMake 在 infra target 缺失时仍只打印 STATUS 并省略 live target，fail-closed 修正与完整组合验收属于后续执行票。
- `INetworkHost::WaitHandlersDone` 的非超时错误当前会记录失败后继续收束；执行票需补足“该错误是否已证明 handler 排空”的 fail-closed 语义，不能把当前行为扩大解释为真实 handler 安全证据。

### 范围与后续交接

- 本 Resolution 只关闭本地决策节点，不实现源码/测试，不关闭或修改 GitHub #8，不代表 Redis/Mongo live、HTTP 端到端、组合业务链路或 Issue #8 已交付。
- 后续执行若要实现或验收，必须以本 Resolution 为输入：补齐真实组合 fixture 与错误/关闭矩阵、修正 live target 缺失时的门禁行为，并对 `WaitHandlersDone` 非超时错误做独立契约核验。
