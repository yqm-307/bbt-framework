---
label: wayfinder:research
blocked_by: [runtime-lifetime, tick-driving-modes, remove-scheduler-stop, resource-close-boundary]
assignee:
claim_session:
status: closed
outcome: resolved
archive:
---
# 契约与历史决策污染清理

## Question
哪些 coroutine、infra、framework 契约、ADR、README、AGENTS 和 Issue/PR 记录需要更新，才能让新生命周期成为唯一真源，并保留必要的历史兼容说明。

## Done when
形成按仓库分层的文档影响清单、冲突处理原则和发布/兼容门禁；不直接改文档。

## Resolution
契约与历史决策清理按以下规则冻结（2026-09-30）：

### 1. 真源与更新原则
- 新生命周期的规范真源按层分开：`bbtools-coroutine/agent-docs/2026-09-07-core-runtime-contract.md` 负责 coroutine runtime/Tick/等待语义；`bbtools-infra/docs/decisions/` 负责 Connection、adapter、资源 `Close()` 与基础错误语义；`bbt-framework` 的 HostLifecycle/架构文档负责服务级优雅关闭编排。
- 代码注释、README、API reference、示例必须与对应规范真源同步；不能让 README 或旧 ADR 单独继续宣称旧 Stop 链。
- 历史 Issue/PR、已归档测试报告和已完成验收证据不改写原文。它们保留为“当时版本的事实”，在新的规范/迁移说明中注明 superseded by 新契约及适用版本；不能把历史通过结果解释为新契约已通过。
- 分布式架构中的“服务实例重启/instance incarnation/endpoint revision”仍是业务与部署层语义，不等于 coroutine `RuntimeGeneration`；本次只删除 coroutine runtime generation，不删除跨进程故障与实例身份模型。

### 2. coroutine 仓文档影响
**必须更新为新真源：**
- `AGENTS.md`：移除“Stop 仍是核心停机语义”的旧前提，改为 process-lifetime runtime、单次初始化、业务关闭由上层编排；保留“协作式取消/等待语义”与仓库职责。
- `agent-docs/2026-09-07-core-runtime-contract.md`：重写执行模型、生命周期、等待/唤醒和原 §6；删除公共 `Scheduler::Stop()`、runtime generation、CompletionSignal/CancellationToken 的契约声明；补充三种 Tick 模式、`CoWaiter::WaitWithCallback`/PENDING 早到事件和 process-lifetime 约束。
- `agent-docs/api-reference.md`、`agent-docs/user-guide.md`、`README.md`：删除 Stop API/示例和 Start→Stop→Start 用法，说明运行时随进程存在；示例退出依赖进程结束，不把 OS 回收描述成业务资源清理。

**需要复核并标注历史/迁移状态：**
- `agent-docs/2026-09-07-m1-core-contract-plan.md`、`asio-cross-platform-plan.md`、`2026-09-07-usage-readiness-assessment.md`、`2026-09-09-real-client-acceptance.md`、`2026-09-07-m1-04c-hook-error-matrix-gap.md`：保留历史证据和原始日期，但加“旧 Stop/generation 设计，待按新契约重验”的说明；不把旧验收矩阵直接沿用。
- `tests/reports/archive/**`：只在索引/新总结中标注旧契约版本，不修改原始报告。
- `example/`、`benchmark_test/`、`debug/`：作为迁移消费者更新；不再把 Stop 调用作为库正确性证据。

### 3. infra 仓文档影响
**必须新增/修订当前契约：**
- `docs/decisions/0002-co-network-contract-v1.md`、`0005-co-io-adapter-contract-v1.md`：补充修订记录并把 `RequestClose/WaitClosed` 改为 framework manager 主动调用资源同步 `Close()`；关闭直接丢弃未发送数据、pending 不发送、迟到响应只消费不交付、连接关闭只交付一次终态；删除 coroutine Stop/generation 作为资源收口前提。
- `README.md`、`examples/README.md`：同步 Connection 统一网络管理、协议 adapter 分模块实现、`Close()` 和基础错误语义；移除旧关闭链示例。

**按实际依赖逐项修订：**
- `docs/decisions/0003-redis-client-hiredis-dependency.md`：保留 hiredis async + strand 作为当前实现事实，移除/改写“deadline 由 CompletionSignal 承担”等过时依赖；不把 CoTCP 候选实验写成已迁移。
- `docs/decisions/0004-mongo-client-mongocxx-dependency.md`、`0006-infra-foundation-and-dynamic-config.md`、`0007-mongo-owner-handles.md`：更新 CompletionSignal/WaitClosed 依赖描述，分别保留 Mongo worker bridge、配置资源 owner 等仍有效的模块特有事实。
- `AGENTS.md`：若仍引用旧 `RequestClose/WaitClosed` 作为当前公共契约，随上述新契约一起更新；第三方隔离、真实验证和公共 API 变更门禁保留。

历史 ADR 不删除；如果原 ADR 的结论已经被新契约推翻，采用“原结论 + superseded/amended by 新 ADR/修订记录”的方式，避免同一仓库同时存在两个未标状态的真源。

### 4. framework 仓文档/代码契约影响
**必须更新当前 HostLifecycle 契约与实现说明：**
- `framework/include/bbt/framework/internal/HostLifecycle.hpp`、`framework/src/host/HostLifecycle.cc`、`InfraHttpHost.*`：删除 `Scheduler::Stop`、`RequestClose/WaitClosed/ReleaseClosed` 作为正常业务关闭链的前提；改为 framework manager 主动同步 `Close()`，并明确 coroutine runtime 不参与服务资源收口。
- `framework/include/bbt/framework/internal/InboundDispatcher.hpp`、`InboundDispatcher.cc`、`CallOptionsAdapter.*`、`RequestContext.hpp`：移除 CompletionSignal/CancellationToken 的旧公共依赖；请求完成与取消改按 adapter/CoWaiter 新契约迁移，业务层仍可有自己的取消/请求预算语义，但不再把已删除 coroutine 类型当实现面。
- framework README 与 `docs/architecture/` 中描述 HostLifecycle 的部分：更新优雅关闭顺序、超时/异常归属和 manager ownership。

**明确不应误改：**
- `distributed-framework-baseline.md` 中跨进程服务实例重启、instance incarnation、endpoint revision、supervisor kill/restart 等分布式语义继续保留；仅删除其中若有“coroutine RuntimeGeneration”或 Scheduler Stop 作为 framework 资源关闭机制的表述。
- `distributed-framework-evidence.md` 是证据索引；实现前不回填“已通过”，实施后重新生成对应 SHA/测试证据。
- Issue/PR 记录保持不可变历史，在新 Issue/PR 中链接迁移决策、替代契约和新验证结果。

### 5. 发布与兼容门禁
- 这是一次有意的破坏性公共 API 迁移，不提供 Stop、RequestClose、WaitClosed、CompletionSignal、CancellationToken、RuntimeGeneration 的兼容壳；不得通过别名或隐藏转发保留旧语义。
- 实施顺序固定为 coroutine → infra → framework → 文档/示例；每个阶段必须使用已验证的上游 SHA，不能让下游先引用未发布候选头。
- 每个仓库在宣称切换完成前，必须同时满足：公共头 self-check/独立消费、对应单测与真实 loopback/live（适用时）、旧符号搜索为零或仅出现在明确标注的历史报告、`git diff --check`、无新增 warning、依赖 SHA/构建目标可复现。
- 公共 API diff、跨仓依赖 SHA、测试删除/替换和发布兼容说明必须在实施前向用户展示并经确认；本工单只冻结文档影响面和发布门禁，不授权改三仓生产代码。

### 6. 未覆盖边界
Connection handler 的长期协程/按需 handler、worker 绑定、事件预算和性能阈值仍是实现阶段的基准问题；本次文档清理不替它们作决定。

**证据**：三仓真实搜索结果；coroutine 核心契约 §2/§6、API reference Scheduler 段；infra ADR 0002/0005 的关闭与 adapter 章节；framework `HostLifecycle.hpp/.cc` 当前关闭链；`distributed-framework-baseline.md` 的实例重启语义区分。
