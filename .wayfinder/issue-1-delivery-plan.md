# Issue #1 可执行推进计划（草案）

## 本轮简单兼容方案裁决（2026-10-02）

- 决策依据：用户授权 Alice 按“比较简单、可兼容的方案”裁决当前阻塞；这里的兼容指保留可用业务路径、协议与稳定公共面，并兼容已确认的新生命周期，不恢复已删除的旧关闭协议。
- 本轮只定方案并更新记录；不启动源码实现、生产路径切换、系统安装、CI 改动、commit/push 或归档。决策消解不等于缺陷已修或验收已通过。
- 已核验基线：infra PR #53 已合并，`origin/main=9983b92dc3aa6a516677c42d1fe45b2535946c22`；合并树与候选 `02567ed8017fd2afef21ffc32d18d9f85c2d0b93` 的 tree 均为 `c3449775e0ecb229ee9e6b1b4e9a4f111626be32`。framework Issue #8 仍 OPEN。主工作树不更新、不覆盖。

| 问题 | 已定方案 | 实施与验收边界 |
|---|---|---|
| 旧 API 兼容 | 不恢复 `Scheduler::Stop`、`RequestClose/WaitClosed`、`CompletionSignal`、`CancellationToken`、运行时代际或双轨兼容壳 | 保留仍有效的业务签名、协议和错误语义；消费者按新 `Close` / `CoWaiter` 契约迁移 |
| Redis 路径 | 保留当前默认 hiredis async + strand；CoTCP binding 只保留历史候选快照，不进入 PR 或切换默认路径 | 不删除候选；其旧 Stop/WaitClosed 证据不能放行新生命周期。只有明确需求与新基线 adapter 级证据出现才重评迁移 |
| Mongo 路径 | 保留已有有界 worker bridge，以及已合入的 Start/Close 发布配对 | 不替换驱动、不强制 raw CoTCP、不新建统一网络/协议抽象 |
| 旧 P4-B/P4-C | 不继续叠加旧 hard-stop/WaitClosed 工作包；公共面删减只对照新基线核查真正剩余项 | 不把旧候选整包合入，也不把“已被新契约取代”写成旧任务全部验收完成 |
| HTTP F2 | 同族失败路径沿用已修分支的门内 `Abort()` → `Finish()` 顺序 | 保持错误码和消息；用读/写失败等受控场景验证离开 owner 账本前 fd/body 已释放，不靠延时或 Close 后轮询 |
| HTTP H2 | socket open/connect/close 与 Abort 共用现有串行保护域；composed connect 的内部每一步也必须受保护 | 仅给最终 callback 加锁不算闭合。优先现有 executor/IoGate；必要时局部展开 endpoint 步进，不削减多 endpoint 语义、不新建连接框架 |
| transport R1 | 保留已公开的任意线程 Close 与 NO_LOOP 模式，不靠限制合法调用者规避风险 | 先用受控 pending Dial/Close 探针核实自驱动；若有自依赖，在现有 owner/worker 唤醒链修复，不新增全局线程、不依赖 Close 后 Tick |
| DNS H1 | 保持现有 Asio resolver，不为后台 getaddrinfo 增加新 resolver 或同步 join 层 | 对象自有 fd/buffer 仍须同步释放、晚回调不能访问已释放资源；第三方独立后台查找的寿命另行明确，不宣称 cancel 等于 join |
| Mongo M2 与 sanitizer | 保留现有锁协议；在最终树重跑决定性 start-close probe | 有效 probe 不能由日志/单测字节相同替代。按工具链支持做定向 sanitizer；不新增默认 CI job，无法有效插桩时如实留缺口，重复绿灯不是 sanitizer 证据 |
| transport 剩余覆盖 | sealed/adopt 与 Close 竞态的资源断言放 infra；通用等待登记/唤醒不变量仍归 coroutine | 不再为已删除的 `RequestClose → Scheduler::Stop` 协议新增测试，不把旧协议的排除扩大为新 Close 已安全 |
| framework #8 | 复用现有 Resource Registry / App / Service，只补工厂、Create/Start、失败逆序回滚与 manager 主动同步 Close | handler 排空后关闭资源；不恢复旧 token/signal；真实 Redis/Mongo consumer 验收仍必需，不以资源未释放时的超时返回冒充完成 |

- 接续顺序：先按影响补 infra 的 F2/H2/R1 正确性和 M2 版本绑定验证，再承接 framework #8 的新生命周期最小切片与真实 consumer 验收。DNS 口径随对应修复核对；协议扩展、全平台、压力扩展不成为当前基本迁移的隐含前置。
- 接续输入：实施时重新固定实际消费 SHA、公开 target、依赖 provenance 和可用后端；用独立 worktree，不把主树旧源码或本机安装前缀当新主线。独立审查和既有提交/生产授权门禁保持不变。
- 证据入口：<https://github.com/yqm-307/bbtools-infra/pull/53>、<https://github.com/yqm-307/bbt-framework/issues/8>；infra 专项残余来自 PR #53 正文与整体审查范围声明。本节是决策，不是新增运行证据。

## 历史计划正文

以下保留原计划的阶段结构及当时快照；其中 PR/CI 状态、本地落后数量、Stop/WaitClosed 相关表述不当作当前事实。当前兼容与接续取舍以上节为准，未覆盖验收不因本轮裁决自动完成。

- 目标 Issue：<https://github.com/yqm-307/bbt-framework/issues/1>
- 计划性质：实施编排与验收计划，不是实现授权，不替代各子 Issue 的契约。
- 当前状态：`Issue #1 OPEN / PRD-backlog`；本地 `main` 落后 `origin/main` 7 个提交，工作树有既有 `AGENTS.md` 修改和未跟踪 `.wayfinder/`，实施必须另建干净 worktree。
- 关联决策：`.wayfinder/coroutine-tick-lifecycle/` 已完成决策收口，但它只是 runtime/网络/迁移边界，不等于 Issue #1 已交付。

## 1. Issue #1 的真正完成目标

Issue #1 不是要求再写一份架构说明，而是交付一个可以被第三方 fork 消费、能按文档构建运行、能用真实后端验证的框架基线：

1. 明确 framework、extension、application 以及配置/测试/构建入口的目录职责。
2. 在干净 fork/等价隔离消费副本中，应用层新增非内置业务 Service，完成构建、测试、启动、真实请求、错误和关闭验收。
3. 至少一个分布式扩展完成真实注册/发现与 Stub 调用链；不能用单机 HTTP 示例替代。
4. 固定实际消费的 infra/coroutine SHA、公开构建 target、依赖方向和装配条件；公共头不泄漏第三方类型。
5. 业务、framework、通用 extension 的变更和上游修复吸收边界清楚；有真实验证、PR/版本和未覆盖项记录。

## 2. 当前基线与缺口

### 已有可复用产物

- framework 已有 `CoApp`、`CoService`、RPC 方法表、`CoRpcReq/CoRpcResp`、HTTP/RPC bridge、RequestContext、Actor/Concurrent 执行策略和生命周期测试。
- Issue #3 的 Req/Resp 公共面已落地；PR #29 已合并，跨语言 wire client 的 CI 修复在对应 run 中 Build/Test 均通过。
- infra 的 RPC wire profile 已按分层交接，Protobuf 版本和跨语言 wire 验证已有基础证据。
- framework 的 CMake、README 和 tests 入口已存在；不是从零搭建工程。
- `.wayfinder/coroutine-tick-lifecycle/` 已明确 coroutine process-lifetime、Tick、Stop 删除方向、Connection/adapter 分层和三仓迁移顺序。

### 当前阻塞或未完成

- **Issue #8 OPEN**：命名 Resource Registry 的首个 API 已有 PR #25，但资源工厂/Create/Start、启动失败回滚、统一 Close、超预算强持有和真实 Redis + Mongo consumer acceptance 仍未闭环。
- **Issue #4 OPEN**：infra wire 已完成一层，但业务 `.proto`/IDL 真源、deadline → remaining budget、接收端 local-min、至少一种非 C++ 真实业务互通、业务兼容升级/类型错误/Error 传播仍未验收。
- **Issue #5 OPEN / PR #28 OPEN**：双服务示例候选存在，但远端 Build 失败，Test 被跳过，不能作为完成证据。
- **Issue #9 OPEN**：Compose 资源受限、多服务 smoke/短压测尚未完成；必须遵守 CPU/内存、并发、速率、时长和清理边界。
- **Issue #30 OPEN**：其记录的 CI 修复已通过并可作为历史证据，但 Issue 仍开放；需决定是否按已完成证据关闭，不能把它误当 Issue #4 业务验收。
- **运行时大迁移尚未实施**：`.wayfinder/coroutine-tick-lifecycle` 的删除 Stop/CompletionSignal/CancellationToken、infra Close、framework 生命周期变更仍需公共 API diff、逐仓实现和独立 review。
- **当前 PR #28 不能作为基线**：实时检查为 `Build (pinned deps)=fail`、`Test=skipping`。

## 3. 实施阶段与依赖

### P0：基线、契约和消费版本锁定

**目标**：建立可复现的实施起点，不在脏工作树或未发布候选上继续叠加。

**动作**：

- 以 `origin/main` 完整 SHA 建立独立 worktree；保留现有本地 `AGENTS.md` 与 `.wayfinder` 改动，不覆盖。
- 读取并核对 Issue #1、#4、#5、#8、#9、#30、相关 PR、三仓 AGENTS/README/ADR 和实际构建入口。
- 固定本阶段使用的 `bbtools-infra`、`bbtools-coroutine` 完整 SHA、公开 CMake target、Protobuf provenance 和真实后端地址/版本。
- 形成一份跨仓消费矩阵：framework 公开头/target → infra 公开头/target → coroutine 公开头/target → 第三方依赖；记录每个阶段允许的 SHA。

**出口证据**：干净 worktree、消费矩阵、依赖 SHA、配置/后端前置条件、构建命令和禁止项均可复核。

**不做**：不在 P0 修改生产代码，不更新共享依赖，不用当前脏工作树作为验收基线。

### P1：完成 Resource Registry 与真实资源消费（Issue #8）

**前置**：P0；infra Redis/Mongo 已验收公开 target 和可消费 SHA。

**动作**：

- 完成 `(资源类型, 逻辑名称)` 注册/查找/重复登记错误。
- 完成工厂路径：Scheduler/runtime 启动后创建资源、Start 一次、失败回滚、不留下半装配资源；保留预创建 `shared_ptr` 测试入口。
- 完成两个 Service 共享同名资源、不同名资源隔离的真实测试。
- 按已冻结的新生命周期迁移关闭路径：handler 排空 → manager 主动资源 `Close()` → 连接/driver 资源同步物理释放；不把 coroutine Stop 当资源验收。
- 用真实 Redis + Mongo 完成 cache hit/miss、写后失效、not-found、后端错误、deadline/cancel、关闭中请求。

**出口证据**：Issue #8 的每个 checkbox、真实后端日志/结果、framework API 头自洽、无 hiredis/mongocxx/bsoncxx 泄漏、资源 owner/重复/隔离/回滚/关闭测试。

**依赖关系**：Issue #8 是 P1 主线；Issue #5 的双服务示例应消费 P1 的稳定 API，不能反过来用示例掩盖 P1 缺口。

### P2：完成业务 Protobuf 与非 C++ 真实互通（Issue #4）

**前置**：infra wire profile 已锁定；P1 可不必全部完成，但公共 Resource/关闭契约不能再变化到破坏 RPC consumer。

**动作**：

- 选择一个最小真实业务 `.proto`，确定 `.proto` 为唯一 schema 真源和可重复生成/校验命令。
- 保持现有 C++ `Parse<T>()`/`From(...)` 业务面，通过 `ProtoCodec` seam 接入。
- 落实发送端 deadline → remaining budget、接收端 local-min 和取消/关闭语义；明确 `OutcomeUnknown`，不把超时冒充远端未执行。
- C++ server + Python 或另一种非 C++ client 真实跨进程调用；覆盖未知字段、类型错误、业务错误、Error 传播和 schema 兼容升级。

**出口证据**：业务 `.proto`、生成/校验命令、两个独立进程、非 C++ 客户端真实请求/错误/预算结果、兼容矩阵和 CI 结果。

**不做**：不引入 gRPC，不在本阶段定义通用 codegen 平台，不把 wire profile 的通过当作业务 `.proto` 已完成。

### P3：双 Service 可运行示例（Issue #5）

**前置**：P1 资源消费 API、P2 业务 wire/业务 schema（若示例包含跨语言部分）稳定。

**动作**：

- 修复/重做 PR #28 的 pinned-deps 构建失败；先在干净 worktree 复现，再修复。
- 提供单进程双 Service、两进程静态路由、真实 client/server 请求、业务错误、超时和优雅关闭。
- 示例只 include framework/infra 公共头；不 include internal/Binder/Envelope，不创建额外 io thread。
- README 提供从干净 clone 可复跑的单一路径，并标注开发/CI 示例、非生产模板和依赖版本。

**出口证据**：PR CI Build/Test 均通过；干净消费副本一条命令完成 build/run/request/error/close；示例对外 API 不泄漏内部类型。

### P4：受限 Compose smoke 与短压测（Issue #9）

**前置**：P3 通过；CI/runner 资源和 Docker 权限明确。

**动作**：

- 两个 framework service + Redis + Mongo + 一次性 driver；CPU 总预算 ≤ 2、内存总预算 ≤ 1.5 GiB。
- 固定并发 ≤ 16、速率 ≤ 200 req/s、时长 ≤ 30s、总请求 ≤ 5000、单请求 timeout ≤ 2s、job timeout ≤ 8min。
- 覆盖正常 RPC、业务错误、Redis/Mongo 故障、deadline、关闭；输出成功数、错误分类、p50/p95/p99、最大在途和容器峰值资源。
- 使用独立 Compose project/network；trap/CI `always()` 只清理本项目资源，不影响其他容器/卷/网络。

**出口证据**：干净 clone 一条命令 build/up/health/smoke/load/down；机器可判定断言；0 未分类错误、无 OOM、失败路径同样清理；结果 artifact 可追溯。

**不做**：不把 30 秒短压测写成性能承诺，不做无边界 soak，不修改共享 runner。

### P5：Fork 验收、文档收口和 Issue #1 关闭准备

**前置**：P1–P4 的出口证据齐全；runtime 生命周期迁移若进入本期，必须完成对应三仓集成；否则显式作为后续版本边界。

**动作**：

- 建立干净 fork/等价隔离消费副本，在应用层增加一个不属于框架内置的业务 Service。
- 按 README 完成构建、测试、启动、真实请求、错误和关闭；记录实际依赖 SHA 和生成命令。
- 至少一个真实扩展完成注册/发现与 Stub 调用链；若当前只能证明静态路由，Issue #1 checkbox 不得打勾，需先明确扩展范围/后端。
- 生成 Issue #1 交付索引：PR、merge commit、依赖版本、验证命令/结果、未覆盖项、回滚/升级边界和上游修复吸收规则。
- 逐项回填 Issue #1 五个验收 checkbox；缺任一项保持 OPEN，不以“架构决定完成”替代实现证据。

## 4. 推荐执行顺序

```text
P0 基线与版本矩阵
  ├─ P1 Resource Registry + Redis/Mongo 真实消费
  │    └─ P3 双 Service 示例
  │         └─ P4 Compose smoke/短压测
  └─ P2 业务 Protobuf + 非 C++ 互通
       └─ P3/P5（按示例是否纳入跨语言路径决定）

P1/P2/P3/P4 出口证据齐全
  └─ P5 fork 验收 + Issue #1 交付索引 + 关闭判断
```

Issue #30 的 CI 修复证据可并入 P0/P2 的历史索引，但不阻塞 P2 重新完成业务 `.proto` 与非 C++ 互通验收。

## 5. 当前第一步与停止条件

**下一步只做 P0：**核对 `origin/main`、三仓消费 SHA、公开 target、依赖 provenance 和现有 worktree/未提交产物，然后形成跨仓消费矩阵；不同时开 P1/P2/P3。

进入 P1/P2 实现前必须获得对应公共 diff 和实施范围确认。发现上游 SHA、后端、Docker 权限、真实凭据或 CI 资源缺失时，标记 BLOCKED，不用 mock 或放宽检查伪装完成。

## 6. 与当前决策地图的关系

- `coroutine-tick-lifecycle`：已完成“为什么改、边界是什么、怎么迁移”的决策；不包含 Issue #1 的实现排程和真实验收。
- 本计划：把 Issue #1 的目标拆成可执行阶段、依赖和出口证据；它是下一阶段实施的交接产物，不代表任何代码已实现。
- 当前不归档 `coroutine-tick-lifecycle`：先由用户确认这份 Issue #1 计划是否作为实施基线；确认后可将决策地图正常归档，并保留计划作为后续执行入口。
