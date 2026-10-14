---
label: wayfinder:grilling
blocked_by: [research-contract-alignment, p0-consumption-matrix, resource-registry-contract, business-rpc-slice]
assignee:
claim_session:
status: closed
outcome: resolved
archive: complete
---
# 首个业务 RPC 切片的 P0 范围与证据门槛

## Question
在消费基线、资源 owner/关闭契约、业务 RPC 切片和研究对齐结论明确后，用户愿意为首个切片承诺哪些分布式语义？哪些证据足以允许它进入实施与最终验收，哪些未覆盖项必须阻塞或有依据地排除？

## Done when
- 用户确认切片目标、业务副作用模型、非目标和成功判据；复用 `business-rpc-slice` 的业务/schema 决定，不另选第二个业务或重做 wire profile。
- 对 V01–V14 逐项记录“适用且必验 / 有依据的范围排除 / 前提不足而阻塞”，并明确每项 oracle、M/I/D 层、证据来源和预期运行环境；覆盖表是验收 spec，不填造通过记录。
- `OrderedIngress`/`ActorSerial` 的 V07/V08 排除必须有代码入口与调用路径证据；V13/V14 按新生命周期契约说明业务拒新、同步关闭、晚到回调与硬终止，不以 runtime Stop 验收。draining/注销的 N/A 不等于远端路由已经移除。
- 预算/错误/身份的真实 wire、提交后丢回复的 `OutcomeUnknown`、超时后的资源计数、晚到回调与硬终止各有明确验证边界；缺实际链路不能用模型结果替代，也不强制在决策探索期间运行这些测试。
- 用户确认幂等前提和 retry 边界：默认不重试；若要开放，须说明唯一责任层、总预算和尝试限制。是否纳入 P1 的 V19 另行明确，不把研究工作包当作强制实现全部 retry/hedging 的决定。
- 分开“分析路径建议”“获准执行动作”“实现验收通过”：任何条件表都定义输入域、优先级、未知值与兜底，范围/宣称否决不被授权门禁吞掉；research、改源码、运行故障试验和外部写入不能共享笼统授权。
- 得到用户确认的最小验收 spec 和重审条件，注明 owner、版本、依赖就绪判据及未覆盖项；本工单只锁定决定，不发布执行票、不实现、不声称 P0 已通过。

## Dependencies
依赖的是本图四张决策工单经主协调者核验的 `resolved` 结论，不是 GitHub 执行 issue 已关闭、全部 infra 模块完成或某次 CI 为绿。只读已有材料不能替代依赖 Resolution；研究准备和执行票可有自己的进度，本图不复制。

- [研究与新契约对齐](research-contract-alignment.md)：决定哪些旧命题可以保留或替代。
- [跨仓消费基线](p0-consumption-matrix.md)：决定实际可用版本、公共 target 和环境前提。
- [资源契约](resource-registry-contract.md)：决定 owner、回滚与同步 Close 边界。
- [业务 RPC 切片](business-rpc-slice.md)：决定具体业务/schema/跨语言范围。

## Evidence
- [研究输入摘录](../other/evidence/research-input-context.md)。其中 P0-A～F 只是执行前拆分草案，不转换成本图实现工单。
- [验收矩阵](../../../../docs/architecture/distributed-framework-validation-matrix.md)及[架构基调](../../../../docs/architecture/distributed-framework-baseline.md)。结合对齐工单使用，不默默延续旧 Stop/代际措辞。

## Resolution

### 用户确认的 P0 承诺

- 首个 P0 切片固定为 `GetValue(key) → {found, value}` 的**只读、无外部副作用**示例；服务端使用确定性的 in-memory fixture，不在本 P0 连接或写入 Redis/Mongo，不承诺事务、补偿、持久化一致性或 exactly-once effect。真实后端组合属于 #8/#9 的后续执行边界。
- 业务 `.proto` 仍是唯一 schema 真源，复用 infra 版本化 `RpcEnvelope` body profile；不引入 gRPC、第二套 wire、通用 codegen 或新的业务模型。Python 标准库 client 只作为跨进程验收驱动，不把 `google.protobuf` runtime 变成环境前置。
- 默认不重试；P0 记录幂等前提与 `OutcomeUnknown`，不实现通用 retry/hedging。未来若开放 retry，必须在唯一责任层明确总预算、attempts 上限和 jitter；V19 另列 P1 决策，不因本票的 GetValue 只读性质自动开放重试。
- **用户新增裁定：拆分 P0-A/P0-B。** P0-A 允许在本机和 CI 的未认证 loopback 环境实施和验收业务 fixture，但所有结果必须明确标注 `auth_state=unauthenticated_loopback`，不得宣称身份来源可信或授权已验收。P0-B 单独承载真实认证 adapter、身份来源绑定、独立授权 oracle 和伪造 metadata 否决证据；V10 只阻塞 P0-B/可信身份验收，不阻塞 P0-A。

### V01–V14 最小验收 spec

“适用且必验”表示后续执行票必须提供对应层级的机器断言；“范围排除”表示本切片不作该承诺，不能记为通过；“前提不足而阻塞”表示缺口必须先补齐，不能用 M 层模型、静态检查或 loopback 假身份替代。矩阵当前仍是验收 spec，所有项目初始状态均为 NOT RUN。

| ID | P0 分类 | Oracle/否决条件 | 层级 | 证据来源与运行环境 |
| --- | --- | --- | --- | --- |
| V01 | 适用且必验 | 子 deadline 不晚于父；排队消耗预算；发送前过期不做 I/O，返回 `TimedOut` | M/I | `CallOptionsAdapter`/`RequestContext` 固定 SHA 与 `CoWaiter`；真实 coroutine 调度 + 动态 loopback，记录 parent/local deadline |
| V02 | 适用且必验 | 跨 A→B→C 只传剩余预算；每跳不重置总等待；不宣称远端副作用在原始截止时刻硬停止 | I/D | infra `RpcWire` formal body、#4 framework bridge；独立进程、动态端口和可控延迟；缺正式 bridge 不得通过 |
| V03 | 适用且必验 | completion/deadline 竞争至多一个调用终态；回调解绑、operation/资源释放各一次；无悬挂访问 | M/I | operation state/`CoWaiter::Notify` seam、真实协程/infra handler 交错；不得以退出码或 sleep 代替 |
| V04 | 适用且必验 | 请求挂起并换 worker 后，`CurrentRequestContext` 仍绑定同一逻辑协程；身份、request id、预算不串请求 | I | `RequestScope`/`RequestContext` 固定 SHA；真实跨 worker handler，至少两并发请求并核对结构化记录 |
| V05 | 适用但收窄 | 强制断连/回复丢失时交付 `OutcomeUnknown` 或等价未知事实；不把 timeout/disconnect 解释为远端未执行或已执行。因 GetValue 无副作用，不验证写入提交效果 | I/D | infra `OutcomeUnknown` 映射、#4 formal envelope；独立 client/server 断连注入；未知必须可机器分类，默认不重试 |
| V06 | 范围排除 | 本切片没有幂等写、operation-key 去重或业务副作用；不宣称冲突 payload、重放、InProgress/幂等结果语义。以后加入写入或 retry 必须重审 | — | `GetValue` Resolution 的只读边界；不执行 V06，不把普通 query 重复调用当 operation-key 证据 |
| V07 | 范围排除 | 本切片不启用 ordered ingress；不对 `next_sequence`、终态缓存淘汰、`ResultExpired` 或旧代际拒绝作 P0 承诺 | — | 固定 SHA `CoApp.cc:198,280` 装配 `OrderedIngress`，`InboundDispatcher.cc:203-314` 消费 ordered stamp，`OrderedIngress::{GrantStream,Admit,Complete}`；该路径不在 GetValue 调用图中 |
| V08 | 范围排除 | 本切片不承诺 ActorSerial/Actor A↔B 互调或重入；不以普通 Concurrent Service 通过替代非重入等待环证据 | — | 固定 SHA `ExecutionPolicy.hpp`、`InboundDispatcher.cc:289-395`、`ActorMailbox::{TryEnqueue,_Drain}`；`_Drain` 单消费者且挂起保留资格，但 fixture 不启用该路径 |
| V09 | 适用且必验 | 空 key、malformed/type/schema 错误、远程业务错误和未知/伪造 error code 均得到稳定可解析错误；无未控异常、未知码不变成功；details 按限制保真 | I/D | infra `RpcWire` error mapping + framework `GetValue` validation；Python stdlib client/独立进程，结构化结果而非日志判定 |
| V10 | P0-B 前提不足而阻塞；P0-A 范围外 | 接收端必须从认证通道获得身份并独立授权；伪造 `peer_principal`/`fw.*` 不得获信任。当前无认证 adapter，禁止以 loopback 或私有构造通过 | I/D | 固定 SHA `RequestContext.hpp` 只接收已验证字段；infra `HttpServerImpl.cc:285` 首闭环置空 `peer_principal`，`Test_framework_f2b3.cc:148` 明确无 authenticated principal；待真实认证 adapter/授权 oracle |
| V11 | 适用且必验 | 调用方先超时而 handler/driver 仍运行时，inflight 与资源计数继续覆盖存活工作；容量不因逻辑错误提前释放；物理 Close 前保持 owner 强持有 | I | framework handler + infra operation ledger；真实挂起 handler/driver，观察超时后计数、迟到 completion 和同步 Close |
| V12 | 适用且必验 | mailbox、出站并发、等待/字节等固定上限始终成立；耗尽返回可观测 `Overloaded`，不生成无界补偿队列 | I | `ServiceOptions`/`ActorMailbox` 与 infra `NetworkLimits`；真实有限并发/队列压力，拒绝数和峰值计数机器可读；不形成生产容量基线 |
| V13 | 适用且必验 | `StopAccepting → drain handlers → 同步 Close → release`；晚到回调不 UAF；预算耗尽进入 `ShutdownIncomplete` 并继续强持有，不用 runtime Stop | I/D | 固定 SHA `HostLifecycle.cc:96-138`、`CoApp::request_shutdown`、infra `ICoCloseable::Close()`；真实在途 I/O/handler、晚到回调和独立进程正常关闭；不得恢复 `RequestClose/WaitClosed` |
| V14 | 适用且必验但只证明非优雅结果 | supervisor 硬停机时记录非优雅终止、未完成请求与未知结果；重启后不宣称请求全完成；不等价于 V13 优雅关闭 | D | 外部 supervisor/driver harness，给独立 service 进程硬截止并收集结构化 journal；框架不提供硬杀 API，不能用 `Scheduler::Stop()` 代替 |

### 责任、入口与重审条件

- **M 层**只验证纯状态机/不变量，不能代替 I/D；**I 层**必须经过真实 coroutine/infra 资源与 handler；**D 层**必须是独立进程、真实网络或 supervisor 事件。模型通过、进程退出码、日志存在、mock 后端或 CTest skipped 都不算对应层完成。
- V07/V08 的排除有代码入口证据，但不是这些能力已经通过 P0；若 #5 示例改用 `OrderedIngress`、`ActorSerial`、有序 producer/actor 回调或声明非重入跨服务等待，必须重新打开本票或建立明确依赖的决策节点。
- V10 的恢复条件仍是 P0-B 专属：补入可验证认证 adapter、身份来源绑定和接收端授权 oracle，并在独立进程上验证伪造 metadata 不获信任。仅将 loopback `peer_principal` 填成字符串不能解除 P0-B 阻塞；这不妨碍 P0-A 继续做未认证业务 fixture。
- V05 的未知结果只表达“调用者不知道远端事实”；它不提供查询、补偿、去重或写入回滚。任何外部副作用、operation key、retry、hedging、业务查询恢复或支付/库存语义都必须触发重审。
- V13 遵循当前同步 `ICoCloseable::Close()` 和 process-lifetime runtime；V14 的 supervisor 硬终止是外部运维观察，不能恢复或间接引入 `Scheduler::Stop()`、旧 token、单操作业务取消或兼容壳。
- 此 Resolution 关闭的是本地 P0 决策节点；它不实现 `.proto`、bridge、认证、测试、Compose 或 supervisor harness，不表示 V01–V14 已运行通过，不发布执行票，不改变 GitHub issue 状态。

## Checkpoint
- 已确认：用户授权建立本决策问题；范围聚焦首个可验收业务 RPC，不扩展为 P0–P4 平台路线，也不含源码实施授权。
- 已确认：四项前置决策均已形成且经本轮状态脚本核验为 `closed/resolved`；三仓固定基线对象均存在；GitHub #1/#4/#5/#8/#9 仍 OPEN，#31 CLOSED。前置本地图结论不等于实现、PR、CI 或 issue 完成。
- 本轮证据：`docs/architecture/distributed-framework-validation-matrix.md:7-22` 的 V01–V14；`docs/architecture/distributed-framework-baseline.md:120-157,182-186,221-245`；`business-rpc-slice.md#resolution` 的 `GetValue`/formal wire/预算/error 边界；`research-contract-alignment.md#resolution` 的同步 `Close()`、无单操作取消和 `OutcomeUnknown` 口径；`resource-registry-contract.md#resolution` 的资源 owner/独立性；`service-example-boundary.md#resolution` 与 `compose-smoke-boundary.md#resolution` 的示例/Compose 边界。
- 已裁定：用户确认只读 in-memory `GetValue`；V05 收窄为 `OutcomeUnknown` 诚实记录，V06–V08 范围排除；进一步确认拆分 P0-A/P0-B：V10 无认证通道只阻塞 P0-B，P0-A 允许未认证 loopback 但必须标注安全边界；V13/V14 均需真实证据但 V14 只记录非优雅未完成；默认不重试，V19/P1 后置。
- 已完成：V01–V14 逐项分类、oracle、M/I/D 层、固定 SHA 证据/运行环境、排除理由、V10 恢复条件和 retry/幂等重审条件已写入上方 Resolution。
- 未完成且不计入本地 Resolution：所有 V01–V14 仍为 NOT RUN；V10 认证 adapter 是 P0-B/可信身份验收的硬阻塞，但不是 P0-A 未认证 loopback fixture 的启动阻塞；源码、测试、依赖、PR、CI、Compose 和 supervisor harness 尚未实施；本票仅锁定决策，执行工单回链见 MAP。
- 认领/在途：本票 Resolution 已写完并关闭；无子代理、构建、运行探针、容器或外部写入在途，准备释放本票认领。
- 下一步：P0-A 可按本执行方案转出并先完成本机/CI 工具链预检、业务 fixture 和未认证 loopback 验收；P0-B 仍等待 V10 认证产物。当前不把任一执行 issue 或本地方案状态写成代码/CI/PR/GitHub issue 完成。
- 归档检查点（2026-10-05）：用户明确选择并确认仅正常归档本票；本次只标记决策记录归档，不改写以上决策时点的执行状态。Resolution、Done when 和 MAP 唯一索引已核对；无认领或 live 子代理，未登记在途调查或其他写入者。本地图仍为 active，其余工单不变。执行进度继续以 MAP 回链的 GitHub issue/PR 为真源。

## Archive
- 时间：2026-10-05T09:33:35+08:00。
- 类型：`complete`；范围仅为 `framework-issue-1-decision/tickets/p0-scope-evidence-gate.md`，不包括整图、其他决策工单或 WebUI 会话。
- 用户确认：在获知正常归档判据和权限边界后，明确选择“确认正常归档 p0-scope-evidence-gate”。
- 判据与证据：本票七项 Done when 已由上方 Resolution 的 P0 承诺、V01–V14 分类/oracle/层级、范围排除与重审条件覆盖；四项前置均为 `closed/resolved`，MAP 中已有唯一 Resolution 索引；状态脚本返回 `ok=true`、无异常/认领/在途标识，本会话无 live 子代理。
- 边界：归档完成的是决策与验收 spec，不是 P0 全部实测。原有 Resolution、依赖和历史检查点保留；源码交付、CI 与 GitHub issue 状态不由此归档推导，不 commit/push、不闭票。
- 接续：执行与验收沿 MAP 的 GitHub 回链继续；若改变只读业务、默认不重试、认证分界或其他已定承诺，须明确重开本票或建立关联决策节点。
