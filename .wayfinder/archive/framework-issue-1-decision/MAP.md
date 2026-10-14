---
label: wayfinder:map
status: archived
---
# bbt-framework Issue #1 实施前决策探索

## Destination
在 bbt-framework Issue #1 进入多线实现前，锁定可复现的跨仓消费基线、Resource Registry 生命周期公共契约和首个业务 RPC 互通切片；对齐旧分布式研究与新生命周期契约，明确该切片的 P0 适用范围和验收证据门槛。完成判据：每个范围内决策都有可核验的证据、明确的 owner 与边界，用户确认首个业务切片的承诺及最小验收 spec，足以安全转出为执行方案；决策终点不是实现或 P0 实测通过。本地图不执行实现、提交或发布。

## Notes
- 项目：`bbt-framework`；目标 Issue：<https://github.com/yqm-307/bbt-framework/issues/1>
- 既有交接计划：[`../issue-1-delivery-plan.md`](../coroutine-tick-lifecycle/other/notes/issue-1-delivery-plan.md)。本地图补充实施前仍需澄清的决策，不复制执行进度。
- 当前 infra 已合并基线为 `bbtools-infra origin/main=9983b92d…`；本地图的 P0 工单仍须重新核对实际消费 SHA、公开 target 和依赖 provenance，不把聊天或旧日志当作唯一证据。
- 当前进行中的 infra HTTP/R1/M2 收口属于另一张地图/执行工作面，不纳入本图状态，也不因本图创建而改变其授权或依赖。
- 工单类型为 `wayfinder:research` 或 `wayfinder:grilling`：前者查事实与条件性建议，后者由用户确认取舍；不自动认领、不启动代码实现。
- 本次扩展范围已由用户确认：聚焦首个可验收业务 RPC 切片，并将 Issue #5/#9 的示例与 Compose 验收边界纳入同一张 map；不另开 P0–P4 全局演进图、不重开已归档生命周期决定。
- [研究与新契约对齐入口](tickets/research-contract-alignment.md)引用[研究输入摘录与版本指纹](other/evidence/research-input-context.md)；[P0 范围与证据门槛入口](tickets/p0-scope-evidence-gate.md)承接研究对齐、消费基线、资源契约和业务切片的已定结论。开放工单与 frontier 以 tickets 元数据为准，不在 MAP 重复维护状态列表。
- 新增研究输入只保存必要摘录，不把旧基线报告、自检统计或执行前 P0-A～F 拆分稿升级为当前公共契约、测试通过或实现授权；完整原稿不可取得时明确记录证据缺口。
- 本次覆盖盘点纳入 framework 当前及历史相关 Issue：#1、#2、#3、#4、#5、#6、#8、#9、#10、#19、#30、#31、#32、#33。已完成的基础/证据 issue 只作回链，不重新派发；开放 issue 必须有对应决策节点或明确的执行边界。
- 当前生命周期冲突按用户确认处理：同步 `ICoCloseable::Close()` 是唯一当前口径；#8 历史正文中的 `RequestClose`/`WaitClosed`/`Scheduler::Stop`/旧 token 不作为本图契约，也不恢复兼容壳。

## Decisions so far
- [`issue-1-delivery-plan.md`](../coroutine-tick-lifecycle/other/notes/issue-1-delivery-plan.md)：已记录 Issue #1 的阶段目标、依赖顺序和“简单兼容方案”边界；本图只补足进入 P1/P2 前仍需核实的决策。
- [`../coroutine-tick-lifecycle/MAP.md`](../coroutine-tick-lifecycle/MAP.md)：coroutine process-lifetime、Tick 和三仓职责决策已归档；不在本图重开。
- [P0 跨仓消费基线](tickets/p0-consumption-matrix.md#resolution)：已锁定 framework `b1620aa555ce0d91818cbe1e69bbf31c6d51407d`、infra `0a40b702935bd6a640c836d4aadefc9b87d974df`、coroutine `7bcda3b078f975ff2978424be7f6ba38e04fb7f6` 的最小公开 target 消费闭包；干净快照 consumer 和依赖来源已核验，完整矩阵、复现及未覆盖边界见 [#31 最终证据](https://github.com/yqm-307/bbt-framework/issues/31#issuecomment-5966930958)。
- [首个业务 RPC 切片](tickets/business-rpc-slice.md#resolution)：用户已确认 `GetValue(key) → {found, value}` 作为 #4 首个示例程序/跨语言验收 fixture；业务 `.proto` 为唯一 schema 真源，复用 infra body-envelope profile，Python 标准库手写 wire client，正式 framework bridge 与 budget/error 矩阵仍需后续执行票实现和实测。
- [研究与新契约对齐](tickets/research-contract-alignment.md#resolution)：已按固定三仓消费 SHA 对齐 process-lifetime runtime、同步 `Close()`、deadline/等待/业务取消、owner 物理释放与 runtime generation 边界；不恢复旧 Stop/WaitClosed/CompletionSignal/CancellationToken，也不新增单操作业务取消句柄。
- [Resource Registry 公共契约与生命周期边界](tickets/resource-registry-contract.md#resolution)：用户确认资源相互独立且不承诺顺序，预创建指针表示 ready、框架只统一 Close；真实 Redis/Mongo 组合验收仍属后续执行，不代表 GitHub #8 已交付。
- [双 Service 与跨进程示例的边界](tickets/service-example-boundary.md#resolution)：#5 锁定公开双 Service 单进程 fixture 与显式静态路由 caller/server 跨进程 fixture；#4 负责唯一业务 schema/正式 bridge，#8 负责真实资源，#9 负责 Compose/短压；本节点不实现示例。
- [Compose 多服务冒烟与有界短压测边界](tickets/compose-smoke-boundary.md#resolution)：#9 固定 driver→service-a→service-b→Redis/Mongo 拓扑、机器断言、2 CPU/1.5 GiB 与 16/200/30s/5000 负载上限、项目级清理和 fail-closed 条件；本节点不运行 Compose。
- [首个业务 RPC 切片的 P0 范围与证据门槛](tickets/p0-scope-evidence-gate.md#resolution)：用户确认只读 in-memory `GetValue`、默认不重试、V01–V05/V09/V11–V14 的边界与 V06–V08 排除；用户进一步确认拆分 P0-A 未认证 loopback 业务 fixture 与 P0-B 认证/授权安全验收，V10 只阻塞 P0-B，不阻塞 P0-A；所有 V01–V14 仍 NOT RUN，本节点只锁定 spec，不代表 P0 或实现通过。
- [已完成基础与 CI 证据](https://github.com/yqm-307/bbt-framework/issues/2)：#2/#3/#6/#10/#19 已关闭，#30 的 protobuf CI 与 xlang 证据作为 #4 输入；这些 issue 不重新派发，也不替代 #4/#5/#8/#9/#33 的剩余验收。

## 执行 issue 回链
- [P0 跨仓消费矩阵与可复现基线（#31）](https://github.com/yqm-307/bbt-framework/issues/31)：由 `p0-consumption-matrix` 转出；节点已完成并归档，后续以 issue 为执行与验收真源。
- [Resource Registry 公共契约与生命周期边界（#8）](https://github.com/yqm-307/bbt-framework/issues/8)：复用既有 Issue #8，并追加 `resource-registry-contract` 决策节点；旧 Close 文字按地图当前契约解释。
- [首个业务 RPC 最小切片与跨语言互通边界（#4）](https://github.com/yqm-307/bbt-framework/issues/4#issuecomment-5975276838)：由 `business-rpc-slice` 和 `p0-scope-evidence-gate` 转出未认证 loopback 实现说明；复用既有 issue，原研究记录保留，执行与验收沿 issue。
- [双 Service、跨进程与 Docker 编排（#5）](https://github.com/yqm-307/bbt-framework/issues/5#issuecomment-5975277535)：由 `service-example-boundary` 转出公开双 Service/静态路由 loopback 实现说明；业务协议消费 #4，Compose 仍归 #9，执行与验收沿 issue。
- [Compose 多服务冒烟与有界短压测（#9）](https://github.com/yqm-307/bbt-framework/issues/9)：由 `compose-smoke-boundary` 作为 #5 的子范围，依赖示例、业务 RPC 和资源契约决策。
- [framework Host/等待迁移（#32）](https://github.com/yqm-307/bbt-framework/issues/32)：独立执行票；迁移已删除生命周期 API 的 framework 调用者，已完成，不以前置等待 infra 未完成 issue。
- [framework fork/扩展最终验收（#33）](https://github.com/yqm-307/bbt-framework/issues/33)：独立 draft 执行票；补齐干净 fork、真实注册/发现与 Stub 链路，不自动关闭 #1。

- [P0-A runner 镜像与 protobuf 门禁（#37）](https://github.com/yqm-307/bbt-framework/issues/37)：由消费基线与 P0 范围决定转出环境/CI 工具链工单；不隐含 runner 部署授权。
- [P0-B 认证、身份绑定与授权验收（#38）](https://github.com/yqm-307/bbt-framework/issues/38)：由 P0 安全范围决定转出后续需求工单；认证设计需另行确认，不阻塞 #4/#5 loopback。

## Issue 覆盖索引
- [#1 PRD 主目标](https://github.com/yqm-307/bbt-framework/issues/1)：本图 Destination；其最终关闭仍需 #33 的逐项证据对账。
- [#2 Service/Actor 契约](https://github.com/yqm-307/bbt-framework/issues/2)、[#3 RPC 公共面](https://github.com/yqm-307/bbt-framework/issues/3)：已关闭基础决定，作为 #4/#5 的历史输入，不重派。
- [#6 最小 CI 门禁](https://github.com/yqm-307/bbt-framework/issues/6)、[#10 共用构建镜像](https://github.com/yqm-307/bbt-framework/issues/10)、[#19 Actor 关闭期修复](https://github.com/yqm-307/bbt-framework/issues/19)：已关闭基础/稳定性输入，不重派。
- [#30 锁定 protobuf CI 与跨语言测试](https://github.com/yqm-307/bbt-framework/issues/30)：保留其已完成 CI 证据作为 #4 输入，不将 CI 通过扩大为业务 `.proto`、bridge、budget 或真实业务互通完成。

## 执行计划索引
- [P0 执行快照](waypoint/p0-rpc-fixture-service-example/proposal.md)：revision 2 四文件原样迁入；后续进度见 service-config-closeout。

## Not yet specified
无。

## Out of scope
- 不在本图修改 framework/infra/coroutine 源码、测试、构建配置或公共 API。
- 不由本图自动创建或修改 GitHub Issue/PR；仅在用户明确调用 `mwc-wfer-to-issues` 后，通过执行入口发布已定范围的方案快照。不 commit/push/merge，不部署、不安装系统依赖、不切换 Redis/Mongo 生产路径。
- 不重开已归档的 coroutine-tick-lifecycle，不重复研究已由 infra wire profile 解决的基础 wire contract。
- 不把 research Resolution 当作实现授权；后续转执行须另行形成目标仓可执行方案并遵守独立审查与提交门禁。
- 不在本次扩展中认领或解决新旧工单、不改写已有执行 issue 回链；不把 P0-A～F 草案转成实现任务，不提前建设动态发现 provider、owner/fencing、持久 Actor、workflow/outbox 或通用平台。

## Lifecycle
- 2026-10-05T15:51:38+08:00：用户在获知本图已达成决策目的地、执行验收仍独立跟踪及整图归档范围后，确认“那就全map归档吧”；本图由 `active` 原地标记为 `archived`。范围仅当前 `framework-issue-1-decision`，不涉及其他地图或 WebUI 会话。

- 2026-10-09 20:24:59 +0800 archived → active：临时恢复以整理归档索引（confirmed=true）
- 2026-10-09 20:25:00 +0800 active → archived：保留已转执行的未决项，按留缺口归档；历史不删除（confirmed=true）
## Archive
- 类型：`complete`；范围为整张 `framework-issue-1-decision` 决策地图，不批量修改子工单的归档标记。
- 用户确认：本次主会话先展示正常归档判据及剩余执行边界，用户明确确认整图归档；Alice 在会话 `7c74c41d85c2` 串行写入本记录。
- 目的地证据：跨仓消费基线、资源公共契约、业务 RPC 切片、旧研究与新生命周期对齐、示例/Compose 分工、P0 范围与最小验收 spec 均已有 Resolution，见上方 `Decisions so far` 的唯一索引；执行工单已有回链。本图终点是决策收敛并可转出实施，不是代码或 P0 全部实测通过。
- 归档前核验：状态脚本返回 `ok=true`；7 张工单全部 `closed/resolved` 且有 Resolution，无数据异常、认领、决策阻塞或 frontier；未登记待展开迷雾或在途调查标识，本会话无 live 子代理。已归档的两张子工单及其余五张子工单的标记保持原样。
- 未完成的执行边界：P0-A 剩余验收、P0-B 认证/授权、公开示例、真实资源组合、Compose 与最终 fork/扩展验收仍沿上述 GitHub issue/PR 跟踪，不由整图归档判为完成。旧消费 SHA、NOT RUN 与检查点作为决策时点记录保留，不冒充实时实现进度。
- 接续：执行与验收沿 `执行 issue 回链` 继续；若改变本图已定承诺或发现范围内新的决策缺口，须明确重开相应范围。本次未移动或删除文件、未 commit/push、未关闭 GitHub issue，也未执行部署或聊天归档。
- 2026-10-09 20:25:00 +0800 归档（archive=incomplete）：保留已转执行的未决项，按留缺口归档；历史不删除
