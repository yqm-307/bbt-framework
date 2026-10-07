---
label: wayfinder:map
status: active
---
# bbt-framework Issue #1 实施前决策探索

## Destination
在 bbt-framework Issue #1 进入多线实现前，锁定可复现的跨仓消费基线、Resource Registry 生命周期公共契约和首个业务 RPC 互通切片；对齐旧分布式研究与新生命周期契约，明确该切片的 P0 适用范围和验收证据门槛。完成判据：每个范围内决策都有可核验的证据、明确的 owner 与边界，用户确认首个业务切片的承诺及最小验收 spec，足以安全转出为执行方案；决策终点不是实现或 P0 实测通过。本地图不执行实现、提交或发布。

## Notes
- 项目：`bbt-framework`；目标 Issue：<https://github.com/yqm-307/bbt-framework/issues/1>
- 既有交接计划：[`../issue-1-delivery-plan.md`](../issue-1-delivery-plan.md)。本地图补充实施前仍需澄清的决策，不复制执行进度。
- 当前 infra 已合并基线为 `bbtools-infra origin/main=9983b92d…`；本地图的 P0 工单仍须重新核对实际消费 SHA、公开 target 和依赖 provenance，不把聊天或旧日志当作唯一证据。
- 当前进行中的 infra HTTP/R1/M2 收口属于另一张地图/执行工作面，不纳入本图状态，也不因本图创建而改变其授权或依赖。
- 工单类型为 `wayfinder:research` 或 `wayfinder:grilling`：前者查事实与条件性建议，后者由用户确认取舍；不自动认领、不启动代码实现。
- 本次扩展范围已由用户确认：聚焦首个可验收业务 RPC 切片，不另开 P0–P4 全局演进图、不重开已归档生命周期决定。
- [研究与新契约对齐入口](tickets/research-contract-alignment.md)引用[研究输入摘录与版本指纹](evidence/research-input-context.md)；[P0 范围与证据门槛入口](tickets/p0-scope-evidence-gate.md)承接研究对齐、消费基线、资源契约和业务切片的已定结论。开放工单与 frontier 以 tickets 元数据为准，不在 MAP 重复维护状态列表。
- 新增研究输入只保存必要摘录，不把旧基线报告、自检统计或执行前 P0-A～F 拆分稿升级为当前公共契约、测试通过或实现授权；完整原稿不可取得时明确记录证据缺口。

## Decisions so far
- [`issue-1-delivery-plan.md`](../issue-1-delivery-plan.md)：已记录 Issue #1 的阶段目标、依赖顺序和“简单兼容方案”边界；本图只补足进入 P1/P2 前仍需核实的决策。
- [`../coroutine-tick-lifecycle/MAP.md`](../coroutine-tick-lifecycle/MAP.md)：coroutine process-lifetime、Tick 和三仓职责决策已归档；不在本图重开。
- [P0 跨仓消费基线](tickets/p0-consumption-matrix.md#resolution)：已锁定 framework `b1620aa555ce0d91818cbe1e69bbf31c6d51407d`、infra `0a40b702935bd6a640c836d4aadefc9b87d974df`、coroutine `7bcda3b078f975ff2978424be7f6ba38e04fb7f6` 的最小公开 target 消费闭包；干净快照 consumer 和依赖来源已核验，完整矩阵、复现及未覆盖边界见 [#31 最终证据](https://github.com/yqm-307/bbt-framework/issues/31#issuecomment-5966930958)。

## 执行 issue 回链
- [P0 跨仓消费矩阵与可复现基线](https://github.com/yqm-307/bbt-framework/issues/31)：由 `p0-consumption-matrix` 转出；执行方案快照已发布，后续以 issue 为执行与验收真源。
- [Resource Registry 公共契约与生命周期边界（#8）](https://github.com/yqm-307/bbt-framework/issues/8)：复用既有 Issue #8，并追加 `resource-registry-contract` 执行方案快照。
- [首个业务 RPC 最小切片与跨语言互通边界（#4）](https://github.com/yqm-307/bbt-framework/issues/4)：复用既有 Issue #4，并追加 `business-rpc-slice` 执行方案快照。
- [framework Host/等待迁移（#32）](https://github.com/yqm-307/bbt-framework/issues/32)：独立 draft 执行票；迁移已删除生命周期 API 的 framework 调用者，不以前置等待 infra 未完成 issue。
- [framework fork/扩展最终验收（#33）](https://github.com/yqm-307/bbt-framework/issues/33)：独立 draft 执行票；补齐干净 fork、真实注册/发现与 Stub 链路，不自动关闭 #1。

## Not yet specified
- 首个业务 RPC 切片确定后，Issue #5 双 Service 示例与本切片验收是否重合、是否仍有独立决策需求尚待辨识；不凭编号预设实现前置，也不自动扩大本图目标。消费基线、资源契约、业务切片、研究对齐和 P0 证据门槛已经分别开单，不再重复放入迷雾。

## Out of scope
- 不在本图修改 framework/infra/coroutine 源码、测试、构建配置或公共 API。
- 不由本图自动创建或修改 GitHub Issue/PR；仅在用户明确调用 `mwc-wfer-to-issues` 后，通过执行入口发布已定范围的方案快照。不 commit/push/merge，不部署、不安装系统依赖、不切换 Redis/Mongo 生产路径。
- 不重开已归档的 coroutine-tick-lifecycle，不重复研究已由 infra wire profile 解决的基础 wire contract。
- 不把 research Resolution 当作实现授权；后续转执行须另行形成目标仓可执行方案并遵守独立审查与提交门禁。
- 不在本次扩展中认领或解决新旧工单、不改写已有执行 issue 回链；不把 P0-A～F 草案转成实现任务，不提前建设动态发现 provider、owner/fencing、持久 Actor、workflow/outbox 或通用平台。
