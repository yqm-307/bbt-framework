# 分布式研究接入：输入摘录与来源边界

这是供[研究与新契约对齐工单](../tickets/research-contract-alignment.md)使用的最小持久输入，不是新架构决定、完整五路报告副本或实现证据。原稿不搬迁；这里只保存能陈述本次问题的摘录与版本指纹，不依赖私有会话或本机绝对路径才能理解工单。

## 输入身份

研究基线日期为 2026-09-24，综合日期为 2026-10-03。原稿绑定：
- framework：`0f7873740450cf4715150cc71b903962ad3debfe`
- infra：`3e6da6205cd02387a9685f746244a3d75277265d`

本次建图只读核对的 framework HEAD：`45e98668b74acdbacd717e2b864169803ea70de9`。本地 `AGENTS.md` 已有未提交改动，`.wayfinder/` 未跟踪；HEAD 不代表这些候选规范已经发布。未在本次固定或构建当前 infra/coroutine 消费版本，后续由[消费基线工单](../tickets/p0-consumption-matrix.md)负责。

| 原稿文件名 | SHA-256（本次读取内容） | 证据等级 |
|---|---|---|
| `ARCHITECTURE-BASELINE-CROSSCHECK-V1.md` | `5966c932d30d47c5023f2a3b5d93a8c5f763083dccd69b713a6cfbca57032708` | 固定旧基线的研究综合，不是批准 ADR |
| `P0-DECISION-MATRIX.md` | `76d5df9334a58617af8550e412bbf7a01d7020e2a3312afc6becec5dd9da087d` | 补证路径/门槛草案，不是实现授权 |
| `P0-EVIDENCE-TASK-SLICES.md` | `9c1857dbca9fbebe222b4c05bac404192e39bb43f2d8356762a6a1bacdf5bb6e` | 执行前拆分草案，不是已派发任务 |

指纹用于区分版本，不证明内容正确或获得批准。摘录之外的结论未在这里复制；若研究需要完整原稿而当前不可取得，应标注证据缺口，不根据指纹猜测正文。

## 与工单直接相关的摘录

以下均保留原稿的建议/未运行等级，不改写成当前能力：

- 综合稿第 17–22 行提出 Service-first/Actor-optional、数据面/控制面分离、结果未知、权威边界和验证分层；第 33 行明确 V01–V30 都是 NOT RUN 的测试设计。
- 综合稿第 268–278 行的建议停止顺序是：
  > 关闭 readiness / 拒绝新请求 → 发布 draining 或注销（尽力但有期限）→ 等待或取消在途工作 → 关闭 transport/provider → 确认 completion/close → 释放服务资源 → 停止 runtime。
- 决策矩阵第 46 行 H7 仍以 `runtime stop` 收尾；第 91–109 行把 A（文档/模型）与 B（真实链路）的授权拆开，`PROCEED` 只建议路径，不授予改代码或故障注入权限。
- 决策矩阵第 123–125 行说明：无发布通道时记 N/A 但仍验证旧 endpoint 本地拒新；V07/V08 排除须有代码入口和调用路径证据。
- 工作包稿第 9–20 行聚焦一个 Service/HTTP 调用切片的预算、unknown、operation/attempt、retry、资源计数、停机、身份/错误及 M/I/D 证据；缺真实两进程链路和隔离环境不能称 P0 通过。

这些文字把问题暴露出来，不代表新生命周期决定应迁就旧测试设计，也不把 HTTP 固定为业务 API 或通用 RPC 核心依赖。

## 本项目的核对入口

- 规范定位：[AGENTS.md](../../../AGENTS.md)。
- 本仓设计与测试输入：[架构基调](../../../docs/architecture/distributed-framework-baseline.md)、[验收矩阵](../../../docs/architecture/distributed-framework-validation-matrix.md)、[证据索引](../../../docs/architecture/distributed-framework-evidence.md)。这些文件也含旧基线内容，须结合较新生命周期决定复核。
- 已确认的较新决定：[归档地图](../../coroutine-tick-lifecycle/MAP.md)、[Stop/等待机制删除](../../coroutine-tick-lifecycle/tickets/remove-scheduler-stop.md)、[资源关闭职责](../../coroutine-tick-lifecycle/tickets/resource-close-boundary.md)。归档 Resolution 明确移除公共 Stop/WaitClosed/旧 token/运行时代际，并确认同步 Close；这里只引用，不重开或推定各消费方已经迁移完成。

## 使用限制

本摘录未检查当前实现，未运行 V01–V30，不是完整源材料或独立批准。后续 Resolution 必须引用真实版本、源码/规范位置和必要证据；新增依赖、公共接口破坏、付费或生产操作仍须另行授权。原五路材料及本轮文档自检不能证明首个业务切片已经可交付。
