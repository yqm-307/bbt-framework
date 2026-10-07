---
label: wayfinder:grilling
blocked_by: [research-contract-alignment, p0-consumption-matrix, resource-registry-contract, business-rpc-slice]
assignee:
claim_session:
status: open
outcome:
archive:
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
- [研究输入摘录](../evidence/research-input-context.md)。其中 P0-A～F 只是执行前拆分草案，不转换成本图实现工单。
- [验收矩阵](../../../docs/architecture/distributed-framework-validation-matrix.md)及[架构基调](../../../docs/architecture/distributed-framework-baseline.md)。结合对齐工单使用，不默默延续旧 Stop/代际措辞。

## Checkpoint
- 已确认：用户授权建立本决策问题；范围聚焦首个可验收业务 RPC，不扩展为 P0–P4 平台路线，也不含源码实施授权。
- 待决/阻塞：除已归档的 `p0-consumption-matrix` 外，其余三项前置决定尚未在本图形成 Resolution；本工单未认领，不是 frontier；用户尚未确认具体业务承诺与最终验收 spec。
- 下一步：前置满足后，通过一次 HITL `mwc-wfer-continue` 逐问确认取舍；不替用户回答、不因研究建议直接关单。
