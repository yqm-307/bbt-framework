---
schema: wfer-execution/v1
plan_id: infra-config-slice-acceptance
revision: 1
artifact: tasks
---
# config 首切片收口任务
## 任务
- [x] CF-T1 固定合入版本与契约
  - 依赖：[]
  - 交付物：完整源码SHA/依赖、公开target及旧Stop条款替代说明
  - 完成判据：R1/R4/R6输入确实可读，已完成部分不重建
  - 验证：main/PR68祖先关系、源码与当前文档
  - 证据：infra b024b2cff6f7526593d718c477a9eb33b9381bf4、coroutine7bcda3b干净；PR68合入提交是祖先；公共头自给/消费通过；现行Close替代原Stop已在持久报告明确。
- [ ] CF-T2 定向配置与独立消费
  - 依赖：[CF-T1]
  - 交付物：config.unit/config.watch/contract.basics和consumer真实结果
  - 完成判据：R2/R3/R5及实际场景可判定；未知时序明确保留
  - 验证：隔离构建、目标CTest和consumer
  - 证据：本批fresh config.unit/config.watch/contract.basics 3/3、consumer16/16、Example全部exit0；持久infra35-verification.json。R3要求的确定性去重/关闭时序仍未证，故任务保留未勾。
- [ ] CF-T3 逐项验收与证据复核
  - 依赖：[CF-T2]
  - 交付物：R1–R6/S1–S6完整映射、必要独立复核、未覆盖项
  - 完成判据：每项有可读实际证据；观察窗口/原Stop替代不偷换
  - 验证：源码/报告/结果读回，必要只读独立意见
  - 证据：逐项R1–R6对账已保存infra35-acceptance.md；原确定性判据与关闭承诺不齐，无新候选审查，不报通过。
- [ ] CF-T4 最终出口
  - 依赖：[CF-T3]
  - 交付物：本地完整报告与明确是否可收口结论
  - 完成判据：全部要求满足才done，有必需缺口则blocked；不自动关单/发布
  - 验证：产物版本与对账结果、当前issue状态
  - 证据：本批达到部分本地验收出口，不满足完整收口；#35仍OPEN，无远端写入。
## 最终验收
S1/R1→CF-T1/T3；S2/S3/R2/R3→CF-T2/T3；S4/R4→CF-T1/T3；S5/R5→CF-T2；S6/R6及约束/出口→CF-T3/T4。

证据根目录：`artifacts/service-config-closeout-e8319524/`，不依赖24小时清理的scratch为唯一证据。

## 检查点
当前恢复事实引用 ../../checkpoint.json；本包独立于#5 runner前置。
