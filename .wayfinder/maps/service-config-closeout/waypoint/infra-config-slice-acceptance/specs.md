---
schema: wfer-execution/v1
plan_id: infra-config-slice-acceptance
revision: 1
artifact: spec
---
# 配置验收要求
## 要求与场景
R1/S1：公共快照/源/watch契约与独立 target 可消费，无framework或driver类型泄漏。
R2/S2：内存与真实临时文件读取/更新；格式/读取错误不发布伪成功。
R3/S3：初始、重复版本去重、更新、失败/恢复与同步Close；关闭后不回调，晚到通知边界有证据。
R4/S4：原强制Stop判据须显式对照已确认process-lifetime/synchronous Close契约，不声称执行已删除API；现有观察窗口不等于确定挂起点。
R5/S5：可运行读取→业务校验→接受/拒绝消费示例，明确远程provider/业务schema/资源热切换未交付。
R6/S6：固定源码/依赖、定向验证、必要独立审查、最终版本与未覆盖矩阵。
## 约束与不变项
C++17、无新依赖、无隐藏IO线程、异步回投协程执行域、配置更新不隐式重建Redis/Mongo。真实与替身证据分开，docs-only CI不是CTest。
