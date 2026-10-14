---
schema: wfer-execution/v1
plan_id: framework-service-example-loopback
revision: 1
artifact: design
---
# 复用与核验
## 实现方向
沿现有 examples/getvalue、two_service、lifecycle_matrix；干净 worktree 固定当前 main，依赖精确沿 deps.lock；已完成候选不重做。
## 关键取舍
只在当前调用链有明确缺口时补定向测试，不新增测试框架、配置层或依赖。#37 实际 runner 消费身份是外部条件，不能用本地 protobuf 或 skipped 手动 job 冒充。
## 变更面
主仓仅新交付地图/计划；运行产物 task scratch；现有脏改动保留。当前未改任何产品源码。
## 风险与恢复
R4 各维度无证据则不勾 EX-T4 全部；关键契约失效回 draft、普通测试失败回 in_progress。实际 runner 输入缺失 blocked；本批不替其部署，其他独立 #35 可继续。
