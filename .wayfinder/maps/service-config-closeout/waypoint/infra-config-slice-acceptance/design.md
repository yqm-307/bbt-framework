---
schema: wfer-execution/v1
plan_id: infra-config-slice-acceptance
revision: 1
artifact: design
---
# 配置收口设计
## 实现方向
隔离检出已合入main、固定实际coroutine输入；读现有 config.unit/config.watch/contract.basics 与独立consumer。复用合入实现和审查，按变更影响补测。
## 关键取舍
不以关闭/去重 sleep观察窗口独立证明决定性时序；先对齐已有证据，缺失则明确保留，不改用零值/削弱断言制造通过。
## 变更面
本地计划及运行scratch；如有产品/测试必须修项先报告其与既定契约关系，不顺手扩到远程provider或其他模块。
## 风险与恢复
依赖版本或环境不可用只停本项，不安装系统依赖或改共享前缀。关键预期失效draft，普通验证失败in_progress；缺必要证据blocked。
