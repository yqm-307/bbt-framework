---
schema: wfer-execution/v1
plan_id: framework-service-example-loopback
revision: 1
artifact: tasks
---
# EX-T1–EX-T5 接管对账
## 任务
- [x] EX-T1 当前版本与消费输入
  - 依赖：[]
  - 交付物：干净 worktree、deps.lock 与 protobuf 锁校验
  - 完成判据：本地输入固定；不代表 CI-T3 已验收
  - 验证：源码 SHA、configure 的版本/checksum 与 ldd
  - 证据：本会话工具输出及 artifacts/service-config-closeout-e8319524/framework5-verification.json；framework0258cc0f、infra162bb5fd、coroutine7bcda3b
- [x] EX-T2 单进程双 Service
  - 依赖：[EX-T1]
  - 交付物：已合入公开 fixture
  - 完成判据：R1/S1 机器结果与退出检查通过
  - 验证：examples.two_service
  - 证据：framework5-two_service-results.json，summary failures=[]；本会话 fresh 构建与测试成功
- [x] EX-T3 双进程路由
  - 依赖：[EX-T2]
  - 交付物：已合入 caller/server/stdlib driver
  - 完成判据：R2/S2 真实网络及四类非法路由拒绝
  - 验证：examples.getvalue.s2s
  - 证据：framework5-LastTest.log：四类badroute、NotFound、业务请求及caller/callee rc=0，S2S-ALL-PASS
- [ ] EX-T4 容量/上下文/关闭完整对账
  - 依赖：[EX-T3]
  - 交付物：R3–R5/S3–S5 全维度矩阵及缺口
  - 完成判据：未测出站/等待容量不得算完成；已测资源寿命保持
  - 验证：既有源码/fixture与定向测试；必要补证前核契约
  - 证据：已测8个lifecycle结果均pass，见 framework5-lifecycle-results.json；父级复核framework5-capacity-verified.md：出站机制存在但公共路径未补证；响应字节方向未补证，等待容量语义需澄清，保留未勾
- [ ] EX-T5 文档与最终出口
  - 依赖：[EX-T4]；就绪条件：全部R/S对账及 #37 CI-T3 实际工具链证据就绪
  - 交付物：完整本地报告、适用独立复核和远端证据索引
  - 完成判据：所有要求满足且未核实在途清楚，缺项 blocked，不自动关单
  - 验证：README/公共消费/版本/CI与实际产物读回
  - 证据：六项本地CTest 6/6；PR48合入后CI成功仅为局部，#37实际runner身份仍未知
## 最终验收
S1→EX-T2；S2→EX-T3；S3–S5→EX-T4；S6及安全/版本/文档/出口→EX-T1/EX-T5。R4缺口及#37不能被其他绿灯抵消。
## 检查点
恢复事实见 ../../checkpoint.json 中 scope/引用；本任务已确认候选保持只读，无产品候选变更。

证据根目录：`artifacts/service-config-closeout-e8319524/`；容量只读调查sa-0-29885624已返回；Alice已读回报告/源码核验，采纳部分并纠正入出站共用计数等过度判断，详见framework5-capacity-verified.md。
