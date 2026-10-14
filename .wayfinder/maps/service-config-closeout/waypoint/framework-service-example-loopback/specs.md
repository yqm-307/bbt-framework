---
schema: wfer-execution/v1
plan_id: framework-service-example-loopback
revision: 1
artifact: spec
---
# 双 Service 收口规格
## 要求与场景
R1/S1：公开 CoApp 双 Service 调用，成功/业务错误/deadline 与正常同步关闭可观察。
R2/S2：独立 caller/server、白名单静态路由；空/重复/不完整项启动前拒绝，未配置目标 NotFound。
R3/S3：两并发挂起的上下文不串；读超时后物理工作仍占容量，迟到完成前 owner 存活。
R4/S4：有限出站/等待/字节容量，耗尽明确拒绝、无无界排队。ActorSerial/mailbox 因本切片明确排除，不扩进实现；其余维度不能由服务 max_inflight 替代。
R5/S5：拒新、drain、同步 Close 与 release；ShutdownIncomplete 先保留 owner 再收口；hard-kill 只报未完成/未知。
R6/S6：干净副本 README 路径、公开消费边界、固定依赖及 #37 实际 CI 工具链身份成立。
## 约束与不变项
全部业务结果 auth_state=unauthenticated_loopback，无 retry/新业务取消、无旧 Stop/WaitClosed/token；不恢复归档决策图。有限正常退出不是 sanitizer 无 UAF 证明，未执行插桩明确保留。缺证据与代码缺陷分开。
