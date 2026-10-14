---
schema: wfer-execution/v1
plan_id: infra-rpc-outcome-unknown
revision: 1
artifact: tasks
---
# 出站请求阶段与未知结果任务

## 任务

- [ ] T1 基线与契约核对**
  - 依赖：[]
  - 交付物：当前 infra/main、coroutine 依赖 SHA；HTTP client operation 状态机；现有 Error/RpcWire 映射；#37/#62 直接不变项清单。
  - 完成判据：R1/R4/R5 的输入事实可回读，明确完整写出线性化点和不可证明边界。
  - 验证：源码、公开头、现有 HTTP/RPC 回归和固定版本记录；不把旧日志当当前主线实测。
  - 证据：尚未执行。

- [ ] T2 阶段事实与终态实现**
  - 依赖：[T1]；就绪条件：T1 固定当前 operation owner 和公开消费面。
  - 交付物：最小阶段/结果表示、RequestCommitted 发布点、终态映射及兼容文档。
  - 完成判据：R1–R5；未提交与已提交错误分界可被消费者机械读取；无 message/errno 猜测。
  - 验证：定向构建、公开头消费编译、静态检查和直接单测。
  - 证据：尚未执行。

- [ ] T3 真实阶段矩阵与回归**
  - 依赖：[T2]；就绪条件：T2 候选在隔离构建可消费。
  - 交付物：S1–S7 的动态 loopback/事件控制测试、结构化结果、清理证据。
  - 完成判据：未建立/写中/已提交丢回复/正常回复/deadline-close/keep-alive 全部机器断言；一次终态、无 retry、物理清理与配额归还成立。
  - 验证：普通 + ASan/UBSan 适用定向运行；不使用固定 sleep 或放宽断言。
  - 证据：尚未执行。

- [ ] T4 独立审查与交付出口**
  - 依赖：[T1,T2,T3]；就绪条件：当前候选和证据冻结，失败记录已保留。
  - 交付物：独立 review、必要回修/定向复审、PR 与当前 head required CI。
  - 完成判据：R1–R6、S1–S7、不变项和跨仓交接说明齐全；无阻塞必修，PR 可合并但不自动合并。
  - 验证：审查绑定最终候选 SHA；PR CI 全绿；回链 framework #4。
  - 证据：尚未执行。

## 最终验收

- R1–R3 → T2/T3：阶段发布、未提交/已提交分界、一次终态和未知结果安全。
- R4–R5 → T1/T2/T4：现有消费者兼容、公开结果不泄漏第三方类型。
- R6 → T3/T4：结构化证据、版本绑定、清理和审查。
- S1–S7 → T3：每个场景均有最终候选上的机器断言；任何缺项保持 OPEN/BLOCKED。
- 不变项 → T1/T3/T4：#8/#37/#62 既有契约和回归不退化。
- 交付出口 → T4：独立 review + current-head CI + framework #4 消费交接；不自动关闭聚合票。

## 检查点

方案已定，已发布为 [`bbtools-infra#64`](https://github.com/yqm-307/bbtools-infra/issues/64)；以 issue 为执行真源，本地地图只保留索引，不复制任务进度。若公开契约或完整写出语义无法成立，方案回 `draft` 并记录原因，不以猜测放行。
