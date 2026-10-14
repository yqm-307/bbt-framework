---
schema: wfer-execution/v1
plan_id: p0-rpc-fixture-service-example
revision: 2
artifact: tasks
---
# P0 业务 RPC fixture 与双 Service 示例任务

本文件为 revision 2 发布拆分快照，不是运行中的任务状态。T1→#37；T2/T3 的实现与业务矩阵→#4；双 Service/路由与相应 T4–T7 集成→#5；T8 后续认证需求→#38。各 issue 的独立任务 ID、依赖、验收及后续修订是唯一执行真源。

本任务图拆为两个轨道：P0-A 是本机/CI 未认证 loopback 业务 fixture；P0-B 是独立认证/授权安全验收。P0-B 不阻塞 P0-A，但 P0-A 的任何结果都必须带 `auth_state=unauthenticated_loopback`，不能升级为身份可信或授权通过。

## 任务

- [ ] T1 P0-A 本机/CI 工具链与版本门禁预检**
  - 依赖：[]
  - 交付物：固定三仓候选 SHA、公开 target/依赖 manifest、隔离 `.deps/protobuf-3.21.12`、`protoc 3.21.12` 版本与 checksum 记录、CI runner 镜像 tag/digest/基础镜像 digest/工具版本记录、实际 job 消费镜像证据、独立工作区与构建目录约定。
  - 完成判据：本机可由 `scripts/prepare_protobuf.sh` 在空前缀生成并验证锁定 protobuf；CI 能在固定 runner 镜像中复现同一前缀并 fail-closed；不使用系统 `/usr/local`、浮动 `latest`、`master` 或缺少 provenance 的工具链。P0-A 不因 V10 认证产物缺失而阻塞；认证状态明确为未认证 loopback。
  - 验证：读取实际 Git 提交和工作树；运行隔离 protobuf 准备/校验；核对 Dockerfile、workflow、runner 标签、image digest 和依赖 manifest。不得安装共享系统依赖、覆盖共享前缀或启动无关服务。
  - 证据：尚未执行。

- [ ] T2 schema、golden vectors 与生成入口**
  - 依赖：[T1]；就绪条件：T1 产出当前候选版本的 protobuf 前缀和构建输入。
  - 交付物：示例 `.proto`、V1/V2 wire vectors、生成到构建目录的配置、字段号/未知字段校验入口。
  - 完成判据：R1、R2、S1–S5 的输入向量可复现；V2 未知字段被 v1 安全忽略；没有第二套手写 schema；结构化测试结果可带 `auth_state=unauthenticated_loopback`。
  - 验证：干净构建目录重复生成并比较输出；标准库 client 可按 vectors 编解码；缺 `protoc` 或版本/checksum 不符时 fail-closed。
  - 证据：尚未执行。

- [ ] T3 formal RpcWire bridge 与 typed adapter**
  - 依赖：[T2]；就绪条件：schema/vector 版本已固定。
  - 交付物：最小 typed adapter、formal `RpcEnvelope` body 接线、出站 remaining-budget 计算、入站 local-min、错误/未知结果映射和 request-id/schema echo 断言。
  - 完成判据：R3、R5、R6、S3–S7 的协议/预算/错误路径具备可观察机器结果；不依赖 `x-bbt-*` 正式 metadata，不引入 gRPC 或 retry；明确这是未认证 loopback 结果。
  - 验证：M 层不变量检查 + I 层真实 coroutine/infra handler 交错测试 + D 层抓取正式 body；分别证明发送前过期不做 I/O、双终态不会发生、断连可交付 `OutcomeUnknown`。
  - 证据：尚未执行。

- [ ] T4 GetValue server 与双 Service 单进程 fixture**
  - 依赖：[T3]；就绪条件：formal bridge 在独立测试入口可调用。
  - 交付物：确定性 in-memory `GetValue` handler；两个公开 `CoService<T>` 的单进程入口；成功、miss、业务错误、deadline、request_shutdown 场景和可复制 README 入口。
  - 完成判据：R4、R8、R10、R11；S1–S3、S6、S8、S9 可由公开 API 运行并产生结构化结果；不依赖 internal seam、真实 Redis/Mongo 或额外 io thread。
  - 验证：目标仓增量构建/直接耦合单测/单进程运行；核对 handler drain、同步 owner Close、迟到回调和进程退出；结果带未认证状态。
  - 证据：尚未执行。

- [ ] T5 双进程 caller/server、静态路由与 Python client**
  - 依赖：[T3, T4]；就绪条件：#4 bridge 和单进程公开调用路径可用。
  - 交付物：独立 caller/server `CoApp` 入口；动态 loopback 端口；显式静态路由配置；Python 标准库 client/driver；路由非法输入测试。
  - 完成判据：R7、R9；S1–S10 的跨进程部分可由机器结果判定；只有白名单目标可达，未配置目标 `NotFound`，不发生自动发现/自动连接；结果标注未认证 loopback。
  - 验证：独立子进程 readiness、成功/miss/error/timeout/unknown-field/invalid-route 请求；结果不依赖日志或固定端口；异常路径也回收自有进程。
  - 证据：尚未执行。

- [ ] T6 资源寿命、正常关闭与 supervisor 观察**
  - 依赖：[T4, T5]；就绪条件：双进程 driver 可稳定启动和产生结构化 journal。
  - 交付物：挂起 handler/迟到 completion 场景；正常 `request_shutdown()` 结果；外部 supervisor hard-stop harness；未完成/未知 journal。
  - 完成判据：R10、R11；S7、S8、S11、S12 与 V13/V14 证据可区分；正常关闭不使用 runtime Stop，硬终止不被写成优雅完成；安全状态仍是未认证 loopback。
  - 验证：I/D 层真实进程、回调和 owner 计数；核对至多一个终态、Close 前强持有、硬终止后未完成/未知记录。不得把退出码单独当成功。
  - 证据：尚未执行。

- [ ] T7 P0-A 矩阵回归、独立审查与交付准备**
  - 依赖：[T1, T2, T3, T4, T5, T6]；就绪条件：所有 P0-A 场景有当前候选版本和结构化证据，且没有 skipped/Not Run 被当作通过。
  - 交付物：R/S/T 追踪表、精简日志/结构化 artifacts、README 构建运行说明、未覆盖项、独立审查输入和目标仓 issue/PR 草稿材料（仅本地，不发布）。
  - 完成判据：P0-A 的 S1–S12、R1–R12、V01–V05/V09/V11–V14 全部有对应证据；V06–V08、Redis/Mongo、Compose 和 V10 可信身份验收没有被误报通过；所有结果清楚标注 `auth_state=unauthenticated_loopback`。
  - 验证：在当前候选/目标仓门禁上重跑直接耦合测试；`git diff --check`；审查来源、SHA、镜像 digest、protobuf provenance、日志和错误分类；不得执行全量测试、发布、commit 或 push，除非另行授权。
  - 证据：尚未执行。

- [ ] T8 P0-B 认证 adapter、身份绑定与授权 oracle（已拆出 #38，不属于 P0-A 完成条件）**
  - 依赖：[]；就绪条件：认证/身份拥有者提供版本化 adapter、来源绑定说明、独立授权 oracle、测试身份/证书 fixture 和独立进程验证入口。
  - 交付物：真实认证通道、接收端独立授权检查、伪造 `peer_principal`/`fw.*` metadata 否决证据，以及与 P0-A fixture 的独立安全测试报告。
  - 完成判据：V10 的 I/D 证据成立；结果不依赖客户端自报身份、loopback 字符串或私有构造；未满足就保持 `blocked`，不影响 P0-A。
  - 验证：独立进程、真实认证通道和授权 oracle；核对合法身份、错误身份、伪造 metadata、未认证连接的机器结果。不得以 P0-A 业务成功替代。
  - 证据：尚未执行；当前状态 `BLOCKED`。

## 最终验收

- P0-A 环境门禁 → T1 → 本机/CI 的固定 runner 镜像、image digest、protobuf 3.21.12 provenance、三仓 SHA 和公开 target 可复核；缺失则 fail-closed。
- R1–R3 → T2/T3 → `.proto` 唯一真源、正式 `RpcEnvelope` body、无 `x-bbt-*` 正式旁路的生成/抓包证据。
- R4 → T4/T5 → known/miss/empty-key 结构化业务结果；不连接 Redis/Mongo。
- R5–R6 → T3/T5/T6 → budget clamp、发送前过期、错误分类、单终态和 `OutcomeUnknown` 证据。
- R7 → T5 → C++/Python 独立进程、动态端口、标准库 client 的跨语言结果。
- R8–R9 → T4/T5 → 公开双 Service 与白名单静态路由行为；不使用 internal seam 或自动发现。
- R10–R11 → T4/T6 → 超时后仍持有存活工作、正常 drain/同步 Close、supervisor 硬终止的未完成/未知 journal。
- R12 → T1/T7 → loopback 监听和未认证标记成立，不提供可信身份。
- V10 → 已拆出 T8/#38 → 真实认证来源、接收端授权 oracle、伪造 metadata 否决；不属于 P0-A 完成条件。#38 当前认证实现设计尚未选择，先确认方案。
- 约束与不变项 → T1/T3/T7 → 三仓消费 SHA、infra wire schema、同步 Close、无业务取消/默认不重试、排除项和 M/I/D 证据等级均与决策地图一致。
- 安全标记 → T4/T5/T6/T7 → 所有 P0-A 结果含 `auth_state=unauthenticated_loopback`，不得写成身份/授权通过。
- 交付出口 → T7 → 仅在用户另行授权且目标仓审查/CI/PR 条件明确后转出；当前本地草稿不构成代码完成或发布完成。

## 检查点

- 已确认：决策地图 `framework-issue-1-decision` 7/7 ticket closed/resolved；`GetValue` 为只读 in-memory fixture；#5 使用公开双 Service 和静态路由；#8/#9 不纳入 P0-A 实现。
- 已确认：用户选择拆分 P0-A/P0-B；本地和 CI 可配置工具链与镜像，P0-A 采用未认证 loopback；V10 只阻塞 P0-B/可信身份验收。
- 已确认：固定三仓基线对象存在；GitHub #4/#5/#8/#9 仍 OPEN；本地决策状态不等于代码、PR、CI 或 issue 完成。
- 未知/阻塞：T1–T7 尚未执行；T8 缺认证 adapter、身份绑定和授权 oracle，当前 `BLOCKED`；所有 V01–V14 仍 NOT RUN。
- 认领/在途：本地方案修订过程中无子代理、构建、容器、测试进程或外部写入在途；本方案不持有代码工作树认领。
- 当前候选：`ready`，revision 2；P0-A 方案可转出/开工，P0-B 保持独立阻塞。
- 发布索引：#4 https://github.com/yqm-307/bbt-framework/issues/4#issuecomment-5975276838；#5 https://github.com/yqm-307/bbt-framework/issues/5#issuecomment-5975277535；#37 https://github.com/yqm-307/bbt-framework/issues/37；#38 https://github.com/yqm-307/bbt-framework/issues/38。工单已回读核对；后续状态、任务和接续只沿 issue，不更新此发布快照。建票不等于实现授权。
