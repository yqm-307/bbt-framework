---
schema: wfer-execution/v1
plan_id: p0-rpc-fixture-service-example
revision: 2
artifact: proposal
status: ready
---
# P0 业务 RPC fixture 与双 Service 示例执行方案

## 目的

把 `framework-issue-1-decision` 已确认的首个业务 RPC 和双 Service 示例边界整理成可独立交接的候选执行包，分成两个安全边界清楚的轨道：P0-A 先在本地/CI 的未认证 loopback 环境交付业务 fixture；P0-B 单独处理真实认证与授权。目标是交付一个真实可运行的 framework 业务验收 fixture：C++ framework server、Python 标准库 client、正式 infra `RpcEnvelope` body profile、双 Service 单进程示例和显式静态路由的跨进程 caller/server 示例。

本方案只把已确认决定转成执行契约，不改变公共业务承诺，不代表已经开始开发，也不代表 GitHub issue、PR、CI 或任何 V01–V14 已通过。

## 范围

### 做

- #4：示例专用 `GetValue(key) → {found, value}` 业务 `.proto`、生成/校验入口、C++ typed adapter、正式 body-envelope bridge、预算/错误/未知结果的机器断言。
- #5：一个公开 API 双 Service 单进程 fixture；两个独立 `CoApp` 的 caller/server 跨进程 fixture；动态 loopback 端口；显式 `service_name → endpoint` 静态路由；正常返回、业务错误、有限 deadline、同步关闭证据。
- P0-A 范围内 V01–V05、V09、V11–V14 的执行任务和验收映射；P0-A 明确运行于未认证 loopback，只验证业务/传输行为，不宣称身份可信。V06–V08 是已确认的范围排除，不在本方案实现或验收。
- 本机和 CI 统一使用隔离的 `protoc 3.21.12` 前缀；CI 通过固定 runner 工具链镜像、镜像 digest/依赖 manifest 和 fail-closed 预检复现该前缀。
- P0-B 单独保留 V10 认证 adapter、身份来源绑定、独立授权 oracle 和伪造 metadata 否决证据，不阻塞 P0-A 的未认证 loopback fixture。

### 不做

- 不接入 Redis/Mongo，不实现 Resource Registry 真实后端组合；这些属于 #8 的独立执行范围。
- 不创建 Docker Compose、短压测或真实后端故障注入；这些属于 #9 的独立执行范围。
- P0-A 不实现认证 adapter，也不把 loopback 假身份当安全证据；P0-A 只能宣称未认证隔离环境中的业务 fixture 完成。V10 认证 adapter、身份来源绑定、独立授权 oracle 和伪造 metadata 否决证据归 P0-B，未完成时只阻止 P0-B/可信身份验收。
- 不引入 gRPC、第二套 wire/client、通用 codegen 平台、业务取消 token、retry/hedging、幂等写、OrderedIngress、ActorSerial、Actor A↔B 互调、支付/库存语义或生产容量基线。
- 不修改 infra wire schema/profile；不把生成的业务消息头暴露为 framework 公共头；不使用 `x-bbt-*` 作为正式业务 metadata。

## 影响与依据

### 已核实输入

- framework 基线：`b1620aa555ce0d91818cbe1e69bbf31c6d51407d`。
- bbtools-infra 基线：`0a40b702935bd6a640c836d4aadefc9b87d974df`。
- bbtools-coroutine 基线：`7bcda3b078f975ff2978424be7f6ba38e04fb7f6`。
- 决策来源：
  - `.wayfinder/framework-issue-1-decision/tickets/business-rpc-slice.md`
  - `.wayfinder/framework-issue-1-decision/tickets/service-example-boundary.md`
  - `.wayfinder/framework-issue-1-decision/tickets/p0-scope-evidence-gate.md`
- 现状边界：framework 当前仍缺正式业务 `.proto`、业务 typed bridge 和 formal body-envelope dispatch；现有 xlang/echo 路径不能替代 #4 业务验收。
- 运行输入：锁定前缀已有 `protoc 3.21.12` 证据；Python client 必须使用标准库手写 wire codec，不把 `google.protobuf` runtime 作为前置。

### 未知与硬前提

- P0-A 的本机/CI loopback、protobuf、镜像和 runner 输入可由执行任务配置；V10 认证 adapter、身份来源绑定和独立授权 oracle 当前不存在或未被核验，作为 P0-B 的独立阻塞。
- 本方案尚未执行任何构建、测试、跨进程驱动、故障注入或关闭验证；所有任务证据均为“尚未执行”。
- #4/#5 GitHub issue 当前仍 OPEN；本地地图 ticket 的 `resolved` 只表示决策完成，不表示实现交付。

## 交付约定

本包是 revision 2 来源快照，拆分发布至 #4/#5/#37/#38；其元数据状态只表示发布时 P0-A 方案判定，不是四张 issue 的共同执行状态。#38 是后续设计草稿，未选择认证实现。

- 当前交付：revision 2 方案拆分至 #4/#5/#37；P0-A 的方向和范围已定。#38 只记录后续安全需求，认证方案尚未选择，不称为可执行认证实现。
- 用户已授权建好工单、后续逐步推进：复用 #4/#5，并独立记录 #37 工具链镜像、#38 后续认证。当前仅发布说明、回读和地图回链；不创建 PR，不修改源码，不安装依赖，不启动容器，不 commit/push。
- 发布后各 issue 是各自范围的执行与验收真源；本地四文件是 revision 2 发布快照，不独立维护执行状态。建票不授权开工或基础设施部署。
- 候选执行出口：P0-A 在目标仓形成可复制的示例构建/运行入口、机器可判定的未认证跨进程验收结果、固定工具链镜像/依赖证据和对应文档，并通过仓库要求的独立审查/CI；P0-B 另有认证安全出口。实际实现出口尚未发生。
- 包级状态：`ready`（表示 P0-A 方案和任务已足够开工，不表示输入已配置或代码已实现）。P0-B 保持独立 `blocked`，原因是 V10 认证产物尚未核验；P0-A 不得将 loopback 结果称为身份/授权验收。
