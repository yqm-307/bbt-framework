---
label: wayfinder:research
blocked_by: []
assignee:
claim_session:
status: closed
outcome: resolved
archive:
---
# 双 Service 与跨进程示例的边界（#5）

## Question
Issue #5 的可运行示例应如何与首个业务 RPC（#4）、Resource Registry（#8）及 Compose 验收（#9）分工，才能证明 framework 的公开 App/Service 消费面，而不把示例、业务 schema、真实后端和短压测重复验收？

## Done when
- 明确 #5 的最小责任：双 Service 注册/调用、正常与业务错误、关闭，以及静态路由跨进程示例的公开消费边界。
- 明确跨语言业务 schema、`.proto` 真源、deadline/budget、错误信封和非 C++ 真实互通只由 `business-rpc-slice`（#4）决定；#5 不另造第二套协议或 schema。
- 明确需要 Resource Registry/真实 Redis/Mongo 的场景交给 `resource-registry-contract`（#8）及其执行验收，不用示例替代真实后端证据。
- 明确 #9 只消费本节点锁定的示例入口，负责 Compose 资源约束、smoke 和有界短压测，不重复定义 Service 公共契约。
- 形成可回链到 #5 的范围、依赖、非目标和验收证据边界；本节点只做决策，不实现示例。

## Evidence
- [Issue #5](https://github.com/yqm-307/bbt-framework/issues/5)
- [Issue #9](https://github.com/yqm-307/bbt-framework/issues/9)
- [业务 RPC 切片](business-rpc-slice.md)
- [Resource Registry 契约](resource-registry-contract.md)

## Resolution

### #5 的最小责任与两层示例

- **单进程层**：提供一个可从干净 clone 构建运行的双 Service fixture。两个 `CoService<T>` 由同一个公开 `CoApp` 经 `add_service<T>(ServiceOptions)` 注册；一个服务通过受保护的 `this->call(service, method, CoRpcReq, ..., CallOptions)` 调用另一个服务。fixture 至少有正常返回、明确的业务错误、有限 deadline 超时和 `request_shutdown()` 后的正常退出断言。它证明 App/Service 注册、调用、错误和生命周期消费面，不证明跨进程或真实后端。
- **两进程层**：提供两个独立 `CoApp` 进程（caller/server），caller 的出站目标只由 `CoAppOptions::static_routes` 显式配置为 `service_name → http endpoint`；server 只注册自己的 Service。静态路由是白名单，不自动发现、不自动连接任意服务；空/重复/不完整路由在 `run()` 前拒绝，未配置目标返回 `NotFound`。测试/fixture 使用动态 loopback 端口和自有进程，不能依赖固定共享端口或本机安装前缀。
- caller 的对外触发和业务负载必须消费 `business-rpc-slice`（#4）提供的唯一 `.proto`/正式 body-envelope fixture；#5 不新造 schema、IDL、错误信封、budget 规则或第二套跨语言 client。#5 只断言静态路由把一次公开 Service 调用送到 server，以及正常/业务错误/有限 deadline/关闭结果可观察；字段级业务互通、未知字段兼容、remaining-budget/local-min、`OutcomeUnknown` 和非 C++ client 断言归 #4。

### 资源、Compose 与实现边界

- #5 不登记 Resource Registry，不依赖 Redis/Mongo，不把预创建资源或 factory/Start/Close 矩阵作为示例替代证据；真实资源 owner 与组合后端验收归 `resource-registry-contract`（#8）及后续执行票。
- #9 只消费本节点确定的双 Service/静态路由入口，负责 Compose 两服务的资源约束、健康检查、项目范围清理、真实 Redis/Mongo smoke 和有界短压测；#5 不定义 CPU/内存/并发/速率/时长基线，也不把 Compose 当生产部署模板。
- 示例只能包含公开 `Framework.hpp`/`CoApp.hpp`/Service/RPC/配置入口，不包含 `internal/`、Binder、Envelope、测试 seam 或直接注入发送函数，不创建额外 `io_thread`。现有 `Test_framework_f1b2` 的 internal seam loopback 和 `tests/rpc_xlang_*` 的 infra envelope echo 是证据输入，不是 #5 交付物或 #4 业务完成证据。
- 关闭断言沿当前契约使用 `request_shutdown()`、handler 排空及同步 owner Close 的最终结果；不引入单操作取消、不恢复 `Scheduler::Stop()`/`RequestClose`/`WaitClosed`/旧 token。超时表示调用结果超预算，不能被示例解释成远端 I/O 已取消或业务副作用已回滚。

### 可核验的实现出口与当前缺口

- #5 后续执行票的最小产物是：README 可复制的一条构建/运行路径；一个双 Service 单进程 fixture；一个消费 #4 正式业务 fixture 的 caller/server 跨进程 fixture；动态端口与静态路由配置；正常、业务错误、超时、优雅关闭的机器断言；以及回链 #9 的稳定入口说明。
- 当前固定消费树已证明公开入口和调用链存在：`README.md:6-29`、`framework/include/bbt/framework/CoApp.hpp:67-83,99-234`、`framework/include/bbt/framework/ICoService.hpp:4-12,51-59`、`framework/src/host/CoApp.cc:40-175,244-263`。当前树没有独立 `examples/` 目录或 #5 的可运行产物；`tests/Test_framework_f1b2.cc:611-727` 只能证明测试缝下的真实 HTTP loopback，不能作为公开示例。
- 因此本 Resolution 关闭的是本地决策节点，不是实现交付：GitHub #5 仍 OPEN；#4 的 `.proto`/formal bridge、#8 的真实 Redis/Mongo 组合验收和 #9 的 Compose/短压仍未完成。

## Checkpoint
- 已确认：#5 的最小责任是公开 API 双 Service 单进程 fixture + 显式静态路由 caller/server 跨进程 fixture；#9 接走 Compose 资源约束、真实 Redis/Mongo smoke 与有界短压测。
- 已确认的边界：#4 独占业务 `.proto`、formal body-envelope、预算/错误/跨语言字段断言；#8 独占 Resource Registry 与真实资源 owner/组合验收；#5 不另造协议、不依赖真实后端、不使用 internal seam。
- 证据：GitHub Issue #5 及评论；`README.md:6-29`；`framework/include/bbt/framework/CoApp.hpp:67-83,99-234`；`framework/include/bbt/framework/ICoService.hpp:4-12,51-59`；`framework/src/host/CoApp.cc:40-175,244-263`；`tests/Test_framework_f1b2.cc:611-727`；`tests/rpc_xlang_server.cc`、`tests/rpc_xlang_run.py`、`tests/rpc_xlang_client.py`。
- 未完成且不计入本地 Resolution：仓内尚无独立 `examples/`/Compose 交付物，未运行 #5 示例；#4/#8/#9 的实现/真实验收仍未完成，GitHub #5 仍 OPEN。
- 认领/在途：本次只读调查和边界决策已完成；无子代理、构建、运行探针或外部写入在途，准备释放本票认领。
- 下一步：#5 可转为独立执行方案时按本 Resolution 实现；地图可推进依赖它的 `compose-smoke-boundary`，但本次不领取下一张 ticket。
