---
label: wayfinder:research
blocked_by: [service-example-boundary, business-rpc-slice, resource-registry-contract]
assignee:
claim_session:
status: closed
outcome: resolved
archive:
---
# Compose 多服务冒烟与有界短压测边界（#9）

## Question
Issue #9 如何在 #5 的可运行示例、#4 的业务 RPC 切片和 #8 的资源契约确定后，定义仅用于开发/CI 的 Compose smoke 与短压测，既验证真实服务链路，又不把一次有界短测误称为生产性能承诺？

## Done when
- 固定 #9 消费的双 Service/业务 RPC/Resource Registry 入口，区分真实后端证据、测试替身和静态路由证据。
- 明确 Compose 的 CPU/内存、并发、速率、时长、请求总量、动态端口、健康检查和项目范围清理边界；失败路径同样可验证清理。
- 明确正常请求、业务错误、Redis/Mongo 故障、deadline、关闭和未分类错误的机器判定结果；不以日志存在替代断言。
- 明确短压测只证明该边界内的稳定性/无泄漏/无死锁观察，不形成长期性能基线，不扩大为生产部署方案。
- 形成可回链到 #9 的依赖、验收矩阵和 BLOCKED 条件；本节点只做决策，不运行 Compose 或修改 CI。

## Evidence
- [Issue #9](https://github.com/yqm-307/bbt-framework/issues/9)
- [双 Service 示例边界](service-example-boundary.md)
- [业务 RPC 切片](business-rpc-slice.md)
- [Resource Registry 契约](resource-registry-contract.md)

## Resolution

### 拓扑与消费边界

- #9 的最小 Compose 拓扑固定为：一次性 `driver` → #5 的 `service-a` caller →（显式静态路由）`service-b` server → #8 命名 Resource Registry 中的 Redis `cache` 与 Mongo `docs`。两个业务进程都是独立 `CoApp`；service-a 不直连后端，service-b 不绕过 `ServiceContext::resource<R>(name)`。
- service-a→service-b 的业务调用、`.proto`、formal body-envelope、deadline/budget、错误信封和非 C++ 触发 client 只消费 `business-rpc-slice`（#4）的 `GetValue` fixture；#9 不新增业务 schema、第二套 wire/client 或 gRPC。#9 只验证该入口在真实 Redis/Mongo 资源装配下的端到端场景，不把 #4 的字段级协议断言重复记账。
- Compose 网络只使用独立 project/network 和服务名解析；不发布固定宿主端口，不连接本机已有 Redis/Mongo，不依赖 `/usr/local` 历史安装。每次 job 使用唯一 project name 和唯一数据前缀，Redis key、Mongo database/collection 或文档 ID 不污染其他运行。
- `healthcheck` 只表达 Redis、Mongo、service-a、service-b 的就绪条件；driver 必须再做机器可判定的业务探针，不能以容器 Running、日志出现或 healthcheck 单独替代业务成功证据。

### 场景与机器判定

| 场景 | 必须观察的结果 | 失败判定 |
| --- | --- | --- |
| 就绪/正常请求 | driver 能通过 service-a 触发 service-b，得到 #4 约定的成功结果；调用链使用真实 HTTP/Protobuf 与真实后端 | 路由、业务响应、schema 或后端读写任一缺失即失败 |
| cache hit | Redis 预置的命名 key 经 service-b 返回可核对值 | 返回错误、错误来源未分类或值不匹配即失败 |
| cache miss + Mongo hit | Redis miss 后按 #8 资源边界访问 Mongo，返回可核对持久值；不宣称事务/强一致 | 未访问预期资源、错误 fallback 或结果不可判定即失败 |
| not-found | Redis/Mongo 均无记录时返回 #4 约定的正常 miss/not-found 结果，而非日志或未分类错误 | 把 not-found 误报成功、异常或未分类错误即失败 |
| 业务错误/协议错误 | 空 key 等业务错误、类型/schema/malformed payload 分别得到对应机器错误；错误 envelope 可解析 | 错误被吞、混成成功或只能靠日志判断即失败 |
| Redis/Mongo 故障 | 逐一隔离后端并发起请求，结果落在约定的 unavailable/transport/backend error 类别；不伪造 fallback 成功 | 后端停用后仍报成功、错误未分类或 job 卡死即失败 |
| deadline | 单请求有限预算耗尽时返回 `TimedOut`/#4 约定等价结果；不宣称远端 I/O 已取消或业务副作用已回滚 | 超时变成无限等待、错误不可分类或违反总预算即失败 |
| orderly shutdown | 通过 #5 公开控制路径停止接纳，等待既有 handler，service-a/service-b 与资源正常退出；Redis/Mongo owner 的 Close/物理收束证据可读 | 提前杀进程、跳过 handler 排空、残留资源或仅凭退出码通过即失败 |

Redis/Mongo 故障、deadline、关闭和未分类错误都必须由 driver 的退出码/结构化结果判定；日志仅作诊断。短压测不得把单次成功数、p99 或容器峰值解释为生产性能承诺。

### 资源限制、短压测与隔离

- job 总资源上限为 `2 CPU`、`1.5 GiB`；建议并固定执行票默认上限：service-a `0.35 CPU/192m`、service-b `0.35 CPU/192m`、Redis `0.25 CPU/128m`、Mongo `0.60 CPU/512m`、driver `0.10 CPU/96m`。Mongo `wiredTigerCacheSizeGB <= 0.25`，测试数据量有上限；任何超限、OOM 或资源限制未实际生效都不是通过。
- 短压测上限为并发 `16`、速率 `200 req/s`、持续 `30s`、总请求 `5000`；单请求 timeout `2s` 以内，完整 job timeout `8min`。输出成功/错误分类、p50/p95/p99、最大在途数和容器峰值资源，但不设长期性能基线、不形成生产容量承诺、不做无界 soak。
- 负载阶段只允许正常/预先定义的业务结果；数据错误、未分类错误、死锁、泄漏、OOM 或未收束 handler 任一出现即失败。故障注入与 orderly shutdown 是独立场景，不能被短压测的统计掩盖。
- 真实后端是必需证据，测试替身只可用于非 live 单测；当前 `Test_framework_resource_live.cc` 的 `BBT_TEST_REDIS_ADDR`/`BBT_TEST_MONGO_URI` 入口、`SKIP_RETURN_CODE 77` 和独立 Redis/Mongo 用例只能作为资源 API/错误边界输入，不能直接算 #9 的 Compose 通过。

### 构建、清理与 BLOCKED 条件

- 执行票必须从固定消费 SHA 和显式 `BBT_HIREDIS_PREFIX`、成对的 `BBT_MONGOCXX_PREFIX`/`BBT_MONGOC_PREFIX` 配置构建，并记录依赖 manifest；未注册 `bbt::infra_redis`、`bbt::infra_mongo`、#4 正式业务 bridge 或 #5 稳定 caller/server 入口时 fail-closed。不能接受当前 CMake “target 缺失则不注册 live test”作为绿色结果，也不能把 CTest skipped 记作通过。
- job 必须用 `trap`/CI `always()` 执行且只针对本 job project：`docker compose -p <unique-project> down -v --remove-orphans`。清理后机器核对该 project 无容器、卷、网络残留；不得使用无 project 限定的全局 prune 或删除其他 job 资源。清理失败路径与正常路径同样必须核对。
- runner 只沿 #6 已授权的受信 branch/PR 运行；保留精简日志和结构化结果 artifact，凭据不写入 Compose 文件或日志。Docker daemon、镜像/依赖前缀、真实后端、#4/#5/#8 任一就绪条件缺失时，状态为 BLOCKED/SKIPPED（明确原因），不是 Passed。
- 当前固定消费树只有 `docker/bbtools-common-image` 与 `docker/bbtools-runner`，没有 `docker-compose.yml` 或 #9 专用服务/driver；因此本 Resolution 是后续执行的验收 spec，不代表 Compose 已实现、已运行或 GitHub #9 已关闭。

## Checkpoint
- 已确认：#9 消费 #5 的静态路由双 Service、#4 的 `GetValue`/正式业务 wire、#8 的命名 Redis/Mongo 资源；拓扑固定为 driver→service-a→service-b→Redis/Mongo。
- 已确认的硬边界：总资源不超过 `2 CPU/1.5 GiB`；并发 `<=16`、速率 `<=200 req/s`、持续 `<=30s`、总请求 `<=5000`、单请求 timeout `<=2s`、job timeout `<=8min`；不做生产部署或长期性能基线。
- 已确认的证据门槛：真实后端、机器断言、健康检查与业务探针、故障/超时/关闭场景、project-scoped failure cleanup；日志和容器状态不能替代断言，缺依赖 target/后端/稳定入口必须 fail-closed。
- 证据：GitHub Issue #9；#4/#8 当前 OPEN 及其边界评论；本地 `tests/CMakeLists.txt:77-128`、`tests/Test_framework_resource_live.cc:1-10,75-90,397-558`；`docker/bbtools-common-image/Dockerfile`、`docker/bbtools-runner/Dockerfile`；`scripts/build_stack.sh`、`scripts/run_ctest.sh`；固定 `deps.lock` 与三仓基线 SHA。
- 未完成且不计入本地 Resolution：当前树没有 Compose 文件、#9 专用 service/driver、实测 Compose/短压或真实 Redis+Mongo 组合证据；GitHub #9 仍 OPEN。本轮未启动 Docker、未修改 CI/源码、未安装依赖。
- 认领/在途：本次只读调查和范围决策已完成；无子代理、构建、容器或外部写入在途，释放本票认领。
- 下一步：本票可转独立执行方案；本图剩余 frontier 仅为需要用户确认的 `p0-scope-evidence-gate`，本次不自动进入该 HITL ticket。
