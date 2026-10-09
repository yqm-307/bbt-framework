# examples/lifecycle_matrix — 容量 / 并发上下文 / 关闭生命周期验收（Issue #5 / EX-T4）

一个**自有 fixture 进程** + 外部 **Python 驱动器**，经**真实公开入口**验证框架的
容量、并发上下文与关闭生命周期行为：

- fixture：公开 `CoApp`（`inbound_bridge=ProtoWireV1`）+ 真实 infra HTTP loopback
  宿主，注册公开 `CoService<T>`（`lifecycle::LifecycleGateService`）；业务
  schema / typed codec **完整复用 #4 的 getvalue**（`getvalue_proto` +
  `ProtoCodec` 特化），#5 不另造第二套 schema/协议/codec；
- 驱动器：用 #4 的 Python 标准库 wire codec（`getvalue_client.py`）打真实
  `POST /rpc`。

```
driver ──真实 wire──▶ fixture(自有进程, 动态 loopback)
                         └ CoApp → ProtoWireV1 → LifecycleGateService
                                Hold(挂起/占容量) / Fast(即时)
```

> `auth_state=unauthenticated_loopback`：本验收只证明公开 App/Service 消费面、
> 容量与生命周期行为，**不证明**身份来源可信或授权完成（认证轨道见 #38）。

## 为什么不是「测试缝/桩」

严格遵守 #5 边界：只使用公开头（`CoApp.hpp`/`CoService.hpp`/`ICoService` 的
`call`/`RpcMethods.hpp`/`ICoCloseable.hpp`），另按 #4 已验收示例口径使用
coroutine 的进程级启动配置访问头 `bbt/coroutine/detail/GlobalConfig.hpp`（仅在
调度器启动前设置栈大小/静态线程数）与公开同步原语
`bbt/coroutine/sync/CoWaiter.hpp`（handler 的验收挂起点）。**不使用**
`framework/internal/`、Binder、`CoAppSeam`、`MakeCoAppForTest`、
`DispatchInboundForTest`、`INetworkHost` 桩或任何发送函数注入；不创建额外
`io_thread`（只调用公开 `CoApp::run()`/`request_shutdown()`）。

验收控制面（**仅验收用**，不是生产注入）：fixture 的 stdin 逐行命令
`shutdown` → `request_shutdown()`；`open` → 放行进程内验收闩 `OpenGate`；
`quit`/EOF → 收尾。断言全部基于 journal 事件、HTTP 结果与进程退出码，
驱动器不用固定 sleep 猜时序：先等 journal 里的可观察握手（`handler_entered`/
`handler_resumed`/`shutdown_state`）再推进。

## 构建与运行（一条路径）

前置与 `examples/getvalue`、`examples/two_service` 完全相同（`-DNEED_TEST=ON`
且 `-DBBT_PROTOBUF_PREFIX=<protobuf 3.21.12 前缀>` 使 `bbt_infra_rpc` 注册）：

```bash
cmake --build <build> --target lifecycle_fixture --parallel $(( $(nproc) / 4 ))
ctest --test-dir <build> -R '^examples\.lifecycle_matrix$' --output-on-failure
```

手跑：`lifecycle_fixture fixture <port_file> <meta_file> <journal_file> <max_inflight> <step_budget_ms>`。

## 覆盖（真实运行断言，全部 PASS）

| 场景（结果键） | 动作 | 机器断言 |
|---|---|---|
| `r3-concurrent` | 两项 `Hold` 并发挂起（max_inflight=2），journal 握手两项 `handler_entered` 后放行 | 每项 `handler_resumed` 的 `ctx_id == req`、`same_ctx=1`、`same_co=1`（上下文/协程跨挂起不串）；迟到回复各为 `held:<自己的 id>`，`success=true` |
| `r3-read-timeout-still-inflight` | 客户端读超时（连接保持打开），work 仍物理在途 | 两项客户端读均 `timeout`；此时探测请求 `r4-overloaded`（见下）证明仍计 inflight |
| `r4-overloaded` | 容量满（2 项在途）时新请求 `Fast` | `error.code=7`（Overloaded）、HTTP 200；该请求**未进 handler**（`Fast` 的 `handler_entered` journal 无 `r3-probe`）——耗尽即拒、不无界排队 |
| `r4-recovered` | 放行后新探测请求 | `success=true`（容量恢复接纳，非原调用 retry） |
| `r4-byte-limit` | 发送 body（8128B）> `NetworkLimits.max_body_bytes`（4096） | HTTP `413`（拒绝）；随后正常请求仍成功（服务器健康） |
| `r5-graceful` | 一次正常请求后 stdin `shutdown` | 退出码 0；journal `rc=0 state=3(Closed) owner_closed=1 pending=0`；`resource_close` 恰一次且发生在最后一次 handler 事件**之后**（handler 排空后同步 Close） |
| `r5-shutdown-incomplete` | 在途 `Hold` 被闩扣住时 `shutdown`（step_budget=300ms） | 退出码 3（`kExitShutdownLate`）；进入 `ShutdownIncomplete` 时 `pending≥1`、fixture 仍存活、owner 未被 Close、新请求 `connection-refused`（拒新）；放行后 `handler_resumed` 在 `resource_close` **之前**、进程如期退出（迟到完成无 UAF） |
| `r5-hard-kill` | 在途 `Hold` 被扣住时 supervisor `SIGKILL` | 进程 `rc=-9`；journal 含 `handler_entered`（未完成证据），**无** `event=shutdown`/`resource_close`/`state=3`——只标未完成/未知，**不称优雅完成** |

journal 每行均带 `auth_state=unauthenticated_loopback`；`peer_principal` 来自真实
transport（loopback 无认证 → 空串），不被 metadata 升级。

## 已知限制 / 未覆盖（不伪报）

- **迟到回复送达不是框架承诺**：`r5-shutdown-incomplete` 中关闭序列在 handler
  返回后即同步收口网络，迟到完成的**回复**可能不再写出（本机实测 `delivered=false`）；
  无 UAF 的正面证据是「进程如期以 rc=3 退出 + handler 恢复记录在案 + 资源 Close
  在迟到完成之后」，不是回复送达。
- **ActorSerial / mailbox 容量维度未覆盖**：本票约束「不启 ActorSerial」，故
  `mailbox_capacity` 耗尽路径不在此切片（服务容量维度以 Concurrent 的
  `max_inflight` 覆盖）。
- **出站 / 跨 hop 预算未在本切片重验**：本 fixture 无静态路由（未配置即
  `NotFound`）；Service→Service 真实出站与预算矩阵由 #4 的
  `examples.getvalue.s2s` / `framework.getvalue.codec` / `Test_framework_matrix`
  承载。
- **单进程自有 fixture**：本切片是「一个 fixture 进程 + 外部 driver」的独立网络/
  进程证据；**两个独立 CoApp 进程 + 静态路由（EX-T3）** 不在本目录。
- **关闭观测的时序**：`shutdown_state` 由 fixture 观测线程轮询「变化即记录」；
  瞬时 `Closing` 相位可能被轮询错过（信息项，不作断言）。
- **未跑 sanitizer**：本切片未在 ASan/TSan/UBSan 下重跑；迟到完成无 UAF 的证据是有界退出、handler 恢复记录和 Close 顺序，不等同 sanitizer 结论。
- **真实后端 / Compose / 认证**：无 Redis/Mongo、无容器编排、无认证 adapter；
  不替代 #8/#9/#38，也不替代真实身份的至少性结论。
