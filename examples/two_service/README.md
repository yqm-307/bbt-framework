# examples/two_service — 双 CoApp Service→Service 验收（Issue #5 / EX-T2）

两个**互异 identity** 的公开 `CoApp` 进程（框架约束进程内同一时刻至多一个
活跃 CoApp，V2 会话又拒绝 self-connection）：

```
driver ──V2 会话──▶ caller CoApp（local_identity=two-service-caller）
                     │ 宿主 getvalue::GetValueCallerService
                     │ this->call<GetValueRequest, GetValueResponse>(
                     ▼     "bbt.example.v1.GetValueService", "GetValue", …)
                   静态路由 → callee 的真实 loopback endpoint
                   callee CoApp（local_identity=two-service-callee）
                     └ 宿主 getvalue::GetValueService
```

> `auth_state=unauthenticated_loopback`：本 fixture 只证明公开 App/Service
> 注册、调用、错误与生命周期消费面，**不证明**身份来源可信、`peer_principal`
> 或授权完成（认证轨道见 #38）。

## 为什么不依赖进程内捷径

框架出站只有一条公开路径：`find_route` 静态路由门 → 真实 V2 会话
（`SessionFrame` + u32be 分帧 + Hello/HelloAck）。caller 的唯一静态路由指向
callee 进程的实际 endpoint；未配置的目标一律 `NotFound`，没有自动发现，也没有
「同进程直连」后门。两个 identity 与驱动器客户端三方互异，既满足 V2 的
self-connection 约束，也走完整的握手与 epoch 协商。

业务 schema、服务实现与 typed codec 全部复用 `examples/getvalue`（#4 的唯一
`.proto` 真源 + `ProtoCodec` 特化 + `ProtoMethod` 声明糖）；#5 不另造第二套
schema、协议、错误信封或跨语言 client。

严格遵守 #5 边界：框架消费面只使用公开头（`CoApp.hpp`/`CoService.hpp`/
`ICoService` 的 `call`/`RpcMethods.hpp`/`ICoCloseable.hpp`）；另按 #4 已验收
示例口径使用 coroutine 的进程级启动配置访问头
`bbt/coroutine/detail/GlobalConfig.hpp`，仅在调度器启动前设置示例所需的栈大小和
静态线程数。除此之外不使用 `framework/internal/`、Binder、`CoAppSeam`、
`MakeCoAppForTest`、`DispatchInboundForTest`、`INetworkHost` 桩或发送函数注入；
不创建额外 `io_thread`（只调用公开 `CoApp::run()` / `request_shutdown()`）。

## 构建与运行（一条路径）

前置与 `examples/getvalue` 完全相同：`-DNEED_TEST=ON`、
`-DBBT_INFRA_HTTP_NET=ON -DBBT_INFRA_RPC_NET=ON` 且
`-DBBT_PROTOBUF_PREFIX=<protobuf 3.21.12 前缀>` 使 `bbt_infra_rpc_net` 注册（否则
examples 入口根本不加入，不会静默降级）：

```bash
cmake -S . -B build-deps/project-build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DNEED_TEST=ON \
  -DBBT_INFRA_SOURCE_DIR=<bbtools-infra 源码树> \
  -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树> \
  -DBBT_INFRA_HTTP_NET=ON -DBBT_INFRA_RPC_NET=ON \
  -DBBT_PROTOBUF_PREFIX=<protobuf-3.21.12 前缀> \
  -DCMAKE_POLICY_VERSION_MINIMUM=3.5
cmake --build build-deps/project-build --target two_service \
  --parallel 2
ctest --test-dir build-deps/project-build -R '^examples\.two_service$' \
  --output-on-failure
```

手跑（两个自有进程 + 动态 loopback 端口，caller 需要 callee 的实际端口）：

```bash
BIN=build-deps/project-build/examples/two_service/two_service
"$BIN" callee /tmp/callee.port /tmp/callee.meta /tmp/journal &
# 读 /tmp/callee.port 得 PORT，然后：
"$BIN" caller /tmp/caller.port 127.0.0.1:PORT /tmp/caller.meta /tmp/journal &
# 读 /tmp/caller.port 后按正式 V2 会话发请求（或用 ctest 的驱动器）；
# 先关 caller stdin 触发 request_shutdown()，再关 callee stdin。
```

`examples.two_service`（ctest）会 spawn 两个自有进程、等 port 文件就绪、用
`tests/rpc_v2_wire.py` 真实发起 V2 Call、读共享 journal 断言、最后按序关
stdin 并核对两个进程的退出码；异常路径也回收自有进程。

## 覆盖（真实运行断言）

| 场景 | 动作 | 机器断言 |
|---|---|---|
| 正常请求 | caller.Forward(key=alpha) | 成功响应；`request_id`/`response_schema` 回显；payload 解出 `found=true, value="value-alpha"` |
| 正常 miss | caller.Forward(key=no-such-key) | 成功响应 + `found=false, value=""`（业务结果，不是错误） |
| 业务错误 | caller.Forward(key="") | callee 的 `InvalidArgument` 经受保护 `this->call` 与 V2 错误帧原样回传，不被转成成功 |
| 有限 deadline（接收端预算门） | caller.Forward(budget=1ms) | `TimedOut`，且 callee handler 计数不变（进 handler 前被拒） |
| 有限 deadline（发送前已过期） | caller.ForwardExpired | `TimedOut`，且 callee handler 计数不变（发起 I/O 前拒绝） |
| 未配置路由 | caller.ForwardNoRoute | `NotFound`，且 callee handler 计数不变（`find_route` 门拒绝，不做 I/O） |
| 关闭 + 同步 Close | 先 caller 后 callee 关 stdin | 两进程退出码均为 0；journal `state=Closed(3)`、`owner_closed=1`；`resource_close` 恰一次且出现在**最后一次 callee handler 进入之后**（handler 排空后同步 Close）；全部 journal 行带 `auth_state=unauthenticated_loopback`，`peer_principal` 未被 metadata 升级 |

journal 交叉对照：只有真正到达 callee handler 的 3 个请求（known/miss/empty）
出现 `callee_handler_entered`；budget=1、ForwardExpired、ForwardNoRoute 三个被
拒场景**没有**新增 entry（正反对照，不用日志推测）。两个进程以 append 方式写
同一 journal，行顺序即真实发生顺序。

## 已知限制 / 未覆盖（不伪报）

- **不是「单进程双 Service」**：V2 拒绝 self-connection，框架又约束进程内至多
  一个活跃 CoApp，因此本示例是两个 CoApp 进程；跨进程调用是真实 socket 会话，
  不是进程内捷径。跨进程 typed 出站的更完整场景（含黑障丢 reply、一次性
  OutcomeUnknown）由 `examples.getvalue.s2s` 承载。
- **未覆盖容量/并发/迟到 lifecycle 矩阵**：容量耗尽 `Overloaded`、超时后 inflight
  强持有、`ShutdownIncomplete` 迟到完成、supervisor 硬终止（EX-T4）不在本切片；
  见 `examples/lifecycle_matrix`。本切片的关闭是「无在途 handler 的正常关闭」。
- **未覆盖真实后端/Compose/认证**：无 Redis/Mongo、无容器编排、无认证 adapter；
  本示例**不是** `examples/dual_service`，也不替代 #8/#9/#38。
- **协程栈**：handler 内做 protobuf 编解码 + 入站分发 + 真实出站；默认 12KB 栈
  不足以承载（同 #4 实测记录）；两个 fixture 进程都经 coroutine 公开配置宏
  `g_bbt_coroutine_config` 把栈调到 512KB 并给 4 个静态线程（该配置
  process-global 且须在调度器 Start 前设置）。512KB 只范围化覆盖本示例负载；
  框架默认栈对任意 protobuf 业务不保证安全，属框架层待办。
