# examples/two_service — 单进程双 Service fixture（Issue #5 / EX-T2）

一个**公开** `CoApp` 经 `add_service<T>` 注册两个公开 `CoService<T>`；其中一个
服务的 handler 经受保护的公开调用路径 `this->call<Req,Resp>(service, method,
req, ...)` 调用另一个服务：

```
driver ──wire──▶ caller(getvalue::GetValueCallerService)
                   │ this->call<GetValueRequest, GetValueResponse>(
                   │     "bbt.example.v1.GetValueService", "GetValue", …)
                   ▼  静态路由 → 本进程自己的 loopback 端点
                 callee(getvalue::GetValueService)   ← 同一 CoApp 的入站分发
```

> `auth_state=unauthenticated_loopback`：本 fixture 只证明公开 App/Service
> 注册、调用、错误与生命周期消费面，**不证明**身份来源可信、`peer_principal`
> 或授权完成（认证轨道见 #38）。

## 为什么不依赖进程内捷径

框架出站只有一条公开路径：`find_route` 静态路由门 → 真实 HTTP 出站。本 fixture
因此把**唯一**静态路由指向本进程自己的 loopback 端点，让 Service→Service 调用
经真实 socket 走一次完整的 ProtoWireV1 body profile（`POST /rpc` +
`application/x-protobuf`）回到同一宿主的入站分发器。未配置的目标一律
`NotFound`，没有自动发现，也没有「同进程直连」后门。

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

前置与 `examples/getvalue` 完全相同：`-DNEED_TEST=ON` 且
`-DBBT_PROTOBUF_PREFIX=<protobuf 3.21.12 前缀>` 使 `bbt_infra_rpc` 注册（否则
examples 入口根本不加入，不会静默降级）：

```bash
cmake -S . -B build-deps/project-build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DNEED_TEST=ON \
  -DBBT_INFRA_SOURCE_DIR=<bbtools-infra 源码树> \
  -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树> \
  -DBBT_PROTOBUF_PREFIX=<protobuf-3.21.12 前缀> \
  -DCMAKE_POLICY_VERSION_MINIMUM=3.5
cmake --build build-deps/project-build --target two_service \
  --parallel $(( $(nproc) / 4 ))
ctest --test-dir build-deps/project-build -R '^examples\.two_service$' \
  --output-on-failure
```

手跑（单个自有进程 + 动态 loopback 端口）：

```bash
build-deps/project-build/examples/two_service/two_service \
    fixture /tmp/port /tmp/meta /tmp/journal &
# 读 /tmp/port 后按正式 wire 发请求（或用 ctest 的驱动器），随后关闭 stdin 触发
# request_shutdown()，进程按 run() 返回码退出（0 = 按期关闭）。
```

`examples.two_service`（ctest）会 spawn 自有 fixture 进程、等 port 文件就绪、
用 #4 的 Python 标准库 wire codec（`getvalue_client.py`）真实 `POST /rpc`、读
journal 断言、最后关 stdin 并核对退出码；异常路径也回收自有进程。

## 覆盖（真实运行断言）

| 场景 | 动作 | 机器断言 |
|---|---|---|
| 正常请求 | caller.Forward(key=alpha) | HTTP 200；`success=true`；`request_id`/`response_schema` 回显；payload 解出 `found=true, value="value-alpha"`；`1 ≤ remaining_budget_ms ≤ 30000`（该字段只由正式 wire profile 回填并被接收端 clamp） |
| 正常 miss | caller.Forward(key=no-such-key) | `success=true` + `found=false, value=""`（业务结果，不是错误） |
| 业务错误 | caller.Forward(key="") | callee 的 `InvalidArgument` 经受保护 `this->call` 与正式 wire 信封原样回传，不被转成成功 |
| 有限 deadline（接收端预算门） | caller.Forward(budget=1ms) | `TimedOut`，且 callee handler 计数不变（进 handler 前被拒） |
| 有限 deadline（发送前已过期） | caller.ForwardExpired | `TimedOut`，且 callee handler 计数不变（发起 I/O 前拒绝） |
| 未配置路由 | caller.ForwardNoRoute | `NotFound`，且 callee handler 计数不变（`find_route` 门拒绝，不做 I/O） |
| 关闭 + 同步 Close | stdin EOF → `request_shutdown()` | 进程退出码 0；journal `state=Closed(3)`、`owner_closed=1`；`resource_close` 恰一次且出现在**最后一次 callee handler 进入之后**（handler 排空后同步 Close）；全部 journal 行带 `auth_state=unauthenticated_loopback`，`peer_principal` 未被 metadata 升级 |

journal 交叉对照：只有真正到达 callee handler 的 3 个请求（known/miss/empty）
出现 `callee_handler_entered`；budget=1、ForwardExpired、ForwardNoRoute 三个被
拒场景**没有**新增 entry（正反对照，不用日志推测）。

## 已知限制 / 未覆盖（不伪报）

- **跨进程由 `examples/getvalue` 承载（#5 EX-T3 复用）**：本 fixture 是单进程
  「双 Service」，同一宿主的自环出站。两个独立 `CoApp` 进程 + 显式
  `service_name→endpoint` 白名单静态路由（EX-T3）由 **`examples/getvalue` 的
  `getvalue_server` + `getvalue_caller` + `getvalue_s2s_run.py`（ctest
  `examples.getvalue.s2s`）**承载，不在本切片；#5 不另起一套 caller/server/driver。
  其中四类**非法**静态路由（空 `service_name` / 空 `transport` / 空 `endpoint` /
  重复 `service_name`）的「run 启动任何组件前拒绝」由公开入口
  `getvalue_caller badroute <kind> <result_file>` + s2s 驱动器断言（`rc=1`、
  `lifecycle_failures` 恰 1 条且为对应原因、`bound_endpoint` 为空、资源工厂计数 0）。
  注意区分：**空路由列表合法**（等于不允许任何出站目标，未配置目标在运行期
  `NotFound`），与空字段/重复项的启动期拒绝不同；endpoint 格式校验未纳入该断言。
- **未覆盖容量/并发/迟到 lifecycle 矩阵**：容量耗尽 `Overloaded`、超时后 inflight
  强持有、`ShutdownIncomplete` 迟到完成、supervisor 硬终止（EX-T4）不在本切片；
  本切片的关闭是「无在途 handler 的正常关闭」。
- **未覆盖真实后端/Compose/认证**：无 Redis/Mongo、无容器编排、无认证 adapter；
  本示例**不是** `examples/dual_service`（那个是显式开关下的真实 Redis/Mongo
  双进程示例），也不替代 #8/#9/#38。
- **动态端口的实现口径**：`two_service` 先向内核申请一个空闲 loopback 端口并
  立即释放，再把它同时用于 `listen` 与指向自身的静态路由；就绪后严格核对
  `bound_endpoint()` 的端口等于所选端口，不符即失败（竞态落败报错而非假通过）。
  因为静态路由在 `CoApp` 构造期固定，单进程自环必须在构造前确定端口。
- **协程栈**：handler 内做 protobuf 编解码 + 入站分发 + 同进程 loopback 出站，
  默认 12KB 栈不足以承载（同 #4 实测记录）；fixture 经 coroutine 公开配置宏
  `g_bbt_coroutine_config` 把栈调到 512KB 并给 4 个静态线程（该配置 process-global
  且须在调度器 Start 前设置）。512KB 只范围化覆盖本示例负载；框架默认栈对任意
  protobuf 业务不保证安全，属框架层待办。
