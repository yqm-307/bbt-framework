# 网络宿主与 RPC V2 消费

> 状态：**ADR 0022 的现行装配（已落地）**。`LegacyHeaders`/`ProtoWireV1`/`TcpV1` 模式选择与 `x-bbt-*` 编解码已按 ADR 0022 删除，RPC 只走 V2 会话；`RpcWireBridge`/`RpcHttpBridge` 已由 `RpcBridge.{hpp,cc}`（`InstallRpcAdmission` + `RpcEgress`）取代。映射与理由见 `bbtools-infra/docs/decisions/0022-rpc-single-protocol.md`。

> v2 现状：`InfraHttpHost` 是统一 v2 `CoApp` 的内部网络宿主，Engine 由 App 根持有；宿主 `Create` 只做 Idle 装配并暴露 `engine_config()`，`CoApp` 在 Create 完成后经 `HostGate` 发布 Engine，入口 native host hook 在主线程执行唯一一次 `Run`，`Start` 经 `Engine::CoWaitRunning` 等 Ready。framework 不持有 runner/future/join，也不从 Co 域调用 `Engine::Run`。Redis/Mongo 只经 `v2::RedisComponent`/`v2::MongoDBComponent`；旧 Resource 公共面已删除，能力只经组件装配（见 `framework-contract.md` §5.2/§12.5/§13）。唯一真源见 `bbtools-infra/docs/architecture/native-lifecycle-seam-contract.md` 与 coroutine `docs/architecture/core-runtime-contract.md` §12.2。

## 职责卡

| 项 | 内容 |
|---|---|
| @brief | `InfraHttpHost` 是框架内部网络宿主：独占入站 Engine、共享同一个 `rpc::CoRpcClient` 会话 owner 与 `rpc::CoRpcServer`，并为 HTTP 能力保留 `http::Server`/`http::Client`。 |
| @par 职责 | Engine/资源装配（Idle 阶段）、共享 Engine 借用、服务端 Start 时序、入站 handler 的在途记账与业务门、RPC Shutdown、绑定地址回填；HTTP 关闭步骤单独按其接口处理。 |
| @par 不负责 | 业务 Service/Actor 路由与调度（归 `InboundDispatcher`）、出站路由门与策略（归 `AppRuntimeState`/`RpcPolicy`）、连接物理回收的对外承诺。 |
| @par 约束 | `Create` 只在 Engine Idle 时装配，不联网不建线程；全部 Idle 阶段装配完成后由应用发布 Engine 并交给入口 native host hook 执行一次 `Run`；Engine 发布 Running 后才启动 Server 的接纳消费。framework `Close` 幂等且不持有 native thread/future/join；逻辑关闭与物理回收分开观察。 |
| @par 真源 | `framework/include/bbt/framework/internal/InfraHttpHost.hpp`、`framework/src/host/InfraHttpHost.cc`；RPC 入站/出站装配在 `framework/include/bbt/framework/internal/RpcBridge.hpp` 与 `framework/src/host/RpcBridge.cc`。已删除的 `RpcWireBridge.*`/`RpcHttpBridge.*` 不再存在。 |

## 装配与消费入口

`framework/src/CMakeLists.txt` 直接链接 `bbt::infra_http_net` 与公开 `bbt::infra_rpc_net`；后者缺失即 configure `FATAL_ERROR`，没有 header 桥或能力宏降级。所有构建必须传 `BBT_INFRA_HTTP_NET=ON`、`BBT_INFRA_RPC_NET=ON` 与固定 `BBT_PROTOBUF_PREFIX`。

```cpp
// 形状示意（未编译、未运行）：rpc::CoRpcClient::Create / rpc::CoRpcServer::Create 返回 result，
// 装配前必须解包并短路错误（net::Engine::Create 直接返回 shared_ptr）。
// 一个 Engine 同时承担 accept 与 dial：Server 把完成握手的入站 link 交给同一
// session owner，Call/Send 与入站复用同一条双向 link。
auto engine = net::Engine::Create(/* RawHandler */ {});

auto made_client = rpc::CoRpcClient::Create(engine, rpc::ClientOptions{}
                    .SetLocalIdentity(options.local_identity)      // 非空 ≤256 UTF-8
                    .SetTrustedNetworkPlaintext(true));            // 明文模式唯一入口
if (!made_client) return result<void>::err(std::move(made_client.error()));
auto client = std::move(made_client).value();

auto made_server = rpc::CoRpcServer::Create(engine, client, admission,  // 同一 owner
                    rpc::ServerOptions{}
                        .SetListenAddress(net::Address::Ip(host, port, ec))
                        .SetMaxFrameBytes(limits.max_body_bytes)
                        .SetMaxConnections(limits.max_connections)
                        .SetMaxInflight(limits.max_inflight)
                        .SetMaxHandlerBytes(limits.max_body_bytes)
                        .SetIncomingTimeout(limits.incoming_timeout));
if (!made_server) return result<void>::err(std::move(made_server.error()));
auto server = std::move(made_server).value();
```

`NetworkLimits` → `ServerOptions` 的映射是逐字段的（见上）；`CoRpcServer::Start` 要求 Engine Running 且 coroutine scheduler 已启动，因此时序固定为：runtime 值配置冻结 → runtime `Start` → 提交前短同步 hook → 提交主 Co 任务 → `Create`（Engine Idle 装配并冻结网络配置）→ 发布 Engine 给入口 host hook → host hook 调用一次 `Engine::Run` → 主任务 `CoWaitRunning` 等 Running → `CoRpcServer::Start()` → `LocalAddress()` 回填 `bound_endpoint()`。提交前 hook 不调用 `Engine::Run`、不等待 Ready、不 join；`Run` 只由提交后 host hook 调用。`CoRpcServer::Start()` 在受管协程内直接调用：infra 已把可挂起的队列/accept 任务登记移出 server 状态锁，并对 Start/Close 竞争给出显式裁决（`Closed`），不需要也不允许 framework 为它新开 native 线程。

入站没有 envelope/handler 桥：`AdmissionHandler` 把 owning `AdmissionItem` 有界转移到框架自有队列，由 worker 协程执行 `InboundDispatcher::Dispatch(const rpc::Request&)`，再经 `ReplyCompletion::Complete/Fail` 写终态、`AdmissionLease::Release()` 结清。`Request::Context()`（deadline local-min / peer_identity / peer_principal）是唯一入站字段来源；测试侧的同步派发入口是 `DispatchInboundForTest(app, const rpc::Request&)`，不再接收 `IncomingCallContext`。

HTTP 能力（`HttpComponent`/`MonitorService`）继续消费 `bbt::infra::http` 的 `Server`/`Client` 与 `IncomingCallContext`，与 RPC 会话互不替代；HTTP 侧不承载 RPC envelope。

**HTTP 与 RPC 是两个 listener**：`InfraHttpHost::Create` 显式拒绝同时安装 HTTP 与 RPC handler（`HTTP and RPC cannot share a listener`）。装配 HTTP handler 的宿主只有 `http::Server`，装配 RPC admission 的宿主只有 `CoRpcServer`；两者不能靠改名字互换。

**公开 HTTP 业务入口**：构造期调用 `AppOptions::AddHttpRoute(HttpServiceRoute{path,
service, method, request_schema, response_schema})`。选用 HTTP 后，该 App 的默认 listener
只消费 HTTP/1.1，RPC 出站仍复用同一 Engine 的 client；未配置 HTTP 路由时保持 RPC 入站。
路由是精确 path 的 POST/protobuf 映射，不是任意 URL router，也不暴露内部 dispatcher。

- 路由、Service、方法与 schema 只取装配值；请求 body 是 protobuf 字节，且
  `Content-Type` 必须唯一且精确为 `application/x-protobuf`，缺失、重复或其他值
  在进入 dispatcher 前返回 415。
- 必须提供唯一非空 `X-Request-Id`（不超过 128 个字母数字、`-_.`）；它只作关联，
  不是授权凭据。HTTP 自报头不能设置主体、deadline、路由或系统元数据。
- 上下文期限与主体取 infra HTTP parser 的 `IncomingCallContext`，在同一
  `InboundDispatcher` 内继续执行方法/schema 校验、Service 额度、执行策略及 RequestScope。
- 成功返回 200、`application/x-protobuf` 与相同请求 ID；非法输入/未找到/超时分别
  返回 400/404/504，其余业务错误返回 503。未知路径 404、非 POST 405。
  错误 body 是文本，`X-Bbt-Error-Code` 是 framework `ErrorCode` 的数值，不是 RPC wire enum。
- `examples/ecommerce_cluster` 的 `run.py` 与 `run_storage.py` 使用 Python
  `http.client` 真正发 HTTP，内部跨进程仍走 typed V2 RPC；不使用测试 seam。
  既有 `framework.inbound_http` 的 seam 用例只承担低层回归，不代替公开入口证据。

**公开 RPC 出站策略与发现表**：`AppOptions::SetRpcPolicy(shared_ptr<RpcPolicy>)`
与 `SetDynamicRoutes(shared_ptr<DynamicRouteTable>)` 只在构造期装配固定共享 owner。
空策略默认放行；发现 provider 只发布所选表的快照。策略在解析路由之后、发送之前
执行，期限只能收紧；具体可执行场景见 `examples/rpc_policy/README.md`。

## 执行与关闭

- 入站在 Idle 时装配唯一 server 接纳消费者；全部 Idle 阶段装配完成后应用发布共享 Engine，入口 host hook 在主线程调用一次 `Run`，主任务观察到 Running 且绑定地址可读后才调用 `CoRpcServer::Start`。`Run` 执行归应用入口，framework 业务协程不调用。
- 宿主占 1 个 server driver 槽 + 1 个 client driver 槽（同一 Engine 的双向会话），另留 1 个业务 worker，即至少 3 个固定 scheduler worker；纯出站至少 2 个。独立测试客户端与多宿主按实际同时存活的 driver 数相加，不按机器核数扩并发。
- 宿主 `StopAccepting` 在 `HandlerState` 的同一锁内关闭业务门，RPC 迟到请求由 admission 同步拒绝；不会调用已删除的 RPC `StopAccepting`。HTTP 分支仍调用 `http::Server::StopAccepting`。已过门的 handler 继续记账。
- `WaitHandlersDone(deadline)` 等宿主 `inflight` 归零后，在同一业务门锁内拒新；排空只证明业务责任归零，不调用 RPC Shutdown/Drain/WaitStopped，也不以任何关闭调用返回证明物理释放。HTTP 分支保留其关闭步骤，封口统一归 `Close(CloseOptions)`。
- framework `Close` 发布首次关闭原因，对 RPC/HTTP server/client 调用模块公共 `Close(CloseOptions)`（RPC 面为 Co 调用）；组件 `OnStop(options)` 与旧宿主关闭都沿同一个 `CloseOptions`。Engine 不由此路径关闭：主任务结束后由入口 host hook `RequestClose`，`Run` 返回即引擎收口。
- 业务对象与方法表放进 `AppRuntimeState`（shared_ptr 运行时状态）：入站接纳闭包与出站回调复制该指针，不借用 CoApp 成员、不捕获 CoApp this，CoApp 先析构也不影响在途请求。关闭序列经 `Release` 收回对业务对象的引用；在途分发靠本地强引用与自持分发表项收尾。
- `Close` 返回只表示 framework/资源的逻辑关闭观察完成；它不证明 callback 销毁、连接 Released 或 Engine `Stopped`。framework 不在析构或业务路径等待 native thread/future/join；需要物理证据时用对应模块的 `wait_release=true` 或 `WaitStopped`，Engine 的凭据是 `Run` 已返回。

## 能力与待补齐边界

RPC 控制面以 infra 的 `Close(CloseOptions)` 为准（见 infra `docs/architecture/close-options-contract.md` 与 `docs/guides/rpc-net.md`）；宿主自身的 `StopAccepting`/`WaitHandlersDone` 是业务门编排，不是 RPC 生命周期接口。`CloseFlag`/`timeout_ms` 形态只存在于旧迁移材料，不是当前契约；framework 只消费逻辑终态与绝对 deadline。

- `target` 只接受 `tcp://<numeric-ip>:<port>`：无 DNS/TLS/代理/重定向/自动重试；不支持形态返回 `InvalidArgument`，不隐式解析域名。路由表只校验 endpoint 非空，地址语法归 infra（单一真源）。
- 入站限额：`max_frame_bytes`/`max_handler_bytes` 取 `NetworkLimits.max_body_bytes`，`max_inflight`/`max_connections`/`incoming_timeout` 逐字段映射；admission 队列满返回 `Overloaded`，不无界排队。
- `peer_principal` 在 trusted-network 明文模式下恒空；`peer_identity` 是自报事实，不是认证凭据，不用于授权。
- 逐 peer/epoch 观测仍缺（`ClientStats`/`ServerStats` 只是 owner 汇总）；`HttpComponent` 的 HTTP 边界（body/header 上限、仅 `http://` 数值 IP）保持原状。
- `Test_framework_resource_live.cc` 的 Redis/Mongo live 目标只在 Redis ON 配置下做了编译覆盖（已迁到 Co 生命周期：宿主 Co 面 + `co_start`/`co_wait_shutdown_request`/`co_stop`，不再是旧 `run()`/旧 `INetworkHost`）；本轮未运行任何 live 用例、未连接 Redis/Mongo，编译通过不等于 live 验收。
- 本地显式 `BBT_INFRA_SOURCE_DIR` 候选集成不证明远端 exact-SHA 构建通过；远端完整门禁不由本地测试替代。
