# 网络宿主与 RPC V2 消费

> 状态：**ADR 0022 的现行装配（已落地）**。`LegacyHeaders`/`ProtoWireV1`/`TcpV1` 模式选择与 `x-bbt-*` 编解码已按 ADR 0022 删除，RPC 只走 V2 会话；`RpcWireBridge`/`RpcHttpBridge` 已由 `RpcBridge.{hpp,cc}`（`InstallRpcAdmission` + `RpcEgress`）取代。映射与理由见 `bbtools-infra/docs/decisions/0022-rpc-single-protocol.md`。

## 职责卡

| 项 | 内容 |
|---|---|
| @brief | `InfraHttpHost` 是框架内部网络宿主：独占入站 Engine、共享同一个 `rpc::CoRpcClient` 会话 owner 与 `rpc::CoRpcServer`，并为 HTTP 能力保留 `http::Server`/`http::Client`。 |
| @par 职责 | Engine 装配与 Run 线程、服务端 Start 时序、入站 handler 的在途记账与业务门、RPC 同步 Shutdown 与 Engine 收口、绑定地址回填；HTTP 关闭步骤单独按其接口处理。 |
| @par 不负责 | 业务 Service/Actor 路由与调度（归 `InboundDispatcher`）、出站路由门与策略（归 `AppRuntimeState`/`RpcPolicy`）、连接物理回收的对外承诺。 |
| @par 约束 | `Create` 只在 Engine Idle 时装配，不联网不建线程；`Start` 之后才监听。`Close` 幂等且 noexcept，只封口不等待业务 handler；仍被业务占用的驱动线程留给析构 join。 |
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

`NetworkLimits` → `ServerOptions` 的映射是逐字段的（见上）；`CoRpcServer::Start` 要求 Engine Running 且 coroutine scheduler 已启动，因此时序固定为：`Create`（Idle 装配）→ scheduler `Start` → Engine `Run`（自有 native 线程）→ 等 `Running` → `CoRpcServer::Start()` → `LocalAddress()` 回填 `bound_endpoint()`。

入站没有 envelope/handler 桥：`AdmissionHandler` 把 owning `AdmissionItem` 有界转移到框架自有队列，由 worker 协程执行 `InboundDispatcher::Dispatch(const rpc::Request&)`，再经 `ReplyCompletion::Complete/Fail` 写终态、`AdmissionLease::Release()` 结清。`Request::Context()`（deadline local-min / peer_identity / peer_principal）是唯一入站字段来源；测试侧的同步派发入口是 `DispatchInboundForTest(app, const rpc::Request&)`，不再接收 `IncomingCallContext`。

HTTP 能力（`HttpComponent`/`MonitorService`）继续消费 `bbt::infra::http` 的 `Server`/`Client` 与 `IncomingCallContext`，与 RPC 会话互不替代；HTTP 侧不承载 RPC envelope。

## 执行与关闭

- 入站 Engine 在 Idle 时装配唯一 server 接纳消费者，`Run` 在自有 native 线程执行；Running 且绑定地址可读后才调用 `CoRpcServer::Start`。
- 宿主占 1 个 server driver 槽 + 1 个 client driver 槽（同一 Engine 的双向会话），另留 1 个业务 worker，即至少 3 个固定 scheduler worker；纯出站至少 2 个。独立测试客户端与多宿主按实际同时存活的 driver 数相加，不按机器核数扩并发。
- 宿主 `StopAccepting` 在 `HandlerState` 的同一锁内关闭业务门，RPC 迟到请求由 admission 同步拒绝；不会调用已删除的 RPC `StopAccepting`。HTTP 分支仍调用 `http::Server::StopAccepting`。已过门的 handler 继续记账。
- `WaitHandlersDone(deadline)` 等宿主 `inflight` 归零后，在同一业务门锁内拒新，再调用同步 `CoRpcServer::Shutdown()`。它不调用 RPC `Drain`/`WaitStopped`，也不以 Shutdown 返回证明物理释放。HTTP 分支保留其 `Shutdown(deadline)`/`Drain(deadline)`、`Close()`、`WaitStopped(deadline)` 顺序。
- 宿主 `Close(reason)` 发布首次关闭原因，对 RPC server/client 调用同步 `Shutdown(reason)`，对 HTTP server/client 调用 `Close(reason)`，再 `RequestClose` Engine；不等待业务 handler 返回，正在等待排空的宿主收到关闭原因后不得当作已排空。
- 业务对象与方法表放进 `AppRuntimeState`（shared_ptr 运行时状态）：入站接纳闭包与出站回调复制该指针，不借用 CoApp 成员、不捕获 CoApp this，CoApp 先析构也不影响在途请求。关闭序列经 `Release` 收回对业务对象的引用；在途分发靠本地强引用与自持分发表项收尾。
- 网络线程已结束时由 `Close` 回收；尚未结束的线程留给宿主析构 join，因此析构仍可能等待迟到 handler。`Close` 返回不证明 callback 销毁、连接 Released 或线程已全部回收。

## 能力与待补齐边界

RPC 的统一 `Close(CloseFlag, timeout_ms, Error)` 与同步强制 `Shutdown` 以 infra 的 `docs/guides/rpc-net.md` 为准；宿主自身的 `StopAccepting`/`WaitHandlersDone` 是业务门编排，不是被删除的 RPC 生命周期接口。

- `target` 只接受 `tcp://<numeric-ip>:<port>`：无 DNS/TLS/代理/重定向/自动重试；不支持形态返回 `InvalidArgument`，不隐式解析域名。路由表只校验 endpoint 非空，地址语法归 infra（单一真源）。
- 入站限额：`max_frame_bytes`/`max_handler_bytes` 取 `NetworkLimits.max_body_bytes`，`max_inflight`/`max_connections`/`incoming_timeout` 逐字段映射；admission 队列满返回 `Overloaded`，不无界排队。
- `peer_principal` 在 trusted-network 明文模式下恒空；`peer_identity` 是自报事实，不是认证凭据，不用于授权。
- 逐 peer/epoch 观测仍缺（`ClientStats`/`ServerStats` 只是 owner 汇总）；`HttpComponent` 的 HTTP 边界（body/header 上限、仅 `http://` 数值 IP）保持原状。
- 本地显式 `BBT_INFRA_SOURCE_DIR` 候选集成不证明远端 exact-SHA 构建通过；远端完整门禁不由本地测试替代。
