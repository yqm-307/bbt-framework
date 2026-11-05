# HTTP 宿主与协议迁移

## 消费入口

`framework/src/CMakeLists.txt` 直接链接 `bbt::infra_http_net`。所有构建必须
传 `BBT_INFRA_HTTP_NET=ON`；`scripts/build_stack.sh` 自动补齐该开关。
`BBT_INFRA_RPC_NET=ON` 与固定 `BBT_PROTOBUF_PREFIX` 仍是正式 RPC wire
的必要条件；缺失时选择 ProtoWireV1 必须失败，不降级为 header 协议。

`InfraHttpHost` 是框架内部网络宿主，不是旧网络 API 的替代包装层。
LegacyHeaders 的 `x-bbt-*` 请求/错误 header 编解码保留，入站直接消费
`http::IncomingRequest`，回复直接使用 `http::Response`。出站桥先经
`http::Client::BuildRequest` 构造 `PreparedRequest`，再 `bbt_co_await Send`。
ProtoWireV1 继续使用公开 `rpc::Client` / `HttpRpcTransport`，并与 header
协议共用宿主的一个 HTTP client pool。

## 执行与关闭

- 入站 Engine 在 Idle 时装配唯一 server 接纳消费者，Run 在自有 native
  线程执行；Running 且绑定地址可读后才调用 Server::Start。
- 出站单独使用一个 client-only Engine，不复用入站接纳 Engine。每个入站
  宿主占 2 个 server driver 槽 + 1 个 client driver 槽，另留 1 个业务 worker，
  即至少 4 个固定 scheduler worker；纯出站至少 2 个。独立测试客户端与
  多宿主按实际同时存活的 driver 数相加，不按机器核数扩并发。
- StopAccepting 先在 HandlerState 的同一锁内关闭业务门，迟到包装调用
  不再访问业务；已过门的 handler 仍由 scope guard 记账，异常也归还计数。
- `WaitHandlersDone(deadline)` 先等业务 handler 归零，再调用 Server 的
  `Shutdown(deadline)` 等已授权回复结束，随后 `Close / WaitStopped` 核对物理收口。
  超时保持拒新；正常宿主路径按原有策略继续等待迟到收尾——infra 新契约下
  仍 Draining 的重复 `Shutdown` 返回 Overloaded，续等经 `Drain` 承担（同一
  排空谓词），不再用第二次 `Shutdown` 等待。
- `Close(reason)` 立即封口，向关联等待发布首次关闭原因，关闭 server/client 并请求
  Engine 退出，不以业务 handler 返回作为成功前提。正在等待排空的宿主会收到关闭
  原因，不能把中断当作已排空，也不能据此销毁仍被在途任务访问的业务对象。
- Service/Component 实例与方法表放进 `AppRuntimeState`（shared_ptr 运行时
  状态）：入站 handler 闭包与出站回调复制该指针，不借用 CoApp 成员、不捕获
  CoApp this，CoApp 先析构也不影响在途请求。关闭序列经 `Release` 收回状态
  对业务对象的引用；在途分发靠本地强引用与自持分发表项收尾。
- 网络线程已结束时由 Close 回收；尚未结束的线程留给宿主析构 join，因此当前析构
  仍可能等待迟到 handler。Close 返回不证明 callback 销毁、连接 Released 或线程已
  全部回收；需要物理收口凭据的调用方须显式等待。立即关闭不能撤销远端副作用。

实现真源：`framework/include/bbt/framework/internal/InfraHttpHost.hpp`、
`framework/src/host/InfraHttpHost.cc` 与 `RpcHttpBridge.cc`。

## 能力与待补齐边界

- HTTP client 仅 `http://` 数值 IP；无 DNS/TLS、代理、重定向或自动重试。
  不支持的地址返回 InvalidArgument，不隐式解析域名或降级 TLS。
- 宿主唯一池的 active socket 上限为 `min(max_connections,max_inflight)`；
  没有额外排队。keepalive 也不能超过该连接预算。响应 body 的 parser 上限
  取 `max_body_bytes`，入站 ServerOptions 使用原 NetworkLimits。
- LegacyHeaders 在框架边界校验请求 body、请求头与响应头字节数。
- 宿主创建唯一 HTTP client 时把 `NetworkLimits.max_body_bytes` 同时映射到
  响应 body 与请求 body 上限，把 `max_header_bytes` 映射到响应 parser 的
  header 上限。ProtoWireV1 的 `rpc::Client` 在编码完整 envelope 后交给同一
  client，因此 payload 未超限但 wire body 超限仍会在拨号前拒绝；LegacyHeaders
  保留框架边界的 header/body 返回值校验作为协议校验。

本地显式 `BBT_INFRA_SOURCE_DIR` 候选集成不证明远端 exact-SHA 构建通过。
聚焦测试保留真实 loopback、错误/请求阶段与关闭断言；远端完整门禁不由
本地测试替代。
