# GetValue 示例：首个业务 RPC 切片（Issue #4 / P0-A）

这是 bbt-framework 第一个**真实业务**跨语言 RPC 切片，也是 #5 消费的业务
schema / bridge 接口。它由三部分构成：

1. 业务 `.proto`（唯一 schema 真源）；
2. C++ framework 服务端：公开 `CoApp`/`CoService` + **正式 infra body wire
   bridge**（不走 `x-bbt-*` header 旁路、不用测试缝/Binder 伪造结果）；
3. Python **标准库**客户端：手写 proto3 wire 编解码，零依赖（无
   `google.protobuf` runtime）。

> `auth_state=unauthenticated_loopback`：本路径只证明业务/传输行为，**不证明
> 身份来源可信或授权完成**。认证 adapter、身份绑定、接收端授权与伪造 metadata
> 否决证据由 #38 承载；本切片任何成功不得写成身份/授权完成。

## 1. schema（唯一真源）

`proto/bbt/example/v1/get_value.proto`（package `bbt.example.v1`）：

| message | 字段 | 号 | 类型 | 语义 |
|---|---|---|---|---|
| `GetValueRequest` | `key` | 1 | string | 业务键；空 → `InvalidArgument` |
| `GetValueResponse` | `found` | 1 | bool | 命中 |
| `GetValueResponse` | `value` | 2 | string | 命中值；未命中为空 |

- **无双写漂移**：C++ 方法表从生成物 descriptor `full_name()` 推导 schema
  字符串（`bbt::framework::ProtoMethod` + `internal::MethodTable`），业务不手写
  schema。生成头只落构建目录。
- 兼容升级样例（V2，不建第二套真源）：V2 客户端在请求追加可选
  `client_note = 2`，V1 服务端按 proto3 忽略未知字段照常处理。

## 2. wire（envelope 归 infra）

传输信封是 bbtools-infra Issue #8 冻结的版本化 profile（**不是**本示例的
`.proto`）：HTTP `POST /rpc`、`Content-Type: application/x-protobuf`、body 为
`RpcEnvelopeMsg`（`service`/`method`/`request_id`/`request_schema`/
`response_schema`/`remaining_budget_ms`/`payload`/`metadata`/`outcome`）。

- 服务名：`bbt.example.v1.GetValueService`；方法名：`GetValue`。
- `request_schema` / `response_schema` = 上表 message 的 `full_name`。
- 接收端 local-min 预算：`min(入站传输 deadline, now + remaining_budget_ms)`；
  `remaining_budget_ms == 0` 由 wire 解码期拒绝（不进 handler）。
- 身份：`peer_principal` 只来自 infra 入站上下文，不被 envelope `metadata` 升级。

## 3. C++ 侧最小 typed codec / 装配

业务侧（[`getvalue_service.hpp`](getvalue_service.hpp)）：

```cpp
// payload codec：只经公开 SerializeToString/ParseFromArray
namespace bbt::framework::rpc_detail {
template <> struct ProtoCodec<bbt::example::v1::GetValueRequest> {
  static constexpr bool kDefined = true;
  static result<std::vector<std::uint8_t>> Encode(const T&);
  static result<T>                         Decode(const std::vector<std::uint8_t>&);
};
}

class GetValueService final : public fw::CoService<GetValueService> {
  static constexpr std::string_view kServiceName = "bbt.example.v1.GetValueService";
  fw::CoRpcResp GetValue(fw::CoRpcReq req) {        // req.ParseProto<GetValueRequest>()
    ...                                             // fw::CoRpcResp::FromProto(resp)
  }
  static constexpr auto kRpcMethods = fw::RpcMethods(
      fw::ProtoMethod<&GetValueService::GetValue,
                      bbt::example::v1::GetValueRequest,
                      bbt::example::v1::GetValueResponse>("GetValue"));
};
```

服务端装配（[`getvalue_server.cc`](getvalue_server.cc)）：公开 `CoAppOptions`
选择正式桥——

```cpp
fw::CoAppOptions options;
options.listen = {"127.0.0.1", 0};               // 动态端口，仅 loopback
options.inbound_bridge = fw::RpcInboundBridge::ProtoWireV1;
fw::CoApp app(options);
app.add_service<getvalue::GetValueService>({ExecutionPolicy::Concurrent, 64, 0,0,false,0,0,0});
app.run();
```

框架侧（本仓改动）：
- `framework/.../CoApp.hpp`：新增 `RpcInboundBridge{LegacyHeaders,ProtoWireV1}`
  与 `CoAppOptions::inbound_bridge`（默认 `LegacyHeaders`，保留既有行为）；
- `framework/.../internal/RpcWireBridge.{hpp,cc}`：正式 body bridge（入站
  `ParseRpcWireHttpRequest → local-min → dispatch_inbound → MakeRpcWireHttpResponse`）；
- `framework/.../RpcMethods.hpp` + `internal/MethodTable.hpp`：`ProtoMethod`
  声明糖，schema 取自 descriptor；
- `framework/src/CMakeLists.txt`：仅当 `bbt_infra_rpc` 可用时编译本桥（未配置
  protobuf 前缀时框架只有迁移期 header 桥，显式选择 ProtoWireV1 会 fail-closed）。

## 4. 构建 / 运行 / 验收入口

需锁定 protobuf 3.21.12 前缀（`scripts/prepare_protobuf.sh`）。deps 与 protobuf
前缀可由 `scripts/fetch_deps.sh` 与 `prepare_protobuf.sh` 准备：

```bash
cmake -S . -B build-deps/project-build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DNEED_TEST=ON -DBBT_INFRA_SOURCE_DIR=<infra-162bb5fd> \
  -DBBT_COROUTINE_SOURCE_DIR=<coroutine-7bcda3b> \
  -DBBT_PROTOBUF_PREFIX=<protobuf-3.21.12> -DCMAKE_POLICY_VERSION_MINIMUM=3.5
cmake --build build-deps/project-build --target getvalue_server getvalue_caller \
  Test_framework_getvalue_codec --parallel 2
ctest --test-dir build-deps/project-build -j1 --output-on-failure \
  -R "examples.getvalue.xlang|examples.getvalue.s2s|framework.getvalue.codec"
```

- `examples.getvalue.xlang`：C++ server + Python 标准库 client 真实跨进程
  （driver 动态端口/readiness/有限等待/finally 清理，并做 protoc 重复生成无漂移
  与 journal 断言）。
- `examples.getvalue.s2s`：`getvalue_server`（callee）+ `getvalue_caller`（caller）
  两个独立 CoApp 进程 + 真实黑障端点的正式出站验收，并含 #5 EX-T3 的四类非法
  静态路由启动拒绝负例（见 §5.1）。
- `framework.getvalue.codec`：descriptor/字段号/类型、方法表 schema 推导、
  golden vectors、handler 分支（Boost.Test 内嵌）。

手跑：`getvalue_server server <port> <meta> <journal>`（读 stdin EOF 后优雅关闭）；
`getvalue_client.py --port <port>`。

## 5. 场景与机器断言（全部 PASS，带 auth_state 标记）

known / miss / empty-key(InvalidArgument) / unknown-method(NotFound) /
schema-mismatch(TypeMismatch) / malformed-payload(ProtocolError) /
malformed-envelope(ProtocolError) / wrong-content-type(InvalidArgument) /
unknown-field-envelope(前向兼容) / v2-compat(未知字段) / budget-zero(0 预算拒绝且未进
handler) / **budget-expired（budget=1ms 可解码但 handler 可见期限已过 → TimedOut
且未进 handler，非用 0 预算冒替）** / **budget-clamped（声明 2400000ms 超本地
incoming_timeout 30s → 响应剩余预算被 clamp ≤ 30000，不原值回显）** /
disconnect-unknown / unknown-error-code / schema-drift-guard / golden vectors。

journal 断言：`budget0/budget1/schema/route/wire` 被拒请求**未进 handler**；
`budgetlarge` 等真正处理的请求 `handler_entered` 存在（正反对照）；
所有记录 `auth_state=unauthenticated_loopback`；`peer_principal` 未被 metadata 升级。

## 5.1 跨进程 Service→Service 与 #5 EX-T3 静态路由负例（复用本路径）

`examples/getvalue` 的跨进程路径同时承载 #4 的正式出站**与 #5 EX-T3 的「双进程
静态路由」**——EX-T3 不另起一套 caller/server/driver（#5 禁止复制 #4 已有实现）：

- **两个独立 CoApp 进程**：`getvalue_server`（callee，宿主 `GetValueService`）与
  `getvalue_caller`（caller，宿主 `GetValueCallerService`，handler 内经公开
  `service.call<Req,Resp>` 出站），各自 `CoApp`、各自 loopback **动态端口**
  （`listen={"127.0.0.1",0}` + 轮询 `bound_endpoint()` 写 port 文件）。
- **显式 `service_name→endpoint` 白名单静态路由**：caller 只登记 callee 与
  blackhole 两条；出站必经 `find_route` 门，未配置目标一律 `NotFound`（无自动发现、
  无同进程直连后门）。
- **真实验收驱动器** `getvalue_s2s_run.py`（ctest `examples.getvalue.s2s`，TIMEOUT 120）：
  两进程 + 真实黑障端点；结构化结果全部带 `auth_state=unauthenticated_loopback`。

### 非法静态路由：公开入口，run 启动任何组件前拒绝

`getvalue_caller` 增加一个公开验收子命令，构造四类**非法**静态路由后经公开
`CoApp`/`add_service`/`add_resource`/`run` 装配（不使用任何测试缝）：

```
getvalue_caller badroute <kind> <result_file>
  kind ∈ {empty-service, empty-transport, empty-endpoint, duplicate}
```

| kind | 非法输入 | `_ValidateConfig` 拒绝原因（`lifecycle_failures` 恰 1 条） |
|---|---|---|
| `empty-service` | 空 `service_name` | `CoApp: static route with empty service_name` |
| `empty-transport` | 空 `transport`（`endpoint` 非空） | `CoApp: static route '<name>' has empty transport/endpoint` |
| `empty-endpoint` | 空 `endpoint`（`transport` 非空） | 同上 |
| `duplicate` | 两条相同 `service_name` | `CoApp: duplicate static route service_name` |

机器可读观测（进程自写 `<result_file>` JSON；断言由驱动器独立核对，不信任自报
`status`）：`rc==1`（`HostLifecycle::kExitRejected`，启动任何组件前返回）、
`lifecycle_failures` 恰 1 条且含对应原因、`bound_endpoint==""`（未绑定任何 ready
端口）、`resource_factory_calls==0`（经公开 `add_resource` 注册的延迟工厂未被调用，
证明拒绝发生在资源/网络/服务启动之前）。四项均带
`auth_state=unauthenticated_loopback`。`getvalue_s2s_run.py` 以四个**有界**子进程
运行上述四例，并入 `RESULTS`/`_FAILURES` 与清理后的总判定（超时/异常/崩溃一律记
fail 并回收自有进程）。

**语义边界（勿混淆）**：

- **空路由列表合法**：`static_routes` 为空表示「不允许任何出站目标」，配置本身合法；
  此时**未配置目标**的出站调用在运行期由 `find_route` 门拒绝为 `NotFound`
  （`s2s-noroute` 场景）——这是运行期路由未命中，**不是**启动期配置拒绝。
- **空字段 / 重复 `service_name` 非法**：`service_name`/`transport`/`endpoint` 任一
  为空，或两条路由同名，都在 `run()` 启动任何组件前被拒绝（本子命令四例）。
- **端点格式检查未纳入该断言**：当前 `_ValidateConfig` 只判空，不做 endpoint 结构
  （如缺端口）或 transport 白名单语义校验；本切片**不新增**这些语义，也不声称它们
  被拒绝。
- 未覆盖：EX-T4 容量/并发/迟到 lifecycle 矩阵、EX-T5 整体验收、认证/授权（#38）。
  本切片不证明身份/授权完成。

## 6. 已知限制 / 未覆盖（不伪报）

- **协程栈**：入站 handler 在传输层协程内运行，默认栈 12KB 不足以承载
  protobuf 业务 codec（实测 SIGSEGV）。示例 server 经 coroutine **公开配置入口**
  `g_bbt_coroutine_config`（GlobalConfig 访问宏；头
  `bbt/coroutine/detail/GlobalConfig.hpp`，见 coroutine agent-docs
  api-reference.md §12）在 `app.run()` 前把栈调到 512KB——该配置非线程安全、
  process-global。**512KB 只范围化覆盖本示例负载**；框架默认 12KB 栈对任意
  protobuf 业务（更大/更深嵌套 payload）不保证安全，属框架层待办。本切片未新增
  CoApp 栈配置层、未改默认 runtime。
- **出站正式化**：`inbound_bridge=ProtoWireV1` 现在让**入站与出站同
  profile**：`HttpEgress` 增加 `ProtoWireV1` 分支（同一 HttpClient 发送 root，
  只换 wire 编解码；`ToWireEnvelope`/`FromWireEnvelope` helper），egress 发起
  I/O 前再次按本地 deadline 换算 remaining_budget_ms（过期 → TimedOut 无 I/O），
  回复校验 request_id/response_schema 并保留 Error（含 `OutcomeUnknown`）。当前
  候选固定 infra merge `162bb5fd…`，Release 构建下已验证 7 个耦合场景，
  caller/callee 均 rc=0；本轮未在 ASan/UBSan 构建下重跑。结果均带
  `auth_state=unauthenticated_loopback`。
- **R2/R4 部分**：已覆盖接收端 local-min、0 预算与「可解码但过期」不进 handler、
  预算回显 clamp；driver 已覆盖发送前过期、非法路由和断连，但**跨 hop 预算不放大、
  排队耗预算的发送端观察**未在专门矩阵中重验。
- **R5**：客户端断连→unknown 为真实观测；「已提交后丢 reply → 远端
  OutcomeUnknown」已在本切片产生，证据 = `framework.egress.phase` 的 T3
  （完整写出后丢 reply → OutcomeUnknown、phase=RequestCommitted、对端只连接
  一次）+ `examples.getvalue.s2s` 的 `s2s-blackhole-unknown`（code=14、
  blackhole_accepts=1）。服务端 in-memory 无副作用。
- **R6/R7/R8 并发/容量/关闭矩阵**：本轮只验证双进程正常退出和固定 infra 修复后的
  基础关闭路径；既有 `Test_framework_f1b2` 覆盖并发不串扰，非本切片新路径。容量耗尽
  Overloaded、超时后 inflight 强持有、ShutdownIncomplete、独立 supervisor 硬停机
  journal 未新增。
- **V06–V08 排除**（OrderedIngress/ActorSerial/写幂等）；**V10 留 #38**。
- 不宣称 any 身份/授权、生产性能或真实后端（无 Redis/Mongo/Compose、无重试/取消）。

## 7. 给 #5 的固定接口

| 项 | 值 |
|---|---|
| 业务 schema | `examples/getvalue/proto/bbt/example/v1/get_value.proto` |
| request/response full name | `bbt.example.v1.GetValueRequest` / `bbt.example.v1.GetValueResponse` |
| service / method | `bbt.example.v1.GetValueService` / `GetValue` |
| C++ typed codec seam | `bbt::framework::rpc_detail::ProtoCodec<T>` 特化 |
| C++ 声明 | `fw::ProtoMethod<&Svc::GetValue, Req, Resp>("GetValue")` |
| 业务 API | `CoRpcReq::ParseProto<T>()` / `CoRpcResp::FromProto(msg)` |
| 正式入站桥 | `CoAppOptions::inbound_bridge = RpcInboundBridge::ProtoWireV1` |
| 正式出站 profile | 选 `ProtoWireV1` 时 `HttpEgress` 自动走正式 body 信封（与入站同 profile） |
| typed 出站调用 | `service.call<Req, Resp>(service_name, method, req, ...)`（schema 由 descriptor 推导） |
| wire | infra `POST /rpc` + `application/x-protobuf` + `RpcEnvelopeMsg` |
| 运行/验收 | ctest `examples.getvalue.xlang`、`framework.getvalue.codec` |
