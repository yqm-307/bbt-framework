---
label: wayfinder:research
blocked_by: []
assignee:
claim_session:
status: closed
outcome: resolved
archive:
---
# 首个业务 RPC 最小切片与跨语言互通边界

## Question
在 infra wire profile 已完成、而 framework Issue #4 仍未完成的前提下，首个真实业务 `.proto`/IDL、deadline → remaining budget、接收端 local-min、错误传播和非 C++ consumer 应如何取最小范围，才能证明业务互通而不把 framework 策略下沉到 infra 或预建通用 codegen 平台？

## Done when
- 选定首个业务切片的范围与 `.proto` 唯一真源，明确生成/校验入口和兼容升级样例。
- 明确发送端 deadline 到 remaining budget、接收端 local-min、取消/关闭、`OutcomeUnknown` 与错误传播的责任层和观察断言。
- 明确 C++ server + 一个非 C++ client 的真实跨进程验收矩阵，覆盖未知字段、类型错误、业务错误、超时/预算和 schema 兼容升级。
- 核对现有 framework/infra 公共 API 和 wire profile 的边界，列出不引入 gRPC、不建设通用 codegen 平台、不修改 infra wire contract 的限制。

## T1 evidence (read-only)

固定核对 framework `b1620aa555ce0d91818cbe1e69bbf31c6d51407d`、infra `0a40b702935bd6a640c836d4aadefc9b87d974df`、coroutine `7bcda3b078f975ff2978424be7f6ba38e04fb7f6`：

- infra `include/bbt/infra/rpc/RpcWire.hpp` / `src/rpc/RpcWire.cc` 已有版本化 Protobuf `RpcEnvelope`、body `application/x-protobuf`、schema/error/remaining-budget 字段与 `OutcomeUnknown` 映射；这部分是 wire 输入，不在本票重做。
- framework `framework/include/bbt/framework/CoRpc.hpp` 已有 `ProtoCodec<T>`、`FromProto`、`ParseProto` seam；`ICoService::call()` 与 `MethodTable` 仍负责 framework schema/方法绑定，但固定树没有真实业务 `.proto` 或生成物。
- framework `framework/src/host/RpcHttpBridge.cc` 仍把 request/response schema 及错误适配到 `x-bbt-*` header；Issue #4 的正式业务接线尚未完成，不能用 header bridge 或 infra-only loopback 关闭本票。
- infra `docs/decisions/0002-co-network-contract-v1.md` 已规定 deadline→remaining budget、接收端 local-min、错误信封、断连 `OutcomeUnknown` 与“不自动重试”；framework 仍需把这些 wire 语义接到业务 schema 和非 C++ client 的机器断言。
- Issue #4 的既有候选 `storage.get(key)`/`gateway.fetch(key)` 仍依赖 #8 资源生命周期；Mongo miss fallback 不能作为业务成功证据。Echo 只能作 transport smoke，不能满足真实业务验收。

### T1 conclusion

技术分层已足够选择切片；用户已确认采用最小查询示例：

1. **已确认的最小路径**：沿用现有双 Service 候选，定义 `GetValue(key)`：请求为 `GetValueRequest { string key = 1; }`，响应为 `GetValueResponse { bool found = 1; string value = 2; }`。首轮服务端使用可控 fixture/in-memory 数据，先证明 `.proto` 真源、错误/预算和 C++↔非 C++ 互通；待 #8 解决真实资源 owner 后再接 Redis/Mongo。
2. **业务语义路径**：采用用户举例的“支付成功后再库存”的两步业务流程；能验证顺序、业务错误和未知结果，但需要额外锁定幂等/副作用/补偿边界，范围大于首个最小切片。
3. **仅 Echo/transport smoke**：可快速复用现有 bridge，但不满足 Issue #4 的真实业务验收，明确排除。

第 1 项已由用户确认；支付→库存不纳入本票首个切片，Echo/transport smoke 不作为 #4 完成证据。仍需继续 T2/T3，确定非 C++ client、生成/校验入口和完整观察矩阵；这不授予源码实现或依赖安装权限。

## T2/T3 evidence from the fixed consumer trees

- infra `proto/bbt/infra/rpc/v1/rpc_envelope.proto` is the **wire-envelope** source; it explicitly assigns business payload schema and language choice to framework #4. `RpcWire.hpp` exposes `Make/ParseRpcWireHttpRequest/Response`, `ToWireEnvelope` and `FromWireEnvelope`; HTTP contract is `POST /rpc` + `Content-Type: application/x-protobuf`, and `x-bbt-*` is forbidden as formal metadata.
- framework already has `CoRpcReq::FromProto/ParseProto`, `CoRpcResp::FromProto`, a `ProtoCodec<T>` specialization seam, and `MethodTable` schema slots. It does **not** have a built-in codec specialization for generated business messages; the current test sample uses only raw/positional payloads.
- framework default `RpcHttpBridge` still maps envelope fields/errors to `x-bbt-*` headers and sends raw payload as body. `CoServiceCall.cc` constructs logical `RpcEnvelope`, while infra wire conversion needs an explicit nonzero `remaining_budget_ms`; inbound `IncomingCallContext.deadline` currently reflects listener-local budget only. Thus existing `framework.rpc_xlang` and infra wire tests do not exercise framework business dispatch over the formal profile or remote-budget local-min.
- framework `tests/rpc_xlang_client.py` demonstrates a viable zero-dependency Python approach: standard-library HTTP and a hand-written proto3 envelope codec; its server directly uses infra wire and only echoes envelope payload. This is transport evidence, not `GetValue` business behavior.
- A temporary scratch-only generation probe used the already locked protobuf `3.21.12` `protoc` and successfully emitted C++ and Python message sources for the candidate request/response. The Python generated module imports `google.protobuf`; that runtime is absent from available Python interpreters, so do not make runtime installation a prerequisite. Scratch outputs are not repository evidence or source deliverables.

## Resolution

- **Business schema source**: add one example-only proto source in the framework-owned sample, package `bbt.example.v1`, with `GetValueRequest { string key = 1; }`, `GetValueResponse { bool found = 1; string value = 2; }`, plus a separately named `GetValueRequestV2 { string key = 1; string client_note = 2; }` compatibility-test message. V2 field 2 is sent to a v1 server and ignored as an unknown field; do not claim the compatibility message is a production API.
- **Sample behavior**: an executable C++ `GetValue` service uses deterministic in-memory fixture data (e.g. `demo:user:1 → Alice`); known key returns `{found:true,value:...}`, unknown key returns `{found:false,value:""}` as normal business result, empty key returns a machine-readable `InvalidArgument`. No Redis/Mongo or external side effect. One Python standard-library client sends/receives both envelope and payload wire, with test vectors/field-number checks tied to the `.proto`; no `google.protobuf` package requirement.
- **Generated C++ and codec**: `protoc 3.21.12` from the pinned prefix generates C++ message types only under the build directory. Keep `.proto` as the sole schema source. The example must use the existing `ProtoCodec`/`FromProto`/`ParseProto` public path for the generated messages; add the smallest typed adapter required by #4, rather than duplicate a C++ field table or expose generated headers from framework public headers. A configure/build check fails closed if the locked protoc/runtime is unavailable. Re-run generation to a clean build dir and compare outputs; a compat test checks the V2 unknown request field is ignored by v1 handler.
- **Formal bridge**: #4 implementation must connect framework outbound and inbound bridge to infra `RpcWire` body helpers, not add a second profile and not use `x-bbt-*`. On egress, compute remaining milliseconds from the already parent-clamped absolute deadline immediately before sending; preserve minimum 1 ms and reject expired before I/O. On ingress, derive effective deadline as `min(listener-local deadline, now + envelope.remaining_budget_ms)` and pass that narrowed context to dispatch; expired/invalid budget never reaches the handler. Keep user/business policy in framework, wire encoding in infra.
- **Outcome/error**: successful response maps the `RpcWireEnvelope` success arm to logical response payload and retains existing request-id/schema echo validation. Error response maps the full `RpcError` to `infra::Error`/framework domain unchanged under current sanitization rules; handler validation and remote `OutcomeUnknown` stay machine distinguishable. Pre-I/O expiry is `TimedOut`; after I/O begins, do not infer remote non-execution from timeout/disconnect, and do not auto-retry. Coroutine/runtime `Close()` remains governed by the already confirmed lifecycle ticket; this RPC slice adds no operation cancel token or business compensation.
- **Executable matrix**: C++ framework process + Python stdlib process, dynamically allocated loopback port; assert found/miss/empty-key error, malformed payload/type/schema error, V2/unknown-field compatibility, error details, request-id/schema echo, parent/local-min budget clamp and expiration-before-handler, plus disconnect/ambiguous completion represented as `OutcomeUnknown` where the transport can prove ambiguity. Driver waits for server readiness and verifies orderly synchronous `Close()`/process exit; no mock result is counted as cross-process evidence.
- **Compatibility/check scope**: do not change infra wire schema/profile or add gRPC/general codegen. The example is a framework acceptance fixture, not a production registry/backend. No live Redis/Mongo, Compose, perf baseline, business payment/inventory semantics, cancellation API, retry, or source implementation is performed by this research node.

### User decision and boundary

- **示例性质**：`GetValue` 是 #4 的首个业务 RPC 示例程序/跨语言验收 fixture，不是用户最终生产业务模型，也不宣称 fixture 数据等同于真实后端。用户已确认采用该最小示例；支付→库存及 Echo-only smoke 不纳入本票。
- **协议与工具**：业务 `.proto` 是 schema 唯一真源；复用 infra 正式 envelope profile，Python 标准库手写 wire client；`protoc` 使用锁定的 3.21.12 前缀。生成式 Python runtime 不作为环境前置。
- **实现约束**：框架当前 `x-bbt-*` bridge 与正式 body-envelope profile 不一致，且没有 generated-message 的默认 `ProtoCodec`。#4 后续实现须用跨语言 fixture 证明桥接与 budget/error seam，不得拿 infra echo 或静态 codec 检查冒充业务验收。
- **未完成边界**：当前 Resolution 仅冻结切片和验收方法，不代表 .proto/bridge/业务示例已经实现，不代表 #4 GitHub issue 已关闭，也不代表真实跨语言用例已通过。
## Checkpoint
- 已确认：infra wire/profile 已完成；framework 业务 schema、正式 bridge、预算接线和真实非 C++ 业务互通未完成。
- 已完成：T1 只读核对，以及 T2/T3 的决策研究：确定 `GetValue(key) → {found, value}`，业务 `.proto` 作为唯一 schema 真源，锁定 Protobuf 3.21.12 `protoc`、Python 标准库手写 wire client、未知字段兼容样例和预算/错误/关闭矩阵。
- 已裁定：`GetValue` 是 #4 的首个业务 RPC 示例程序/跨语言验收 fixture，不是生产业务；支付→库存与 Echo-only smoke 不纳入本票。
- 证据边界：临时 `.proto` 生成仅在 scratch 完成；未改三仓源码/构建配置，未安装依赖，未写 GitHub，未运行跨进程业务测试。现有 infra xlang/框架 header bridge 不能替代正式 framework business bridge 证据。
- 已知环境缺口：锁定前缀有 `protoc 3.21.12`；全局无 `protoc`，可用 Python 解释器无 `google.protobuf` runtime，因此后续 client 继续采用标准库 wire codec，不把 Python runtime 安装列为前置。
- 认领/在途：T1/T2/T3 只读决策已完成；本票无子代理、构建、探针或外部写入在途，释放本次认领。
- 下一步：由后续执行票在获得实现授权后落 `.proto`/C++ fixture/正式 body-envelope bridge，并按本票矩阵运行；地图继续推进 `resource-registry-contract` 与 `service-example-boundary`，未因本票解决而关闭 #4/#8/#5/#9。
