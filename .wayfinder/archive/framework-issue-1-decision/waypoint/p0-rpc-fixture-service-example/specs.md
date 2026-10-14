---
schema: wfer-execution/v1
plan_id: p0-rpc-fixture-service-example
revision: 2
artifact: spec
---
# P0 业务 RPC fixture 与双 Service 示例规格

## 要求与场景

### 要求

- **R1 业务 schema 真源**：提供一个示例专用 `.proto`，定义 `bbt.example.v1.GetValueRequest { string key = 1; }`、`GetValueResponse { bool found = 1; string value = 2; }`，以及只用于兼容性测试的 V2 请求字段；`.proto` 是唯一 schema 真源，生成物不反向成为手写字段表。
- **R2 生成与 codec 边界**：使用已锁定的 `protoc 3.21.12` 生成 C++ 消息到构建目录；通过现有 `ProtoCodec`/`FromProto`/`ParseProto` 公共路径接入，不要求 Python generated runtime，不向 framework 公共头泄漏生成头。
- **R3 正式 wire bridge**：业务请求和响应必须经 infra 版本化 `RpcEnvelope`、`application/x-protobuf` body profile 传输；不得以 `x-bbt-*` header 或 infra-only echo 作为正式业务协议。
- **R4 只读业务语义**：确定性 in-memory fixture 中已知 key 返回 `{found:true,value}`；未知 key 返回 `{found:false,value:""}`；空 key 返回机器可解析 `InvalidArgument`。没有 Redis/Mongo、写入、事务、补偿或 exactly-once effect。
- **R5 预算传播**：发送端从已收紧的绝对 deadline 计算剩余预算，发送前过期不得做 I/O；接收端使用 listener-local deadline 与 envelope remaining budget 的最小值。每跳不得重置总等待，不宣称远端 I/O 在原始截止时刻被强制取消。
- **R6 错误与未知结果**：业务校验错误、malformed/type/schema 错误、远端业务错误、transport failure 和 `OutcomeUnknown` 必须机器可分类；超时/断连后不能推断远端一定未执行或已执行；默认不重试。
- **R7 跨语言验收**：C++ framework server 与 Python 标准库 client 必须通过动态 loopback 端口完成真实 HTTP/Protobuf 请求；client 不依赖 `google.protobuf` runtime。
- **R8 双 Service 单进程 fixture**：一个公开 `CoApp` 注册两个 `CoService<T>`；一个 Service 通过公开调用路径调用另一个；验证正常返回、业务错误、有限 deadline 和 `request_shutdown()` 后排空/同步关闭。
- **R9 双进程静态路由 fixture**：caller/server 是两个独立 `CoApp` 进程；caller 只使用显式 `service_name → endpoint` 静态路由；server 只注册自己的 Service；空、重复、不完整路由在 `run()` 前拒绝，未配置目标返回 `NotFound`。
- **R10 请求上下文与资源寿命**：请求挂起、换 worker、调用方先超时或关闭时，request id、身份字段、预算、inflight/资源 owner 不能串用或提前释放；同步 `ICoCloseable::Close()` 前保持必要强持有。
- **R11 关闭与硬终止证据**：正常关闭按 `StopAccepting → drain handlers → synchronous Close → release` 验证；外部 supervisor 硬终止只记录未完成/未知，不宣称优雅完成，不恢复 runtime Stop 或单操作取消。
- **R12 安全边界**：P0-A 只监听 loopback，动态端口，不提供可信身份；结果标注 `auth_state=unauthenticated_loopback`。认证来源、独立授权 oracle 和伪造 metadata 否决归 #38/P0-B，不是 P0-A 的开工或完成前置。

### 场景

- **S1 已知 key 成功**
  - 前置：server 已 ready，fixture 含确定性 key，合法有限 deadline。
  - 动作：Python client 发起正式 `GetValue` 请求。
  - 可观察结果：结构化成功响应、`found=true`、value 匹配；request id/schema echo 正确；没有正式 metadata header 旁路。

- **S2 未知 key 正常 miss**
  - 前置：server ready，key 不在 fixture。
  - 动作：发起 `GetValue`。
  - 可观察结果：成功业务响应 `found=false,value=""`，不是异常、未分类错误或日志-only 结果。

- **S3 空 key 业务错误**
  - 前置：server ready。
  - 动作：发送空 key。
  - 可观察结果：可解析 `InvalidArgument`，错误 domain/code/details 稳定且不被转换为成功。

- **S4 malformed/type/schema 错误**
  - 前置：server ready。
  - 动作：发送截断、错误 wire type、错误 body schema 或未知/伪造错误码样本。
  - 可观察结果：错误被稳定分类；无未控异常；未知码不变成功；不依赖日志判断。

- **S5 未知字段兼容**
  - 前置：V2 请求只增加兼容测试字段，v1 server 已 ready。
  - 动作：发送 V2 字段给 v1 handler。
  - 可观察结果：未知字段被安全忽略，原有 key 语义保持；不把兼容 fixture 宣称为生产 API。

- **S6 预算收紧与发送前过期**
  - 前置：父 deadline、子调用 deadline、可控延迟均可记录。
  - 动作：分别发送仍有预算和发送前已过期请求。
  - 可观察结果：子预算不晚于父预算；过期请求返回 `TimedOut` 且发送前不做 I/O；跨 hop 不重置总等待。

- **S7 完成/超时竞争与 OutcomeUnknown**
  - 前置：可控地让 reply 丢失或强制断连，并让 handler 可能仍在运行。
  - 动作：在 I/O 已开始后触发 deadline/断连。
  - 可观察结果：至多一个调用终态；回复未知时交付 `OutcomeUnknown` 或等价未知事实；不自动重试，不声称远端执行事实。

- **S8 请求上下文与并发隔离**
  - 前置：至少两个并发请求，其中一个挂起并换 worker。
  - 动作：交错完成请求、超时和回调。
  - 可观察结果：request id、未认证身份状态、预算和结构化 operation 记录不串请求；回调解绑、operation 与资源释放各一次。

- **S9 双 Service 单进程**
  - 前置：干净 clone 可构建，公开 `CoApp` 注册两个 Service。
  - 动作：Service A 经公开调用路径调用 Service B，覆盖成功、业务错误、有限 deadline，再请求关闭。
  - 可观察结果：没有 internal seam、直接注入发送函数或额外 io thread；handler 排空后同步 owner Close，进程正常退出。

- **S10 双进程静态路由**
  - 前置：独立 caller/server、动态端口、显式静态路由。
  - 动作：caller 发起成功/业务错误/超时请求；再测试空、重复、不完整路由和未配置目标。
  - 可观察结果：只有白名单目标可达；配置错误在 `run()` 前拒绝；未配置目标为 `NotFound`；不自动发现或连接任意服务。

- **S11 有限资源与超时后寿命**
  - 前置：可观测 inflight、handler、owner 计数；有挂起 handler。
  - 动作：调用方先超时，随后 handler completion，再执行同步 Close。
  - 可观察结果：存活工作仍计入容量；owner 未在迟到回调前释放；无 UAF、重复释放或把超时当作完成。

- **S12 正常关闭与 supervisor 硬终止**
  - 前置：独立进程有在途请求和可读结构化 journal。
  - 动作：先验证正常 `request_shutdown()`；另一次由外部 supervisor 在硬截止时终止。
  - 可观察结果：正常关闭完成排空和同步 Close；硬终止记录未完成/未知和非优雅终止，不宣称所有请求完成。V14 不替代 V13。

### P0-A/P0-B 安全边界

- **P0-A 运行环境**：本机和 CI 使用未认证 loopback、动态端口和自有进程。所有结构化结果必须标注 `auth_state=unauthenticated_loopback`；P0-A 可以证明业务 schema、formal body、budget/error、双 Service、静态路由和关闭行为，但不能证明 `peer_principal`、`fw.*` 或授权策略可信。
- **P0-B 独立出口**：真实认证 adapter、身份来源绑定、接收端独立授权 oracle、伪造 metadata 否决证据和相应独立进程测试属于 P0-B。P0-B 未完成不阻止 P0-A，但 P0-A 结果不得升级为身份/授权通过。
- **CI 工具链前置**：P0-A 本机可由 `scripts/prepare_protobuf.sh` 生成隔离 `protoc 3.21.12` 前缀；CI 必须消费固定 runner 镜像并核对 image digest、工具版本、protobuf provenance 和依赖 manifest。缺失时 P0-A fail-closed，不以 `skipped` 或系统 `/usr/local` 版本替代。

### 已确认的范围排除

- V06：无幂等写、operation-key、重放/去重或外部副作用，不验写入提交效果。
- V07：不启用 `OrderedIngress`，不验 sequence、终态缓存淘汰或 `ResultExpired`。
- V08：不启用 `ActorSerial` 或 Actor A↔B 互调，不以普通 Concurrent 通过替代非重入等待环证据。
- Redis/Mongo 资源组合、Compose 健康检查、故障注入和短压测由 #8/#9 独立方案承载。

## 约束与不变项

- 固定消费 SHA：framework `b1620aa555ce0d91818cbe1e69bbf31c6d51407d`、infra `0a40b702935bd6a640c836d4aadefc9b87d974df`、coroutine `7bcda3b078f975ff2978424be7f6ba38e04fb7f6`；实际实施前必须重新核对候选版本，不把旧工作树状态当作输入。
- 只使用公开 framework/infra 消费面；不使用 `internal/`、Binder、测试 seam、私有构造伪造可信身份或直接注入发送函数。
- 不修改 infra wire contract；业务 schema 归 framework 示例，协议 envelope/transport/错误基础归 infra。
- 使用动态 loopback 端口和自有进程；不依赖固定共享端口、本机已有服务、`/usr/local` 未核实安装或全局 Docker prune。
- `GetValue` 为只读 in-memory fixture；不引入真实后端副作用、事务、补偿、幂等写、生产业务语义或长期性能承诺。
- 默认不重试；V05 的 `OutcomeUnknown` 只表达调用者不知道远端事实，不提供查询、补偿、去重或回滚。
- 使用当前同步 `ICoCloseable::Close()` 和 process-lifetime runtime；不恢复 `Scheduler::Stop()`、`RequestClose`、`WaitClosed`、旧 token 或 framework 单操作取消。
- M/I/D 证据分层不可互换：模型不代替真实 coroutine/infra；独立进程/网络/supervisor 证据不以日志、退出码、mock 或 CTest skipped 替代。
- V10 是 #38/P0-B 的安全验收门禁；P0-A 不等待认证 adapter，只能交付隔离 loopback 业务证据，不得报告 V10 或完整可信身份验收通过。
- 资源、队列、等待和 Close 的实际上限与失败行为须由机器结果判定；不把单次成功、p99 或进程 Running 写成生产容量/可用性承诺。
