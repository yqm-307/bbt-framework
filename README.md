# bbt-framework
通用后端框架

## 现状

业务公共面：handler 签名统一 `CoRpcResp(CoRpcReq)`，请求/回复经位置参数 codec
（`CoRpcReq::From` / `req.Parse<Ts...>` / `CoRpcResp::From`）；`kRpcMethods` +
`this->call` 声明/发起 RPC，`v2::CoService<Components...>` 继承服务
（`ConstructKey` 受控构造、protected 析构、`kServiceName` 供给名字），
`v2::CoApp::Create` 是唯一装配根。跨语言/结构化负载走 Protobuf 适配
（`ProtoCodec` seam）。

```cpp
#include <bbt/coroutine/coroutine.hpp>
#include <bbt/framework/V2.hpp>

class EchoSvc final : public fw::v2::CoService<> {
public:
    static constexpr std::string_view kServiceName = "echo";
    explicit EchoSvc(fw::detail::ConstructKey k) : fw::v2::CoService<>(k) {}
    fw::CoRpcResp Ping(fw::CoRpcReq req);   // req.Parse<std::string>() 取第 0 个位置参数
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&EchoSvc::Ping>("ping"));
protected:
    ~EchoSvc() noexcept override = default;
    friend class fw::detail::Registry;
};

int bbtco_main() {
    auto app = fw::v2::CoApp::Create(app_opts);            // 唯一创建入口（受管协程内）
    (void)bbt_co_await app->CoInstallService<EchoSvc>(service_opts);
    if (auto started = bbt_co_await app->CoStart(); !started) return 1;
    (void)bbt_co_await app->CoWaitShutdownRequest();       // 任意线程 RequestShutdown() 唤醒
    return bbt_co_await app->CoStop() ? 0 : 3;
}

BBTCO_MAIN();   // runtime 生成 native main：Start → 提交主协程 → 退出码
```

入口归 coroutine runtime：应用只写 `int bbtco_main()`（可用 `BBTCO_MAIN_WITH_SETUP()`
在 Start 前冻结协程配置），framework 不启动调度器、不建 native 线程、不等待
future/join。关闭由 `RequestShutdown()` 唤醒主协程后在同一协程内 `CoStop()` 收口。

`Framework.hpp` 汇总服务/RPC 声明头；`V2.hpp` 聚合 v2 唯一类型面
（`v2::CoApp`/`v2::CoService`/`v2::CoComponent`/`v2::CoActor`），`App.hpp` 是宿主头
（`CoInstallService` 会展开方法表）。编解码、envelope、分发器、发送注入在框架内部；
测试缝在 `internal/`。旧公共面 `CoApp.hpp`/`ICoService.hpp`/`CoService.hpp` 与
`CoApp::add_service`/`add_resource`/`run()` 已删除。

能力对象只有一条受管路径：`v2::RedisComponent`（借用共享 `net::Engine`）与
`v2::MongoDBComponent`（自有 worker）经 `v2::CoApp::CoInstallComponent` 安装、
`CoBindComponent` 绑定、`ComponentHandle::Borrow` 借用；`OnStop` 把
`CoStop(CloseOptions)` 的同一个值转发给对应 infra 对象。产品
`RedisNetResource`/`MongoDbResource` 与并列资源缝（`CoApp::add_resource`/
`ResourceSpec`/framework `ICoResource`/`ServiceContext::resource`）已删除，
`ServiceContext` 只保留请求上下文；第二条能力路径不再存在。

共享 Engine：默认装配路径由 `v2::CoApp` 以 `AppOptions::SetNetworkOptions`
以 Idle 装配创建（`CoApp::engine()`），宿主与组件借用同一实例；`CoStart`
在宿主 Create 之后把 `{engine, config}` 与主任务唤醒口发布给入口宿主闸门
（`HostGate`），应用入口的 `bbtco_main_host_hook` 调用
`bbt::framework::HostGate::RunHostGate` 在主线程执行唯一一次 `Engine::Run`；
主任务结束 → `RequestClose`，`Run` 提前失败 → 唤醒主任务。

公共目标 `bbt::framework`（静态库）。

## 构建与测试

依赖：`bbtools-infra`（其自身再依赖 `bbtools-coroutine`）。本地默认使用显式路径内的当前源码，允许未提交候选；指定版本时按指定版本构建，coroutine 可传 `-DBBT_COROUTINE_PINNED_SHA=<完整 commit>`。设计需指定版本，完整规则见 infra `docs/guides/development.md`「依赖版本选择」。

```bash
INFRA=/path/to/bbtools-infra            # 含顶层 CMakeLists.txt 与 include/
CO=/path/to/bbtools-coroutine           # 含顶层 CMakeLists.txt 与 bbt/

cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug -DNEED_TEST=ON \
  -DBBT_INFRA_SOURCE_DIR=$INFRA -DBBT_COROUTINE_SOURCE_DIR=$CO \
  -DBBT_INFRA_HTTP_NET=ON -DBBT_INFRA_RPC_NET=ON \
  -DBBT_PROTOBUF_PREFIX=<protobuf-3.21.12 前缀>

# 构建按目标进行，固定 `--parallel 2`；同机多 agent 时构建/测试经宿主已有
# 共享验证队列串行执行（一次只跑一个重任务），不按逻辑核数放大并发。
# 下列目标覆盖下面三条聚焦 ctest 选集所需的全部可执行文件/fixture
# （tests/ 与 examples/ 各自 add_executable，ctest 不负责构建）。
cmake --build build --parallel 2 --target \
  bbt_framework \
  Test_framework_v2_core Test_framework_rpc_surface Test_framework_request_context \
  Test_framework_coapp_lifecycle Test_framework_inbound_http \
  Test_framework_actor_mailbox Test_framework_ordered_egress \
  Test_framework_ordered_ingress Test_framework_ordered_ingress_rpc \
  Test_framework_v1_absent \
  Test_framework_shutdown Test_framework_rpc_codec Test_framework_close_gate \
  Test_framework_component_lifecycle Test_framework_egress_request_phase \
  Test_framework_getvalue_codec Test_framework_rpc_matrix rpc_xlang_server \
  getvalue_server getvalue_caller two_service rpc_policy lifecycle_fixture \
  component_services Test_framework_dynamic_routes

# 测试一律经统一 profile 入口（run_test_profile.sh，内部 ctest -j1）；
# 本地 smoke/focused 快速集，全量走发布/手动入口；PR CI 为 smoke + 必要 focused。
ctest --test-dir build -j1 -L smoke --output-on-failure
ctest --test-dir build -j1 -R 'examples\.' --output-on-failure
```

真实 Consul 电商集群示例为可选目标，需单独启用 infra Consul 支持；构建与运行说明见
[集群示例入口](examples/ecommerce_cluster/README.md)。

说明：

- `BBT_INFRA_SOURCE_DIR` / `BBT_COROUTINE_SOURCE_DIR` 默认为空；不给
  `BBT_INFRA_SOURCE_DIR` 时回退 `find_package(bbt_infra CONFIG QUIET)`，
  仍无 `bbt::infra_common` 目标则 `FATAL_ERROR` 并指明两条路径选项。
- `-DNEED_TEST=ON` 只开启本仓 `tests/`；上游 coroutine/infra 的同名
  `NEED_TEST`、`BUILD_TESTING` 开关在接入时被局部屏蔽。
- 上游 infra RPC 只保留 V2 会话：`bbt::infra_rpc_net` 目标承载
  `SessionFrame`（proto 2）+ u32be 分帧 + Hello/HelloAck；需要
  `BBT_INFRA_RPC_NET=ON` + `BBT_PROTOBUF_PREFIX`。框架默认宿主
  `InfraHttpHost` 仍消费 `bbt::infra_http_net`（`BBT_INFRA_HTTP_NET=ON`），
  两个目标都是 `bbt::framework` 的必需前置，缺失时 configure fail-closed。
  NEED_TEST=ON 的本仓验收（`framework.rpc_xlang` 等强制用例）同样要求
  RPC target 在场；不存在 HTTP header 或旧 wire 的降级路径。
- HTTP 宿主装配与执行槽、共享池、`Close` 与优雅排空的区别、运行时状态
  shared_ptr 寿命模型（`AppRuntimeState`）、客户端字节限额及无 DNS/TLS 的
  边界见 [HTTP 宿主指南](docs/guides/http-host.md)；RPC 只经 V2 会话消费，
  映射见 [框架契约 §7.1](docs/architecture/framework-contract.md)。
- 构建产物只进已忽略的 `build/`。
- 示例与消费骨架只写 `bbtco_main()`（`BBTCO_MAIN*` 生成 native main，参数经
  `BBT_EXAMPLE_*` 环境变量传入）：不自行启停 scheduler、不建 runner/观察线程、
  不 `future.wait/get`。本仓 `examples/` 已全部迁移到该入口，规则见
  [代码约定 §7](docs/conventions.md)。

## 跨语言 RPC 示例：GetValue

`examples/getvalue/` 是首个真实业务切片：业务 `.proto`（唯一 schema 真源）
+ C++ framework 服务端（V2 会话入站）+ Python 标准库客户端。
详见 [`examples/getvalue/README.md`](examples/getvalue/README.md)。

- 业务 schema：`examples/getvalue/proto/bbt/example/v1/get_value.proto`
  （`bbt.example.v1.GetValueRequest/Response`；`GetValue(key) -> {found, value}`）。
- V2 会话 wire：`SessionFrame`（`session_frame.proto`，profile 2）+ u32be
  分帧 + Hello/HelloAck 身份协商；`AppNetworkOptions::SetLocalAppId` 必填且与
  peer 互异（拒绝 self-connection）。`remaining_budget_ms` 由 CALL 帧携带，
  接收端按 local-min 收紧；0 预算在解码期以 `SESSION_ERROR(ProtocolError)`
  拒绝。没有 HTTP header 旁路或版本化 envelope。
- 跨语言客户端共用 `tests/rpc_v2_wire.py`（SessionFrame 编解码 + 分帧 +
  握手），运行/验收（ctest 名 `examples.getvalue.xlang` 与
  `framework.getvalue_codec`）。所有结构化结果带
  `auth_state=unauthenticated_loopback`。

## 双服务资源示例：dual_service（默认不构建）

`examples/dual_service/` 是三个真实进程的跨服务示例（`svc_a` storage /
`svc_b` gateway / `driver` 客户端，`svc_a` 用真实 Redis + Mongo 资源），
由根 CMake 的显式开关控制：

```cmake
option(BBT_ENABLE_DUAL_SERVICE_EXAMPLE "…" OFF)
```

- 默认 OFF：普通 framework 构建与测试不加入该示例，不要求
  hiredis / mongo-cxx-driver，也不要求真实 Redis/Mongo 后端。
- 显式 ON：fail-closed。必须同时具备 `NEED_TEST=ON`、protobuf 前缀
  （`bbt_infra_rpc_net`）、`BBT_INFRA_REDIS_NET=ON` + hiredis 前缀
  （`bbt::infra_redis_net`）、
  `BBT_MONGOC_PREFIX`+`BBT_MONGOCXX_PREFIX`（`bbt::infra_mongo`）；
  任一缺失都在 configure 阶段 `FATAL_ERROR`，不静默跳过。
- 协议：三个进程统一 V2 会话（`SessionFrame` + u32be + Hello/HelloAck，
  `local_identity` 互异）；CALL 帧携带 `remaining_budget_ms`，服务端按
  local-min 继承调用方预算（示例含可判定证据）。
- 覆盖边界：默认 CI 不传该开关，所以它的绿灯**不**覆盖本示例；真实资源
  验收入口是 `examples/dual_service/run_acceptance.sh`（临时 Redis/Mongo
  容器 + 全场景 + 残留复核），当前仅本地/受控环境运行。资源验收 CI job
  尚未接线——本轮不新增 `ubuntu-latest`/新 runner 架构，推荐方向是派生一个
  预装 hiredis/mongoc/mongocxx 固定前缀的 runner 镜像 + infra 固定 recipe，
  待其落地并冻结 SHA 后经 `run_acceptance.sh` 复用。默认 CI 另有一个负向
  门禁步，断言「显式 ON 且缺前缀必须 configure 失败」。
- 资源身份与清理都 fail-closed：`run_acceptance.sh` 拉容器前校验
  recipe 身份（manifest 与 CMakeCache 前缀一致、组件 commit 与 `cache_key`
  互绑、每个已记录 `.so` 的 `sha256`、`svc_a` 的 `ldd` 逐个解析到对应私有
  前缀且无 `not found`），teardown 只删/只复核自己创建的对象，`docker`
  查询失败同样计为未证实并让退出码非零。

详见 [`examples/dual_service/README.md`](examples/dual_service/README.md)。

## 文档地图

- 架构基调（设计真源）：[`docs/architecture/distributed-framework-baseline.md`](docs/architecture/distributed-framework-baseline.md)
- 验收矩阵（测试设计，非实测结果）：[`docs/architecture/distributed-framework-validation-matrix.md`](docs/architecture/distributed-framework-validation-matrix.md)
- 示例总览（需求→例程→公开 API/边界）：[`examples/README.md`](examples/README.md)
- 示例：[`examples/getvalue/README.md`](examples/getvalue/README.md)、
  [`examples/two_service/README.md`](examples/two_service/README.md)、
  [`examples/dual_service/README.md`](examples/dual_service/README.md)、
  [`examples/lifecycle_matrix/README.md`](examples/lifecycle_matrix/README.md)、
  [`examples/rpc_policy/README.md`](examples/rpc_policy/README.md)、
  [`examples/component_services/README.md`](examples/component_services/README.md)、
  [`examples/ecommerce_cluster/README.md`](examples/ecommerce_cluster/README.md)
- Agent 协作与门禁：[`AGENTS.md`](AGENTS.md)
- CI 说明：[`docs/guides/ci.md`](docs/guides/ci.md)
- 测试规范：[`docs/guides/testing.md`](docs/guides/testing.md)
