# bbt-framework

通用后端框架：受管 `CoApp` / `CoService` / `CoComponent` 对象模型，统一生命周期与关闭语义，
基于 `bbtools-infra` V2 会话的 RPC 入站/出站与出站路由门。

## 现状

- **公共面唯一**：业务只使用 `bbt::framework`。聚合头 `bbt/framework/Framework.hpp`，宿主头
  `bbt/framework/CoApp.hpp`。机器面头在 `bbt/framework/internal/`，业务不直接包含。
- **已实现**：受管对象装配与卸载（`CoInstallService`、`CoInstallComponent`、`CoUninstallService`、
  `CoUninstallComponent`）、Actor 类型登记（`CoRegisterActorType`）、声明式与运行期绑定
  （`ServiceOptions::BindComponent`、`CoBindComponent`、`CoUnbindComponent`）、主体内的 `GetComponent`、
  `CoObserve` 事件、`CoStart` / `CoStop`、`RequestShutdown` / `CoWaitShutdownRequest`、
  `State` / `PendingCleanup` / `LifecycleFailures` 观察面、V2 RPC 入站（`InstallRpcAdmission`）与
  出站（`RpcEgress`）、静态路由（`AppOptions::AddRoute`）与 `DynamicRouteTable`。
- **未实现**：
  - 非拥有关系边 `CoBindObject` / `CoUnbindObject` 恒返回 `UnsupportedRoute`（`bbt/framework/host/CoApp.cc`）。
  - Actor 实例创建与销毁（`CoCreateActor` / `CoDestroyActor`）与 Actor 消息分发未接线；
    `CoRegisterActorType` 只登记类型。服务用 `ExecutionPolicy::SerialByKey` 按 key 串行执行 RPC。
    `CoInstallActor` 和 `ExecutionPolicy::ActorSerial` 保留兼容。
  - `MonitorService`、`HttpComponent`、`AppInspector`、`AppSession`、`Resolver`、`CoJoin` 在源码中不存在；
    框架契约中出现的这些名字是目标设计，不是现有 API。
- 目标设计与当前实现的差异见 [框架契约](docs/architecture/framework-contract.md) §1 与 §12。

## 最短用法

类型声明（服务名、RPC 方法表、受控构造）：

```cpp
#include <bbt/framework/Framework.hpp>

class EchoService final : public bbt::framework::CoService<> {
public:
    static constexpr std::string_view kServiceName = "echo";

    bbt::framework::CoRpcResp Ping(bbt::framework::CoRpcReq req);

    static constexpr auto kRpcMethods = bbt::framework::RpcMethods(
        bbt::framework::Method<&EchoService::Ping>("ping"));

    explicit EchoService(bbt::framework::detail::ConstructKey key) : CoService(key) {}

protected:
    ~EchoService() noexcept override = default;

private:
    friend class bbt::framework::detail::Registry;
};
```

装配（在返回 `result<void>` 的受管协程入口中）：

```cpp
bbt::framework::AppLimits limits;
limits.max_objects = 64;
limits.max_bindings = 64;
limits.max_observers = 64;
limits.observer_queue_capacity = 64;
auto app = bbt::framework::CoApp::Create(
    bbt::framework::AppOptions{}
        .SetAppLimits(limits)
        .SetNetworkLimits(bbt::infra::NetworkLimits{
            /*max_connections*/ 64, /*max_inflight*/ 64,
            /*max_header_bytes*/ 16 * 1024, /*max_body_bytes*/ 64 * 1024,
            /*incoming_timeout*/ std::chrono::milliseconds{30000}})
        .SetShutdownBudget(std::chrono::milliseconds{3000})
        .SetLifecycleBudget(std::chrono::milliseconds{3000})
        .Listen("127.0.0.1:0")
        .SetLocalIdentity("echo-server"),
    "echo-server");

auto installed = bbt_co_await app->CoInstallService<EchoService>(
    bbt::framework::ServiceOptions{}.SetMaxInflight(64));
if (!installed) return installed;
auto started = bbt_co_await app->CoStart();
if (!started) return started;
auto waited = bbt_co_await app->CoWaitShutdownRequest();  // 外部线程 RequestShutdown() 唤醒
auto stopped = bbt_co_await app->CoStop();
// 若 State() == StopIncomplete，用 PendingCleanup() 定位并带新期限继续收口。
return stopped ? waited : stopped;
```

- 对象只能经 `CoApp::CoInstall*` 创建。业务不能栈构造、`new` 或 `make_shared`。
- `AppLimits::observer_queue_capacity` 当前只参与启动的正值校验，观察派发不据此限制队列。
- 配置监听时必须给非零 `SetNetworkLimits`：`bbt::infra::NetworkLimits` 默认全零，
  会被 `CoStart` 的网络配置校验以 `max_connections out of range` 拒绝。
- `Co` 前缀方法必须在受管协程内调用，宿主/非协程上下文返回 `InvalidContext`。
  `RequestShutdown()` 与 `State()` 例外，可在任意线程调用。
- 服务内受管出站 `CoServiceBase::call`（`CoRpcReq` 与 typed `call<Req,Resp>` 两个
  重载）以 `bbt_co_async(result<...>)` 声明，只能在受管协程内经 `bbt_co_await`
  消费；非协程环境下 `bbt_co_await` 先抛 `std::logic_error`（实参与函数体不执行），
  通过后沿当前协程同步执行、期间可挂起，没有 Future/并行调度。
- **新增服务**：参照上面的 `kRpcMethods` 和 `CoInstallService` 注册，完整宿主见 [`getvalue`](examples/getvalue/README.md)。
- **使用共享组件**：声明 `ServiceOptions::BindComponent`，在服务内用 `GetComponent` 查当前绑定，用句柄的 `lock()` 持有该实例，见 [`component_services`](examples/component_services/README.md)。
- **跨进程调用**：用 `AppOptions::AddRoute` 指向远端，在调用方 Service 内调用 `call`，见 [`two_service`](examples/two_service/README.md)。
- 可选的 Redis / Mongo 组件随对应 infra 目标构建和安装，使用方同时链接对应 infra 库。装配见 [`dual_service`](examples/dual_service/README.md)。

## 构建与测试

从仓库根执行 `git submodule update --init --recursive`，检出固定版本的 pub 及其角色提示词。`git submodule status --recursive` 可核对两级提交。

以下示例从仓库根运行。将三个相对路径换成已准备好的 infra、coroutine 源码树及 protobuf 3.21.12 前缀。`realpath` 将选定路径传给 CMake。

```bash
INFRA=../deps/infra-main
CO=../deps/coroutine-main
PB=../deps/protobuf-3.21.12

flock /tmp/bbt-build.lock cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DNEED_TEST=ON \
  -DBBT_INFRA_SOURCE_DIR="$(realpath "$INFRA")" -DBBT_COROUTINE_SOURCE_DIR="$(realpath "$CO")" \
  -DBBT_INFRA_CONFIG=ON -DBBT_INFRA_NET=ON -DBBT_INFRA_HTTP_NET=ON \
  -DBBT_INFRA_RPC_NET=ON -DBBT_PROTOBUF_PREFIX="$(realpath "$PB")"

# smoke 会启动多个测试程序和跨语言驱动。先构建当前配置的默认目标集。
flock /tmp/bbt-build.lock cmake --build build --parallel 2

# 测试经统一入口 scripts/ci/run_test_profile.sh（内部 ctest -j1）。
bash scripts/ci/run_test_profile.sh --build-dir build --profile smoke
```

- 构建并发固定 `--parallel 2`，经 `flock /tmp/bbt-build.lock` 串行；测试 `ctest -j1`。
- `NEED_TEST=ON` 注册 CTest 名 `framework.*`（`tests/`）与 `examples.*`（`examples/`）。
  测试分层、label 与 profile 见 [testing](docs/guides/testing.md)，CI 门禁见 [ci](docs/guides/ci.md)。
- `BBT_INFRA_RPC_NET=ON` 与 `BBT_PROTOBUF_PREFIX` 必需，缺失时 configure fail-closed。`BBT_INFRA_HTTP_NET=ON`
  同样必需（框架默认宿主 `InfraHttpHost` 消费 `bbt::infra_http_net`）。
- `BBT_INFRA_SOURCE_DIR` / `BBT_COROUTINE_SOURCE_DIR` 默认为空；为空时回退
  `find_package(bbt_infra CONFIG QUIET)`，仍无 `bbt::infra_common` 则 `FATAL_ERROR`。
- `-DNEED_TEST=ON` 开启本仓 `tests/` 及满足构建条件的示例。上游 coroutine/infra 的同名 `NEED_TEST` 与 `BUILD_TESTING`
  在接入时被局部屏蔽。
- 构建产物只进已忽略的 `build/`。

以下路径**需要外部后端与显式授权，默认不运行**，本仓普通 CI 也不覆盖：

- Redis/Mongo 真实资源验收：`framework.resource_live_*`（源码已迁移到当前公共 API；
  live 目标仅在 infra 相应模块在场时构建，本仓无真实后端运行授权，未运行）与
  [`dual_service`](examples/dual_service/README.md)
  的 `run_acceptance.sh`（临时容器）。`dual_service` 需 `BBT_ENABLE_DUAL_SERVICE_EXAMPLE=ON`，默认关闭。
- 真实 Consul 动态发现：[`ecommerce_cluster`](examples/ecommerce_cluster/README.md) 需
  `BBT_INFRA_CONSUL=ON` 与 `BBT_CONSUL_BINARY`；未设置时 CTest 不注册。

## 网络与 RPC

- RPC 只经 infra V2 会话（`SessionFrame` + u32be 分帧 + Hello/HelloAck），不存在 HTTP header 或旧 wire 降级路径。
- 入站与出站装配、执行槽、关闭顺序：[网络宿主与 RPC V2 消费](docs/guides/http-host.md)；
  RPC 映射见 [框架契约 §7.1](docs/architecture/framework-contract.md)。
- 身份：`AppOptions::SetLocalIdentity` 必填（配置监听时），与 peer 互异。当前明文 trusted-network 下
  `peer_principal` 恒为空，示例结果带 `auth_state=unauthenticated_loopback`，不证明授权完成。

## 示例

[示例总览](examples/README.md) 按需求索引每个例程的公开面、运行条件与边界。

## 文档地图

- 公共规范、角色提示词与工具从 `third-party/bbtools-pub/README.md` 进入。framework 的源码与 API 约定见[本仓开发细则](docs/conventions.md)，lint、复杂度与推送门禁的用法见[开发指南](docs/guides/development.md)。
- 架构基调（设计真源）：[distributed-framework-baseline.md](docs/architecture/distributed-framework-baseline.md)。
- 框架契约（目标设计 + 已实现边界）：[framework-contract.md](docs/architecture/framework-contract.md)。
- 验收矩阵（测试设计，非实测结果）：[distributed-framework-validation-matrix.md](docs/architecture/distributed-framework-validation-matrix.md)。
- 开发流程与本地验证：[development.md](docs/guides/development.md)。
- 测试规范：[testing.md](docs/guides/testing.md)；CI 说明：[ci.md](docs/guides/ci.md)。
- 网络宿主与 RPC V2：[http-host.md](docs/guides/http-host.md)。
- 示例总览：[examples/README.md](examples/README.md)。
- Agent 协作与门禁：[AGENTS.md](AGENTS.md)。
