# 示例总览

按“想演示什么 → 例程 → 用到的公开面 → 边界”索引；每个例程的细节以各自
`README.md` 为准。断言由 C++ fixture 与 Python 驱动器共同执行；框架契约断言另见 `tests/`。

| 需求 | 例程 | 公开面 | 边界 |
|---|---|---|---|
| 组件式业务复用：两个 Service 共享一个受管组件实例 | [`component_services/`](component_services/README.md) | `v2::CoApp::CoInstallComponent/CoInstallService`、`ServiceOptions::BindComponent`、`CoService<C>::GetComponent`、`ComponentHandle::Borrow` | 同进程、同一 App/Registry 内共享实例；跨进程只能复用代码/协议 |
| 组件热拔插与 hook | 同上 | `CoBindComponent/CoUnbindComponent`、`CoUninstallComponent/CoInstallComponent`、`CoComponent` 生命周期/`OnBindingChanged`、`CoObserve`/`ObserverToken`、`App::LifecycleFailures` | 换的是受管组件实例，不是 dlopen/代码热更新；不跨进程热共享 |
| 跨进程模块复用（代码层） | 同上（`node` 模式） | 同一业务头 `store.hpp` 在独立进程各自装配 | 两个进程各有独立组件实例，不是 RPC、不是集群、不是服务发现 |
| 结构化 RPC / 跨语言 wire | [`getvalue/`](getvalue/README.md) | `CoApp::add_service`、`kRpcMethods`、`ProtoWireV1` + `ProtoCodec` | 需要 protobuf 前缀与 `bbt_infra_rpc_net`；单服务 |
| 单进程双 Service 调用 | [`two_service/`](two_service/README.md) | `this->call<Req,Resp>`、`find_route` 静态路由、真实 loopback 出站 | 路由是静态自指，不证明发现或跨进程 |
| 出站调用策略 | [`rpc_policy/`](rpc_policy/README.md) | `RpcPolicy`、`CoRpcReq/Resp` | 复用 getvalue 业务，无外部服务 |
| 容量 / 并发上下文 / 关闭生命周期 | [`lifecycle_matrix/`](lifecycle_matrix/README.md) | `ExecutionPolicy::Concurrent`、`max_inflight`、`ShutdownIncomplete`、`request_shutdown` | 真实 loopback HTTP，不代表生产部署 |
| 多进程 + 真实 Redis/Mongo 资源 | [`dual_service/`](dual_service/README.md) | `add_resource` 工厂/`ICoCloseable`、真实 Redis/Mongo 客户端 | 显式 opt-in（`BBT_ENABLE_DUAL_SERVICE_EXAMPLE=ON`）；资源示例，不是注册中心 |
| 真实注册中心 + 多进程动态发现（gateway→order→catalog，catalog 双实例） | [`ecommerce_cluster/`](ecommerce_cluster/README.md) | `DynamicRouteTable`/`CoAppOptions::dynamic_routes`、`CoApp::find_route` 动态优先、`bbt::infra_consul`、`add_resource` 工厂 | 需真实 Consul 二进制与 `BBT_INFRA_CONSUL=ON`；TCP check 健康、5s 陈旧边界、每服务 round-robin、无重试/权重 |

## 运行 component_services

组件业务代码本身不使用 RPC；但仓库当前 `NEED_TEST=ON` 配置会进入
`tests/CMakeLists.txt` 的强制 wire 验收门，因此仍需要 protobuf 前缀及 infra 的
HTTP/RPC 开关。这里是唯一可复制的配置入口；
[`component_services/README.md`](component_services/README.md) 链接回此处。

先准备可读的 infra 源码树、一个干净的 coroutine Git checkout（必须是完整
`.git` 检出，HEAD 精确为 `b0ba3e0239c06bf91a874f988187c7e2f56bf13d`，不能用
`git archive` 导出的无 `.git` 目录）和 protobuf 3.21.12 前缀。然后在仓库根目录
显式设置路径并核验 coroutine pin：

```bash
export INFRA="$PWD/build/deps/infra"
export CO="$PWD/build/local/coroutine-src"
export PROTOBUF_PREFIX="<protobuf-3.21.12 prefix 的绝对路径>"
test -d "$INFRA" && test -d "$CO" && test -d "$PROTOBUF_PREFIX"
test "$(git -C "$CO" rev-parse HEAD)" = b0ba3e0239c06bf91a874f988187c7e2f56bf13d

flock /tmp/bbt-build.lock cmake -S . -B build/local -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
  -DNEED_TEST=ON -DNEED_EXAMPLE=ON \
  -DBBT_INFRA_SOURCE_DIR="$INFRA" -DBBT_COROUTINE_SOURCE_DIR="$CO" \
  -DBBT_INFRA_HTTP_NET=ON -DBBT_INFRA_RPC_NET=ON \
  -DBBT_PROTOBUF_PREFIX="$PROTOBUF_PREFIX"

flock /tmp/bbt-build.lock cmake --build build/local --target component_services --parallel 2
ctest --test-dir build/local -j1 -R '^examples[.]component_services$' --no-tests=error --output-on-failure
```

需要手动检查 journal 时，使用被忽略的 `build/local` 下自有临时目录：

```bash
JOURNAL_DIR="$(mktemp -d build/local/component-services-journal.XXXXXX)"

build/local/examples/component_services/component_services shared \
  --journal "$JOURNAL_DIR/shared.journal"
build/local/examples/component_services/component_services node \
  --journal "$JOURNAL_DIR/node.journal" --price 700
```

退出码 `0` 表示全部检查通过；journal 中每行 `event=check name=... ok=1|0
detail=...` 可机器判定。

## 运行 ecommerce_cluster（真实注册中心 + 动态发现）

Issue #70 第五项已实现：framework 侧新增 `DynamicRouteTable`（公开扩展，经
`CoAppOptions::dynamic_routes` 接入 `find_route`，动态配置的服务名优先于静态路由），
infra 侧新增 `bbt::infra_consul`（Consul agent HTTP adapter）。示例
`ecommerce_cluster/` 用真实 Consul + 多进程（gateway → order → catalog 双实例）
验收落点变化、陈旧边界与恢复；细节与边界见
[`ecommerce_cluster/README.md`](ecommerce_cluster/README.md)。

除上面 component_services 的配置外，本示例还需要：

```bash
export CONSUL="<consul 1.20.2 可执行文件绝对路径>"   # 只落 build/，非系统安装

flock /tmp/bbt-build.lock cmake -S . -B build/local -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
  -DNEED_TEST=ON -DNEED_EXAMPLE=ON \
  -DBBT_INFRA_SOURCE_DIR="$INFRA" -DBBT_COROUTINE_SOURCE_DIR="$CO" \
  -DBBT_INFRA_HTTP_NET=ON -DBBT_INFRA_RPC_NET=ON -DBBT_INFRA_CONSUL=ON \
  -DBBT_CONSUL_BINARY="$CONSUL" \
  -DBBT_PROTOBUF_PREFIX="$PROTOBUF_PREFIX"

flock /tmp/bbt-build.lock cmake --build build/local --parallel 2 \
  --target ecommerce_cluster Test_framework_dynamic_routes
python3 examples/ecommerce_cluster/run.py \
  --binary build/local/examples/ecommerce_cluster/ecommerce_cluster \
  --consul "$CONSUL" --output build/local/cluster-run/result.json
```

不设置 `-DBBT_CONSUL_BINARY` 时 CTest `examples.ecommerce_cluster` 不注册
（与 Redis/Mongo live 门禁同一口径），驱动器仍可手工直接跑。

静态 `two_service` 路由、`dual_service` 的 Redis/Mongo 资源示例、
`component_services` 的进程内共享仍**不能**替代真实发现；它们的边界没有变化。
