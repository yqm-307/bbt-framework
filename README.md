# bbt-framework
通用后端框架

## 现状

业务公共面：handler 签名统一 `CoRpcResp(CoRpcReq)`，请求/回复经位置参数 codec
（`CoRpcReq::From` / `req.Parse<Ts...>` / `CoRpcResp::From`）；`kRpcMethods` +
`this->call` 声明/发起 RPC，`CoService<T>` / `ICoService` 继承服务，`CoApp` 注册并运行。
跨语言/结构化负载走 Protobuf 适配（`ProtoCodec` seam）。

```cpp
#include <bbt/framework/Framework.hpp>
#include <bbt/framework/CoApp.hpp>

class EchoSvc final : public fw::CoService<EchoSvc> {
public:
    static constexpr std::string_view kServiceName = "echo";
    fw::CoRpcResp Ping(fw::CoRpcReq req);   // req.Parse<std::string>() 取第 0 个位置参数
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&EchoSvc::Ping>("ping"));
};

fw::CoApp app{opts};
app.add_service<EchoSvc>(service_opts);
app.run();
```

`Framework.hpp` 汇总服务/RPC 声明头，不含宿主。`CoApp.hpp` 是宿主头
（`add_service` 会展开方法表）。编解码、envelope、分发器、发送注入在框架内部；
测试缝在 `internal/`。

资源缝：服务在受管生命周期内经 `context().resource<R>(name)` 取客户端
（未装配 → `nullptr`）。两种装配入口：
- `add_resource<R>(name, shared_ptr<R>)`：预创建实例，登记即对服务可见；
- `add_resource<R>(name, factory)`：延迟工厂——`factory` 返回
  `result<shared_ptr<R>>`，在运行时初始化后、绑定服务与开始接纳之前执行
  `Create`；若 `R` 暴露 `result<void> Start()` 由框架随即调用一次，若实现
  `bbt::infra::ICoCloseable` 则由关闭序列统一 `Close()` 同步收口
  （handler 排空之后、网络组件关闭之前）。任一资源创建/启动失败 →
  启动失败回退，已启动资源按序收束，不留半装配状态。

公共目标 `bbt::framework`（静态库）。

## 构建与测试

依赖：`bbtools-infra`（其自身再依赖 `bbtools-coroutine`）。本机/候选分支开发经显式路径覆盖接入：

```bash
INFRA=/path/to/bbtools-infra            # 含顶层 CMakeLists.txt 与 include/
CO=/path/to/bbtools-coroutine           # 含顶层 CMakeLists.txt 与 bbt/

cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug -DNEED_TEST=ON \
  -DBBT_INFRA_SOURCE_DIR=$INFRA -DBBT_COROUTINE_SOURCE_DIR=$CO
ninja -C build
ctest --test-dir build -j1 --output-on-failure
```

说明：

- `BBT_INFRA_SOURCE_DIR` / `BBT_COROUTINE_SOURCE_DIR` 默认为空；不给
  `BBT_INFRA_SOURCE_DIR` 时回退 `find_package(bbt_infra CONFIG QUIET)`，
  仍无 `bbt::infra_common` 目标则 `FATAL_ERROR` 并指明两条路径选项。
- `-DNEED_TEST=ON` 只开启本仓 `tests/`；上游 coroutine/infra 的同名
  `NEED_TEST`、`BUILD_TESTING` 开关在接入时被局部屏蔽。
- 构建产物只进已忽略的 `build/`。

## 跨语言 RPC 示例：GetValue（Issue #4 P0-A）

`examples/getvalue/` 是首个真实业务切片：业务 `.proto`（唯一 schema 真源）
+ C++ framework 服务端（正式 infra body wire bridge）+ Python 标准库客户端。
详见 [`examples/getvalue/README.md`](examples/getvalue/README.md)。

- 业务 schema：`examples/getvalue/proto/bbt/example/v1/get_value.proto`
  （`bbt.example.v1.GetValueRequest/Response`；`GetValue(key) -> {found, value}`）。
- 正式 wire：`CoAppOptions::inbound_bridge = RpcInboundBridge::ProtoWireV1`
  → HTTP `POST /rpc`、`Content-Type: application/x-protobuf`、body 为 infra
  版本化 `RpcEnvelope`；不使用 `x-bbt-*` header 旁路。选择该桥需配置锁定
  protobuf 前缀 `-DBBT_PROTOBUF_PREFIX=<prefix>`（未配置时框架只提供迁移期
  header 桥，选择 ProtoWireV1 会在启动前显式失败）。
- 运行/验收（ctest 名 `examples.getvalue.xlang` 与 `framework.getvalue.codec`）；
  Python 客户端零依赖（手写 proto3 wire 编解码）。所有结构化结果带
  `auth_state=unauthenticated_loopback`。

## 双服务资源示例：dual_service（Issue #5，默认不构建）

`examples/dual_service/` 是三个真实进程的跨服务示例（`svc_a` storage /
`svc_b` gateway / `driver` 客户端，`svc_a` 用真实 Redis + Mongo 资源），
由根 CMake 的显式开关控制：

```cmake
option(BBT_ENABLE_DUAL_SERVICE_EXAMPLE "…" OFF)
```

- 默认 OFF：普通 framework 构建与测试不加入该示例，不要求
  hiredis / mongo-cxx-driver，也不要求真实 Redis/Mongo 后端。
- 显式 ON：fail-closed。必须同时具备 `NEED_TEST=ON`、protobuf 前缀
  （`bbt_infra_rpc`）、hiredis 前缀（`bbt::infra_redis`）、
  `BBT_MONGOC_PREFIX`+`BBT_MONGOCXX_PREFIX`（`bbt::infra_mongo`）；
  任一缺失都在 configure 阶段 `FATAL_ERROR`，不静默跳过。
- 协议：三个进程统一 ProtoWireV1（infra 正式 body wire），只有它携带
  `remaining_budget_ms`，服务端才能真正继承调用方预算（示例含可判定证据）。
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
