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
  `result<shared_ptr<R>>`，在 Scheduler 启动后、绑定服务与开始接纳之前执行
  `Create`；若 `R` 暴露 `result<void> Start()` 由框架随即调用一次，若实现
  `bbt::infra::ICoCloseable` 则由关闭序列统一 `RequestClose`/`WaitClosed`
  （handler 排空之后、网络组件关闭之前，纳入 shutdown budget 与
  `ShutdownIncomplete` 记账）。任一资源创建/启动失败 → 启动失败回退，
  已启动资源按序收束，不留半装配状态。

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
