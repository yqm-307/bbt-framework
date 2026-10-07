# examples/dual_service — 双 Service / 跨进程示例（Issue #5）

三个独立进程，经 framework 默认 HTTP 桥做真实 client/server。协议为
**ProtoWireV1**（infra 正式 body wire profile：`POST /rpc` +
`application/x-protobuf`），与 `examples/getvalue` 同口径：

```
driver ──wire──> svc_b(gateway) ──wire──> svc_a(storage)
                 fetch/store/del/          put/get/del/sleep_ms
                 slowcall/missing/explode  Redis+Mongo 资源（add_resource）
                 this->call("storage",…)
```

- `svc_a` 托管 `storage` 服务（Concurrent），真实后端经
  `CoApp::add_resource<CoRedisCli>("cache", factory)` 与
  `add_resource<CoMongoCli>("docs", factory)` 工厂装配为命名资源，
  handler 经 `context().resource<...>` 取用；无 KvStore 替身。
  Redis 工厂在交付前**显式 Connect 到 Connected**（`Connect` 只允许在协程
  上下文调用，工厂投递到 Scheduler 并等待），否则 handler 首个命令会得到
  `RuntimeUnavailable "redis client not connected"`。
- `svc_b` 托管 `gateway` 服务，`static_routes` 把 `storage` 显式指到
  `svc_a` 的真实 `host:port`——未配置目标一律 `NotFound`，无自动发现。
- `driver` 是第三个进程：`NetworkRuntime + HttpClient`，用 infra 公开头
  `<bbt/infra/rpc/RpcWire.hpp>` 直接组/解正式 wire 请求（不引用 framework
  internal 头，也不读 `x-bbt-*` header）。

## 为什么必须是正式 body wire（ProtoWireV1）

只有正式 wire body 携带 `remaining_budget_ms`，服务端才能在 handler 可见前
把入站预算收敛为 `min(传输硬看门, now+budget)` 并据此中止。迁移期 header 桥
（`LegacyHeaders`）没有任何预算字段，服务端只能吃传输层 `incoming_timeout`
——调用方预算根本到不了对端，做不到真正继承。因此本示例三个进程统一走
ProtoWireV1（该 profile 需要 `bbt_infra_rpc`，即 protobuf 前缀在场）。

## 覆盖路径

driver 按序执行并各自打印 PASS/FAIL + 实际结果：

1. `store` + `fetch` 正常回路（经 gateway → storage 真实跨进程，Redis 双写）。
2. `fetch` 未写入的 key → 业务错误 `NotFound` 透传。
3. `explode` → 业务错误 `InternalError`（gateway 本地）。
4. `missing` → 未路由服务名 → `NotFound`（无 I/O）。
5. `slowcall` → **服务端预算继承**（见下）。
6. `del` 本轮 key + 复核 `fetch` → `NotFound`（只清本轮命名空间）。
7. storage 下线 → `fetch` 返回 `TransportError`（连接失败如实上报）。
8. `SIGINT`/`SIGTERM` 优雅关闭（`run()` 返回 0）。

### 场景 5：服务端继承预算（不是「仅客户端超时」）

`gateway.slowcall` 经 `this->call("storage","sleep_ms",req,{},
CallOptions{now+600ms})` 给 storage 一个显式**子预算**（`AdaptCallOptions`
取 min，不得延长父预算）。该预算随 wire 的 `remaining_budget_ms` 到达
storage，服务端据此在 handler 内中断 `sleep_ms` 并**自行**返回 `TimedOut`；
gateway 再把该错误原样透传给 driver。

driver 用 **8000ms** 客户端预算发起调用，并断言：

- 错误码 `TimedOut`；
- 错误文本来自 storage handler（含 `sleep_ms`）→ 服务端确已收到并继承预算；
- 耗时 `< 3000ms`（实测约 590ms）→ 既不是 storage 睡满 5000ms，也不是
  客户端在 8000ms 处掐断。

若预算只在客户端生效（如走 header 桥），本调用要么把 storage 拖到传输层
30s 看门（被客户端 8s 先掐断），要么在 8s 处由客户端超时——两种都不会产生
「服务端自产 TimedOut」。因此本断言把「服务端继承」与「仅客户端超时」
区分开。既有框架侧证据：`tests/Test_framework_resource_live.cc` 与
`examples/getvalue/getvalue_client.py` 的预算用例（budget=1ms → 进 handler
前 `TimedOut`；超上限预算被 clamp 到本地期限）。

## 构建：显式能力开关（默认 OFF）

本示例**不是**普通 framework 构建的一部分，由根 CMake 的显式开关控制：

```cmake
option(BBT_ENABLE_DUAL_SERVICE_EXAMPLE "…" OFF)
```

- **OFF（默认）**：不加入本目录，不要求 hiredis / mongo-cxx-driver，也不
  要求真实 Redis/Mongo 后端；只想链接 `bbt::framework` 的消费路径不变。
- **ON**：显式选择该能力后严格 fail-closed——下列四项前置条件任一缺失都在
  **configure 阶段** `FATAL_ERROR`，不会静默跳过（跳过会让
  `storage_main.cc` 等核心示例代码永不编译，形成「CI 绿灯但没有覆盖」）：

  1. `-DNEED_TEST=ON`（examples 入口由它开启）；
  2. `-DBBT_PROTOBUF_PREFIX=<protobuf 前缀>`（使 `bbt_infra_rpc` 注册）；
  3. `-DBBT_HIREDIS_PREFIX=<hiredis 前缀>`（使 `bbt::infra_redis` 注册，`svc_a` 必需）；
  4. `-DBBT_MONGOC_PREFIX=<mongo-c 前缀>` 与 `-DBBT_MONGOCXX_PREFIX=<mongo-cxx 前缀>`
     （使 `bbt::infra_mongo` 注册，`svc_a` 必需）。

前缀布局沿用 bbtools-infra 契约（hiredis：`include/hiredis` + `lib/cmake/hiredis`；
mongoc：`lib/cmake/mongoc-*` + `bson-*`；mongocxx：`lib/cmake/mongocxx-*` +
`bsoncxx-*`），`find_package` 全部限定 `NO_DEFAULT_PATH`，不会回落 `/usr/local`。

```bash
cmake -S . -B build-deps/project-build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DNEED_TEST=ON \
  -DBBT_INFRA_SOURCE_DIR=/path/to/bbtools-infra \
  -DBBT_COROUTINE_SOURCE_DIR=/path/to/bbtools-coroutine \
  -DBBT_PROTOBUF_PREFIX=/path/to/protobuf \
  -DBBT_HIREDIS_PREFIX=/path/to/hiredis \
  -DBBT_MONGOC_PREFIX=/path/to/mongoc \
  -DBBT_MONGOCXX_PREFIX=/path/to/mongocxx \
  -DBBT_ENABLE_DUAL_SERVICE_EXAMPLE=ON
cmake --build build-deps/project-build -j"$JOBS" --target svc_a svc_b driver
# 产物：build-deps/project-build/examples/dual_service/{svc_a,svc_b,driver}
```

`scripts/build_stack.sh` 经 `BBT_CMAKE_ARGS` 透传同一组参数；
`scripts/local_build.sh` 是它的包装。

## 运行

### 一条命令（推荐）：临时容器 + 全场景 + 残留复核

```bash
examples/dual_service/run_acceptance.sh [build_dir]
```

`run_acceptance.sh` 是本示例**唯一**的临时容器生命周期管理者：创建私有
bridge 网络与两个临时容器（`redis:7-alpine` / `mongo:8.0`，仅
`127.0.0.1` 动态主机端口），等真实命令往返就绪，跑 `run_demo.sh` 全场景，
最后删除**自己创建的**容器/网络并复核已移除。可覆盖：

- `BBT_DEMO_REDIS_IMAGE` / `BBT_DEMO_MONGO_IMAGE`：后端镜像（派生固定
  recipe/digest 后经此传入即可复用，脚本不硬编码任何未知 SHA）；
- `BBT_DEMO_NETWORK`、`BBT_DEMO_ACCEPT_LOG_DIR`；
- `BBT_DEMO_EXPECT_CACHE_KEY`：可选，把 recipe cache_key 钉到调用方给定的
  确切值。

拉容器前先做**资源身份校验**（不复制构建配方，只消费 manifest 记录的身份与
摘要）：CMakeCache 记录的三前缀必须同属一个 recipe 根；
`resource-deps.manifest.json` 的 `install_root`/`cmake_prefix_paths`/组件
`commit`/`tag` 必须与之一致，且 `cache_key` 必须精确等于由三个组件 commit
前 8 位拼出的键（源身份与 recipe 互绑）；manifest 记录的每个 `.so` 都按
`sha256` 与实际文件逐一核对；`svc_a` 的 `ldd` 必须让 hiredis / libmongoc2 /
libbson2 / libmongocxx1 / libbsoncxx1 逐个解析到对应私有前缀下，且输出不得
含 `not found`。任一项读不到、命令失败或核对不过都以非零退出（fail closed），
不降级成「未验证的通过」。

teardown 同样 fail-closed：只删除、也只复核本脚本实际创建过的容器与网络；
`docker rm` / `docker ps` / `docker network ls` 任一命令失败都算「清理未证实」
并让退出码非零——查询失败不得当成「没有残留」。

### 直接用已起的后端跑 demo

```bash
./examples/dual_service/run_demo.sh [build_dir]      # 一条路径全场景
```

- 后端地址：`BBT_DEMO_REDIS_ADDR`（默认 `127.0.0.1:16379`）、
  `BBT_DEMO_MONGO_URI`（默认 `mongodb://127.0.0.1:27017/`）。**显式设空串
  即 FATAL**（用 `${VAR-}` 取值，空值不会被默认值掩盖）。
- 端口：`BBT_DEMO_PORT_A/B`，**未给则动态挑 loopback 空闲端口**。
- key 命名空间：`BBT_DEMO_KEY_PREFIX`（未给则生成每轮唯一前缀）。所有写入键
  带该前缀，driver 只 `del` 自己的键——不覆盖共享后端的固定键。
- 日志：默认 `$BUILD_DIR/run-demo-logs/<本轮唯一名>/`（**不落源码树、不落
  系统 `/tmp`**），文件名带本轮唯一后缀 `svc_a-<tag>.log` / `svc_b-<tag>.log` /
  `driver-<tag>.log` / `driver-down-<tag>.log`；`BBT_DEMO_LOG_DIR` 可覆盖。
  `BBT_DEMO_KEEP_LOGS=0` 只删本轮自己写出的这几个文件——不递归删目录、不碰
  目录里别人的日志（不做 `rm -rf`）。
- 后端残留复核：给 `BBT_DEMO_REDIS_CONTAINER` / `BBT_DEMO_MONGO_CONTAINER`
  时，脚本用 `redis-cli --scan --pattern "<prefix>*"` 与
  `countDocuments({_id:{$regex:"^<prefix>"}})` **独立复核**本轮 key 已清除；
  有残留即 FAIL（清理失败不得静默通过）。未给容器名时该项如实标
  `unverified`，不冒充已验证。
- 有界停止：进程先 `SIGTERM`，超 `BBT_DEMO_STOP_GRACE`（非负整数，默认 10s）
  未退出升级 `SIGKILL`；只作用于本脚本自己起的 PID（不做模式匹配误伤他人
  进程）。非法 `BBT_DEMO_STOP_GRACE` 按普通诊断退出（64），不进入算术比较。
  场景 7/8 的停机走同一有界路径，并断言进程**真实退出码**为 0（被 KILL 升级
  即 137 → FAIL），不用无界 `wait` 挂住验收。
- 退出码：driver 取管道首段（driver 自己）的真实退出码，`tee` 的退出码单独
  校验——日志写失败不得被 tee 的成功掩盖成通过。

手动单进程方式：

```bash
./build-deps/project-build/examples/dual_service/svc_a 38101 127.0.0.1:16379 \
    mongodb://127.0.0.1:27017/ &
./build-deps/project-build/examples/dual_service/svc_b 38102 127.0.0.1:38101 &
BBT_DEMO_KEY_PREFIX=my-run- ./build-deps/project-build/examples/dual_service/driver \
    127.0.0.1:38102
# 观察正常/错误/服务端超时路径后：
kill -TERM %1 %2   # 两进程各自优雅关闭，exit code 0
```

## 覆盖边界与 CI（诚实说明）

- 默认 CI（`.github/workflows/ci.yml` 的 build/test job）**不构建本示例**：
  它 `NEED_TEST=ON` 但不传 `BBT_ENABLE_DUAL_SERVICE_EXAMPLE`，默认绿灯
  **不能**作为本示例的验收证据。
- 默认 CI 另有一个负向门禁步骤：显式传
  `-DBBT_ENABLE_DUAL_SERVICE_EXAMPLE=ON` 而缺上述前置条件时必须 configure
  失败（断言失败即 job 失败），保证开关语义不会被改回「静默跳过」。
- **真实资源验收的 CI job 尚未接线**：本轮**不**新增 `ubuntu-latest`/新
  runner 架构，也不发布/切换 runner。当前运行证据来自本地/受控环境
  （`run_acceptance.sh`），状态对默认 CI 为 `UNVERIFIED`——不得用默认 CI
  绿灯代替。
- 推荐接线方向（待父级/拥有者决定）：在既有 runner 体系上派生一个预装
  hiredis/mongoc/mongocxx **固定前缀**的镜像，配由 infra 侧提供的
  Redis/Mongo 固定 recipe（镜像 digest）。届时复用的入口就是本目录的
  `run_demo.sh`（`--prefix` 经 CMake 传入）与 `run_acceptance.sh`
  （recipe 经 `BBT_DEMO_*_IMAGE` 传入）。**未发布文件不当可用依赖**：
  recipe 落地并冻结 SHA 后才接线，在此之前不加必然失败或假绿的 job。
  「本机当前没有该镜像」只是现状，不代表不可搭建。

### 本示例真实需要的 runtime 与路径（记录，供派生镜像）

构建期（CMake 显式传入，见上）：protobuf 前缀、hiredis 前缀、mongoc 前缀、
mongocxx 前缀，以及 infra/coroutine 源码树路径。运行期：`docker`（起
Redis/Mongo 临时容器）与 loopback 动态端口即可；driver/host 侧不需要
`redis-cli`/`mongosh`（清理走后端接口，复核经 `docker exec`）。
RPC 监听固定 `127.0.0.1`、明文 HTTP、单目录进程编排——只展示框架公共面
用法，**不作为部署形态**。
