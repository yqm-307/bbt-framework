# examples/dual_service — 双 Service / 跨进程示例（Issue #5 阶段 2）

三个独立进程，经 framework 默认 HTTP 线桥做真实 client/server：

```
driver ──HTTP──> svc_b(gateway) ──HTTP──> svc_a(storage)
                 fetch/store/slowcall      put/get/del/sleep_ms
                 this->call("storage",…)   Redis+Mongo 资源（add_resource）
```

- `svc_a` 托管 `storage` 服务（Concurrent），真实后端经
  `CoApp::add_resource<CoRedisCli>("cache", factory)` 与
  `add_resource<CoMongoCli>("docs", factory)` 工厂装配为命名资源，
  handler 经 `context().resource<...>` 取用；无 KvStore 替身。
- `svc_b` 托管 `gateway` 服务，`static_routes` 把 `storage` 显式指到
  `svc_a` 的真实 `host:port`——未配置目标一律 `NotFound`，无自动发现。
- `driver` 是第三个进程：`NetworkRuntime + HttpClient`，`x-bbt-*`
  线格式在本地重新实现（framework internal 头不进入业务示例）。

覆盖路径：正常请求/响应、业务错误透传（miss → NotFound、explode →
InternalError）、超时（slowcall 继承父预算到期 → TimedOut）、未路由
服务名（missing → NotFound，无 I/O）、storage 下线（连接失败如实
返回错误）、SIGINT/SIGTERM 优雅关闭（`run()` 返回 0）。

## 构建

沿用仓内统一入口（源码树接入 core/coroutine/infra，pin 见 deps.lock）：

```bash
./scripts/local_build.sh          # Release + NEED_TEST=ON 全量
# 产物在 build-deps/project-build/examples/{svc_a,svc_b,driver}
```

svc_a 需要真实后端与对应 infra 模块：配置 `BBT_HIREDIS_PREFIX` 与
`BBT_MONGOCXX_PREFIX`+`BBT_MONGOC_PREFIX` 构建 `bbt::infra_redis` /
`bbt::infra_mongo`（缺失即 configure 失败，不降级为替身）。

## 运行

```bash
./examples/dual_service/run_demo.sh          # 一条路径全场景
# 后端地址默认 127.0.0.1:16379 / mongodb://127.0.0.1:27017/，
# 可用 BBT_DEMO_REDIS_ADDR / BBT_DEMO_MONGO_URI 覆盖。
# 或手动：
./build-deps/project-build/examples/svc_a 38101 127.0.0.1:16379 \
    mongodb://127.0.0.1:27017/ &
./build-deps/project-build/examples/svc_b 38102 127.0.0.1:38101 &
./build-deps/project-build/examples/driver 127.0.0.1:38102
# 观察正常/错误/超时路径后：
kill -TERM %1 %2   # 两进程各自优雅关闭，exit code 0
```

`run_demo.sh` 顺序执行：起两进程 → driver 跑场景 1..5 → SIGTERM
svc_a → driver `--expect-storage-down` 验对端下线错误 → SIGTERM
svc_b → 校验全部退出码。

## 非生产模板

固定 `127.0.0.1` 监听、明文 HTTP、单目录进程编排——只展示框架
公共面用法，不作为部署形态。Docker Compose 编排属 Issue #5 阶段 3。
