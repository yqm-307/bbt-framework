# ecommerce_cluster — 动态发现与真实订单结算

一个可执行文件按角色运行，gateway、catalog、order、settlement 各有声明和实现目录，
验收时各自运行在独立进程。业务经 `v2::CoService::call<Req, Resp>` / `ProtoMethod`
发送真实 protobuf RPC；服务地址来自 Consul 健康视图，不注入静态实例地址。

公开 HTTP 路由与策略装配、状态/错误映射见 [网络宿主指南](../../docs/guides/http-host.md)。
gateway 接收 HTTP POST 与 protobuf body，后续仍走 typed V2 RPC；identity 使用每进程
incarnation，避免同角色多实例在会话层合并。

报价模式保留 `Checkout → Quote → Price`，`run.py` 继续验收原 28 项动态发现行为。
存储模式必须显式传 `--storage`，提供独立 Redis/Mongo 配置；缺构建资源或后端即失败。Order/Settlement 的 OnStart 经真实 Borrow 做一次
只读 Mongo readiness 查询；owner Start 只启动 worker，不能代替后端可用性证据。

```text
run_storage.py → HTTP POST /CreateOrder → Gateway.CreateOrder → Order.CreateOrder → Catalog.Price → Redis
                                           │
                                           ├─ Mongo orders：先保存 pending
                                           └─ Settlement.Settle → Mongo bills
                                                   │ 确认成功后订单才变 settled
Gateway.GetOrder → Order.GetOrder → Mongo orders
Gateway.GetBill → Settlement.GetBill → Mongo bills
```

| 入口 | 职责 |
|---|---|
| `services/gateway/gateway_service.hpp` / `.cc` | 报价入口、创建订单、订单/账单查询；typed 调用 Order 与 Settlement |
| `services/catalog/catalog_service.hpp` / `.cc` | 目录价，以 Redis Get/Set 做 cache-aside |
| `services/order/order_service.hpp` / `.cc` | typed 调用 Catalog / Settlement；拥有 orders 资源 |
| `services/settlement/settlement_service.hpp` / `.cc` | 独立结算服务，校验优惠与金额；拥有 bills 资源 |
| `services/common/storage.hpp` / `.cc` | 配置、输入校验、Mongo 文档和 protobuf Record 转换 |
| `services/common/proto_codec.hpp` | 唯一业务 protobuf codec 特化；schema 取自 descriptor |
| `services/common/hop_log.hpp` / `.cc` | 报价逐跳日志和入站 envelope ID |
| `services/common/node_identity.hpp` / `.cc` | 进程与 incarnation 身份 |
| `discovery.hpp` / `.cc` | Consul blocking query、自注册/注销、DynamicRouteTable 发布 |
| `ecommerce_cluster_main.cc` | 参数、CoApp / 服务 / 资源工厂装配和生命周期 |
| `proto/bbt/example/ecommerce/v1/ecommerce.proto` | 唯一业务 schema；生成物只进 build |
| `run.py` / `run_storage.py` | 原动态发现驱动 / 自有真实后端存储驱动 |

使用前设 `INFRA`、`PROTOBUF_PREFIX`、`RESOURCE_PREFIX` 为所选本地 infra 源码目录、
同源 protobuf 3.21.12 和锁定资源库安装根目录。源码依赖未指定版本时使用目录内当前内容，
允许未提交的 adapter 候选；指定版本时按指定版本，不以系统同名库补齐依赖。
资源前缀须先核 manifest、源 SHA、ABI、库哈希及 ldd，参见 infra 的
`docs/dependencies/resource-prefixes.provenance.md` 和 `scripts/resource_prefix_verify/`。
Coroutine 通过 `BBT_COROUTINE_SOURCE_DIR` 选择本地检出；需要固定版本时显式传 `BBT_COROUTINE_PINNED_SHA`。

在 framework 根目录构建：

```bash
flock /tmp/bbt-build.lock cmake -S . -B build/local -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
  -DNEED_TEST=ON -DNEED_EXAMPLE=ON \
  -DBBT_INFRA_SOURCE_DIR="$INFRA" \
  -DBBT_COROUTINE_SOURCE_DIR="$PWD/build/local/coroutine-src" \
  -DBBT_INFRA_HTTP_NET=ON -DBBT_INFRA_RPC_NET=ON -DBBT_INFRA_CONSUL=ON \
  -DBBT_PROTOBUF_PREFIX="$PROTOBUF_PREFIX" \
  -DBBT_CONSUL_BINARY="$INFRA/build/consul/bin/consul" \
  -DBBT_INFRA_REDIS_NET=ON -DBBT_INFRA_MONGO=ON \
  -DBBT_HIREDIS_PREFIX="$RESOURCE_PREFIX/hiredis" \
  -DBBT_MONGOC_PREFIX="$RESOURCE_PREFIX/mongoc" \
  -DBBT_MONGOCXX_PREFIX="$RESOURCE_PREFIX/mongocxx"
flock /tmp/bbt-build.lock cmake --build build/local \
  --target ecommerce_cluster component_services Test_framework_dynamic_routes --parallel 2
flock /tmp/bbt-build.lock ctest --test-dir build/local -j1 \
  -R '^(framework[.]dynamic_routes|examples[.](component_services|ecommerce_cluster))$' \
  --no-tests=error --output-on-failure
python3 examples/ecommerce_cluster/run_storage.py \
  --binary build/local/examples/ecommerce_cluster/ecommerce_cluster \
  --consul "$INFRA/build/consul/bin/consul" \
  --output build/local/ecommerce-storage/result.json
```

存储编译直接链接已有 `bbt::infra_redis_net` 和 `bbt::infra_mongo` 目标；不新增
资源库或构建开关。未构建这两个目标时可运行报价模式，`--storage` 返回非零。
`examples.ecommerce_cluster` 的 CTest 注册要求显式提供可执行 Consul 路径。
存储驱动不加入普通 CTest；它要求 Docker 及两个本地镜像，使用锁定 digest 和
`--pull=never`，镜像缺失会失败，不下载或跳过。容器只发布 loopback 动态端口，
使用随机数据库/Redis key 前缀，只清理本次自有进程与带本次标签的容器、匿名卷。
容器限制为每个 768 MiB、1 CPU、256 PID；各启动、请求、命令、关闭均有超时。结果 JSON 保存实际 `checks`、`failures`、PID、
镜像与容器 ID、清理结果及真实进程 rc；任何检查/清理失败时驱动返回非零。

手工运行各角色时传同一组存储配置和本次自有 Consul 入口，例如：

```bash
# 生成式入口（bbtco_main）不暴露 argv：角色与依赖地址经环境变量传入。
BBT_EXAMPLE_MODE=order BBT_EXAMPLE_CONSUL="$CONSUL_HTTP" BBT_EXAMPLE_STORAGE=1 \
BBT_EXAMPLE_REDIS="$REDIS_ENDPOINT" BBT_EXAMPLE_MONGO="$MONGO_URI" \
BBT_EXAMPLE_DATABASE="$RUN_DATABASE" BBT_EXAMPLE_KEY_PREFIX="$RUN_KEY_PREFIX" \
  build/local/examples/ecommerce_cluster/ecommerce_cluster
```

把 `BBT_EXAMPLE_MODE` 分别换成 `gateway`、`catalog`、`settlement` 启动四个进程；
每个进程输出动态 `event=ready endpoint=...`，stdin EOF 触发注销和有界关闭。入口是
runtime 的生成式 `bbtco_main`：进程不写 native main、不启停 scheduler、不建 runner
线程，注册/注销与收口都在业务主协程里直接 await；共享 Engine 的唯一一次
`Engine::Run` 由 `bbtco_main_host_hook` 经 `HostGate::RunHostGate` 在主线程执行。
目录进程的 5 个 Service 以 3+2 绑定两个 `v2::RedisComponent`；Order/Settlement 各由
`v2::MongoDBComponent` 持有自己的集合组件，均经 `CoApp::CoInstallComponent`
装配并经 `ComponentHandle::Borrow` 在 borrow 寿命内使用。5 Service 的 Inspect 方法经真实 RPC 与 Borrow 返回组件 ID/generation、进程 ID 和池身份；
驱动只在同一进程内比较，断言前三者同实例、后两者同实例且两组不同。
旧 handle 失效、同一 CloseOptions 转发、首次失败保留 owner 与显式重试，由
`framework.component_lifecycle` 和 `examples.component_services` 的既有判别性用例承担；
不把这些低层无后端用例说成真实存储故障注入。

Mongo URI 建议
传 `retryWrites=false`，明确关闭驱动层写重试；示例 RPC 自身也不自动重试写请求。

发现组件是 v2 `CoComponent`：`OnInstall` 只装配专属 http/consul client（借用共享
Engine，不 Run/不建线程），`OnStart` 登记 watcher，主协程随后显式 `WaitFirstSnapshots` 等首快照，`OnStop` 以同一
`CloseOptions` 停止 watcher 与视图发布、封口 adapter——共享 Engine 的 Run/收口归
应用根与入口 host hook，组件不触碰它。watcher 协程只持共享状态，不借用组件地址。
无后端的 Co 生命周期回归是 `Test_framework_discovery_co_lifecycle`（CTest
`examples.discovery_co_lifecycle`）：只用测试自持的不可达 loopback 地址验证启动失败
收口，不访问任何共享 Consul/Redis/Mongo 后端。

金额全用整数分。`OrderRequest` 由稳定 `order_id`、sku、int64 数量、优惠分组成；
ID/sku 限 1–80 字节字母数字、下划线或连字符。数量须正、优惠非负且不超总价，
乘法先检查 int64 溢出。Catalog 目录价为 book=1299、mug=899、pen=259，未知 sku 拒绝。

Mongo 文档的 `_id` 是业务 order_id，利用其唯一约束去重；`state` 是可直接查询的
pending/settled，`record` 是 protobuf Record 的十六进制字符串，保留整数与原处理
实例身份。相同 ID 同载荷读回相同已完成结果，改变 sku/数量/优惠则返回
`InvalidArgument`，消息含 `id_conflict`。Settlement 还比较总额。
确认重复键才读回；其他 Mongo 错误和 OutcomeUnknown 原样返回。

订单先落 pending，再发起一次 Settlement.Settle，确认账单成功后更新订单为 settled。
两处持久化之间没有跨服务/集合事务。失败时不返回结算成功，订单保持 pending 可查询，
账单可能已经存在；调用方先查询，再显式以同 ID 同载荷重放即可继续对账。
超时不代表未执行；没有 exactly-once、支付、鉴权或生产部署保证。

存储驱动覆盖 Redis 真 miss/hit 与 GET、订单/账单真实 Mongo 查询与计数、同载荷顺序和
并发重放、两服务的冲突拒绝、无效输入/溢出、停止结算服务后的有限失败和 pending
读回、显式重放恢复、三个下游服务重启后的持久性与幂等。它重启应用服务，保持本次
Mongo 容器运行；不声称验证 Mongo daemon 重启或故障注入后的未知结果。

日志 `event=storage_hop` 含 `service/method/order_id/request_id/pid/instance/ok/code`。
`order_id` 在四个进程稳定，`request_id` 是各跳入站 envelope ID，通常各不相同。
驱动把同一业务 ID 的四跳日志与不同 PID 一起写入结果，不把不同 envelope ID 说成
统一 trace。`event=cache result=miss|hit` 指明缓存分支。日志的 `code` 是内部 ErrorCode，
HTTP 错误头使用 framework ErrorCode；直接结算 RPC 错误使用 wire RpcErrorCode，二者数值基准不同。

原 `run.py` 保留双实例命中、扩容/退出/重启、健康视图持续刷新、下游全退有限失败、
Consul 暂停后陈旧视图拒绝与恢复等 28 项。Consul 仅使用 loopback HTTP agent API 和
TCP 健康检查；没有多数据中心、ACL、权重、熔断或业务级 readiness 承诺。
