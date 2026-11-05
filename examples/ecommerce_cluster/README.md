# ecommerce_cluster — 动态发现与真实订单结算

一个可执行文件按角色运行，gateway、catalog、order、settlement 各有声明和实现目录，
验收时各自运行在独立进程。业务经 `CoService::call<Req, Resp>` / `ProtoMethod`
发送真实 protobuf RPC；服务地址来自 Consul 健康视图，不注入静态实例地址。

报价模式保留 `Checkout → Quote → Price`，`run.py` 继续验收原 28 项动态发现行为。
存储模式必须显式传 `--storage`，提供独立 Redis/Mongo 配置；缺构建资源或后端即失败。

```text
run_storage.py → Gateway.CreateOrder → Order.CreateOrder → Catalog.Price → Redis
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

使用前设 `INFRA`、`PROTOBUF_PREFIX`、`RESOURCE_PREFIX` 为 `deps.lock` 锁定的正式 infra 源码检出、
同源 protobuf 3.21.12 和锁定资源库安装根目录；infra 检出的 HEAD 须与锁文件一致且工作区干净。
不使用本地未提交的 adapter 或系统同名库补齐依赖。
资源前缀须先核 manifest、源 SHA、ABI、库哈希及 ldd，参见 infra 的
`docs/dependencies/resource-prefixes.provenance.md` 和 `scripts/resource_prefix_verify/`。
Coroutine checkout 必须匹配 infra `deps.lock` 的完整 40 字符 pin。

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
各启动、请求、命令、关闭均有超时。结果 JSON 保存实际 `checks`、`failures`、PID、
镜像与容器 ID、清理结果及真实进程 rc；任何检查/清理失败时驱动返回非零。

手工运行各角色时传同一组存储配置和本次自有 Consul 入口，例如：

```bash
build/local/examples/ecommerce_cluster/ecommerce_cluster order \
  --consul "$CONSUL_HTTP" --storage --redis "$REDIS_ENDPOINT" \
  --mongo "$MONGO_URI" --database "$RUN_DATABASE" --key-prefix "$RUN_KEY_PREFIX"
```

把 `order` 分别换成 `gateway`、`catalog`、`settlement` 启动四个进程；每个进程
输出动态 `event=ready endpoint=...`，stdin EOF 触发注销和优雅关闭。Redis 仅由
Catalog 的 `RedisNetResource` 使用；Order/Settlement 各由 `MongoDbResource` 持有
自己的集合资源，均经 `CoApp::add_resource` 工厂创建并由宿主关闭。Mongo URI 建议
传 `retryWrites=false`，明确关闭驱动层写重试；示例 RPC 自身也不自动重试写请求。

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
JSON RPC 错误的 `code` 是 wire RpcErrorCode，二者的数值基准不同。

原 `run.py` 保留双实例命中、扩容/退出/重启、健康视图持续刷新、下游全退有限失败、
Consul 暂停后陈旧视图拒绝与恢复等 28 项。Consul 仅使用 loopback HTTP agent API 和
TCP 健康检查；没有多数据中心、ACL、权重、熔断或业务级 readiness 承诺。
