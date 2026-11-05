# component_services — 组件式服务：共享、热拔插、hook、observer

一条最小电商业务故事：`PriceBook` 组件保存目录价与购物车行，
`CatalogService` 在启动时写入目录价，`CartService` 在绑定变化时写入购物车行。
两个服务绑定同一个 App scope 的 `"pricing"` 实例，因此互相看到对方写入的
状态——这不是两个服务各建一份副本。

全部代码只用公开 v2 面，不启网络、不依赖 RPC/protobuf、不需要 Redis/Mongo：

| 文件 | 作用 |
|---|---|
| `store.hpp` | 业务复用模块：`PriceBook` 组件、`CatalogService`、`CartService` |
| `component_services_main.cc` | 普通 `main` + 受管协程入口；`shared` / `node` 两种模式 |
| `component_services_run.py` | Python 标准库驱动器：并发拉起自有进程、解析 journal、断言、清理 |
| `CMakeLists.txt` | `component_services` 可执行目标 + `examples.component_services` CTest |

## 演示内容

`shared` 模式在一个 App 内按真实时序跑完：

1. **显式装配**：`CoInstallComponent<PriceBook>("pricing", v1)`；
   `CatalogService` 安装时用 `ServiceOptions::BindComponent<PriceBook>("pricing")`
   声明式绑定；`CartService` 安装后由场景用 `CoBindComponent` 运行期绑定。
2. **受管借用**：服务在 hook 里 `GetComponent<PriceBook>("pricing")` 拿到
   带 binding generation 的 `ComponentHandle`，再 `Borrow()` 取得 lease 后读写。
   组件的 `s_constructed == 1` 与场景借用读到的 `catalog_items=1`、
   `cart_qty=2` 共同证明两个 subject 用的是同一个实例。
3. **热拔插**：解绑两个 subject → `CoUninstallComponent` 卸载旧实例（`OnStop`
   真实挂起 20ms，deadline 2s）→ 用同一逻辑名 `"pricing"` 装入 v2（单价
   1500）。旧 `ComponentHandle::Borrow()` 返回 `Closed`，重装后
   `FindComponent` 的 id/generation 变化，App 仍在 Running 时新组件直接
   Running；新实例状态是干净的（`catalog_items=0`），不继承旧状态。
4. **清晰错误**：绑定不存在的槽位 → `NotFound`；卸载后再绑定 → `NotFound`。
5. **hook 与 observer**：组件 `OnInstall/OnStart/OnStop/OnUninstall` 与服务的
   绑定 `OnBindingChanged(bound=true/false)` 都写入 journal；`CoObserve` 返回的
   `ObserverToken` 在观察窗口内收到状态事件（含旧实例的 `Destroyed`），显式
   `Revoke()` 后 `IsActive()` 为 false 且不再派发新事件。
6. **失败与收口**：第二个 App 装一个 `OnStart` 失败的组件。`CoStart` 返回
   `InternalError` 且 App 诚实停在 Running；组件状态 `Failed`、`last_error`
   相位 `start`，`App::LifecycleFailures()` 有对应记录。最后 `CoStop` 用有限
   deadline 收口，`PendingCleanup()` 为空。

`node` 模式在**独立进程**里用同一个 `store.hpp` 装配自己的组件实例；驱动器
并发启动两个进程（单价 700 / 1300），断言 PID 不同、各自总额只由本进程
配置决定（1400 / 2600）。这证明的是**代码/模块跨进程复用**，不是共享 C++
对象、RPC 或集群。

## 运行

唯一配置、构建与聚焦测试命令见[示例总览的运行入口](../README.md#运行-component_services)。
`NEED_TEST=ON` 会触发仓库强制 wire 验收，因此需要 protobuf 3.21.12 前缀、infra
HTTP/RPC 开关，以及 HEAD 为 `b0ba3e0239c06bf91a874f988187c7e2f56bf13d` 的完整
coroutine Git checkout；无 `.git` 的 archive 树不满足前置条件。

手动 journal 请按总览中的 `build/local` 自有临时目录示例执行。

期望输出（stdout 逐条 `PASS <check> <detail>`，stderr 一行汇总）：

```text
PASS install_pricing_v1                     ok
PASS two_services_share_one_instance        instance_seq=1 catalog_items=1 cart_qty=2
PASS old_handle_closed                      code=...
...
component_services mode=shared checks=47 failures=0 rc=0
```

退出码 `0` = 全部检查通过，`1` = 有失败检查，`3` = 场景超时，`90` = 用法错误，
`6` = journal 打不开。journal 每行是 `event=<名> k=v ...`（值不含空格），
`event=check ... ok=1|0` 是机器判定入口；驱动器同时核对必需 check 集合、
hook 顺序、两个版本的 `cart_total` 以及两个 node 进程的 PID/总额。

`node --price` 接受完整十进制整数，范围为 `0..INT_MAX/2`（含端点）；固定每行
购物车数量为 2，因此此上界保证总额仍能由 `int` 表示。空值、非数值、尾随字符、
负值和越界值均作为用法错误返回 `90`，并在启动 runtime 或打开 journal 前拒绝。

## 边界（不宣称）

- 热拔插只替换 Registry 里的受管组件实例，不是 `dlopen/dlclose`、代码热更新
  或任意状态迁移；卸载前必须先解绑，否则 `CoUninstallComponent` 返回
  `Overloaded`。
- 组件实例共享仅限同一 App/Registry；跨进程只复用同一份业务头。
- 没有注册中心、动态发现、RPC 或分布式一致性；`node` 两进程之间不通信。
- `CoBindObject/CoUnbindObject`、`OnMonitorCollect` 当前没有可用实现路径，
  本示例不使用。
- 本示例不替代 `tests/` 中的框架契约测试；它只演示使用者视角的公开用法。
