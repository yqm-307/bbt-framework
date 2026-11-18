# bbt-framework 测试规范

用例准入、并发时序与回归设计按 `third-party/bbtools-pub/README.md` 的 QA 路径执行。本文规定 framework 选用的测试框架、注册名和执行范围。CI 配置见 [CI 指南](ci.md)。

## 注册新的 framework 用例

目前框架测试集中在 `tests/`。按 `tests/Test_framework_<行为>.cc` 命名，在 `tests/CMakeLists.txt` 定义相应目标并注册 `framework.<行为>`。例如 `Test_framework_v2_core` 对应 `framework.v2_core`。Boost.Test 用例使用 `<boost/test/included/unit_test.hpp>`，由测试程序内嵌实现，不再链接 Boost.Test 动态库。跨进程示例由 `examples/<name>/CMakeLists.txt` 注册为 `examples.*`。

## 1. 测试分层

`unit`、`integration`、`live`、`perf` 标明测试性质。快速用例额外标记 `smoke`，执行脚本用同名 profile 选出这一组。

| 层级 | label/profile | 范围 | 时机 |
|---|---|---|---|
| 冒烟 | `smoke` | 构建后快速确认基本可用性；秒级 | 本地验证、普通代码 PR 和 main 集成 |
| 聚焦 | `focused`（测试名正则或 label 交集） | 本次改动行为 + 直接耦合行为 + 对应回归 | 本地开发；必要 focused 进 PR（见 §3） |
| 完整回归 | `full` | 全部已注册的非 live/perf 用例 | 要求完整回归的专门验收 |
| 真实服务 | `live` | Redis/Mongo 等真实外部服务 | 发布 Gate / 专门手动任务 |
| 性能 | `perf` | 基线、压力、疲劳 | 性能相关的专门验收 |

注册面以 `tests/CMakeLists.txt` 与 `examples/*/CMakeLists.txt` 为准（不在本文复制计数，避免与源码漂移）：

- `smoke` 包含快速单元和集成用例。例如 `framework.v2_core` 标为 `smoke;unit`，`framework.inbound_http` 标为 `smoke;integration`。
- `framework.shutdown` 的真实排空和 `examples.*` 的跨进程协作标为 `integration`，由 full 或命中的 focused 选择。
- `live` 条件注册：`framework.resource_live_{redis,mongo,combined}` 仅在 `bbt::infra_redis_net` /
  `bbt::infra_mongo` 目标在场时注册；`BBT_REQUIRE_RESOURCE_LIVE_TESTS=ON` 时缺模块即 configure 失败。
  环境缺后端时进程以 77 退出配合 `SKIP_RETURN_CODE` 标 Skipped，不伪装通过。
  **注意**：`Test_framework_resource_live.cc` 与 `Test_framework_resource_combined.cc`
  已迁移到当前公共 API（受管组件 `CoInstallComponent` + 声明式 `BindComponent` +
  `Borrow()`，`CoStart/CoWaitShutdownRequest/CoStop` 生命周期）；live 目标可构建
  但未经真实后端运行验证，不能作为通过证据。
- `examples.ecommerce_cluster` 仅在 `bbt::infra_consul` 存在且 `BBT_CONSUL_BINARY` 指向可执行文件时注册。

## 2. 执行契约（统一入口）

从仓库根使用 `scripts/ci/run_test_profile.sh` 选择已构建的测试。

```bash
scripts/ci/run_test_profile.sh --build-dir <dir> --profile <smoke|focused|full> [选项]
```

| profile | 选集语义 |
|---|---|
| `smoke` | `-L smoke` |
| `focused` | 必须给 `--regex <expr>` 或 `--label-expr <expr>`，否则退出码 2；两者都给取交集 |
| `full` | 默认 `-LE 'live\|perf'`；`--include-labels live` 可从排除集放回 |

脚本内 fail-closed 判据：选集为空 → 失败；`--require-count`/`--require-file`
不满足 → 失败；`ctest -j1 --no-tests=error --output-on-failure` 实跑后比对
JUnit XML——注册集≠实跑集、任一 skipped/Not Run/failure/error 均非零退出。
live 这类主动 SKIP 在选集阶段（`-LE live`）就排除，不靠事后容忍。

常用选项：`--timeout`（默认 120，仅对无 TIMEOUT 属性用例生效）、`--junit`
（默认 `<build-dir>/junit-<profile>.xml`）、`--repeat`（透传 ctest `--repeat`）。

## 3. 各环境执行边界

- **普通 PR CI**：`build_test` job 内一次 Release 构建 + `--profile smoke`
  （`--timeout 300`）；`changes` 产出的 `focused_regex` 非空时同一构建树
  追加一次 `--profile focused --regex <regex>`（`changed_files.py` 的
  `FOCUSED_MAP` 把 `bbt/framework/**`、`tests/`、
  `examples/**` 的变更路径映射到直接耦合的 ctest 名；无命中 → 空串 →
  该步显式跳过）。不跑全量 unit/integration/live/perf。见
  `docs/guides/ci.md`。
- **本地开发**按 [README 的构建步骤](../../README.md#构建与测试) 准备测试目标，运行 smoke，再用 focused 加上本次变更和直接耦合的用例。例如在 `build/` 中运行生命周期与 RPC 接口的聚焦测试：

  ```bash
  bash scripts/ci/run_test_profile.sh --build-dir build --profile focused \
    --regex '^framework\.(coapp_lifecycle|rpc_surface)$'
  ```

- **完整回归**使用 `--profile full`。现行 `ci.yml` 执行 smoke 与必要的 focused，`manual.yml` 提供工具链核验和双服务资源验收。需要全量结果时，须单列 full 的执行证据，普通 PR 的绿灯只覆盖该次实际选集。
- **真实资源**需要独立环境。`--profile full --include-labels live` 将当前已注册的 live 用例纳入选集。`manual.yml` 的双服务资源验收使用真实 Redis/Mongo 容器，其覆盖范围以该任务为准。

## 4. 改动测试选择时

改动测试名、label、注册条件或路径映射时，核对 `tests/CMakeLists.txt`、`scripts/ci/changed_files.py` 与实际 profile 的选集。取消、超时、关闭和资源回收的验收采用 QA 路径中的并发用例方法，用同步信号证明目标阶段已经开始，再检查对外结果与资源收口。

## 5. 覆盖不下降的高风险面

普通 CI 收敛为 smoke 后，以下高风险行为的 smoke/focused 覆盖不下降：
公共 API 面（`framework.rpc_surface`/`rpc_codec`）、生命周期与关闭门
（`framework.coapp_lifecycle`/`close_gate`）、并发与邮箱语义
（`framework.actor_mailbox`）、真实 loopback 接线（`framework.inbound_http`/
`ordered_ingress_rpc`/`rpc_xlang`/`rpc_matrix`）、资源装配与关闭收束
（`framework.resource_lifecycle`）。`framework.shutdown` 的长时排空语义
属 integration 层，由 full profile/发布入口承担，不由 smoke 承担。

## 6. 离线验证

```bash
# profile 入口契约（不跑真实 C++ 构建/ctest）
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest scripts/ci/test_test_profile.py
```
