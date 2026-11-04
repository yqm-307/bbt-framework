# bbt-framework 测试规范

本文是 bbt-framework 测试分层、执行 profile 与验证边界的唯一落点，落实三仓
（bbt-framework / bbtools-infra / bbtools-coroutine）统一测试标准化规范。
编写测试的命名、label 与编码约定以 `docs/conventions.md` §6 为准；CI 组成与
门禁以 `docs/guides/ci.md` 为准，本文不复制其步骤。

## 1. 测试分层

label 表达测试性质（`unit`/`integration`/`live`/`perf`），profile 表达执行
策略（`smoke`/`focused`/`full`），两层不混用：

| 层级 | label/profile | 范围 | 时机 |
|---|---|---|---|
| 冒烟 | `smoke` | 构建后快速确认基本可用性；秒级 | 每次代码 PR + main 集成 |
| 聚焦 | `focused`（测试名正则或 label 交集） | 本次改动行为 + 直接耦合行为 + 对应回归 | 本地开发；必要 focused 进 PR（见 §3） |
| 完整回归 | `full` | 全部非 live/perf 用例 | 手动、定时、发布 |
| 真实服务 | `live` | Redis/Mongo 等真实外部服务 | 发布 Gate / 专门手动任务 |
| 性能 | `perf` | 基线、压力、疲劳 | 性能相关改动、定时、发布 |

当前注册面（`-DNEED_TEST=ON` + `bbt_infra_rpc_net` 在场，22 项）：

- `smoke` 15 项：全部 `unit` 用例 + 快速 `integration`（`framework.close_gate`、
  `framework.inbound_http`、`framework.ordered_ingress_http`、`framework.rpc_xlang`、
  `framework.egress_request_phase`、`framework.rpc_matrix`）。
- 非 smoke `integration` 7 项：`framework.shutdown`（真实 runtime 排空，
  全套实测 ~107s）、`framework.rpc_profile`、5 个 `examples.*`（跨进程真实
  协作）——不进普通 CI smoke。
- `live` 条件注册：`framework.resource_live_{redis,mongo,combined}` 仅在
  `bbt::infra_redis_net`/`bbt::infra_mongo` 目标在场时注册；环境缺后端时进程
  以 77 退出配合 `SKIP_RETURN_CODE` 标 Skipped，不伪装通过。

## 2. 执行契约（统一入口）

所有 profile 走同一入口 `scripts/ci/run_test_profile.sh`（与 infra/coroutine
仓逐字节相同的契约脚本）：

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
  `FOCUSED_MAP` 把 `framework/src/**`、`framework/include/**`、`tests/`、
  `examples/**` 的变更路径映射到直接耦合的 ctest 名；无命中 → 空串 →
  该步显式跳过）。不跑全量 unit/integration/live/perf。见
  `docs/guides/ci.md`。
- **本地开发**：构建经 `flock /tmp/bbt-build.lock` 串行、显式 `--parallel 2`；
  默认验证为 smoke + 本次改动的 focused 集：

  ```bash
  flock /tmp/bbt-build.lock bash scripts/build_stack.sh   # 或 local_build.sh（BBT_SKIP_CTEST=1）
  bash scripts/ci/run_test_profile.sh --build-dir <build-dir> --profile smoke
  bash scripts/ci/run_test_profile.sh --build-dir <build-dir> --profile focused \
    --regex '^framework\.(shutdown|rpc_profile)$'
  ```

  本地不跑全量；完整回归走 CI 手动/定时入口。
- **完整回归/发布**：`--profile full`（排除 live|perf）覆盖全部本地可跑用例；
  live 验收经 `--include-labels live` 放回或由 manual.yml 的
  `resource-acceptance` 任务承担（真实 Redis/Mongo 容器）。

## 4. 新增测试的必要性判定

新增测试必须回答：保护哪条公共行为/错误语义/状态转换/资源生命周期；输入或
事件序列；断言观察的结果/状态/副作用；能区分哪种具体错误。

- **必须新增或修改**：修复真实 bug；新增公共 API/错误语义/状态转换；改动
  取消、超时、关闭、回收、并发、重入；改协议/wire/跨模块边界；改测试选择
  逻辑（label/注册条件/profile 映射）。
- **不新增**：只为凑数；重复已有输入断言；只断言 mock 被调用；只断言进程
  返回 0；复制 fixture；无新风险的复杂代码。

## 5. 覆盖不下降的高风险面

普通 CI 收敛为 smoke 后，以下高风险行为的 smoke/focused 覆盖不下降：
公共 API 面（`framework.rpc_surface`/`rpc_codec`）、生命周期与关闭门
（`framework.coapp_lifecycle`/`close_gate`）、并发与邮箱语义
（`framework.actor_mailbox`）、真实 loopback 接线（`framework.inbound_http`/
`ordered_ingress_http`/`rpc_xlang`/`rpc_matrix`）、资源装配与关闭收束
（`framework.resource_lifecycle`）。`framework.shutdown` 的长时排空语义
属 integration 层，由 full profile/发布入口承担，不由 smoke 承担。

## 6. 离线验证

```bash
# profile 入口契约（不跑真实 C++ 构建/ctest）
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest scripts/ci/test_test_profile.py
```
