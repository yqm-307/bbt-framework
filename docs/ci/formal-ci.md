# bbt-framework 正式普通 CI：`ci.yml`（唯一权威普通入口）

本文件描述 bbt-framework 的正式普通 CI：`.github/workflows/ci.yml`。它由已独立审查并在线
验收的 hosted 影子候选（Issue #50/#53）升级而来，把现役普通 build→test（`ctest -j1`）判据迁到
**hosted-only**（`ubuntu-24.04`，无自托管 runner、无缓存）的两台独立 runner 上，并复用已发布的
framework 分类/结果 reusable callee。影子入口 `.github/workflows/ci-shadow-v1.yml` 已删除，普通
check 只有本文件一个 producer（**非双跑**）。

> 状态：本次为**本地候选**（未 push / 未建 PR / 未触发 Actions）。真实 Boost 下载、依赖 clone、
> protobuf debs 下载、C++ 构建与 19 个 CTest 均属 `UNVERIFIED`（见文末「在线未验」）。

## 1. 文件与操作范围

| 文件 | 作用 |
|---|---|
| `.github/workflows/ci.yml` | 正式普通 CI：`changes → plan → build → test → result` 五个 hosted job + `toolchain-verify` / `resource-acceptance` 两个手动 job |
| `scripts/ci/changed_files.py` | 从真实 git diff 归一受限变更集（通用 recipe，见文件头来源） |
| `scripts/ci/prepare_boost.sh` | 按锁定版本/sha256 源码构建 Boost 1.90 `context` 到工作区私有前缀 |
| `scripts/ci/run_framework_gate.sh` | build 侧「构建+门禁+provenance+封包」与接收侧「sha256 校验+闭集/穿越/symlink 门禁+安全解包」 |
| `scripts/ci/test_formal_ci.py` | 本正式 workflow 的离线冒烟与直接耦合测试（不跑 C++ 全量构建） |
| `docs/ci/formal-ci.md` | 本文件 |

**升级方式**：`.github/workflows/ci.yml` 由影子候选升级；`.github/workflows/ci-shadow-v1.yml` 与
`scripts/ci/test_shadow_ci.py` 已删除（去重复入口与对应影子测试）。

**明确不改**：`AGENTS.md`、`scripts/{fetch_deps,prepare_protobuf,local_build,build_stack,run_ctest,verify_toolchain}.sh`、
`scripts/ci/{changed_files.py,prepare_boost.sh,run_framework_gate.sh}`（复用 recipe，未改）、
`scripts/ci/shared/**`（公共契约真源）、`CMakeLists.txt` 及子目录、`deps.lock`、`docker/toolchain.lock`、
`docker/**`、产品 C++、`examples/**`、`tests/**`。本文件**不复制**分类/结果公共契约，也不新增第二 public API。

## 2. 判据等价与权威身份

产品构建/CTest 判据沿现役；hosted 的 Boost runtime 私有前缀解析检查是额外的工具链闭包
断言，不是现役门禁已有条件。它要求 `libboost_context` 实际解析到 `$BOOST_PREFIX/lib/`，
不能只靠同名系统库通过。

- **同一配方**：`scripts/fetch_deps.sh`（`deps.lock` 固定 SHA）→ `scripts/prepare_protobuf.sh`（锁定
  3.21.12 前缀）→ `scripts/local_build.sh` → `scripts/build_stack.sh`；dual-service 负向门禁、链接来源
  证据、`ctest -j1 --timeout 300 --output-on-failure --no-tests=error` 与 SKIP/Not Run 门禁，均与升级前
  逐条对应（由 `scripts/ci/test_formal_ci.py` 的耦合测试对照，防漂移）。
- **权威身份**：workflow 名保持 `ci`，job 名保持现役 required check 名（`变更类型检测` /
  `Build (pinned deps)` / `Test (ctest -j1)`），避免 required context 悬空；新增 `分类/计划`
  与 `结果汇聚` 两个 callee 消费 job。
- **分类/结果**：`plan` 与 `result` job 通过 `uses:` 调用**已发布、fixed-SHA 的 callee**
  `yqm-307/bbt-framework/.github/workflows/bbtools-classify-v1.yml@1c0b0fb0ebc8e7ca3888aa4f6a74d1baecf08cc7`；
  `changed_files` 由本仓真实 diff 计算后作为**受限数据**传入，`source`（`github.sha`）与 callee
  automation 身份由 callee 自身 `job.workflow_*` 分离解析（caller-callee 分离）。

## 3. 配置（workflow 关键项）

- 触发面：`pull_request`（`main`）、`push`（`main`）与 `workflow_dispatch`（手动）。手动入口保留
  `resource_acceptance` 布尔输入。无 `schedule` / `merge_group`。
- 权限：顶层 `permissions: {}`；每个 job `permissions: {contents: read}`；无 secret、无 OIDC、无写权限。
- concurrency：main/手动每次提交一 run（group 含 `github.run_id`，互不取消，保逐提交结果）；
  PR 只取消**同一 PR** 的旧 run。
- runner：普通 job（`changes`/`build`/`test`）固定 `ubuntu-24.04`；`plan`/`result` 为 reusable callee
  调用（callee 自身 `ubuntu-latest`）；两个手动 job 保留原 `arc-s4-framework`（见 §5）。
- 超时：`changes` 5min、`build` 75min（hosted 冷构建预算，不放宽 CTest 300s）、`test` 20min。
- 远程 `uses` 全部完整 40-hex pin：`actions/checkout@11d5960a326750d5838078e36cf38b85af677262`、
  `actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02`、
  `actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093`。
- 顶层 env 采用与 build 侧同口径的绝对路径（`BBT_WORK_DIR` / `BBT_BUILD_DIR` / `BBT_DEPS_DIR` /
  `BBT_PROTOBUF_PREFIX` / `BOOST_PREFIX`），保证 build 树 RUNPATH 与绝对路径在 artifact 传递到
  test runner 后仍有效。

### 3.1 build → test 跨独立 runner 的闭包

build 与 test 是**两台独立 hosted runner**，构建树经 artifact 传递。为保证 test 侧能使用**实际**
Boost runtime 与 protobuf 工具链：

- **Boost runtime 随归档**：`scripts/ci/prepare_boost.sh` 把锁定 Boost 1.90 `context` 编到
  `$GITHUB_WORKSPACE/boost-prefix`；build 归档**只含 `boost-prefix/lib`**（`.so` 运行库，**不含 headers**，
  test 侧**不重复下载/重编**）。绝对前缀在 test runner 上一致，`LD_LIBRARY_PATH` 指向该前缀，`ldd`
  断言 `libboost_context` 真实可解析（hosted 无 `/opt/boost`）。
- **protobuf 前缀在 test 侧同路径重建**：CTest 配置把 `getvalue` 的 `--protoc/--protoc-libdir` 固化为
  绝对路径；test 是新 runner 新 checkout，故复用 build 侧同一 `scripts/prepare_protobuf.sh` 在**相同
  绝对路径** `$GITHUB_WORKSPACE/.deps/protobuf-3.21.12` 重建锁定前缀（只解包 debs，不装系统包、不碰
  系统 `protoc`）。
- **健康检查**：解包后恢复 `tests/Test_framework_*`、`tests/rpc_xlang_server`、`examples/{getvalue/getvalue_server,
  getvalue/getvalue_caller,two_service/two_service,lifecycle_matrix/lifecycle_fixture}` 执行位（缺任一即失败）。

### 3.2 artifact 纪律

- **same-run only**：下载 `bbt-build-<run_id>`（`download-artifact` 默认仅本 run），名含 `run_id`。
- **真实 tar 摘要**：build 侧对归档算 `sha256`（真实 bytes），作为 job output 传给 test；test 侧**先按该
  摘要 `sha256sum --check --strict` 校验，再解包**。摘要非法/缺失即拒绝，绝不退化为「无校验解包」。
- **闭集成员 / 路径穿越 / symlink 门禁**：`scripts/ci/run_framework_gate.sh verify-archive` 在解包前逐条校验
  成员名（禁绝对路径、禁 `..`、禁空段）、成员必须落在闭集前缀 `.ci-work` 与 `boost-prefix/lib` 内、
  拒绝设备/命名管道/硬链接等非闭集类型、拒绝目标越界的 symlink；再以「realpath 必须落在 dest 内」的方式
  逐条安全解包，**不产生越界 host 写入**。
- **producer/source/run/attempt 审计**：build 侧用**已发布 shared** `scripts/ci/shared/cli.py produce` 产出
  有限 provenance manifest（`source_repo/source_sha/source_head/automation_repo/automation_sha/run_id/
  run_attempt/job`），摘要由 shared 逻辑对真实写入字节计算；test 侧用同一 `cli.py verify-receipt` 复核身份
  （repo / source_sha / workflow_sha / job / run_id / run_attempt / payload_digest）。其中 attempt 来自
  build 输出 `producer_run_attempt`，不是消费者当前 attempt；失败 test 重跑仍验证原成功 build
  身份，缺失或改绑 attempt 继续拒绝。**不新增第二 public API。**

### 3.3 门禁清单（不得放宽）

- **no-SKIP**：`grep -Eq '^[ ]*[0-9]+/[0-9]+ Test #[0-9]+:.*\*\*\*(Skipped|Not Run)'` 命中即失败；并要求
  `100% tests passed, 0 tests failed`。
- **来源门禁**：`link-sources.txt` 命中 `/usr/local/` 即失败（build 侧与 test 侧各一次）；`build_stack.sh`
  额外做 `deps.lock` 逐条 SHA 对账（off-pin 即失败）。
- **dual-service 负向门禁**：默认构建树必须保持 `BBT_ENABLE_DUAL_SERVICE_EXAMPLE=OFF` 且不含
  `examples/dual_service`；显式 `ON` 且缺前缀（不传 hiredis/mongo 前缀）必须 configure 失败且失败原因来自
  该前置条件门禁。**不真实运行 Redis/Mongo**（真实运行在手动 `resource-acceptance`，见 §5）。
- **ldd**：`Test_framework_*` 与 `rpc_xlang_server` 不得有 `not found`；不得链接 `/usr/local` 下的 bbt 旧产物；
  零命中可执行产物即失败。

## 4. 测试数（准确值）

`-DNEED_TEST=ON` 且 `bbt_infra_rpc` 注册（锁定 protobuf 前缀）时的默认配置共注册 **19** 个 CTest：

`tests/`（15）：`Test_framework_f0`、`Test_framework_f2a`、`Test_framework_f2b1`、`Test_framework_f2b2`、
`Test_framework_f2b3`、`Test_framework_f1a`、`Test_framework_f1b1`、`Test_framework_f1b2`、`Test_framework_f3`、
`Test_framework_rpc`、`Test_framework_resource`、`framework.rpc_xlang`、`framework.egress.phase`、
`framework.getvalue.codec`、`framework.matrix`；

`examples/`（4）：`examples.getvalue.xlang`、`examples.getvalue.s2s`、`examples.two_service`、
`examples.lifecycle_matrix`。

`framework.resource.live.redis` / `.mongo` / `.combined` 在无 `hiredis/mongo` 前缀时**不注册**（因此默认
19 个用例中**不产生 SKIP**）。dual_service 示例（`svc_a/svc_b/driver`）默认不构建。

## 5. 手动任务保留（原现役 `ci.yml`）

以下两个手动任务原样保留（非 required，仅 `workflow_dispatch` 运行；不迁专用 ARC/容器、不改镜像）：

- `Toolchain identity (candidate, manual)`：`runs-on: arc-s4-framework`，跑
  `scripts/verify_toolchain.sh --in-image --lock "$GITHUB_WORKSPACE/docker/toolchain.lock"`，证明「实际
  runner 用的工具链」满足本仓 lock。
- `Dual-service resource acceptance (manual)`：`runs-on: arc-s4-framework`，仅在 `inputs.resource_acceptance == true`
  时运行；资源边界不变（`RESOURCE_RECIPE_SHA=1543bbd0…`、Redis/Mongo 镜像 digest、`.resource-recipe`/
  `.resource-deps`/`.dual-resource-work` 专用前缀与清理），只消费 infra 唯一 recipe，不复制 hiredis/mongo 构建逻辑。

## 6. 固定输入（fixed SHA / 锁定值）

- callee：`yqm-307/bbt-framework/.github/workflows/bbtools-classify-v1.yml@1c0b0fb0ebc8e7ca3888aa4f6a74d1baecf08cc7`。
- dependencies：`deps.lock`（`coroutine sha=7bcda3b078f975ff2978424be7f6ba38e04fb7f6`、
  `infra sha=162bb5fd1340e8b3c043086e8ed753aa5ec533cb`）。
- Boost：`docker/toolchain.lock` 的 `BOOST_VERSION=1.90.0`、`BOOST_SHA256=5e93d582aff26868d581a52ae78c7d8edf3f3064742c6e77901a1f18a437eea9`（只编 `context`）。
- protobuf：`docker/toolchain.lock` 的 `PROTOC_VERSION=3.21.12`；debs 与 sha256 来自
  `scripts/prepare_protobuf.sh` 的锁定包集。

## 7. 离线验证命令（本次实际执行）

```bash
# 配方守卫 + archive 接收侧门禁 + changed_files 路由 + 文本/手动任务契约（stdlib，恒跑）：
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest scripts/ci/test_formal_ci.py

# 追加结构契约（需 PyYAML）；离线复用 uv-cache，禁联网新增依赖：
UV_OFFLINE=1 UV_CACHE_DIR=<cache> uv run --no-project --with pyyaml \
  python3 scripts/ci/test_formal_ci.py

# 追加分类路由冒烟（用同一真实 cli.py 校验正式受限输入，不复制契约）：
BBT_CI_SHARED_DIR=<framework>/scripts/ci/shared python3 scripts/ci/test_formal_ci.py
```

结果见 `scripts/ci/test_formal_ci.py` 运行输出；无 PyYAML / 无 shared 时对应用例**显式 SKIP**（`OK (skipped=N)`），
**不等于通过**。`archive` 门禁用 synthetic 归档（非真实 C++ 产物）覆盖正向 + 穿越/绝对/闭集外/逃逸 symlink/
设备成员等拒绝路径。

## 8. 回滚 / 恢复

正式 `ci.yml` 升级与影子删除同一变更包；回滚 = 恢复升级前 `.github/workflows/ci.yml`、恢复
`.github/workflows/ci-shadow-v1.yml` 与 `scripts/ci/test_shadow_ci.py`、删除本文件与
`scripts/ci/test_formal_ci.py`。recipe 脚本（`changed_files.py`/`prepare_boost.sh`/`run_framework_gate.sh`）
与 `scripts/ci/shared/**` 全程未改，无需恢复。

## 9. 在线未验（UNVERIFIED）

以下路径**尚未在真实 GitHub Actions 上以本正式文件运行**（影子候选曾在线通过，见 Issue #53；正式
文件首次由父级授权的 PR/push/main 在线 CI 完成）：

- 本正式 `ci.yml` 在 `pull_request`/`push main`/`workflow_dispatch` 上的真实触发与 required check 映射；
- `scripts/ci/prepare_boost.sh` 下载 Boost 归档并源码编译 `context`（网络 + 编译）；
- `scripts/fetch_deps.sh` / `scripts/prepare_protobuf.sh` 的联网 clone 与 debs 下载；
- `scripts/local_build.sh` 的完整 C++ 构建与 19 个 CTest 的真机通过；
- build→test 两个**独立** runner 上 `libboost_context` / protoc 绝对路径闭包的真实可用性；
- callee（`plan` / `result`）在四 job 汇聚下的真实判定与 rerun 复用 producer attempt 的跨 attempt 行为；
- 两个手动 job 在 `arc-s4-framework` 上的真实运行。

上述任一若在真机失败，即按 `fail-closed` 暴露，不视为本候选已通过。
