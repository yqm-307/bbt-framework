# bbt-framework hosted-only 普通 CI 影子：`ci-shadow-v1`（Issue #50）

本文件描述一个**新增的、仅本地候选**的 GitHub Actions workflow：`.github/workflows/ci-shadow-v1.yml`。
它把 bbt-framework 现役 `.github/workflows/ci.yml` 的 build→test（`ctest -j1`）判据，复刻到
**hosted-only**（`ubuntu-24.04`，无自托管 runner、无缓存）的两台独立 runner 上，并复用已发布的
framework 分类/结果 reusable callee。**不发布、不开 PR、不合并、不改现役 `ci.yml` 或任何已跟踪文件。**

> 状态：本候选**未在线运行**（未 push / 未建 PR / 未触发 Actions）。真实 Boost 下载、依赖 clone、
> protobuf debs 下载、C++ 构建与 19 个 CTest 均属 `UNVERIFIED`（见文末「在线未验」）。

## 1. 新增文件与操作范围

| 文件 | 作用 |
|---|---|
| `.github/workflows/ci-shadow-v1.yml` | 影子 workflow：`changes → plan → build → test → result` 五个 job |
| `scripts/ci/changed_files.py` | 从真实 git diff 归一受限变更集（通用 recipe，见文件头来源） |
| `scripts/ci/prepare_boost.sh` | 按锁定版本/sha256 源码构建 Boost 1.90 `context` 到工作区私有前缀 |
| `scripts/ci/run_framework_gate.sh` | build 侧「构建+门禁+provenance+封包」与接收侧「sha256 校验+闭集/穿越/symlink 门禁+安全解包」 |
| `scripts/ci/test_shadow_ci.py` | 离线冒烟与直接耦合测试（不跑 C++ 全量构建） |
| `docs/ci/framework-shadow-v1.md` | 本文件 |

**明确不改**（0 改动、0 diff）：现役 `.github/workflows/ci.yml`、其它 `bbtools-*.yml`、`AGENTS.md`、
`scripts/{fetch_deps,prepare_protobuf,local_build,build_stack,run_ctest,verify_toolchain}.sh`、
`scripts/ci/shared/**`（公共契约真源）、`CMakeLists.txt` 及子目录、`deps.lock`、`docker/toolchain.lock`、
`docker/**`、产品 C++、`examples/**`、`tests/**`。影子**不复制**分类/结果公共契约，也不新增第二 public API。

## 2. 判据等价与独立身份

产品构建/CTest 判据沿现役；hosted 的 Boost runtime 私有前缀解析检查是额外的工具链闭包
断言，不是现役门禁已有条件。它要求 `libboost_context` 实际解析到 `$BOOST_PREFIX/lib/`，
不能只靠同名系统库通过。

- **同一配方**：`scripts/fetch_deps.sh`（`deps.lock` 固定 SHA）→ `scripts/prepare_protobuf.sh`（锁定
  3.21.12 前缀）→ `scripts/local_build.sh` → `scripts/build_stack.sh`；dual-service 负向门禁、链接来源
  证据、`ctest -j1 --timeout 300 --output-on-failure --no-tests=error` 与 SKIP/Not Run 门禁，均与
  `ci.yml` 逐条对应（由 `scripts/ci/test_shadow_ci.py` 的耦合测试对照，防漂移）。
- **独立身份**：workflow 名 `ci-shadow-v1`、concurrency group 前缀 `ci-shadow-v1-`、artifact 名
  `bbt-shadow-build-<run_id>`（不污染现役 `bbt-build-<run_id>`），不使用 ccache / GitHub cache 命名空间。
- **分类/结果**：`plan` 与 `result` job 通过 `uses:` 调用**已发布、fixed-SHA 的 callee**
  `yqm-307/bbt-framework/.github/workflows/bbtools-classify-v1.yml@1c0b0fb0ebc8e7ca3888aa4f6a74d1baecf08cc7`；
  `changed_files` 由本仓真实 diff 计算后作为**受限数据**传入，`source`（`github.sha`）与 callee
  automation 身份由 callee 自身 `job.workflow_*` 分离解析。

## 3. 配置（workflow 关键项）

- 触发面：仅 `push` 到分支 `ci/issue-50-framework-hosted-shadow`，以及指向 `main` 的 `pull_request`。
  无 `workflow_dispatch` / `schedule` / `merge_group`。
- 权限：顶层 `permissions: {}`；每个本地 job `permissions: {contents: read}`；无 secret、无 OIDC、无写权限。
- concurrency：push 每次提交一 run（group 含 `github.run_id`，互不取消）；PR 只取消**同一 PR** 的旧 run。
- runner：本地 job 固定 `ubuntu-24.04`（reusable callee 自身 `ubuntu-latest`）；`BBT_JOBS=3`。
- 超时：`changes` 5min、`build` 75min（hosted 冷构建预算，不放宽 CTest 300s）、`test` 20min。
- 远程 `uses` 全部完整 40-hex pin：`actions/checkout@11d5960a326750d5838078e36cf38b85af677262`、
  `actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02`、
  `actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093`。
- 顶层 env 采用与 `ci.yml` 同名的 `BBT_WORK_DIR` / `BBT_BUILD_DIR` / `BBT_DEPS_DIR`，值为
  `$GITHUB_WORKSPACE` 下的**绝对**路径，保证 build 树 RUNPATH 与绝对路径在 artifact 传递到 test runner 后仍有效。

### 3.1 build → test 跨独立 runner 的闭包

build 与 test 是**两台独立 hosted runner**，构建树经 artifact 传递。为保证 test 侧能使用**实际**
Boost runtime 与 protobuf 工具链：

- **Boost runtime 随归档**：`scripts/ci/prepare_boost.sh` 把锁定 Boost 1.90 `context` 编到
  `$GITHUB_WORKSPACE/boost-prefix`；build 归档**只含 `boost-prefix/lib`**（`.so` 运行库，**不含 headers**，
  test 侧**不重复下载/重编**）。绝对前缀在 test runner 上一致，`LD_LIBRARY_PATH` 指向该前缀，`ldd`
  断言 `libboost_context` 真实可解析（hosted 无 `/opt/boost`）。
- **protobuf 前缀在 test 侧同路径重建**：`ci.yml` 的 CTest 配置把 `getvalue` 的 `--protoc/--protoc-libdir`
  固化为绝对路径；test 是新 runner 新 checkout，故复用 build 侧同一 `scripts/prepare_protobuf.sh` 在
  **相同绝对路径** `$GITHUB_WORKSPACE/.deps/protobuf-3.21.12` 重建锁定前缀（只解包 debs，不装系统包、不碰
  系统 `protoc`）。
- **健康检查**：解包后恢复 `tests/Test_framework_*`、`tests/rpc_xlang_server`、`examples/{getvalue/getvalue_server,
  getvalue/getvalue_caller,two_service/two_service,lifecycle_matrix/lifecycle_fixture}` 执行位（缺任一即失败）。

### 3.2 artifact 纪律

- **same-run only**：下载 `bbt-shadow-build-<run_id>`（`download-artifact` 默认仅本 run），名含 `run_id`。
- **真实 tar 摘要**：build 侧对归档算 `sha256`（真实 bytes），作为 job output 传给 test；test 侧**先按该
  摘要 `sha256sum --check --strict` 校验，再解包**。摘要非法/缺失即拒绝，绝不退化为「无校验解包」。
- **闭集成员 / 路径穿越 / symlink 门禁**：`scripts/ci/run_framework_gate.sh verify-archive` 在解包前逐条校验
  成员名（禁绝对路径、禁 `..`、禁空段）、成员必须落在闭集前缀 `.shadow-work` 与 `boost-prefix/lib` 内、
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
  该前置条件门禁。**不真实运行 Redis/Mongo**。
- **ldd**：`Test_framework_*` 与 `rpc_xlang_server` 不得有 `not found`；不得链接 `/usr/local` 下的 bbt 旧产物；
  零命中可执行产物即失败。

## 4. 测试数（准确值）

`-DNEED_TEST=ON` 且 `bbt_infra_rpc` 注册（锁定 protobuf 前缀）时的默认配置共注册 **19** 个 CTest：

`tests/`（15）：`Test_framework_f0`、`Test_framework_f2a`、`Test_framework_f2b1`、`Test_framework_f2b2`、
`Test_framework_f2b3`、`Test_framework_f1a`、`Test_framework_f1b1`、`Test_framework_f1b2`、
`Test_framework_f3`、`Test_framework_rpc`、`Test_framework_resource`、`framework.rpc_xlang`、
`framework.egress.phase`、`framework.getvalue.codec`、`framework.matrix`；

`examples/`（4）：`examples.getvalue.xlang`、`examples.getvalue.s2s`、`examples.two_service`、
`examples.lifecycle_matrix`。

`framework.resource.live.redis` / `.mongo` / `.combined` 在无 `hiredis/mongo` 前缀时**不注册**（因此本影子的
19 个用例中**不产生 SKIP**；这也与现役 `ci.yml` 的默认构建一致）。dual_service 示例（`svc_a/svc_b/driver`）
默认不构建。

## 5. 固定输入（fixed SHA / 锁定值）

- callee：`yqm-307/bbt-framework/.github/workflows/bbtools-classify-v1.yml@1c0b0fb0ebc8e7ca3888aa4f6a74d1baecf08cc7`。
- dependencies：`deps.lock`（`coroutine sha=7bcda3b078f975ff2978424be7f6ba38e04fb7f6`、
  `infra sha=162bb5fd1340e8b3c043086e8ed753aa5ec533cb`）。
- Boost：`docker/toolchain.lock` 的 `BOOST_VERSION=1.90.0`、`BOOST_SHA256=5e93d582aff26868d581a52ae78c7d8edf3f3064742c6e77901a1f18a437eea9`（只编 `context`）。
- protobuf：`docker/toolchain.lock` 的 `PROTOC_VERSION=3.21.12`；debs 与 sha256 来自
  `scripts/prepare_protobuf.sh` 的锁定包集。

## 6. release / perf 边界

本影子**不含** release、发布、性能比较、基线读取/写入、memcheck、sanitizer、定期/手动触发；这些仍由现役
流程（如现役 `ci.yml` 的 `toolchain-verify` / `resource-acceptance` 等）承担。影子失败**不改变**现役权威结果，
也不新增 required check。

## 7. 离线验证命令（本次实际执行）

```bash
# 配方守卫 + archive 接收侧门禁 + changed_files 路由 + 文本契约（stdlib，恒跑）：
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest scripts/ci/test_shadow_ci.py

# 追加结构契约（需 PyYAML）；离线复用 uv-cache，禁联网新增依赖：
UV_OFFLINE=1 UV_CACHE_DIR=<cache> uv run --no-project --with pyyaml \
  python3 scripts/ci/test_shadow_ci.py

# 追加分类路由冒烟（用同一真实 cli.py 校验影子受限输入，不复制契约）：
BBT_CI_SHARED_DIR=<framework>/scripts/ci/shared python3 scripts/ci/test_shadow_ci.py
```

结果：全量 `Ran 65 tests ... OK`；无 PyYAML / 无 shared 时对应用例**显式 SKIP**（`OK (skipped=N)`），
**不等于通过**。`archive` 门禁用 synthetic 归档（非真实 C++ 产物）覆盖正向 + 穿越/绝对/闭集外/逃逸 symlink/
设备成员等拒绝路径。

## 8. 回滚 / 恢复

影子与现役完全隔离：**删除本文件列出的 6 个新增文件即可回到现役状态**，已跟踪文件**零 diff**（无需
`git checkout` 恢复）。不影响现役 `ci.yml`、runner、required rules、release 或任何依赖锁。

## 9. 在线未验（UNVERIFIED）

以下路径**尚未在真实 GitHub Actions 上运行**，首次由父级授权的 PR CI 完成：

- `scripts/ci/prepare_boost.sh` 下载 Boost 归档并源码编译 `context`（网络 + 编译）；
- `scripts/fetch_deps.sh` / `scripts/prepare_protobuf.sh` 的联网 clone 与 debs 下载；
- `scripts/local_build.sh` 的完整 C++ 构建与 19 个 CTest 的真机通过；
- build→test 两个**独立** runner 上 `libboost_context` / protoc 绝对路径闭包的真实可用性；
- callee（`plan` / `result`）在三 runner 汇聚下的真实判定。

上述任一若在真机失败，即按 `fail-closed` 暴露，不视为本候选已通过。
