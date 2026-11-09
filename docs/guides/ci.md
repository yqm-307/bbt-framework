# bbt-framework CI 说明

本文是 bbt-framework 持续集成的唯一落点文档（`docs/ci/` 已收敛至此）。它描述当前
`.github/workflows/ci.yml` 与 `.github/workflows/manual.yml` 的组成、门禁清单与固定
输入；workflow 文件本身是权威定义，本文不复制其步骤，只给门禁索引与语义。

## 1. 组成

| 文件 | 作用 |
|---|---|---|
| `.github/workflows/ci.yml` | 普通 CI 唯一入口：`changes → docs_check → build_test → result` 四个 hosted job（`ubuntu-24.04`）。`build_test` 在单 runner 内完成 Release 构建 + smoke profile 测试（必要时同一构建树追加 focused），不跨 runner 传归档；`workflow_dispatch` 仅用于手动重跑同一普通流程 |
| `.github/workflows/manual.yml` | 手动任务（仅 `workflow_dispatch`，非 required）：`toolchain-verify` 与 `resource-acceptance`（`arc-s4-framework`），见 §7 |
| `scripts/ci/changed_files.py` | 从真实 git diff 归一受限变更集，完成路径分类（本仓内联，见 §2），并按 `FOCUSED_MAP` 前缀映射产出 `focused_regex`（见 §5 必要 focused） |
| `scripts/ci/result.py` | result job 的本仓内联汇总：needs.*.result → verdict，fail-closed（见 §3） |
| `scripts/ci/prepare_boost.sh` | 按锁定版本/sha256 源码构建 Boost 1.90 `context` 到工作区私有前缀；可选 `BOOST_SOURCE_CACHE` 只复用已校验的原始 tar.gz（不缓存 prefix） |
| `scripts/ci/run_framework_gate.sh` | `build` 子命令承载 build_test 的「构建+门禁+provenance+封包」；`verify-archive` 接收侧子命令仍在仓内（sha256 校验+闭集/穿越/symlink 门禁+安全解包），当前普通 workflow 无跨 runner 传递，不由其调用 |
| `scripts/ci/check_docs.py` | docs-check：相对链接有效、docs 文件名无过程编号、README 行数上限 |
| `scripts/ci/check_format_diff.sh` | 只检查本 PR 改动行的 clang-format 违规（changes 内 `continue-on-error`，告警不阻断） |
| `scripts/ci/run_test_profile.sh` | 统一测试执行入口：按 profile 选集（`smoke`/`focused`/`full`）→ 注册集=实跑集校验 → skipped/Not Run/failure/error 一律失败；build_test 经其跑 `--profile smoke`，`focused_regex` 非空时追加 `--profile focused`，与 bbtools-infra / bbtools-coroutine 同一份脚本（逐字节相同） |
| `scripts/ci/test_changed_files.py` | 分类规则 + result 契约单测（仅标准库） |
| `scripts/ci/test_check_docs.py` | check_docs 正反例单测 |
| `scripts/ci/test_test_profile.py` | run_test_profile.sh 离线契约测试（选集映射、fail-closed、实跑集对账） |
| `scripts/ci/test_formal_ci.py` | 现役 workflow 的离线冒烟与文本/结构契约测试 |

**分类/结果已本仓内联**：原共享 `bbtools-classify-v1` / `bbtools-verify-v1` reusable
callee 链及其 `scripts/ci/shared/**` 逻辑已删除（方案 D3：各仓用自己的 changes +
result job）。分类规则由 `scripts/ci/changed_files.py` 承载，结果判定由
`scripts/ci/result.py` 承载；对应 callee workflow 文件与 `uses:` 接线均已移除。

## 2. 路径分类（changes job）

`changed_files.py` 把真实仓库 diff 归一为仓库相对路径列表，并按下列规则分类：

| 路径形状 | 判定 |
|---|---|
| `CMakeLists.txt`（任意目录） | code |
| `*.cmake`、`*.py` | code |
| `.github/workflows/**` | code |
| `docs/` 前缀、`*.md`/图片/`*.txt`、`LICENSE` 等根目录文书 | doc |
| 其余（源码、脚本、锁文件等） | code |

变更集判定：**全部路径为 doc → `docs-only`**；含任一 code 路径 → `code`；
空集或分类器非 ok（diff 失败/未知 revision/超预算）→ `unknown`，保守执行全部。
`docs-only` 才允许重型 job（build_test）跳过；`code`/`unknown` 禁止任何跳过。
分类用例集见 `test_changed_files.py`（覆盖 `CMakeLists.txt`、`*.cmake`、`*.py`、
workflows、纯 md、混合、空集、非 ok 状态）。

## 3. 结果汇聚（result job）

`result.py` 把 needs.*.result 汇总为唯一 required check 判定：

- `required` 集合必须非空（空即拒绝，防「全跳过变绿」）；
- 任一 required 为 `failure`/`cancelled`/缺失/`skipped` → verdict=failure；
- results 出现 required∪optional 之外的 job 或非法状态值 → 拒绝；
- optional 非 success 时：`skipped`/缺失仅当 classification=docs-only 放行；
- 无任何真实 success 不得判绿；
- verdict=failure → 退出码 1；契约拒绝 → 退出码 3。

当前接线：required = `changes`、`docs_check`；optional = `build_test`
（`ci.yml` result job 内联 payload 与之一致）。

## 4. build_test 单 runner 内执行

`build_test` 在**同一台** hosted runner（`ubuntu-24.04`）内完成构建、门禁与
smoke profile 测试（changes 的 `focused_regex` 非空时同一构建树追加一次
`--profile focused`），构建树不离开该 runner，不再经 artifact 在 job 间传递：

- **Boost runtime 私有前缀**：`prepare_boost.sh` 把锁定 Boost 1.90 `context` 编到
  工作区 `boost-prefix`；测试经 `LD_LIBRARY_PATH` 指向该前缀，`ldd` 断言
  `libboost_context` 真实解析到前缀内（不许系统预装替代）。
- **protobuf 前缀同路径重建**：`prepare_protobuf.sh` 在
  `$GITHUB_WORKSPACE/.deps/protobuf-3.21.12` 准备锁定前缀（protoc 绝对路径被
  CTest 配置固化）。
- **/usr/local 剔除**：`ldd` 复核与 CTest 步先从 `LD_LIBRARY_PATH` 剔除
  `/usr/local` 项，防止镜像预装的 bbt 旧产物被 NEEDED 抢绑。
- **跨 run 缓存**：只复用两类东西——`ccache` 编译对象（`runner.temp/ccache`，
  上限 500M）与**已按锁定 sha256 校验**的 Boost 源码归档
  （`runner.temp/src-cache`，只存 `boost_1_90_0.tar.gz`）。不复用 build 树、
  `.ci-work`、Boost/deps/protobuf 任何 prefix，缓存步骤的 `path` 只有这两个
  目录。编译器 launcher 经既有 `BBT_CMAKE_ARGS` 透传口传给 cmake
  （`-DCMAKE_CXX_COMPILER_LAUNCHER=ccache`），gate/build 脚本不含 ccache 分支。
  - **键**：`ccache-v1-linux-gcc<g++版本>-cc<ccache真实版本>-deps<固定输入指纹>
    -run<run_id>-a<run_attempt>`；恢复前缀保留完整指纹但去掉 run/attempt，故
    同指纹的后续 run 能命中上一次条目。指纹来自 `CMakeLists.txt`、`deps.lock`、
    `scripts/prepare_protobuf.sh`、`scripts/ci/prepare_boost.sh` 的内容摘要——
    任一固定输入变化即换前缀，**恢复不跨指纹**。Boost 归档键为
    `boost-src-1.90.0-<sha256 前 12 位>`，内容寻址。
  - **ccache 来源**：hosted 临时 runner 上 `apt-get install ccache`（不钉 apt 包
    版本——没有该版本在 hosted 实装的证据），随即解析 `ccache --version` 的实际
    版本进键，只做「主版本 >=4」的语义检查（`--set-config`/`base_dir` 需要），
    版本号解析不出即失败。镜像不自带 ccache，本地主机不安装。
  - **恢复后固化配置**：缓存归档带着上一次的 `ccache.conf`，恢复会覆盖安装时的
    设置，所以 `max_size/compression/compiler_check/base_dir` 在恢复之后重新设置，
    同一处 `ccache --zero-stats`——日志里的命中数只反映本次 run。
  - **缓存目录**：`runner.temp` 在 job 级 `env` 不可用（该位置没有 `runner`
    context），`CCACHE_DIR`/`BOOST_SOURCE_CACHE` 由 build 首个 run 步骤写进
    `$GITHUB_ENV`；同一步按 `CACHE_ALLOWED` 显式设置 `CCACHE_DISABLE`（允许 `0`、
    禁用 `1`），预装 ccache 的镜像也不会被隐式当成 launcher。
  - **可见范围由 GitHub ref scope 决定，不是 key、也不是 `if`**：PR run 写的条目
    落在 `refs/pull/N/merge`，只有该 PR 自己能恢复；main 的 run 只读当前分支/
    默认分支。`key`/`if` 只决定「取哪份、存不存」。
  - **fork PR 不装 ccache、不写对象缓存**（对象是可执行内容）：`CACHE_ALLOWED`
    只决定装不装/存不存，不扩大任何权限；同仓 PR 的 ccache 只写 merge-ref，
    `main` 只消费默认分支可见的缓存，PR 条目不可能回到 main。
  - **缓存故障不改变判据**：恢复/保存步骤 `continue-on-error`，Boost 缓存命中
    与否都要过原有 `sha256sum --check` 门（损坏即告警重下载、再次校验；缓存目录
    不可写只告警），ccache 恢复失败即冷构建。ccache 安装失败或主版本 <4 则显式失败，
    不静默降级。缓存只影响耗时，不影响任何门禁。
  - **判读**：job 末尾打印 `ccache --show-stats -v`，命中率必须进日志；是否提速
    需同 SHA 冷暖对照，未实测不得声称优化达成。
  - **回归测试接线**：`scripts/ci/test_formal_ci.py`（本缓存契约、key、步骤顺序与
    `prepare_boost.sh` 缓存行为用例）在 build 步骤前作为独立步骤执行，失败即阻断
    job；不再只在本地跑。
- **cleanup**：job 末尾 `if: always()` 清理 `.ci-work`、`boost-prefix`、`.deps`
  与本地构建归档（缓存目录在 `runner.temp` 下，不参与清理）。

## 5. 门禁清单（不得放宽）

- **smoke profile**：CTest 步走 `scripts/ci/run_test_profile.sh --profile smoke`——
  选集即 `-L smoke`，脚本内 fail-closed：选集为空、注册集≠实跑集、任一
  skipped/Not Run/failure/error 皆非零退出（判据与旧内联 no-SKIP 门禁同口径，
  实跑对账由 JUnit XML 校验承担）。
- **必要 focused**：`changed_files.py` 按仓内「路径前缀 → ctest 测试名」映射
  （`FOCUSED_MAP`，覆盖 `framework/src/**` 与 `examples/**` 全部源码模块）产出
  `focused_regex`；非空时 build_test 在同一构建树追加
  `run_test_profile.sh --profile focused --regex <regex>`，空串则该步显式跳过。
  focused 是 smoke 之外的增量加强，选集/对账/失败判据与 smoke 同一脚本契约。
- **来源门禁**：`link-sources.txt` 命中 `/usr/local/` 即失败；`build_stack.sh`
  做 `deps.lock` 逐条 SHA 对账。
- **dual-service 负向门禁**：默认构建树必须保持
  `BBT_ENABLE_DUAL_SERVICE_EXAMPLE=OFF` 且不含 `examples/dual_service`；显式 ON 且缺
  hiredis/mongo 前缀必须 configure 失败，且失败原因来自前置条件门禁。
  负向配置仍启用框架必需的 RPC 并传入锁定 protobuf 前缀，确保失败来自缺失的后端依赖。
- **ldd**：`Test_framework_*` 与 `rpc_xlang_server` 不得有 `not found`；不得链接
  `/usr/local` 下的 bbt 旧产物；`libboost_context` 必须解析到锁定 Boost 前缀；
  零命中可执行产物即失败。
- **provenance**：`run_framework_gate.sh build` 内联产出 manifest.json +
  envelope.json（producer repo/source SHA/run/attempt/job 绑定，摘要为真实写入
  字节的 sha256）。单 runner 下无接收侧复核步；`verify-archive` 作为接收侧配方
  保留在仓内（见 §1），供需要跨 runner/归档传递的流程复用。
- **docs-check**：`scripts/ci/check_docs.py --root <repo> --readme-max 200`；
  docs-only PR 只跑 docs-check 与 changes。
- **format-diff**：`scripts/ci/check_format_diff.sh <base>` 只检查本 PR 改动的
  C/C++ 行，不强制格式化存量代码；当前为 changes 内 `continue-on-error`（告警不
  阻断）。
- **缓存中立**：跨 run 缓存（§4）只影响耗时。恢复/保存失败、Boost 缓存损坏都
  退回冷构建；ccache 安装/版本不符显式失败。smoke/focused 选集、`result.py` 判定、
  链接来源与 provenance 判据均不因缓存改动。

## 6. 测试数（当前注册面）

`-DNEED_TEST=ON` 且 `bbt_infra_rpc_net` 注册时的默认配置共注册 **25** 个 CTest：

`tests/`（19）与 `examples/`（6）。ctest 名均为 `<module>.<行为>` 形式
（`framework.*`、`examples.*`）。普通 CI 固定跑 **smoke profile**（`-L smoke`，
18 项：12 个 unit + 6 个快速 integration）+ **必要 focused**（`focused_regex` 命中的
直接耦合用例，可含非 smoke 项）；`framework.shutdown`（单用例实测 ~30s、
全套 ~107s）与 6 个 `examples.*` 用例只标 `integration`（跨进程真实协作，
非冒烟）——均不进普通 smoke。
`framework.resource_live_{redis,mongo,combined}` 仅在 infra 相应目标在场时
注册（`live` label，默认配置不产生）。
测试命名、label 语义与条件 target 的完整契约以 `tests/CMakeLists.txt` 与
`docs/guides/testing.md`（测试规范，落实三仓统一分层）为准；全量/focused
执行入口见该文档。

## 7. 手动任务（manual.yml，非 required，仅 workflow_dispatch）

手动任务已从 `ci.yml` 分离到 `.github/workflows/manual.yml`（`arc-s4-framework`
自托管 runner），普通 PR 流程不携带：

- `Toolchain identity (candidate, manual)`：跑
  `scripts/verify_toolchain.sh --in-image --lock docker/toolchain.lock`。
- `Dual-service resource acceptance (manual)`：仅在
  `inputs.resource_acceptance == true` 时运行；资源边界不变（`RESOURCE_RECIPE_SHA`、
  Redis/Mongo 镜像 digest、`.resource-recipe`/`.resource-deps` 专用前缀与清理），
  只消费 infra 固定 recipe。

## 8. 固定输入

- 远程 action 全部完整 40-hex pin（`actions/checkout`、`actions/cache/restore|save`
  同一 release commit）。
- 依赖：`deps.lock`（coroutine/infra 固定 SHA）。
- Boost：`docker/toolchain.lock` 的 `BOOST_VERSION=1.90.0` + 锁定 sha256（只编
  `context`）。
- protobuf：`docker/toolchain.lock` 的 `PROTOC_VERSION=3.21.12`；debs 与 sha256
  来自 `scripts/prepare_protobuf.sh` 的锁定包集。

## 9. 离线验证命令

```bash
# 分类 + result 契约单测（仅标准库）
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest scripts/ci/test_changed_files.py

# check_docs 单测
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest scripts/ci/test_check_docs.py

# 现役 workflow 文本契约 + archive 接收侧门禁冒烟（stdlib 恒跑；PyYAML 结构契约
# 在无 PyYAML 时显式 SKIP）
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest scripts/ci/test_formal_ci.py

# run_test_profile.sh 离线契约测试
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest scripts/ci/test_test_profile.py

# docs-check 本仓自检
python3 -I scripts/ci/check_docs.py --root . --readme-max 200
```
