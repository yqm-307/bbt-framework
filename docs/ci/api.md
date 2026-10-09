# bbtools classify/verify v1 接口契约（本地候选，未发布）

> 状态：**本地隔离候选，未在任何远端发布**。本目录描述的是 `bbtools-ci-foundation-v1`
> revision 1 的候选接口与离线判据；`ready` 只表示本地候选范围足以开工，不代表已获实现/
> 发布授权，也不代表线上可调用。consumer 不得引用未替换为已发布版本的路径。

唯一真源：`yqm-307/bbt-framework` 独立 CI 命名空间。本候选不改动现役
`.github/workflows/ci.yml`、产品/依赖/发布流程与 required checks。

## 1. 组成

| 文件 | 职责 |
|---|---|
| `.github/workflows/bbtools-classify-v1.yml` | reusable callee：纯分类 + 结果契约 |
| `.github/workflows/bbtools-verify-v1.yml` | reusable callee：source/automation 分离 checkout + 真实元数据 payload + 接收侧校验 |
| `.github/workflows/bbtools-canary-v1.yml` | hosted-only canary caller：仅分支 push 触发，本地 callee 引用 |
| `scripts/ci/shared/` | 纯逻辑（无网络、无命令执行）、真实产物链路与离线测试 |
| `scripts/ci/shared/tests/` | 表驱动正反 fixtures + CLI 冒烟 + 产物正负用例 |
| `docs/ci/caller-canary-publish-patch.md` | canary 发布替换方案（占位模板，未执行） |

未新增 composite：两个 callee 不共享可复用的重复步骤集合，套空 wrapper 反而违反“不新造
通用执行器/空 wrapper”约束。

## 2. 分类与结果契约

### 2.1 分类（`scripts/ci/shared/classification.py`）

输入是**调用方提供的受限变更数据**（仓库相对路径列表），不是任意命令或 YAML：

- 所有路径均为文档类（`docs/` 前缀、`.md/.png/.jpg/.svg/...` 后缀、`LICENSE` 等）→ `docs-only`；
- 存在代码路径 → `code`；
- 变更集为空，或 `classifier_status != ok`（失败/未知）→ `unknown`，**保守执行全部**；
- `docs-only` 才允许 `optional`（heavy/perf）跳过；`code`/`unknown` 禁止任何跳过。

### 2.2 结果（`scripts/ci/shared/result_contract.py`）

`required` 集合必须非空（空即 `E_REQUIRED_EMPTY`，防止“全跳过变绿”）。
任一 `required` 为 `failure`/`cancelled`/空值/`skipped` → 整体 `failure`；
`required` 出现集合外的 job 或非法状态值 → 拒绝。

### 2.3 产物 envelope（`scripts/ci/shared/envelope.py`）

`envelope` 绑定 producer 身份：`producer_repo`、`producer_source_sha`、`producer_run_id`、
`producer_run_attempt`、`workflow_sha`、`job`、`artifact_digest`(`sha256:<64hex>`)、
`artifact_path`（仓库相对、禁逃逸）。身份缺失 → `E_ENV_MISSING`；非身份运行信息
（`toolchain/tests/retry/cache_id/artifact_id`）缺失记为 `unknown`，**不伪造**。

`artifact_digest` 必须来自**真实产物字节**：verify callee 的 produce 步骤写入 `out/manifest.json`
后对该文件字节计算 SHA256 并填入 envelope；**不接受调用方传入摘要**，也不写全零/占位值。
真实产物职责与摘要口径见 §2.5。

下游全量/仅失败重跑：`verify_rerun_reuse(producer, rerun, expected)` 要求两者绑定同一
期望 source/workflow/job，且 identity 逐字段相等；把 producer 改绑到消费者自身
run/attempt 即 `E_ENV_REBOUND`，错误 source 即 `E_ENV_IDENTITY_MISMATCH`。

### 2.4 输入（`scripts/ci/shared/input_contract.py`）

只接受闭集字段 `repo/source_sha/profile/concurrency/timeout_minutes/event/changed_files/
required_checks/optional_checks/classifier_status/results`。
`profile` 首版仅 `hosted`；`self-hosted` 无准入证据 → `E_PROFILE_NO_ADMISSION`。
`source_sha` 必须完整 40 位十六进制；`concurrency`/`timeout_minutes` 必须为正且在上限内；
`repo` 必须为合法 `owner/name`（两段以字母数字开头/结尾，拒绝 `.`/`..` 等 dot 段）；
路径禁绝对/`..`/反斜杠；未知字段、疑似凭据值一律拒绝。**拒绝信息不回显输入值**。

分类与结果评估解耦：调用方未提供 `results`（缺失或空对象）时只输出分类，不评估、不阻断；
仅当提供真实非空 `results` 时 `cli classify` 附带 `evaluation`，并由 `result` job 依
`evaluate` 退出码门禁（verdict=failure → 非零）。

非 JSON / 文件读取解析失败由 CLI 统一归一为稳定错误 `E_INPUT_JSON`（非零 JSON，无 traceback）。

## 2.5 真实产物链路（verify 的 produce → upload → download → verify-receipt）

- **produce（`scripts/ci/shared/artifact.py` + `cli.py produce`）**：只读读取 source checkout 的
  实际 HEAD（`git -C source rev-parse HEAD`，不执行 source 代码、不扫描/打包源码），结合 job
  上下文生成闭集小型元数据 `out/manifest.json`：`manifest_version / source_repo / source_sha /
  source_head / automation_repo / automation_sha / run_id / run_attempt / job`。
  `source_head` 必须等于声明的 `source_sha`，否则 `E_ART_SOURCE_HEAD`。
- **摘要**：`artifact_digest` 是对**实际写入的 manifest.json 字节**计算 SHA256（`sha256:<64hex>`），
  唯一口径；绝不来自调用方输入或字面量。
- **传输**：payload 与 envelope 由固定 SHA 的 `actions/upload-artifact`
  （`ea165f8d…`，`path: out/`、`name: bbt-canary-payload-v1`、`if-no-files-found: error`、
  `retention-days: 1`）传到本 run 的 consume job；接收侧用固定 SHA 的
  `actions/download-artifact`（`d3f86a1…`）且**不传 `run-id`/`repository`**，只取本 run 产物。
- **摘要口径区分**：`payload_digest` = payload 字节 SHA256（envelope 身份字段）；
  `archive_digest` = upload-artifact 归档 digest，**只作运行证据**，绝不作为身份或比对依据。
- **接收侧（`cli.py verify-receipt`）**：按固定顺序 fail-closed —— receipt 形状 → envelope 结构
  （未知字段/缺字段/摘要格式）→ 声明路径一致 → 期望身份（repo/source_sha/workflow_sha/job/
  run_id/run_attempt/producer 声明摘要）→ root 类型 → payload 非 symlink/存在 → realpath 不出
  root → 真实字节摘要 → JSON 为 object → 闭集 manifest → manifest 与 envelope 交叉绑定。
  任一失败即稳定错误码 + 退出 3；**验证通过前不执行任何 payload 内容**。
- **负向探针（`scripts/ci/shared/hosted_probe.py`）**：对真实产出做字节篡改、摘要替换、payload
  非 object、manifest 未知/缺失字段、manifest 与 envelope 不一致、envelope 未知字段/路径逃逸、
  receipt 路径不一致、期望缺键、错误 source/run/attempt 身份、payload 缺失、payload symlink、
  目录 symlink 逃逸共 16 个用例；每个都必须被拒（退出 3），否则探针退出非零。正向对照失败或
  探针自身异常同样计为失败，避免「全部 skipped 变绿」。

## 3. 错误码（稳定契约）

`E_INPUT_*`（类型/未知字段/repo/SHA/profile/并发/超时/事件/路径/检查 id/状态/凭据）、
`E_PROFILE_NO_ADMISSION`、`E_REQUIRED_EMPTY`、`E_CHECK_OVERLAP`、`E_RESULT_*`、
`E_ALL_SKIPPED`、`E_ENV_*`（类型/未知字段/缺失/repo/SHA/job/run id/attempt/digest/path/
运行信息/凭据/身份不匹配/改绑）。完整定义见 `scripts/ci/shared/contract_errors.py`。

## 4. 权限与身份

- 顶层 `permissions: {}`；仅需 checkout 的 job 授予 `contents: read`；
- 无 checkout 的聚合 job（`report`）`permissions: {}`；
- 无自定义 secret、无 `secrets: inherit`；远程 action 全部完整 SHA pin；
- `automation`（callee 自身 `job.workflow_sha`）与 `source`（`inputs.source_sha`）分别 checkout；
- callee 身份取自 **callee 作用域** 的 `job.workflow_repository` / `job.workflow_sha`（reusable job
  上下文），**不使用**关联 caller 的 `GITHUB_WORKFLOW_REF`/`GITHUB_WORKFLOW_SHA`；缺失或非法即
  fail-closed，不回退 main。

## 5. 离线验证命令（无网络、不安装系统依赖）

```bash
# 纯逻辑 + CLI 冒烟（仅标准库）
cd <worktree>
PYTHONDONTWRITEBYTECODE=1 python3 scripts/ci/shared/tests/run_tests.py

# 含 workflow 静态契约（复用只读 uv-cache，禁止联网新增依赖）
UV_OFFLINE=1 UV_CACHE_DIR=<uv-cache> uv run --no-project --with pyyaml \
  python3 scripts/ci/shared/tests/run_tests.py

# 单条 CLI 冒烟（stdin 传 JSON，拒绝时退出码 3）
echo '{"repo":"yqm-307/bbt-framework","source_sha":"a60edfb4f7cd77e69926be475e2c45e324a3db2b","profile":"hosted","required_checks_json":"[\"lint\"]"}' \
  | python3 scripts/ci/shared/cli.py classify

# 真实产物链路离线闭环（produce 真实字节摘要 → 接收侧校验 → 负向探针）
python3 scripts/ci/shared/cli.py produce --file manifest-inputs.json --out-dir out
python3 scripts/ci/shared/cli.py verify-receipt --file receipt.json
python3 scripts/ci/shared/hosted_probe.py --receipt receipt.json
```

CLI 退出码：`0` 成功（`evaluate` 仅 verdict=success）；`1` 契约通过但门禁失败
（`evaluate` verdict=failure）；`3` 契约拒绝（含输入非 JSON/文件读取失败，稳定 `E_INPUT_JSON`；
产物/manifest 拒绝为稳定 `E_ART_*`/`E_ENV_*`）。

## 6. 未覆盖边界（不得声称已验证）

- 离线逻辑**不能**证明 GitHub 真实 reusable 上下文、callee 身份可信、平台 runner 准入或
  性能/资源；这些属于后续授权 canary/在线发布包。
- `job.workflow_repository`/`job.workflow_sha` 的运行时取值属在线事实：本地只能按 GitHub
  公开语义（reusable job 上下文标识 callee 自身）静态保证接线，未在线实跑。
- **真实产物链路已接线但未在线实跑**：produce 的真实字节摘要、upload/download 固定 SHA 传输、
  接收侧 fail-closed 校验与 16 项负向探针均在本地/离线真实执行；hosted runner 上的
  `upload-artifact` 归档 digest（`artifact-digest` 输出）与 reusable 上下文仍属在线事实，
  离线不造值、不冒充已验证。
- **在线负向边界**：canary 只在 hosted 运行里执行负向探针；未覆盖「预期失败 job +
  continue-on-error + 显式断言」形状，也未覆盖 required checks、perf、跨仓 caller 重跑或权限提升。
- `self-hosted` 无行政准入证据即拒绝，是保守默认，非已实现准入机制。
- 路径逃逸离线已用真实 symlink 用例覆盖（payload symlink 与目录 symlink 逃逸均拒绝）；
  但真实 runner 上 `download-artifact` 的解压布局属在线事实。
- 未运行任何 C++ 全量构建、真实 Actions 远程调用或跨仓写入。
