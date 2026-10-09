# bbtools CI v1 版本与变更记录（本地候选，未发布）

## 版本模型

- 接口族：`bbtools-classify-v1`、`bbtools-verify-v1`（`workflow_call`）。
- 唯一真源：`yqm-307/bbt-framework`，固定基线 `a60edfb4f7cd77e69926be475e2c45e324a3db2b`。
- 发布状态：**v1 候选，未发布（UNPUBLISHED）**。本目录与候选文件仅存在于隔离 worktree
  的本地候选，未 commit/push/建 PR/触发 CI/部署。
- 引用方式：consumer 必须引用**已发布**的完整 commit SHA 路径；候选期的
  `docs/ci/caller-example-unpublished.yml` 用占位 SHA 且明确标注未发布，禁止复制到真实 caller。

## v1（候选，revision 1）

新增：

- `bbtools-classify-v1` / `bbtools-verify-v1`：hosted-only、最小只读权限、无 secret、
  完整 SHA pin 的 reusable callee 候选。
- `scripts/ci/shared/`：纯分类/结果/输入/envelope 逻辑，标准库实现，无网络与命令执行。
- `scripts/ci/shared/tests/`：表驱动正反 fixtures 与 CLI 冒烟。
- `docs/ci/`：本 API、本变更记录、未发布 caller 形状示例。

不包含（明确越界）：不改现役 `ci.yml`、产品代码、`deps.lock`/`toolchain.lock`、docker/runner/
代理、发布流程、consumer required check 或 core；不新增通用调度器/动态任务 DSL/空 wrapper。

## v1（候选，revision 2 — 首次独立审查 C1–C8 修复）

- **C1 身份作用域**：reusable callee 改用 callee 作用域的 `job.workflow_repository` /
  `job.workflow_sha` 定位 automation（不再解析关联 caller 的 `GITHUB_WORKFLOW_REF/SHA`），
  缺失或非 40-hex 即 fail-closed。
- **C2 分类/评估解耦**：`results_json` 默认改为空串；空对象 results 规范为「未提供」，
  `cli classify` 仅在真实非空 results 时评估；`result` job 无 results 时不执行、不阻断，
  verdict 保持空并可在 `report` 观察。
- **C3 死输入**：从 `bbtools-verify-v1` 的 `workflow_call.inputs` 移除无实现的
  `producer_manifest_json`，与输入闭集保持一致。
- **C4 失败闭合**：`cli evaluate` 在 verdict=failure 时退出非零，`result` job 据此使 reusable
  运行整体非零失败；`report` 用 `always()` 仍可观察空/失败 verdict。
- **C5 artifact 摘要**：verify 缺真实 `BBT_ARTIFACT_DIGEST` 即 fail-closed（固定错误、非零），
  删除全零摘要默认值，不再伪造可信身份。
- **C6 真实接线测试**：新增 GitHub 形状 `toJSON(inputs)` 路由用例（默认分类成功、verify 输入闭集、
  evaluate failure 非零、缺 digest 拒绝），并断言自动化身份取自 `job.workflow_*`。
- **C7 仓库坐标**：`REPO_RE` 收紧为合法 `owner/name`（拒绝 `.`/`..` 等 dot 段）。
- **C8 CLI 输入错误**：非 JSON / 文件读取解析失败统一归一为稳定 `E_INPUT_JSON`（非零 JSON，无 traceback）。

未覆盖（在线边界，见 `api.md` §6）：`job.workflow_*` 运行时取值、Actions 归档 digest/runner
准入与宿主隔离、真实 reusable caller != callee 语义均未在线实跑。

## v1（候选，revision 3 — T3.1 hosted artifact canary 有限切片）

- **真实产物链路**：verify produce 步骤只读读取 source checkout 的实际 HEAD，生成有限源码元数据
  `out/manifest.json`（repo/SHA/实际 HEAD/callee SHA/run 绑定；不执行 source、不扫描/打包源码、
  不含秘密），按**真实写入字节**计算 `sha256` 并构造 envelope；删除 `BBT_ARTIFACT_DIGEST`
  外部传入路径（不再有调用方伪造摘要的入口）。
- **有限传输**：payload+envelope 经固定 SHA `actions/upload-artifact`（`ea165f8d…`，`path: out/`、
  `name: bbt-canary-payload-v1`、`if-no-files-found: error`、`retention-days: 1`）传到本 run 的
  consume job；consume 用固定 SHA `actions/download-artifact`（`d3f86a1…`）且不传 `run-id`，
  只消费本 run **成功** producer 的产物。
- **摘要口径区分**：`payload_digest`（payload 字节 SHA256）与 `archive_digest`（upload-artifact
  归档 digest）明确分离，后者只作运行证据，绝不作为身份或比对依据。
- **接收侧 fail-closed**：`cli.py verify-receipt` 校验 payload 字节、envelope 期望
  source/callee/run/attempt/job/path 与闭集 manifest；非 object/未知字段/缺失/篡改/错误身份/
  symlink/目录 symlink 逃逸均拒绝，验证完成前不执行 payload 内容。
- **hosted 负向探针**：`scripts/ci/shared/hosted_probe.py` 16 个用例（含正向对照）在 consume job
  对真实产出执行，任一未拒或探针异常即失败。
- **canary caller**：新增 `.github/workflows/bbtools-canary-v1.yml`，仅
  `push.branches: [ci/issue-50-hosted-canary]` 触发，不响应 PR/tag/main/schedule/workflow_dispatch；
  顶层 `permissions: {}`，调用 job `contents: read`；初版本地 callee 引用，发布替换方案见
  `docs/ci/caller-canary-publish-patch.md`。
- **report 聚合**：改用 `!cancelled()` + 显式 `needs.*.result` 断言失败，不再用 `always()` 吞失败；
  JSON 输出经 env 安全读取，不直接拼入 shell。

## 破坏性变更策略

`v1` 内以下任一变化视为破坏性（需新 major 与保留旧 SHA + 文档）：
输入默认值、输出 schema/check 语义、支持的 `profile`、产物 envelope 安全边界、
权限与身份绑定规则。活跃 consumer 仍依赖旧版本时，保留旧 SHA，不编造弃用期限。

## 远程 Action 固定（真实来源，只读 GET）

以下 SHA 经只读 `git ls-remote` 与 GitHub API `git/ref/tags` 解析确认为真实 commit；
major tag 为浮动 ref，**引用必须使用完整 SHA**。

| Action | SHA | tag | 来源 |
|---|---|---|---|
| actions/checkout | `11d5960a326750d5838078e36cf38b85af677262` | v4 | https://github.com/actions/checkout |
| actions/upload-artifact | `ea165f8d65b6e75b540449e92b4886f43607fa02` | v4 | https://github.com/actions/upload-artifact |
| actions/download-artifact | `d3f86a106a0bac45b974a628896c90dbdf5c8093` | v4 | https://github.com/actions/download-artifact |
| actions/setup-python | `a26af69be951a213d495a4c3e4e4022e16d87065` | v5 | https://github.com/actions/setup-python |

候选 `bbtools-*-v1.yml` 当前仅使用 `actions/checkout`；其余 pin 供后续授权消费/重跑场景，
升级需 diff 与 fixture 复核。

## 未运行声明

所有 F/M 场景**尚未在线执行**；本变更记录不证明线上 callee 身份、Actions policy、
宿主隔离或性能资源。离线证据只覆盖新增纯逻辑与静态结构/耦合。
