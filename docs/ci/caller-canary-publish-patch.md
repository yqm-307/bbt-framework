# canary caller callee 引用发布说明（已在隔离分支执行）

> 状态：**已执行**。`.github/workflows/bbtools-canary-v1.yml` 的两处 `uses:` 已机械替换为已发布
> 隔离分支 `ci/issue-50-hosted-canary` 上模板 commit 的完整 SHA
> `5b04115e5c871b75c6bbf357e2a9bcd2d26ef3f8`（固定的 canary caller commit 为
> `d003182a574f1e42ce916eff34c3d9926445cb5d`）。该模板**未合入 `main`**、**未做 C++/perf 验收**，
> 仅由 hosted canary 在线实跑（run 37870605859 / 37870795091 均 success）。

`.github/workflows/bbtools-canary-v1.yml` 初版用同 commit 的本地 callee 引用，保证同一 commit 内
文件可读：

```yaml
    uses: ./.github/workflows/bbtools-classify-v1.yml
    uses: ./.github/workflows/bbtools-verify-v1.yml
```

父级 push 首批 candidate commit、拿到该 commit 的真实完整 SHA 后，把两处 `uses:` 机械替换为
「已发布完整 40-hex SHA」引用（**不允许** 用分支名、tag 或短 SHA）；本仓已按此替换为
`@5b04115e5c871b75c6bbf357e2a9bcd2d26ef3f8`（两处为同一模板 commit）：

```yaml
    uses: yqm-307/bbt-framework/.github/workflows/bbtools-classify-v1.yml@5b04115e5c871b75c6bbf357e2a9bcd2d26ef3f8
    uses: yqm-307/bbt-framework/.github/workflows/bbtools-verify-v1.yml@5b04115e5c871b75c6bbf357e2a9bcd2d26ef3f8
```

替换等价的最小 patch（已应用；`<SHA>` = `5b04115e5c871b75c6bbf357e2a9bcd2d26ef3f8`）：

```diff
--- a/.github/workflows/bbtools-canary-v1.yml
+++ b/.github/workflows/bbtools-canary-v1.yml
@@
-    uses: ./.github/workflows/bbtools-classify-v1.yml
+    uses: yqm-307/bbt-framework/.github/workflows/bbtools-classify-v1.yml@5b04115e5c871b75c6bbf357e2a9bcd2d26ef3f8
@@
-    uses: ./.github/workflows/bbtools-verify-v1.yml
+    uses: yqm-307/bbt-framework/.github/workflows/bbtools-verify-v1.yml@5b04115e5c871b75c6bbf357e2a9bcd2d26ef3f8
```

约束：

- 替换后仍是同一份 canary 文件（触发面仍只有 `push.branches: [ci/issue-50-hosted-canary]`）；
- 替换后两处 `uses:` 均为完整 SHA pin（同一模板 commit）：
  `scripts/ci/shared/tests/test_workflows.py::test_canary_calls_published_callees_with_explicit_inputs`
  读取真实文件并校验两处引用；`::test_canary_publish_patch_template_applies` 从真实文本反向还原
  本地引用再正向替换，确认恰好两处、其他字节不变；`::test_canary_callee_ref_rejects_bad_pins`
  校验分支/短 SHA/跨仓/错误路径/不同 callee SHA 均被拒绝；
- 本地引用阶段与 SHA 引用阶段都不允许 `secrets: inherit`、写权限或第三方未 pin action；
- 在线语义：**同仓** caller != callee 已由 run 37870795091 证实（callee 身份取自
  `job.workflow_repository`/`job.workflow_sha`）；**跨仓** caller 与在线 rerun 复用仍属未覆盖，
  不得据此声称已验证。
