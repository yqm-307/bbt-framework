# canary caller 发布替换说明（占位方案，未执行）

> 状态：**未发布（UNPUBLISHED）**。本文件是给父级的第二轮替换方案，不是已发布路径，
> 不得把下方占位符当成真实存在的 callee 版本。

`.github/workflows/bbtools-canary-v1.yml` 初版用同 commit 的本地 callee 引用，保证同一
commit 内文件可读：

```yaml
    uses: ./.github/workflows/bbtools-classify-v1.yml
    uses: ./.github/workflows/bbtools-verify-v1.yml
```

父级 push 第一批 candidate commit、拿到该 commit 的真实完整 SHA 后，把两处 `uses:`
机械替换为「已发布完整 40-hex SHA」引用（**不允许** 用分支名、tag 或短 SHA）：

```yaml
    uses: yqm-307/bbt-framework/.github/workflows/bbtools-classify-v1.yml@<PUBLISHED_COMMIT_SHA_40HEX>
    uses: yqm-307/bbt-framework/.github/workflows/bbtools-verify-v1.yml@<PUBLISHED_COMMIT_SHA_40HEX>
```

替换等价的最小 patch（`<SHA>` 由父级填写）：

```diff
--- a/.github/workflows/bbtools-canary-v1.yml
+++ b/.github/workflows/bbtools-canary-v1.yml
@@
-    uses: ./.github/workflows/bbtools-classify-v1.yml
+    uses: yqm-307/bbt-framework/.github/workflows/bbtools-classify-v1.yml@<PUBLISHED_COMMIT_SHA_40HEX>
@@
-    uses: ./.github/workflows/bbtools-verify-v1.yml
+    uses: yqm-307/bbt-framework/.github/workflows/bbtools-verify-v1.yml@<PUBLISHED_COMMIT_SHA_40HEX>
```

约束：

- 替换后仍是同一份 canary 文件（触发面仍只有 `push.branches: [ci/issue-50-hosted-canary]`）；
- 替换后所有 `uses:` 均为完整 SHA pin，由 `scripts/ci/shared/tests/test_workflows.py
  ::test_canary_publish_patch_template_applies` 离线验证替换形状；
- 本地引用阶段与 SHA 引用阶段都不允许 `secrets: inherit`、写权限或第三方未 pin action；
- caller != callee 的在线语义（callee 身份 `job.workflow_repository`/`job.workflow_sha`）
  仍需 hosted 运行证实，本地测试只能验证接线与闭集。
