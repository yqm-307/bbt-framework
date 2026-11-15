# 开发与验证指南

本文承接根 [AGENTS.md](../../AGENTS.md) 的操作规范，只补充 framework 的开发流程与本地验证入口；不新增审批阶段或固定模板。构建与测试命令见 [README](../../README.md)，测试选集与 profile 语义见 [testing](testing.md)，CI 落点见 [ci](ci.md)。

## 规范入口

跨仓共用的设计、编码、Review 与用例设计规范固定从 `third-party/bbtools-pub/README.md` 进入，按任务只展开对应职责路径（任务到入口的映射见根 `AGENTS.md`「真源入口」表）。`AGENTS.md`、本文、`docs/architecture/`、`docs/conventions.md` 补充 framework 的本地契约；发生冲突时以本仓明确约束和实际实现为准，并在交付中报告差异。

干净 clone 后先执行 `git submodule update --init --recursive`，再读取上述入口；submodule 之外的本地 pub 工作树改动不属于本仓规范证据。更新 pub 指针时先核对目标提交已合入其 `main` 并核对变更范围，指针变更单独提交并写明新旧 SHA。

## 本地 push 门禁

- wrapper 在 `.config/bbt-git-hooks/pre-push`，本仓规则配置在 `.config/bbt-git-hooks.toml`；规则协议、review ticket 格式与跳过语法见 `third-party/bbtools-pub/tools/git-hooks/README.md`，不在本文复制。
- 启用：先查 `git config --get core.hooksPath` 与 `.git/hooks/` 非 sample 文件，无冲突再执行 `git config --local core.hooksPath .config/bbt-git-hooks`。
- `--local` 配置在 linked worktree 间共享：启用后所有 worktree 按各自根的 `.config/bbt-git-hooks/` 找 hook，没有 wrapper 的 worktree 不受影响；启用前先确认该共享影响，需要隔离时改用 `extensions.worktreeConfig` 的 worktree 独立 config。
- 目录里只放 `pre-push`：启用本仓 hooksPath 不会顺带启用 pub 自带的 `commit-msg`。
- review ticket 随仓库提交在 `.config/review-tickets/<YYYY-MM-DD>/<被审查 commit>.json`，放在紧随代码 commit 之后的单独 commit（该 commit 只能改 `.config/review-tickets/`）；ticket 绑定代码 commit 哈希，amend/rebase 后须重新审查、重新生成。

## 依赖版本选择

设计指定依赖版本与来源，按指定版本的 API 设计。开发与构建：有指定按指定；无指定默认使用所选本地源码目录（`BBT_INFRA_SOURCE_DIR`、`BBT_COROUTINE_SOURCE_DIR`）的当前内容，不要求为联调先提交。不执行自动 fetch/pull/checkout，不更换用户选定的目录；核对实际模块、源码目录、HEAD 与链接来源，tag 解析到 SHA，`/usr/local` 已安装不等于版本固定。验证记录实际 HEAD 和未提交差异；最新源码验证不能冒充固定版本、发布或 CI 证据。锁版入口 `deps.lock`/`scripts/fetch_deps.sh` 只进私有依赖目录，不 checkout 共享兄弟仓。

## 本地验证

- 本地验证覆盖冒烟、本次功能及直接耦合测试三者全过；本地不跑全量，完整测试走 PR CI。选集与命令见 [testing](testing.md) 与 [README](../../README.md)。
- 构建并发固定为 2：`flock /tmp/bbt-build.lock` 串行重任务，构建显式 `--parallel 2`，configure 使用 ccache；测试 `ctest -j1`（统一入口 `scripts/ci/run_test_profile.sh`）。
- 测试优先自有进程与动态端口；需要外部服务（Redis/Mongo/Consul）的 live/资源验收路径按各示例 README 声明的环境执行，缺环境如实标 `BLOCKED`，不以 stub 或跳过断言伪装完成。
- 文档改动运行 `python3 -I scripts/ci/check_docs.py --root . --readme-max 200` 与 `git diff --check`；pub 文件在仓内文档只写反引号路径或 HTTPS 链接，不写解析到 submodule 的相对 Markdown 链接。
