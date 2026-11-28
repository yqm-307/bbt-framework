# 开发与验证指南

本文承接根 [AGENTS.md](../../AGENTS.md) 的操作规范，只补充 framework 的开发流程与本地验证入口；不新增审批阶段或固定模板。构建与测试命令见 [README](../../README.md)，测试选集与 profile 语义见 [testing](testing.md)，CI 落点见 [ci](ci.md)。

## 固定规范与本地工具

新 clone 或 worktree 按 [README](../../README.md#构建与测试) 初始化两级 submodule。按 `third-party/bbtools-pub/README.md` 选择职责提示词与规范，本仓补充见 [AGENTS.md](../../AGENTS.md#规范与真源入口)。升级 pub 时核对目标提交已合入其 `main`，检查新旧提交间的规范和工具变化，单独提交 gitlink 并注明两个 SHA。

以下命令从 framework 仓库根执行。检查范围分别由源码目录、`.config/lint.toml` 和 Git 基准决定。

```sh
# 与 CI 相同的 C++ 候选报告；建议在干净工作树运行。
python3 -I scripts/ci/check_lint.py --root .

# 需要机器处理的原始报告（不转换退出码）。
python3 -IB third-party/bbtools-pub/tools/lint/lint.py --root . \
  --config .config/lint.toml --format json

# C++ 接口或实现的复杂度审查。需在已有 Python 3.11+、lizard 1.25.0 环境中运行。
python3 third-party/bbtools-pub/tools/complexity/complexity.py scan --root . \
  --config third-party/bbtools-pub/tools/complexity/config.example.toml \
  --base origin/main --format markdown
```

- **lint** 需要 Python 3.11+，使用 pub 的词法候选扫描；报告包含框架库、示例和测试。原始扫描退出 `0` 只表示未报告覆盖错误，退出 `2` 表示覆盖不足或工具错误。候选不是缺陷判定；已知词法局限与工具失败在 [CI](ci.md#1-组成) 中区别处理。规则与输出见 `third-party/bbtools-pub/tools/lint/README.md`。
- **复杂度** 用 pub 的示例配置比较 `bbt/` 下的库代码与 `origin/main` 的共同基准。结果供 reviewer 定位新增或恶化项。缺少依赖、分析失败或覆盖不全时注明原因。工具环境与评分方法见 `third-party/bbtools-pub/tools/complexity/README.md`。

## 本地 Git 门禁

`.config/bbt-git-hooks/` 的 `commit-msg` 和 `pre-push` 转发到 pub 的 commit/push 阶段入口，和手动扫描共用 `.config/lint.toml`（schema 2）。启用后，提交检查暂存快照，推送按实际提交和远端基准校验。候选扫描不在 hook 中运行；头文件实现规则只提示，整文件格式在提交和推送阶段都检查，密钥扫描只在推送阶段查新增行。

- 启用前用 `git config --get core.hooksPath` 和 `git rev-parse --git-path hooks` 核对现有配置与 hook 目录，保留已有 hook 系统。确认可用后，单次操作可用 `git -c core.hooksPath=.config/bbt-git-hooks commit` 或同样方式执行 `push`。
- 需要持久启用时，`git config --local core.hooksPath .config/bbt-git-hooks` 会影响同仓所有 linked worktree。先确认共享影响。已有 `extensions.worktreeConfig` 时可仅设置当前 worktree。
- 审查通过后为被审提交生成 ticket，并以仅含 `.config/review-tickets/` 的下一笔提交保存。范围与生成命令见 `third-party/bbtools-pub/tools/lint/rules/review-ticket/README.md`。hook 校验已有审查凭据，实际独立审查按根 AGENTS 的门禁执行。

## 依赖版本选择

设计指定依赖版本与来源，按指定版本的 API 设计。开发与构建：有指定按指定；无指定默认使用所选本地源码目录（`BBT_INFRA_SOURCE_DIR`、`BBT_COROUTINE_SOURCE_DIR`）的当前内容，不要求为联调先提交。不执行自动 fetch/pull/checkout，不更换用户选定的目录；核对实际模块、源码目录、HEAD 与链接来源，tag 解析到 SHA，`/usr/local` 已安装不等于版本固定。验证记录实际 HEAD 和未提交差异；最新源码验证不能冒充固定版本、发布或 CI 证据。锁版入口 `deps.lock`/`scripts/fetch_deps.sh` 只进私有依赖目录，不 checkout 共享兄弟仓。

## 本地验证

- 本地运行冒烟、本次功能和直接耦合测试。普通 PR 运行 smoke 和按变更路径命中的 focused。完整回归的执行边界见[测试指南](testing.md#3-各环境执行边界)。
- 构建并发固定为 2：`flock /tmp/bbt-build.lock` 串行重任务，构建显式 `--parallel 2`，configure 使用 ccache；测试 `ctest -j1`（统一入口 `scripts/ci/run_test_profile.sh`）。
- 测试优先自有进程与动态端口；需要外部服务（Redis/Mongo/Consul）的 live/资源验收路径按各示例 README 声明的环境执行，缺环境如实标 `BLOCKED`，不以 stub 或跳过断言伪装完成。
- 文档改动运行 `python3 -I scripts/ci/check_docs.py --root . --readme-max 200` 与 `git diff --check`；pub 文件在仓内文档只写反引号路径或 HTTPS 链接，不写解析到 submodule 的相对 Markdown 链接。
