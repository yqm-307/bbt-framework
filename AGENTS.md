# AGENTS.md — bbt-framework

## 工程介绍

可 fork 的 C++ 正式服务框架与工程脚手架（App/Service 基类、启动/生命周期、配置、注册发现、Stub 等分布式基础能力）。使用者在应用层开发业务，框架上游不硬编码具体产品业务；交付可 fork 后新增业务服务的工程，不以仅跑通示例替代。不承载具体业务接口、schema 或第三方协议适配实现（后者归 `bbtools-infra`）。

<!-- bbtools:common:begin -->
- 依赖方向固定为 `bbt-framework → bbtools-infra → bbtools-coroutine`，禁止反向依赖、环依赖和引用兄弟模块内部实现。跨仓公共契约以契约所在仓文档为真源，变更由用户拍板；并行写任务使用独立工作区、索引、build 和依赖目录，保留他人已有改动。
- 统一以 C++17 为基线；优先标准库与 Boost 1.90，模板适度，namespace 不超过三层（`bbt::{大模块}::{小模块}`）。
- 设计、编码、Review 与用例设计的共用判断规范固定在本仓 `third-party/bbtools-pub` submodule（已审阅提交，`git submodule update --init --recursive` 后读取；远端源 https://github.com/yqm-307/bbtools-pub 以其 `main` 已合入内容为准，不读取仓外 pub 工作树的未提交改动），从其 `README.md` 只走当前职责的路径，不全量预载；与本仓 AGENTS 或 `docs/` 冲突时以本仓为准，并在交付中报告差异。
- 新 worktree/clone 不会带入未提交规范；开工先核对实际规范快照与依赖版本。
- 顶层目录：`bbt/`（公共头与实现按模块并置，`bbt/framework/` 为框架库源码）、`tests/`、`examples/`、`docs/`、`scripts/`、`.github/`、`.config/`、`docker/`，另有 `third-party/`（只固定公共规范 submodule，不放本仓实现或运行产物）。不新增顶层目录。
<!-- bbtools:common:end -->

## 规范与真源入口

先按任务选一条路径，不全量预载。

| 任务 | 公共规范（pub submodule） | 本仓真源 |
|---|---|---|
| 架构契约、模块边界 | `third-party/bbtools-pub/docs/10-design/README.md` | `docs/architecture/distributed-framework-baseline.md`（基调真源）＋ 同目录验收矩阵与 `framework-contract.md` |
| 编码/注释/文档细则 | `third-party/bbtools-pub/docs/20-coding/README.md`（C++、注释、文档专项按入口展开） | `docs/conventions.md`；格式以 `.clang-format`、`.editorconfig` 为真源 |
| 评审 | `third-party/bbtools-pub/docs/30-review/README.md` | 本文「审查门禁」；`.config/review-tickets/` 存审查凭证 |
| 设计测试、选择运行范围 | `third-party/bbtools-pub/docs/40-testcase/README.md` | `docs/guides/testing.md`、`docs/conventions.md` §6 |
| 构建与测试入口 | — | `README.md`（不把未实现的分布式扩展写成现有事实） |
| 开发流程、验证、发布、push 门禁 | — | `docs/guides/development.md`、`docs/guides/ci.md`（CI 最终落点；收敛前见 `docs/ci/`） |
| 协议/依赖政策 | — | `bbtools-infra` 仓（按消费版本读其 AGENTS/decisions） |
| 任务与验收 | — | GitHub Issue/PR |

- 优先级：用户最新明确指令 > 本文与本仓 `docs/` > pub；冲突时以本仓为准，并在交付中报告差异；pub 未覆盖的本仓特例写在本仓文档，不改 pub 来迁就本仓。
- pub 指针更新时先核对提交与变更范围，不读取仓外 pub 工作树的未提交改动。

## 本仓特有规则

- **分层**：应用依赖框架/所选扩展；扩展依赖框架公开扩展接口与 infra 能力；核心定义接口但不硬依赖具体扩展；扩展间依赖显式、无环、只用公开契约；框架核心不混在业务目录。
- **协议接入**：HTTP、RPC、MCP 和第三方适配只经 `bbtools-infra` 公开构建目标获得；旧 http/rpc/network/conet 仅为迁移来源，禁止新增直接依赖；注册中心后端 adapter 归 infra，不默认自研注册中心服务。
- **源码布局**：框架库统一在 `bbt/framework/`（公共头与实现同目录并置，内部头在 `internal/`）；公共 include 拼写 `<bbt/framework/...>` 与链接 target `bbt::framework` 是公开契约，不随目录调整改变。
- **上游契约**：上游契约未确定时不各猜一版，先做不依赖它的工作；复用上游须核对源码 SHA、模块与链接路径，tag 须解析到 SHA，`/usr/local` 已安装不等于版本固定；只使用明确交接的候选快照，未发布文件不当可用依赖。依赖版本选择遵循 `bbtools-infra/docs/guides/development.md`「依赖版本选择」：设计指定版本，开发与构建有指定按指定、无指定默认所选本地源码的当前内容；核对实际模块、源码目录与链接来源，不要求为联调先提交。
- **fork 边界**：fork 中业务改动与框架/通用扩展改动隔离；不强制其他语言应用或 collab-services 成为本仓 fork；不预建全部空模块或万能协议/生成器平台。
- **需求真源**：GitHub Issue/PR 是需求、决策、进度与验收的消息真源；消息、契约、实现、测试冲突时显式记录，不静默选择。Issue/任务存在不等于实施授权。
- **实施边界**：用户明确要求实施，或所涉 Issue 已有方案共识时直接实现；改公共 API 或契约、且无共识也未被明确要求直接实施时，先「现状分析 → 方案共识」。只在疑问会改变做法且自己查不到时才问。
- **环境**：不假设链接库或系统工具可用（先查 `CMakeLists.txt` 与邻近文件）；不修改凭据/密钥；不终止他人进程。测试优先动态端口与自有进程；共享服务须声明资源、复用/互斥和清理边界；构建产物只进已忽略的 `build/`。

## 门禁

### 授权

- 只改获授权范围，不覆盖他人已有改动；保留他人已有改动与既有 required checks；不宣称已配置未实查的远端保护。
- 须先获用户授权：commit、push、开 PR、付费调用、部署、破坏性操作、新依赖、CI/发布变更、公共接口或数据布局破坏性变更、系统安装或运行基础设施操作。任务会产生改动时，开工时一次问清是否授权 commit/push/开 PR（无法询问时按未授权处理），授权覆盖整段任务、不逐步再问；未授权则停在 commit 前，交付改动与验证结果。

### 本地构建与验证

- 本地构建并发固定为 2；使用共享队列 `flock /tmp/bbt-build.lock`，构建显式 `--parallel 2`，configure 使用 ccache；测试用 `ctest -j1`。
- 本地验证必须覆盖冒烟、本次功能及直接耦合测试；本地不跑全量，完整测试走 PR CI。交付证据要求非平凡逻辑有可运行验证（干净 fork/隔离副本新增服务，验证构建、测试、启动、真实请求/错误/关闭）；替身不替代承诺的真实集成。
- 不把本地结果说成远端 CI 结果；缺环境、权限或依赖时报 `BLOCKED`，不以 stub、跳过断言或削弱测试伪装完成。

### 提交与合入

- 提交标题使用 `<type>(<scope>): <简洁中文说明>`，单次提交不混无关变更；目标测试通过且无新增警告后才允许 commit。交付前检查无意外改动并报告未覆盖项。Agent author 固定写法：`git commit --author="agent <agent@users.noreply.github.com>"`。
- 本地 push 门禁：wrapper 在 `.config/bbt-git-hooks/`（只放 `pre-push`，启用后不会顺带启用 pub 的 `commit-msg`），本仓规则配置在 `.config/bbt-git-hooks.toml`，规则与 review ticket 格式见 `third-party/bbtools-pub/tools/git-hooks/README.md`。启用前先查已有 `core.hooksPath`，不覆盖其他 hook 系统；`--local` 配置在 linked worktree 间共享，启用后所有 worktree 都按各自根的 `.config/bbt-git-hooks/` 找 hook——没有 wrapper 的 worktree 不受影响，本仓启用需先确认该共享影响或改用 worktree 独立 config（`extensions.worktreeConfig`）。代码 commit 审查通过后，ticket 放在紧随其后的单独 commit（只改 `.config/review-tickets/`）。
- 不直推 `main`、不自行合并或发布；远端写入后回读确认目标对象。CI 失败先读日志定位根因，不靠重跑掩盖；业务代码修复可自主做，CI/CD 配置修改须先获确认。

### 审查门禁

以下改动须独立审查（作者不自审放行），其余按风险自审并跑相关测试：

- 受保护文件（含本 `AGENTS.md`）、公共契约、权限；
- 等待/唤醒/取消竞争、调度状态机、对象寿命与资源收口路径；
- 本仓文档另列须独立审查的类别，从其规定。

审查按风险收敛：通过即停，不因非阻塞措辞反复整树复审。

### 仓库卫生

- 文档一事一处、代码与对应文档同一 PR 更新；README 只保留状态与入口；ADR 使用唯一递增编号，合入后只追加取代关系。
- 注释说明 WHY，不重复 WHAT；必须保留时序与所有权约束，公开头文件开头说明职责；不留 Issue/PR、审查/阶段代号、契约行号或临时版本号。
