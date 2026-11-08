# AGENTS.md — bbt-framework

<!-- bbtools:common:begin -->
依赖方向固定为 `bbt-framework → bbtools-infra → bbtools-coroutine`，禁止反向依赖、环依赖和引用兄弟模块内部实现。
新 worktree/clone 不会带入未提交规范；开工先核对实际规范快照与依赖版本。
跨仓公共契约以契约所在仓文档为真源，变更由用户拍板；并行写任务使用独立工作区、索引、build 和依赖目录，保留他人已有改动。
统一以 C++17 为基线；优先标准库与 Boost 1.90，模板适度，namespace 不超过三层（`bbt::{大模块}::{小模块}`）。
设计、编码、Review 与用例设计的共用判断规范见 `bbtools-pub`（https://github.com/yqm-307/bbtools-pub，以其 `main` 已合入内容为准；本地并列检出 `../bbtools-pub` 可读，但不读其未提交改动），从其 `README.md` 只走当前职责的路径，不全量预载；与本仓 AGENTS 或 `docs/` 冲突时以本仓为准，并在交付中报告差异。
本地构建并发固定为 2；使用共享队列 `flock /tmp/bbt-build.lock`，构建显式 `--parallel 2`，configure 使用 ccache；测试用 `ctest -j1`。
本地验证必须覆盖冒烟、本次功能及直接耦合测试；本地不跑全量，完整测试走 PR CI。
只改获授权范围，不覆盖他人已有改动；Issue/任务存在不等于实施授权。
须先获用户授权：commit、push、开 PR、付费调用、部署、破坏性操作、新依赖、CI/发布变更、公共接口或数据布局破坏性变更、系统安装或运行基础设施操作。任务会产生改动时，开工时一次问清是否授权 commit/push/开 PR（无法询问时按未授权处理），授权覆盖整段任务、不逐步再问；未授权则停在 commit 前，交付改动与验证结果。
提交标题使用 `<type>(<scope>): <简洁中文说明>`，单次提交不混无关变更；目标测试通过且无新增警告后才允许 commit。交付前检查无意外改动并报告未覆盖项。Agent author 固定写法：`git commit --author="agent <agent@users.noreply.github.com>"`。
不直推 `main`、不自行合并或发布；受保护文件（含 `AGENTS.md`）、公共契约、权限，以及等待/唤醒/取消竞争、调度状态机、对象寿命与资源收口路径的改动须独立审查，其余按风险自审并跑相关测试；本仓文档另列须独立审查的类别从其规定。远端写入后回读确认目标对象。
文档一事一处、代码与对应文档同一 PR 更新；README 只保留状态与入口；ADR 使用唯一递增编号，合入后只追加取代关系。
注释说明 WHY，不重复 WHAT；必须保留时序与所有权约束，公开头文件开头说明职责；不留 Issue/PR、审查/阶段代号、契约行号或临时版本号。
<!-- bbtools:common:end -->

## 本仓规则

- 定位：可 fork 的 C++ 正式服务框架与工程脚手架（App/Service 基类、启动/生命周期、配置、注册发现、Stub 等分布式基础能力）。使用者在应用层开发业务，框架上游不硬编码具体产品业务；交付可 fork 后新增业务服务的工程，不以仅跑通示例替代。

真源入口：

| 主题 | 真源 |
|---|---|
| 架构契约 | `docs/architecture/distributed-framework-baseline.md`（基调真源）＋ 同目录验收矩阵 |
| 构建与测试入口 | `README.md`（不把未实现的分布式扩展写成现有事实） |
| 编码/测试/文档细则 | `docs/conventions.md` |
| CI | `docs/guides/ci.md` |
| 协议/依赖政策 | `bbtools-infra` 仓（按消费版本读其 AGENTS/decisions） |
| 任务与验收 | GitHub Issue/PR |

本仓特有门禁：

- 分层：应用依赖框架/所选扩展；扩展依赖框架公开扩展接口与 infra 能力；核心定义接口但不硬依赖具体扩展；扩展间依赖显式、无环、只用公开契约；框架核心不混在业务目录。
- 协议接入：HTTP、RPC、MCP 和第三方适配只经 `bbtools-infra` 公开构建目标获得；旧 http/rpc/network/conet 仅为迁移来源，禁止新增直接依赖；注册中心后端 adapter 归 infra，不默认自研注册中心服务。
- 上游契约未确定时不各猜一版，先做不依赖它的工作；复用上游须核对源码 SHA、模块与链接路径，tag 须解析到 SHA，`/usr/local` 已安装不等于版本固定；只使用明确交接的候选快照，未发布文件不当可用依赖。
- fork 边界：fork 中业务改动与框架/通用扩展改动隔离；不强制其他语言应用或 collab-services 成为本仓 fork；不预建全部空模块或万能协议/生成器平台。
- 交付证据：非平凡逻辑有可运行验证（干净 fork/隔离副本新增服务，验证构建、测试、启动、真实请求/错误/关闭）；替身不替代承诺的真实集成；缺依赖或权限时报 `BLOCKED`，不以 stub 或削弱测试伪装完成。
- 测试优先动态端口与自有进程；共享服务须声明资源、复用/互斥和清理边界，不终止他人进程；构建产物只进已忽略的 `build/`。
- 保留他人已有改动与既有 required checks；不宣称已配置未实查的远端保护。
