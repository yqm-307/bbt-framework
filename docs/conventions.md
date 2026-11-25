# bbt-framework 开发细则

设计、开发、QA 与审查的公共方法从 `third-party/bbtools-pub/README.md` 进入。C++ 命名、类型、头文件布局、协程写法、注释和构建约定由 `third-party/bbtools-pub/docs/cpp/README.md` 索引。新增代码采用这些约定。存量符号改名须核对消费者与兼容性。

## 源码与公共 API

- 框架库在 `bbt/framework/`。业务通过 `<bbt/framework/...>` 和 `bbt::framework` 消费公开接口，宿主实现和 `internal/` 类型留在框架内。上游依赖走 infra 的公开构建目标，`bbtools-core` 已冻结。应用、扩展与核心的依赖方向见 [AGENTS.md](../AGENTS.md)。
- 可失败的公开操作沿用模块已有契约。`bbt/framework/Result.hpp` 直接导出 infra 的 `result<T>`、`Error` 和 `ErrorCode`，业务可以据此判断超时、上下文错误与关闭失败。
- Service、Component 和 Actor 由 `CoApp` 管理。构造、绑定代次和使用租约的有效期是业务接口的一部分。`ComponentHandle::lock()` 共享组件对象寿命。需要资源使用保证的调用方使用显式 `Borrow()`，详见 [组件寿命规则](architecture/framework-contract.md#31-必须实现的实际寿命规则)。公共声明及扩展 hook 需说清调用上下文、所有权、错误与关闭语义。核心模板的头内定义、拆分与 Doxygen 导出验收见[框架契约](architecture/framework-contract.md#53-核心头文件模板与注释组织)。
- 引入上游能力时核对实际模块与链接来源。联调源码与固定依赖使用各自的验证记录，见[开发指南](guides/development.md#依赖版本选择)。

## 格式与文档

- 机械格式取仓根 `.clang-format` 和 `.editorconfig`。framework 保留手工折行（`ColumnLimit: 0`）与函数左花括号同行的风格。CI 的 format-diff 检查改动行。本地 pre-push 的 format 规则对推送中改动的 C/C++ 文件做整文件检查。全仓迁移须单独决策和提交。其它 lint 检查入口见[开发指南](guides/development.md#固定规范与本地工具)。
- 注释采用 `third-party/bbtools-pub/docs/cpp/comments.md`。需要解释关闭时序或责任转移时，可在实现旁放 ASCII 图或 PlantUML。`TODO` 写明改进方向和触发条件。
- `README.md` 提供现状、最短用法及文档入口，上限 200 行。架构契约放 `docs/architecture/`，操作说明放 `docs/guides/`。ADR 按 `docs/decisions/NNNN-<slug>.md` 唯一递增编号，合入后只追加取代关系。文件名用主题，ADR 以外不加过程编号。
- 同一事实只在一处写全，文档与对应代码同一 PR 更新。运行日志、原始报告、构建产物与本机路径留在仓外。文档检查命令见[开发指南](guides/development.md#本地验证)。

## 测试、示例与 CI

- framework 的 Boost.Test 文件命名、CTest 注册、label 及 profile 以[测试指南](guides/testing.md)为准。新增模块与测试接入实际 CMake 目标，编译须无新增警告。
- 示例放 `examples/<name>/`，附运行条件、步骤与期望结果。可重复的行为断言放到 `tests/`。真实后端示例的构建开关和资源要求见[示例总览](../examples/README.md)。
- workflow 负责编排，执行逻辑放 `scripts/ci/`。单个 workflow 文件的维护上限为 300 行。现行 job、选集和门禁见 [CI 指南](guides/ci.md)。
