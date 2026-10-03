---
label: wayfinder:research
blocked_by: []
assignee:
claim_session:
status: closed
outcome: resolved
archive: complete
---
# P0 跨仓消费矩阵与可复现基线

## Question
bbt-framework Issue #1 在进入实现前，实际应固定哪些 `bbtools-infra`、`bbtools-coroutine` SHA、公开 CMake target、依赖来源和构建入口，才能在干净消费副本中复现并证明依赖方向没有被本机安装前缀或脏工作树污染？

## Done when
- 回读 framework、infra、coroutine 的真实源码与构建入口，固定完整 SHA、公开 target、include/link 关系和依赖 provenance。
- 形成一份可核验的消费矩阵：framework 公共头/target → infra 公共头/target → coroutine 公共头/target → 第三方依赖。
- 说明哪些模块是 Issue #1 当前阶段必需、哪些可延后，避免把 HTTP/RPC/MCP 或全部后端设成统一前置。
- 给出干净消费副本的 configure/build/test 最小命令和已知环境前提；不修改生产代码。

## Resolution
- 固定的可复现消费起点为 framework `b1620aa555ce0d91818cbe1e69bbf31c6d51407d`、infra `0a40b702935bd6a640c836d4aadefc9b87d974df`、coroutine `7bcda3b078f975ff2978424be7f6ba38e04fb7f6`；与该 framework 树的 `deps.lock` 一致。最新上游 main、历史候选和本机未提交规范另列，不混入实际输入。
- 必需消费链为 `bbt::framework` → `bbt::infra_common` / `bbt::infra_http` → transport/TCP/UDP → `bbt_coroutine` → Boost.Context 1.90、Threads、dl。公开 target、include/link 和真实加载来源已对账，不依赖旧 bbt 安装产物；当前采用源码 `add_subdirectory`，不假定存在 install/export 包。
- 最小消费所需的 HTTP/transport 与 coroutine 闭包已明确；Redis/Mongo、RPC wire、MCP、真实业务 fork 不设成统一前置，不因本节点完成宣称这些范围已通过。
- 三项完整 SHA 的干净快照和仓外独立 consumer 已配置、构建并实跑，现有 framework/infra 头自检通过，consumer CTest 1/1 通过。完整消费矩阵、源码、configure/build/test 命令、include/link/load 对账和环境前提见 [Issue #31 最终验收证据](https://github.com/yqm-307/bbt-framework/issues/31#issuecomment-5966930958)；四项 Done when 均已满足。
- 缺配置的真实失败与恢复条件已列明：无效源码目录被配置门禁拒绝；`NEED_TEST=ON` 未给 protobuf 前缀时，其 RPC 跨语言测试范围保持 BLOCKED。全量框架测试、live 后端、MCP、业务 fork、ASAN/TSAN/长跑与最新上游组合不在本节点通过结论内。
- 本 Resolution 只收束已核实的消费基线研究，不修改已归档的生命周期决定，不授予源码实施或相邻工单推进权限。执行与验收细节以 [Issue #31](https://github.com/yqm-307/bbt-framework/issues/31) 为真源。

## Checkpoint
- 已确认：#32 / PR #34 合并已解除旧消费闭包的调用者迁移阻塞；#31 的 T1/T2/T3、R1–R4、S1/S2 已验收并关闭，本节点四项 Done when 满足。
- 证据：[最终验收与最小复现](https://github.com/yqm-307/bbt-framework/issues/31#issuecomment-5966930958)；旧失败证据仍保留于下方历史检查点和执行 issue。
- 归档范围确认：用户在看到节点范围、记录未同步缺口及整图仍未完成的说明后，确认“补齐本节点 Resolution、检查点和索引，然后正常归档”；只处理 `p0-consumption-matrix`。
- 认领/在途：本节点无登记的认领或调查；本次 Alice 执行已结束，归档前未发现相关消费探针进程，亦无 live 子代理。当前会话 `ae8abf9ded2f` 串行收尾，不宣称本地 Markdown 提供事务锁。
- 接续：现有消费证据可供相邻研究引用，但不改其状态或依赖；需要改变本节点消费选择时，须明确重开并重新验证受影响范围。

## 历史检查点
- 已确认：当前三仓 main 不能只更新消费 SHA 就直接组合；framework 仍使用上游已删除的生命周期类型。新生命周期消费决定尚未闭环。
- 证据与执行接续真源：[Issue #31 实测检查点](https://github.com/yqm-307/bbt-framework/issues/31#issuecomment-5951074831)；完整 SHA、target 矩阵、真实构建错误与恢复条件均留在执行 issue，不在 map 另记执行进度。
- 决策下一步：以 framework 新生命周期迁移候选重新判断可消费版本；不恢复旧兼容壳，不因已写检查点关单。

## Archive
- 时间：`2026-10-03T16:17:57+08:00`；结果：`complete`，正常归档。
- 范围：仅本决策节点 `framework-issue-1-decision/p0-consumption-matrix`（对应执行 Issue #31）；原地保留文件，整图仍为 `active`，其他工单、依赖与迷雾不变。
- 用户确认：在本次 `/mwc-wfer-archive` 展示节点范围、已有完成证据和本地记录未同步情况后，选择“补齐本节点 Resolution、检查点和索引，然后正常归档”。
- 依据：四项 Done when 均由上述 Resolution 与 [#31 最终验收](https://github.com/yqm-307/bbt-framework/issues/31#issuecomment-5966930958) 支撑；归档前已补齐唯一 MAP 决策索引，并核实执行 issue 保持 CLOSED、证据未变，无本节点在途研究或消费探针运行。
- 边界：此范围无未解决归档缺口；全量测试、RPC/live 后端/业务 fork 和最新上游组合的未覆盖说明继续保留，不将本次归档等同整图或这些执行范围完成。
- 接续：后续可引用已归档消费基线；若要改变本节点结论，须用户明确重开，保留本 Archive/Resolution 历史并重新核验受影响输入。未执行 Git commit/push、目录迁移、删除或 WebUI 会话归档。
