---
label: wayfinder:research
blocked_by: []
assignee:
claim_session:
status: open
outcome:
archive:
---
# Resource Registry 公共契约与生命周期边界

## Question
Issue #8 的 Resource Registry 首个公共切片应如何定义 `(资源类型, 逻辑名称)` 注册/查找、Create/Start、失败逆序回滚、同名共享/不同名隔离和 manager 主动同步 Close，才能不泄漏 hiredis/mongocxx/worker/连接池类型，也不恢复已删除的 Stop/WaitClosed/旧 token 协议？

## Done when
- 回读现有 framework Resource Registry、App/Service、Redis/Mongo consumer 与已合入 infra Close 契约，列出真实调用关系和缺口。
- 明确公共 API 的最小签名/错误语义、资源 owner 归属、重复登记、启动失败回滚和关闭顺序；区分逻辑状态与物理释放。
- 明确两个 Service 共享同名资源、不同名资源隔离、关闭中请求和资源未就绪时的行为矩阵。
- 确认真实 Redis/Mongo consumer 验收所需的最小 fixture、后端前提和不可用边界；不实现、不修改默认生产路径。

## Checkpoint
- 已确认：现有 Registry/factory 可复用，旧等待 bridge 与新同步 Close 冲突；失败逆序回滚不能从 unordered_map 现状推定满足。Mongo 公共头旧注释不构成放宽同步 Close 的决定。
- 证据与执行接续真源：[Issue #8 研究检查点](https://github.com/yqm-307/bbt-framework/issues/8#issuecomment-5951075650)；consumer 前提、契约建议和恢复条件留在执行 issue，不在 map 另记执行进度。
- 决策下一步：核定现有 CoApp owner 的最小迁移与回滚/真实 consumer 验收边界；未实现不报解决。
