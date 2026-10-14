---
schema: wfer-execution/v1
plan_id: framework-service-example-loopback
revision: 1
artifact: proposal
status: blocked
---
# framework #5 接管与最终验收
## 目的
完成 #5 既定 EX-T1–EX-T5 的最终证据对账；不把 PR #47/#48 已合入或六项测试通过等同整票完成。
## 范围
目标仓 yqm-307/bbt-framework。复用已合入双 Service、双进程路由、容量和生命周期 fixture；核对缺失的出站/等待容量及 #37 工具链身份。
排除 Redis/Mongo、Compose/#9、认证/#38、#4 跨 hop 主线、生产和性能承诺；ActorSerial/OrderedIngress 沿已定排除。
## 影响与依据
来源 https://github.com/yqm-307/bbt-framework/issues/5#issuecomment-5975277535 ，旧记录完整保留，只读作为需求/证据。用户本会话明确同意新建独立交付地图接管 #5/#35；以后进度唯一维护此 tasks.md，不再在 issue 独立复制进度。
已核验 framework 0258cc0fd58c188b137a5f7d81be5a1a479dc58e、消费 infra 162bb5fd1340e8b3c043086e8ed753aa5ec533cb、coroutine 7bcda3b078f975ff2978424be7f6ba38e04fb7f6。PR48 合入 CI37876034633成功。
## 交付约定
本批仅本地验收/证据整理、计划更新；必要测试候选须先核清契约，不代改核心、上游或 CI。无 commit/push/PR/merge/关单/部署/系统安装授权。出口为本地完整验收报告；若要求有缺项则 blocked，不能缩减原承诺制造完成。预算一轮 fresh 构建与定向检查、一次有界缺口调查；无需重跑已有效证据，缺前置即记录恢复条件。当前状态 blocked 的前阶段为 verifying：#37 CI-T3 镜像身份缺可核验证据。

本轮父级容量复核已完成：不是产品实现验收。出站框架公共路径/响应字节尚缺证据，等待容量语义待决定；共用配置不等于入出站共用计数。当前可见手动toolchain身份job失败（旧head），无适用成功证据。须明确限定产品测试写入及等待判据后接续，外部CI身份保持blocked。

## 并行 runner 重构隔离边界（用户本轮明确）
runner整套系统由并行需求重构，本包仅消费其验收产物，不维护第二套runner/CI迁移。参考只读源：team-agent-repo/.wayfinder/github-ci-standardization/checkpoint.json 与 framework #50；不更新其任务/绑定。当前三仓hosted影子已验收不等于权威切换。暂停本包任何runner镜像构建/发布/导入/切换、ARC/代理/平台准入、workflow/required/CI模板接线与为本票触发CI；包括能自动触发现役CI的push/PR。保留既有本地证据，不以历史身份job失败要求恢复旧arc-s4-framework/v2方案。
恢复CI验收条件：并行拥有者提供获准可用的目标执行路径、实际工具链/镜像身份与provenance、适用source/head/job门禁及隔离/触发窗口，并明确#37旧判据如何映射；若替换原锁定镜像验收需明确需求确认，不能擅改锁或降低判据。只暂停实际依赖该交接的#5最终出口，不把整个本地图paused。#5容量和#35config本地补证无runner运行依赖，可以在已明确产品写入范围内隔离实施，本轮评估不新增该授权。
