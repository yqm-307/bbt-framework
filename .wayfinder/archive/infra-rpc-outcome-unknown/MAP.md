---
label: wayfinder:map
map_type: decision
title_zh: HTTP/RPC 未知结果与请求阶段契约
status: archived
---
# HTTP/RPC 未知结果与请求阶段契约

## Destination
在 `bbtools-infra` 锁定可核验的出站请求阶段与 `OutcomeUnknown` 分界，使 framework 能区分“尚未完整发出”的确定传输失败与“已完整发出、回复未知”的结果未知；形成可执行 issue 和真实回归证据，不改业务重试语义。

## Notes
- 目标仓库：`yqm-307/bbtools-infra`；下游：`yqm-307/bbt-framework#4`。
- 触发证据：固定 infra `0a40b702935bd6a640c836d4aadefc9b87d974df` 的 HTTP client 对 connect/send/read 失败统一返回 `TransportError`；公开错误没有请求阶段事实。
- 已有相关票：#8（RpcEnvelope wire profile）、#37（HTTP 出站配额）、#62（HttpSession teardown）；本图只处理请求阶段/未知结果，不重开它们的范围。
- 用户已要求补需求票、转执行 issue，并交给子代理推进；本图只记录契约和转出，不在主会话直接实现。

## Decisions so far
- 当前只承诺安全分界：连接未建立或请求未完整写出 → 保持 `TransportError`；请求已完整写出但响应未形成可信终态 → `OutcomeUnknown` 或等价的机器可判定未知结果。
- 不用错误消息、socket errno 或时间猜测“已提交”；阶段事实必须由 HTTP client 自身记录并绑定到终态。
- 不自动重试、不新增幂等语义、不把 framework 业务层或认证纳入本票。

## 执行计划索引
- [infra #64 执行快照](other/history/infra-rpc-outcome-unknown/proposal.md)：四文件原样保留，进度以目标 issue 为准。

## Not yet specified
无。

## Out of scope
- framework #4 的 `.proto`、业务 bridge、跨 hop budget 和业务 fixture；由 framework 继续跟踪。
- `bbtools-infra#8` 的基础 wire profile 重做；只消费其现有 `OutcomeUnknown` 编码能力。
- #37 配额机制、#62 session teardown、TLS/认证、自动重试、幂等键、服务发现、生产部署。
- 不关闭 #2/#1 或 framework #4；执行票达到自身 PR/CI 出口后再由各自父票对账。

## Lifecycle
- 2026-10-06：用户要求补需求票、转执行 issue，并交给子代理推进；创建本图与执行方案，准备发布 `bbtools-infra` 专项 issue。
- 2026-10-06：已发布并读回 [`bbtools-infra#64`](https://github.com/yqm-307/bbtools-infra/issues/64)，状态 OPEN；T1–T4 转入执行，目标仓 issue 为执行真源。

- 2026-10-09 20:24:59 +0800 archived → active：临时恢复以整理归档索引（confirmed=true）
- 2026-10-09 20:25:00 +0800 active → archived：保留已转执行的未决项，按留缺口归档；历史不删除（confirmed=true）
## Archive
- status: incomplete
- reason: 决策已转 bbtools-infra#64；实现载体与完整写出边界仍未决，不代表决策完成。
- retained: other/history/infra-rpc-outcome-unknown 四文件原样迁入，未改内容。
- 2026-10-09 20:25:00 +0800 归档（archive=incomplete）：保留已转执行的未决项，按留缺口归档；历史不删除

- 布局整理说明：已发布的四文件是历史执行快照，保存于other/history；恢复原issued元数据，纠正先前清理误写cancelled。它不是当前本地执行进度，不改任务勾选、原归档含义或远端状态。
