---
schema: wfer-execution/v1
plan_id: p0-rpc-fixture-service-example
revision: 2
artifact: design
---
# P0 业务 RPC fixture 与双 Service 示例设计

## 实现方向

1. **先做输入门禁，再动源码**：从固定三仓消费 SHA、目标仓当前分支和独立构建目录开始；核对 `protoc 3.21.12`、公开 target、依赖 provenance、runner 工具链镜像 digest 和 CI 使用镜像的证据。P0-A 的 V10 状态明确记录为“未认证 loopback”，不以 stub 或跳过测试替代；只有 P0-B 缺少认证产物时进入安全轨道 `blocked`。
2. **以 `.proto` 为唯一业务 schema**：在 framework 拥有的示例/fixture 范围内放置一个最小业务 schema；C++ 生成物仅进构建目录，Python 使用标准库 wire codec 和明确测试向量。V2 兼容字段只用于验收，不成为生产 API。
3. **复用现有 typed RPC seam**：沿 `ProtoCodec`、`FromProto`、`ParseProto` 和公开 Service 调用面接入最小 typed adapter；不把生成头扩散到 framework 公共接口，不手写第二份 field table。
4. **在 framework bridge 处完成 formal profile 接线**：出站在发送前从父级已收紧绝对 deadline 计算 `remaining_budget_ms` 并拒绝过期；入站使用 listener-local deadline 与 envelope budget 的最小值；body 使用 infra `RpcWire` 正式转换，错误沿既有 sanitization 规则传播。
5. **分层交付示例**：先形成公开 API 的单进程双 Service fixture，再形成两个独立 `CoApp` 的 caller/server fixture；两者共享 #4 的业务 schema、wire client 和机器结果格式，不新造协议。
6. **单独 driver 承载跨进程 oracle**：driver 负责动态端口、server readiness、请求向量、结构化结果、断连/硬截止控制和清理；日志仅作诊断。正常关闭由公开控制路径触发，硬终止由外部 supervisor/harness 观察。
7. **拆分安全边界**：P0-A 使用本机/CI 未认证 loopback，只证明业务/传输/生命周期行为，结构化结果必须带 `auth_state=unauthenticated_loopback`；V10 认证 adapter、身份绑定、授权 oracle 和伪造 metadata 否决证据归 P0-B，不阻止 P0-A 开工，也不能被 P0-A 结果替代。#8/#9 的资源与 Compose 设计不嵌入本包。
8. **固定 CI 运行时**：本机通过 `scripts/prepare_protobuf.sh` 在隔离 `.deps` 前缀准备 `protoc 3.21.12`；CI runner 镜像预装可复核工具链，但 workflow 仍必须打印/校验实际镜像和依赖 manifest。不得使用浮动 `latest`、`master` 或缺少 digest 的 runner 作为最终证据。

## 关键取舍

- **in-memory fixture 而非 Redis/Mongo**：首个 #4/#5 fixture 证明业务协议、bridge、预算、错误和 Service 公共面；真实后端组合由 #8/#9 单独验收，避免把资源失败和协议失败混成一张通过条件。
- **Python 标准库手写 codec 而非 Python protobuf runtime**：继承已有 zero-dependency xlang 方向，减少环境前置；`.proto`、field number 和 golden vectors 仍是可核验真源。
- **formal body envelope 而非现有 header bridge**：当前 `x-bbt-*` bridge 与正式 infra body profile 不一致；必须修正业务路径而不是复用 transport echo 冒充完成。
- **静态路由而非自动发现**：#5 只证明公开 caller/server 和白名单路由，避免提前引入注册中心、draining、动态发现或生产部署语义。
- **不重试、不提供业务取消**：GetValue 无副作用并不自动授予通用 retry；`OutcomeUnknown` 仍需诚实表达。当前生命周期决定优先级高于旧研究兼容壳。
- **P0-A/P0-B 分离而非假身份**：loopback、可构造的 `peer_principal` 或私有 C++ 构造不能证明认证/授权；P0-A 明确标为未认证并继续验证业务 fixture，P0-B 在真实认证产物缺失时保持 blocked。

## 变更面

以下是未来获准实施时的预期变更面，不是本轮已发生的修改：

- **framework 示例/fixture**：新增示例 `.proto`、构建时生成入口、C++ typed adapter、GetValue handler、双 Service 进程入口、静态路由配置和公开 README 运行说明。
- **framework RPC bridge**：仅补齐正式 infra `RpcWire` body profile 与业务 typed payload 的接线、预算收紧和错误映射；不改变 infra schema。
- **framework tests/driver**：新增 Python stdlib client、golden wire vectors、单进程/双进程 driver、结构化结果和 supervisor hard-stop harness。
- **build/test metadata**：#4/#5 仅注册所需目标和 fail-closed 检查；#37 承载对应工具链镜像/CI 最小接线，不扩大成集群重构。镜像导入、ARC/共享 runner 切换须另行运维授权，不因为工单存在自动执行。
- **文档**：记录公开构建/运行入口、场景、证据层级和未覆盖范围；不得把 fixture 写成生产业务或把研究结果写成测试通过。
- **不改动**：bbtools-infra wire contract、coroutine runtime contract、Resource Registry 实现、Redis/Mongo adapter、Compose、生产配置、公共取消 API、OrderedIngress/ActorSerial 语义。

## 风险与恢复

| 风险 | 识别/验证 | 恢复方式 |
| --- | --- | --- |
| V10 认证前提缺失 | 预检找不到真实认证来源、身份绑定或授权 oracle | P0-A 继续保持 `auth_state=unauthenticated_loopback`，不得扩大安全宣称；P0-B 保持 `blocked`，等待拥有者提供版本化 adapter 和独立进程伪造 metadata 否决证据 |
| formal bridge 与既有 header bridge 混用 | 结构化抓包/driver 发现业务结果依赖 `x-bbt-*` 或 raw body | 停止验收，回退到 formal body profile；保留失败证据，不扩大 schema |
| `protoc`/公开 target/CI 镜像不可用 | configure 预检失败、生成输出不可复现、实际 runner 镜像与声明不符或目标缺失 | P0-A 在 T1 阻塞；核对固定前缀、镜像 digest/manifest 和消费 SHA，不能改为系统全局版本、浮动 tag 或跳过生成 |
| deadline/OutcomeUnknown 误判 | 竞争场景出现双终态、timeout 被当作远端未执行、自动重试 | 退回 bridge/driver 任务修复，重新执行 S6/S7；不修改用户已确认的“不重试”边界 |
| 示例依赖 internal seam | 代码 review 或运行入口出现 Binder、测试注入、私有头 | 删除依赖并沿公开 API 重做；必要时将方案标记 draft 重规划，不降低验收标准 |
| 关闭释放过早 | 迟到回调后出现 UAF、重复 Close、owner 计数提前归零 | 保留失败样本，修正 handler drain/owner 持有；任何源码契约变化需回到对应决策 ticket 复核 |
| 跨进程端口/进程残留 | driver 无法回收子进程或固定端口冲突 | 使用动态端口、唯一工作目录和 `finally` 清理；不杀无关进程、不做全局清理 |
| V01–V14 与实现边界发生冲突 | 新实现要求写入、重试、ActorSerial、真实认证或资源依赖 | 停止受影响任务，回到地图对应决策请求用户复核；不自动改 Resolution 或扩大方案 |

恢复不得删除既有失败证据、未完成任务或用户已确认边界。若方案关键契约变化，revision 从 1 递增并回到 `draft`；普通实现失败只在执行票中回 `in_progress`，不伪造 done。
