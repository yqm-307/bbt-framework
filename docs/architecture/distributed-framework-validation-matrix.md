# 分布式服务端框架：架构基调验收矩阵

这是 [`distributed-framework-baseline.md`](./distributed-framework-baseline.md) 的测试设计，不是本轮实测结果。所有项目当前状态均为 **NOT RUN**；现有单测是否已经覆盖需进入实施时逐项核对。

分层：M = 有界模型/纯状态机属性测试；I = 真实 coroutine/infra 集成；D = 真实跨进程、网络和持久存储故障。单层通过不能代替其余层。

| ID | 阶段/层 | 故障或事件序列 | 必须成立的 oracle |
|---|---|---|---|
| V01 | P0 M/I | 父预算 100ms，子显式预算 500ms，先排队 60ms | 子本地 deadline 不晚于父；发出前过期不做 I/O |
| V02 | P0 I/D | A→B→C，每跳排队/处理，机器单调时钟原点不同 | wire 传剩余预算而非 time_point；原调用总等待不被每跳重置；不宣称远端副作用硬截止 |
| V03 | P0 M/I | completion/cancel/deadline 同步到达，穷举所有交错 | 观察到至多一个调用终态；回调解绑与资源释放只发生一次，无悬挂访问 |
| V04 | P0 I | 请求挂起后换 worker，另一个请求在原 worker 执行 | CurrentRequestContext 始终对应逻辑协程，不泄漏身份/预算 |
| V05 | P0 I/D | 服务端提交写入，客户端未收到回复就断连/超时 | 客户端记录未知结果，不把超时等同于未执行；无幂等协议时不自动重试 |
| V06 | P0 M/I | 同 operation key 相同 payload 重发；再换 payload | 同请求依协议重放或 InProgress；冲突 payload 被拒绝，不能复用旧成功结果 |
| V07 | P0 M/I | 有序请求已完成，结果缓存超字节/条数淘汰，再重发 | next_sequence 不回退；ResultExpired，不再执行；旧代际请求拒绝 |
| V08 | P0 I | Actor A 等待 B，B 回调同 Actor A | 不偷偷重入；有限 deadline 和调用链可诊断等待环；非重入语义写入文档 |
| V09 | P0 I/D | 校验失败、远程业务错误、伪造 error code、超长 details | 错误映射/线上序列化保真；不因 std::stoi/非法 enum 产生未控异常；不把未知码当成功 |
| V10 | P0 I/D | 伪造 peer_principal/fw.*、越权 method/actor key | 身份只来自认证通道；接收端独立授权；私有 C++ 构造不被当线上安全边界 |
| V11 | P0 I | 请求先超时，底层 handler/driver 仍运行 | inflight/资源计数继续覆盖存活工作；容量不能因为返回错误被提前释放 |
| V12 | P0 I | 等待队列/邮箱/Actor/出站并发/字节耗尽 | 固定上限始终成立，拒绝可观测，不产生无界补偿队列 |
| V13 | P0 I | 关闭时有未完成 I/O、晚到回调、handler 不结束 | StopAccepting→drain→close→release→runtime stop；超预算标 incomplete，不 UAF |
| V14 | P0 D | supervisor 在硬停机时限到达后终止进程 | 记录非优雅终止和未完成请求；重启后不能宣称所有请求已完成 |
| V15 | P1 M/I | 取 snapshot R 后、watch 建立前发生 R+1 更新 | 要么从 R 后无缝补齐，要么显式重建视图；不能静默永久漏更新 |
| V16 | P1 M/I/D | watch 断线、重复事件、压缩历史、provider reset | 版本与 provider epoch 校验；旧视图不覆盖新视图；压缩后重新取快照 |
| V17 | P1 M/D | 注册后端失联，现有数据面仍可达 | 无状态 stale 策略明确且有上限；鉴权撤销/owner 不复用宽松 stale 规则 |
| V18 | P1 D | 两个实例滚动退出，调用方仍持旧 endpoint | 旧实例 readiness 先关闭并明确拒新；调用总预算不刷新；无循环重定向 |
| V19 | P1 M/I | 方法可重试，底层/代理也开启 retry，持续过载 | 责任层/透明尝试记录清晰，retry budget 和 attempts 总量有界，有 jitter，不乘法放大 |
| V20 | P1 I/D | 热更新坏配置/新连接失败，再安装好配置 | 预检失败不改变生效版本；每请求只借用完整资源版本；旧版本排空可验 |
| V21 | P1 D | 同地址端口进程重启，旧缓存/旧连接仍在 | instance incarnation 被区分，旧流/旧结果不自动继承到新实例 |
| V22 | P1 I/D | 滚动运行协议 N/N-1，新增可选字段与错误码 | 兼容矩阵满足声明；不认识的语义显式拒绝或按规则处理，不能静默误解 |
| V23 | P2 M/D | owner A 分区/长暂停，B 接管，A 再写 | 最终资源原子验证 token；B 的屏障成立后 A 写入被拒绝，单靠路由刷新不算通过 |
| V24 | P2 M/D | 旧 owner 请求已排队，新 owner 建立屏障后旧请求继续 | 校验与实际写入不能分离；旧队列越过入口校验也不能提交 |
| V25 | P2 M/D | 持久状态提交后、幂等记录/响应保存前崩溃 | 明确单事务原子边界或恢复协议；否则不能承诺 exactly-once effect |
| V26 | P2 D | owner 切换与序号/缓存重建并发，旧 response 晚到 | 代际隔离；旧 completion 不污染新会话；缺失去重事实不自动重放不可逆操作 |
| V27 | P3 D | 业务事务提交后进程死，outbox 尚未发送；发送后 ack 丢失 | 消息最终可重发，inbox/业务幂等覆盖重复；不要求不可实现的 transport exactly-once |
| V28 | P3 M/I/D | workflow 回放、新版本部署、外部副作用成功但记录失败 | replay 不重复未保护副作用；版本历史兼容；补偿失败是可持久跟进状态 |
| V29 | P4 I/D | hot key、慢下游、连接抖动、资源压力逐级增加 | 输出吞吐、排队和执行 latency 分布、失败率/重试率及资源水位，不仅平均耗时 |
| V30 | 全阶段 M/I/D | 重放一个已发现失败的 seed/history | 模型有确定性反例；真实测试记录环境/版本/故障时间线，不把真实运行标为严格确定性 |

## 记录格式建议（非公共 API）

每次操作至少记录：逻辑 operation_id、attempt_id、client、目标服务/实例/代际、invoke/completion、返回结果与已知事实、route/config version、持久提交证据（若可获得）、故障注入事件及随机 seed。

线性一致性检查只用于承诺线性一致的对象与操作模型。多客户端原始墙钟时间不能直接构造可靠实时序；优先统一测试控制器的因果事件/请求响应记录，必要时显式纳入时钟不确定性。失败/超时操作不能一律删除：可能已生效的操作需要按模型处理 pending/unknown completion。

`checker timeout/unknown` 不是通过；模型边界外副作用不因为 KV history 通过就得到验证。仿真覆盖受控时钟/网络/随机数；内核、第三方 driver、真实线程竞争及性能必须用对应测试补齐。

## 分阶段退出标准

- P0：V01–V14 中与实际承诺相关的路径可运行，未覆盖明确列为阻塞/范围排除。
- P1：P0 不退化，V15–V22 有真实两实例调用证据与故障历史。
- P2：在具体存储/协调机制确定后先审协议，再通过 V23–V26；未通过不得宣传“唯一 owner/故障自动切换下恰好一次”。
- P3：只验证所选扩展与真实业务，不宣布框架普遍 exactly-once。
- P4：性能报告限定硬件、版本、数据量、故障/负载分布；保持安全性检查。


关联设计：[`distributed-framework-baseline.md`](./distributed-framework-baseline.md)。
