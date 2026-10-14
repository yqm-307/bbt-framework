# Close／Stop 重构：coroutine 阶段公共 API 确认稿

- 状态：**待用户确认的实施准备稿；未实现、未编译、未独立审查**。
- 本轮范围：只整理 coroutine 公共 diff、必要连带改动与验收；不改三仓源码，不提交、推送、创建 PR 或更新依赖锁。
- 需求与边界：[`coroutine-tick-lifecycle/MAP.md`](../../MAP.md) 及其已解决工单；本稿不重开前置决定，也不扩到 framework Issue #1 的全部交付。
- coroutine 读取基线：`efcdab3da0a4957b1f3647d9e390bb6517f35c0e`。已用 `git ls-remote origin refs/heads/main` 回读为远端 main；本地主树仍为 `ddfa93c8edad9ce1e6c68b3006dce32df9130d1c`，保留已有 `AGENTS.md` 改动。
- 现状依据均可用 `git show <上述SHA>:<仓库相对路径>` 复核；以下目标签名是**建议**，不是当前接口。

## 1. 已定方向对应的具体 diff

### Scheduler 与对象身份

| 当前公开面 | 目标 |
|---|---|
| `void Scheduler::Stop()` | 删除，不留兼容壳，也不改名藏到退出钩子 |
| `bool Scheduler::IsRunning() const noexcept` | 建议删除，新增 `bool Scheduler::IsInitialized() const noexcept`；初始化完成前 false，完成后 true，不因业务关闭归零 |
| `uint64_t Scheduler::GetRunGeneration() const noexcept` | 删除；内部 `_GetRunGeneration`、代际字段、旧代际拒绝分支同步清除 |
| `using RuntimeGeneration = std::uint64_t`、`CurrentRuntimeGeneration()` | 删除；消费者需要查询初始化时改用 `Scheduler::IsInitialized()`，不再新增同义全局包装 |
| `CoObjectInfo { id, generation, kind, name }` | 只删 `generation`；保留 `CoObjectId`、`id/kind/name`、`ICoObject::GetObjectInfo()` 与 `CreateObjectInfo(kind, name)` |
| `Scheduler::Start(opt)`、`LoopOnce()`、两种 `RegistCoroutineTask` | 保留签名和三种驱动模式；成功初始化一次后，重复 Start 明确拒绝，不能再次重置配置/队列或新增 worker |

初始化完成指 runtime/worker/poller 的必要初始化已成功；不能沿用当前 `_Init()` 提前写 `m_has_started=true` 的位置直接当作完成证据。主线程自动模式的 Start 不返回，初始化标志必须在进入运行循环前发布。重复 Start 的建议错误口径为 `std::logic_error`，保持 `void Start(...)` 签名；初始化失败及并发 Start 的清理/拒绝须在实现时验证，不用布尔字段替代同步。

`CreateObjectInfo` 建议保留初始化前抛 `std::logic_error` 的前置条件，只把代际判定改为初始化标志；ID 仍为进程内递增、不复用，溢出显式报错。停止删除不等于删除对象身份。

**证据**：coroutine `bbt/coroutine/detail/Scheduler.hpp:50–70`、`Scheduler.cc:43–58/255–295`；`bbt/coroutine/object/interface/ICoObject.hpp:8–37`、`object/CoObject.cc:18–41`。

### 完成桥、取消类型与等待选项

```diff
- CompletionSignal.hpp / CompletionSignal.cc
- Cancellation.hpp / Cancellation.cc
- class CompletionSignal
- class CancellationToken
- class CancellationSource

  struct WaitOptions {
      Deadline deadline = Deadline::max();
-     CancellationToken cancel{};
  };

  struct CombinedWaitOptions {
      bool want_readable{false};
      bool want_writeable{false};
      int fd{-1};
      Deadline deadline = Deadline::max();
-     CancellationToken cancel{};
  };
```

`Deadline`、`WaitStatus`、`WaitOptions` 必须先从 `CompletionSignal.hpp` 搬到独立 `bbt/coroutine/sync/WaitTypes.hpp`，保留现有 `bbt::coroutine` 命名空间。`CombinedWaitOptions/CombinedWaitStatus` 保留在现有 `bbt::coroutine::sync`，不顺手改 namespace。删除 include `CompletionSignal.hpp` 的反向依赖后，Chan/CoCond/CoMutex/CoRWMutex/CoSelect/DNS 及伞头仍能独立包含。

**修正调查报告的过度删除判断**：`WaitStatus::RuntimeUnavailable`、`CombinedWaitStatus::RuntimeUnavailable` 仍在当前 CoWaiter 中表示事件建立/登记失败，不能只因 Stop 删除就去掉。建议本阶段保留枚举名与既有顺序，删去停机/旧代际说明，明确仅报告等待基础设施失败；不把它继续解释为可停止的 runtime。其他结果枚举保持。

**证据**：`sync/CompletionSignal.hpp:26–42`、`sync/CoWaiter.hpp:3/11–38/103–126`；`sync/CoWaiter.cc:253–254/285–287/362–374/403–404`。

## 2. 建议新增的最小公共面（本稿待确认重点）

### 带参自定义唤醒

建议新增 `bbt::coroutine::CoEventValue`，落在 `bbt/coroutine/sync/CoEventValue.hpp`：

```cpp
class CoEventValue {
public:
    static constexpr std::size_t kInlineSize = 24;
    bool HasValue() const noexcept;
    template<class T> static CoEventValue From(const T& value) noexcept;
    template<class T> bool Get(T& out) const noexcept;
};
```

- 固定 24 字节内联载荷，不分配；容量是建议，不是前置 map 已定值。
- 支持已登记、可平凡拷贝的整数、指针与小 POD；不满足尺寸/类型约束在编译期拒绝。采用按字节 `memcpy` 存取，不从 byte buffer 强转成 `T*` 读取；最终布局、对齐和 ABI 要由实际编译与 self-check 验证。
- Get 的类型标识不匹配时返回 false，且不修改 out。空值也返回 false。
- 指针只借用，不延寿；对象至少有效到等待者恢复并取用完毕，随后继续使用仍由调用方维持寿命。
- **建议类型检查采用显式稳定 type-id + traits**：常用类型由库给 ID；自定义 POD/指针类型由接口拥有者在公共头登记唯一 ID。同类型跨 DSO 用相同 ID，不依赖 RTTI 或模板静态地址。禁止把所有指针统一成 Pointer、所有 POD 统一成 Custom，否则不能兑现类型不符取失败。
- ID 唯一性是公共类型登记契约，非自动全局检查能力；目标范围内要用不同类型拒绝与双 DSO 同类型互通验证。此形态需用户确认；若不接受显式登记，类型标识方案仍是实施阻塞，不自行回退到不可靠的地址比较。

### CoWaiter 只增通知载荷，不重造 CompletionSignal

在既有 Wait 族外建议增加以下重载；具体名称、参数顺序本稿待确认：

```cpp
// 位于 bbt::coroutine::sync::CoWaiter；既有无参 Notify / Wait 族保留。
int Notify(const CoEventValue& value);
WaitStatus WaitWithCallback(
    const WaitOptions& options,
    const detail::CoroutineOnYieldCallback& on_registered,
    CoEventValue* value = nullptr);
CombinedWaitStatus Wait(
    const CombinedWaitOptions& options,
    const detail::CoroutineOnYieldCallback& on_registered,
    CoEventValue* value);
```

- 新 WaitWithCallback 使用已存在的绝对 Deadline，结果区分完成/超时/协程级取消/上下文错误/登记失败；不新增第二个 deadline 参数。
- 保留 `Wait()`、`WaitWithCallback(cb)`、`WaitWithTimeout(ms)`、`WaitWithTimeoutAndCallback(ms, cb)`、现有两种 options Wait 与无参 Notify。旧相对毫秒族的返回口径不在本次顺手更改。
- 单次流程为准备等待 → 登记成功 → 调用一次投递回调 → 等待恢复。回调前登记失败不投递；早到触发复用 PENDING。不提供 Notify-before-Wait 的粘滞完成能力。新的带参 Notify 在 fresh／无有效等待／已决议时返回 -1，不触发断言、不保存载荷；当前无参 Notify 的 COND_DEFAULT 断言不是新入口的前置条件。
- 第一个胜出的事件原因与载荷一起锁定并发布；失败或输掉竞争的 Notify 不覆盖赢家载荷。定时器/协程取消/FD 胜出时不返回败方 custom 载荷；新 output 在非 custom 完成时置为空值，不留下上次结果。
- 每个 CoWaiter 一次只有一个有效等待；载荷随该 Notify 对应的等待交付，不额外引入广播机制。
- adapter 的业务取消/连接关闭用其自定义载荷表达，经同一个 Notify 唤醒；CoWaiter 报 Completed 代表 custom 通知到达，adapter 再解释成业务 Cancelled/Closed 等终态。协程级 RequestCancel 仍由原有取消事件产生 WaitStatus::Cancelled。
- **不新增 NotifyCancelled，不保留 CancellationToken 壳**。原有 `CoWaiter::Cancel()` 不是唤醒入口，保留旧语义并明确注释，不拿它实施业务取消。
- adapter 请求状态与结果在栈外保活；业务超时/返回后解绑回调订阅。每次请求/等待使用独立 waiter，不让旧请求的晚到 Notify 误唤醒新请求；CoWaiter 没有自动按请求 ID 去重能力。

**证据**：`sync/CoWaiter.hpp:60–138`、`sync/CoWaiter.cc:186–191/260–270/380–391/424–434`；`detail/CoPollEvent.cc:355–365/626`。前置 map 已冻结结果与协议载荷由 adapter 请求状态拥有，CoEventValue 只传小型唤醒信息，不搬业务响应进 runtime。

## 3. 必须随公共 diff 一起落地的内部清理

这些是前置决定的实现扩散面，不是新一轮架构表决：

1. 删除 Scheduler Stop 的 worker/DNS 收口、线程 join、队列强删、parked 强删、restart reset、旧代际拒绝路径；同步删除仅服务停机的 parked 表与 track/untrack 接点。不能只删方法声明留下隐藏停机链。
2. 保留 FD epoch / FD 复用防护，它不是 runtime generation；保留协程级协作取消、通用等待与事件状态机、异常/TLS 边界。
3. `CoPool::Release()` 公共签名与停止接收/等待池协程退出/处理滞留任务的职责保留；移除依赖 Scheduler IsRunning 的停机逃生条件，验证普通线程与协程调用路径。主动 Tick 宿主需要在池排空期间维持进展，不承诺停 Tick 后还能无限等待成功，不为此新增 Scheduler Stop。
4. 当前 `Scheduler::GetInstance()` 的 static unique_ptr 会在退出时析构，并调用 Stop；当前 DNS static 对象持 joinable thread。删除 Stop 后不能仍让这些对象析构，亦不能直接销毁仍运行线程依赖的 poller/queue/config。按已定 process-lifetime 为 runtime 及其依赖建立一致的“不在静态退出期析构”寿命策略，连同 DNS/worker/poller 检查；不得以 std::thread::detach 代替对象寿命证明。
5. GetInstance 的现有 `UPtr&` 公开面建议暂不顺手修改，但它并非用户获准 reset/release 的所有权入口；不支持调用方手工销毁 runtime。不新增 restart 或动态卸载保证。

**证据**：`detail/Scheduler.cc:23–40/298–357`；`detail/Coroutine.hpp:210–221`、`Coroutine.cc:376/399–447/504`；`detail/DnsResolver.cc:7–10/20–22`、`DnsResolver.hpp:78/86`；`pool/CoPool.cc:163–184`。这是一项寿命/并发改动，不是机械删除通过编译即可验收。

## 4. 测试、文档与跨仓交接

- 删除 Scheduler Stop/restart、不展开栈停机、CompletionSignal、CancellationToken 旧契约的专用测试目标；可复用的早到、首胜、可见性、超时、晚到安全用例迁至 CoWaiter 新接口，不删除通用行为覆盖。
- 对象身份保留 ID 不复用与值快照验证，只删代际断言；scheduler API 保留 singleton 与 LoopOnce，补初始化查询与重复 Start 拒绝。
- 每个测试 executable 初始化一次 runtime。不同配置/驱动模式拆进程；其余 fixture 去 Stop，退出前核实自己拥有的池、FD 和活动，不以 OS 回收当资源验收。
- 新接口必须测：同步回调完成、回调前登记失败、并发 Notify 首胜与载荷一致、定时器/协程取消/FD 竞争、无等待者通知、错类型读取、超容量编译拒绝、指针借用、超时后晚到回调、跨 DSO 类型识别。
- 保留并定向运行 `Test_cowaiter_outcome`、`Test_combined_wait`、`Test_cowaiter_regist_fail`、`Test_copollevent_state`、`Test_coroutine_cancel`、DNS/CoPool/Hook/TLS/IoExecutor 直接耦合覆盖。专用旧大测试每个 case 的迁移对应表在实现时补齐，当前没有完成逐 case 验收。
- 本地按 AGENTS 的冒烟 + 本次功能 + 直接耦合单测执行，全量回归走 PR CI。旧流程文档验证阶梯仍写本地全量，和当前 AGENTS 不一致；本轮按已读 AGENTS 的明确本地边界，不擅自跑全量。
- 当前没有已确认的专用公共头 self-check 目标。实施包应补逐公共头独立 TU 编译/独立消费验证，至少覆盖新 WaitTypes/CoEventValue/CoWaiter/对象头与伞头；仅 include 伞头不能证明每个头自给。若新增 CMake 验证目标，随本次范围一起确认，不修改 CI/发布流程。
- 同步改 coroutine 核心 runtime 契约、API/user guide、README/示例与 AGENTS 的旧 Stop 文案；历史 #347/#351 和旧报告保持原文，在新迁移说明标 superseded，不拿旧通过证据当新通过。
- 后续顺序：coroutine 候选完成测试与独立 review → 固定可消费 SHA → infra Close/adapter 迁移 → framework 生命周期与依赖锁迁移。每层自行复验，不同版本不混搭，不留兼容壳。

**跨仓现状仅作风险提示**：framework 当前远端 main `d759e1b1f367f0e41dfe58090e59893979c33481` 的 deps.lock 仍固定 coroutine `5bda5276f42f0200a26d22c8807d2d260a4e3fa7`、infra `0f468d9b7af409227f53920bdbf2ba5bd9882e8d`；不是本稿 coroutine 基线，也不是最终迁移版本。本轮没有改锁文件、核验下游新版集成或执行真实网络测试。

### 关键用例草案（目标接口尚不存在，均未运行）

| 契约与方法 | 具体事件序列／输入 | 必须观察的结果 | 区分的错误 |
|---|---|---|---|
| 登记后才投递；M4/M8 | 注入事件登记失败，再调用新 WaitWithCallback | RuntimeUnavailable，投递回调计数为 0，value 为空 | 先投递后登记造成孤儿请求 |
| PENDING 早到；M4/M9 | on_registered 同步 Notify(整数 7)，不延迟到另一个线程 | Completed，取回同类型值 7，只恢复一次 | 登记与挂起之间丢通知 |
| 首胜载荷；M6/M9 | 用屏障使 Notify(7) 成功后再 Notify(9)，两者均在等待者恢复前发生 | 值仍为 7；第二通知不覆盖；只恢复一次 | 原因首胜但载荷被后到写覆盖 |
| 超时首胜；M4/M9 | 定时器先锁定结果，恢复前另一个线程 Notify(7) | TimedOut，value 为空，不残留 7 | 超时返回败方 custom 载荷 |
| 类型拒绝；M1/M10 | 存 int64_t(7)，用不同登记 ID 的同尺寸 POD 读取，out 预置为另一值 | Get 返回 false，out 不变；同类型读回 7 | 仅按 sizeof 或粗粒度 Custom tag 校验 |
| 固定容量；M2/M10 | 已登记的 23B、24B POD 与 25B POD 分别实例化 From | 前两种可编译，25B 必须编译拒绝 | 小于/小于等于界值写错或 buffer 越界 |
| 非粘滞通知；M4 | fresh waiter 无等待者时 Notify，再建立带期限等待 | 首次 Notify 返回失败；后续不因旧通知立即 Completed | 隐藏完成粘滞位或等待前通知被错误复用 |
| 晚到请求隔离；M4/M9 | 请求 A 超时解绑；请求 B 用独立 waiter；随后 A 旧回调通知 | B 不被 A 唤醒，A 回调没有悬空访问 | 跨请求复用 waiter 误唤醒／UAF |
| 类型跨产物；M10 | 两个独立 DSO 共享公共 POD 的同一登记 ID，一侧 From 另一侧 Get | 同类型识别一致，另一类型被拒绝 | 用模板局部地址作跨 DSO 类型身份 |

初始化三模式、并发 Start、协程取消与 FD 首胜仍需独立有序案例；此表不是完整验收，实际实现依据控制点补齐。暂不选 M5 成对组合，因为首胜风险需要精确事件顺序；不做长压测或 M12 变异测试，因为未来接口尚未实现且本轮是准备，不以循环次数替代确定性断言。

## 5. 确认与停止点

建议确认包：本稿第 1–3 节的公共删除/保留/新增签名、24B + 显式 type-id 载荷方案、以自定义载荷表达 adapter 取消、配套本仓测试目标调整与公共头验证。确认只允许进入 coroutine 隔离实现与相关测试/文档；不授权 infra/framework 代码改动、commit/push/PR/合并/发布、CI、系统安装或基础设施操作。

当前只完成源码与历史决策的准备核验。未编译未来接口、未测静态退出安全、未做独立审查、未逐 case 迁移旧测试。用户未确认此公共 diff 前，不进入实现。
