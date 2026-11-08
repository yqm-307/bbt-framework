# examples/rpc_policy — 公开出站 RPC 策略 fixture（R8 / 3-5）

验证 framework 正式公开的**出站策略入口**（窄 preflight 虚拟点）在真实 socket
路径上可继承、可包装、可替换，且仍受 framework 上下文/路由/期限/错误边界与
infra owner·限额·关联·Close 约束。

## 扩展入口

`framework/include/bbt/framework/RpcPolicy.hpp`：

- `RpcCallView`：只读调用事实（已解析目标 + 不可改写 `rpc::Request` + 已适配
  绝对期限）。
- `RpcPolicyDecision`：`Allow`（放行，可选 `tightened_deadline` 只收紧期限）或
  `Reject`（拒绝，零 I/O）。
- `RpcPolicy::Preflight(const RpcCallView&)`：唯一受控虚拟点。

组装点：`CoAppOptions::rpc_policy`（`std::shared_ptr<RpcPolicy>`，构造期选择；
`nullptr` = 默认 no-op，行为与今天逐字一致）。

插入点：`CoApp::_MakeEgress()` 在静态路由门（`find_route`）**之后**、发送
**之前**调用一次；V2 会话（`RpcEgress` → `rpc::CoRpcClient`）是唯一出站路径，没有
profile 或旧 envelope 分支。

## 本 fixture

caller/callee 是**两个互异 identity 的真实 CoApp 进程**（V2 拒绝
self-connection，框架约束进程内至多一个活跃 CoApp）：caller 进程
（`rpc-policy-caller`）宿主 `PolicyCallerService`，其 handler 内经
`this->call<Req,Resp>` 出站，经 V2 会话到达 callee 进程
（`rpc-policy-callee`）宿主的 `PolicyCalleeService`。**只用公开面**，不 include
`internal/`、不用 `CoAppSeam`/`MakeCoAppForTest`/发送注入。

每场景独立 caller 进程（一策略），由它派生一个 callee 子进程；
`rpc_policy_run.py` 经 `tests/rpc_v2_wire.py` 驱动并断言：

| 场景 | 断言 |
|---|---|
| `allow` | 调用成功；`Preflight` 恰一次；callee handler 恰一次 |
| `reject` | 调用报错（`Cancelled`）；callee handler 0 次（观测；「出站未发送」由 `_MakeEgress` 在 `Send` 前返回的源码结构保证，不由此计数单独证明） |
| `tighten` | callee 可见剩余预算 ≤ 收紧值且 < 原预算（只缩不放） |
| `widen` | 策略放大被忽略：预算仍≈原预算，绝不到上限 |
| `expired` | 收紧到已过期 → I/O 前 `TimedOut`；callee 0 次（同上：计数为观测） |
| `decorator` | 外层包内层：两虚 `Preflight` 各一次、序 `outer→inner` |
| `noroute` | 未配置静态路由 → `NotFound`；`Preflight` 计数 0（路由先于策略） |
| `shutdown` | stdin EOF → `rc=0`/`Closed`/owner 同步 Close |
| `shutdown.after-close` | fixture 在 `app.run()` 返回后、**进程退出前**对自己 bound 端口做有界 nonblocking connect，journal `event=listener_probe phase=after_run_before_exit port=… result=… errno=…`；driver 断言 `result=refused` 且 `errno=ECONNREFUSED` 且 `port` 等于本次服务过真实 RPC 的端口 |

关闭后验收的判别性：探针必须发生在**进程与 `CoApp` 对象仍存活**时；进程回收后再
`connect` 由内核必然 `ECONNREFUSED`，那类断言恒真、不构成证据（B1 已修）。探针的
超时或任何非 `ECONNREFUSED` errno 记 `result=error` 并判失败，不判绿。该探针只证明
「存活期 listener 已关」——它不声明关闭期零额外 I/O，也不替代在途 I/O 排空判定。

ctest：`examples.rpc_policy`。全部结构化结果标注
`auth_state=unauthenticated_loopback`：本路径只证明公开扩展面与治理约束，不证明
身份可信（认证见后续范围）。
