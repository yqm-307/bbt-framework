#pragma once
// bbt-framework Issue #5（EX-T4）：容量 / 并发上下文 / 关闭生命周期“验收专用”服务。
//
// 本头**只**为 EX-T4 验收 fixture 服务，明确“仅验收用”：
//   - 业务 schema / typed codec 完整复用 #4 的 getvalue（getvalue_service.hpp +
//     get_value.proto）；#5 不另造第二套 schema、协议或 codec；
//   - 不引用 framework/internal/、Binder、CoAppSeam、MakeCoAppForTest、
//     DispatchInboundForTest、INetworkHost 桩或任何发送注入；
//   - 唯一的“控制”是进程内的验收闩 OpenGate：Hold 在其上以无界期限挂起
//     （让 worker 让出、在途计容量），由 fixture 的 stdin `open` 命令放行。
//     这不是生产注入点，也不改变框架行为；业务语义不依赖它。
//
// 观测（journal）由 fixture 注入的 JournalFn 写入：handler_entered /
// handler_resumed 记录受管 request_id 与 local coroutine id，用来断言并发
// 上下文跨挂起不串、迟到完成不悬空。全部记录带
// auth_state=unauthenticated_loopback。

#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/example/v1/get_value.pb.h>

#include <bbt/framework/CoRpc.hpp>
#include <bbt/framework/CoService.hpp>
#include <bbt/framework/RequestContext.hpp>
#include <bbt/framework/Result.hpp>
#include <bbt/framework/RpcMethods.hpp>

// ProtoCodec<GetValueRequest/Response> 特化 + in-memory store（#4 唯一真源）。
#include "getvalue_service.hpp"

namespace lifecycle {

namespace co  = bbt::coroutine;
namespace fw  = bbt::framework;
namespace pb  = bbt::example::v1;

inline constexpr const char* kAuthState = "unauthenticated_loopback";

// ── journal 注入（fixture 主程序设置；未设置时 no-op）──────────────────────
using JournalFn = std::function<void(const std::string&)>;
inline JournalFn& JournalFnRef() {
    static JournalFn fn;
    return fn;
}
inline void SetJournal(JournalFn fn) { JournalFnRef() = std::move(fn); }
inline void Journal(const std::string& line) {
    if (JournalFnRef()) JournalFnRef()(line);
}

// ── 验收闩：单次放行、多等待者；Open 早到不丢唤醒 ──────────────────────────
// 与 tests/Test_framework_matrix.cc 的 CoGate 同构；此处作为 fixture 进程的
// 验收控制原语，Open 由 fixture 主线程（stdin `open`）调用。
class OpenGate {
public:
    void Open() {
        std::vector<co::sync::CoWaiter::SPtr> notify;
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            m_open = true;
            notify.swap(m_waiters);
        }
        for (auto& w : notify) w->Notify();
    }
    co::WaitStatus Wait(co::Deadline deadline) {
        auto waiter = co::sync::CoWaiter::Create();
        co::WaitOptions wo;
        wo.deadline = deadline;
        return waiter->WaitWithCallback(wo, [&]() -> bool {
            std::lock_guard<std::mutex> lk(m_mtx);
            if (m_open) waiter->Notify();
            else        m_waiters.push_back(waiter);
            return true;
        });
    }

private:
    std::mutex                            m_mtx;
    bool                                  m_open = false;
    std::vector<co::sync::CoWaiter::SPtr> m_waiters;
};

inline OpenGate& Gate() {
    static OpenGate gate;
    return gate;
}

// ── 验收服务：Hold（挂起/容量）+ Fast（即时）───────────────────────────────
class LifecycleGateService final
    : public fw::CoService<LifecycleGateService> {
public:
    static constexpr std::string_view kServiceName =
        "bbt.example.v1.LifecycleGateService";

    // Hold：快照受管上下文 → 记录 entered → 在验收闩上以无界期限挂起
    // （让出 worker、保持物理在途、占服务容量）→ 放行后重取上下文并记录
    // resumed。key 只随 journal 记录，用于驱动器交叉对照，不改变语义。
    fw::CoRpcResp Hold(fw::CoRpcReq req) {
        auto parsed = req.ParseProto<pb::GetValueRequest>();
        if (!parsed) return fw::CoRpcResp::Error(parsed.error());
        auto ctx = fw::CurrentRequestContext();
        if (!ctx) return fw::CoRpcResp::Error(ctx.error());

        const std::string rid = ctx.value()->request_id;
        const std::uint64_t co_id =
            static_cast<std::uint64_t>(co::GetLocalCoroutineId());
        const std::string principal = ctx.value()->peer_principal;
        Journal("event=handler_entered req=" + rid + " key=" +
                parsed.value().key() + " co_id=" + std::to_string(co_id) +
                " peer_principal=" + principal + " auth_state=" + kAuthState);

        const co::WaitStatus st = Gate().Wait(co::Deadline::max());
        const bool completed = (st == co::WaitStatus::Completed);

        std::string rid2;
        std::uint64_t co2 = 0;
        if (auto ctx2 = fw::CurrentRequestContext()) {
            rid2 = ctx2.value()->request_id;
            co2  = static_cast<std::uint64_t>(co::GetLocalCoroutineId());
        }
        Journal("event=handler_resumed req=" + rid + " ctx_id=" + rid2 +
                " co_id=" + std::to_string(co2) +
                " pre_co_id=" + std::to_string(co_id) +
                " status=" + std::to_string(static_cast<int>(st)) +
                " same_ctx=" + std::string(rid == rid2 ? "1" : "0") +
                " same_co=" + std::string(co_id == co2 ? "1" : "0") +
                " auth_state=" + kAuthState);

        if (!completed)
            return fw::CoRpcResp::Error(
                fw::MakeError(fw::ErrorCode::TimedOut, "hold: gate not opened"));

        pb::GetValueResponse out;
        out.set_found(true);
        out.set_value("held:" + rid);   // 回填=请求自己的 id：串扰即被检出
        return fw::CoRpcResp::FromProto(out);
    }

    // Fast：即时成功（容量探测/恢复观测）。记录 handler 进入，
    // 让容量拒绝场景能判别“探测请求未进入 handler”。
    fw::CoRpcResp Fast(fw::CoRpcReq req) {
        auto parsed = req.ParseProto<pb::GetValueRequest>();
        if (!parsed) return fw::CoRpcResp::Error(parsed.error());
        auto ctx = fw::CurrentRequestContext();
        if (!ctx) return fw::CoRpcResp::Error(ctx.error());
        Journal("event=handler_entered method=Fast req=" +
                ctx.value()->request_id + " auth_state=" + kAuthState);
        pb::GetValueResponse out;
        out.set_found(true);
        out.set_value("fast:" + parsed.value().key());
        return fw::CoRpcResp::FromProto(out);
    }

    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::ProtoMethod<&LifecycleGateService::Hold,
                        pb::GetValueRequest, pb::GetValueResponse>("Hold"),
        fw::ProtoMethod<&LifecycleGateService::Fast,
                        pb::GetValueRequest, pb::GetValueResponse>("Fast"));
};

} // namespace lifecycle
