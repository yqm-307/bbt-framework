#pragma once
// 测试侧 native ↔ Co 桥（仅测试，不是产品面）。
//
// 生产 framework 不再提供 native 生命周期入口：业务主协程在 runtime 的
// bbtco_main 里用 v2::CoApp::CoStart / CoWaitShutdownRequest / CoStop 驱动，
// 共享 Engine 的 Engine::Run 只在入口 native host hook 执行一次。
// 既有测试的逻辑仍由 native 测试线程编排，本头把它们包成「在受管协程内执行、
// native 侧有界等待结果」的辅助，避免每个用例重写编排，同时保留 native 测试
// 线程自己的同步原语（测试不是业务调用者）。
//
// 约束：辅助只在测试里用；产品代码不得包含本头，也不得据此新增 native 生命周期
// 入口。

#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Engine.hpp>

#include <bbt/framework/App.hpp>
#include <bbt/framework/HostGate.hpp>
#include <bbt/framework/Object.hpp>
#include <bbt/framework/internal/CoAppSeam.hpp>
#include <bbt/framework/internal/InfraHttpHost.hpp>

namespace framework_test {

using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

namespace fw = bbt::framework;
namespace inf = bbt::infra;
namespace net = bbt::infra::net;

namespace detail {

struct Latch {
    std::mutex              mtx;
    std::condition_variable cv;
    bool                    done{false};
    fw::result<void>        rc = fw::result<void>::ok();

    void Settle(fw::result<void> r) {
        {
            std::lock_guard<std::mutex> lk(mtx);
            rc = std::move(r);
            done = true;
        }
        cv.notify_all();
    }
    bool Wait(std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lk(mtx);
        return cv.wait_for(lk, timeout, [this] { return done; });
    }
};

} // namespace detail

// result<void> 的布尔判定（显式 operator bool 在断言宏里不可直接使用）。
inline bool CoOk(const fw::result<void>& r) { return static_cast<bool>(r); }

// 前置声明：辅助宿主起停在下面定义，引用本入口。
inline bool RunInCoTask(std::function<void()> fn,
                        std::chrono::milliseconds timeout = 30s);

// 真实入口（bbtco_main）负责启动 runtime；测试的 native 线程直接编排时在这里
// 补上同一步（已初始化则复用，不重复 Start）。受管 handler 的嵌套出站链在
// 12KB 默认栈触底（guard 页 SIGSEGV），测试统一按生产示例放大到 64KB；并发
// 用例需要 app 服务/客户端/出站宿主与 spinner 同时有位可跑，统一给 8 个
// 静态 worker。配置只在首次 Start 前生效。
inline void EnsureRuntime() {
    if (!g_scheduler->IsInitialized()) {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 8;
        g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
        g_scheduler->Start();
    }
}

// 在受管协程内执行 fn（可含 bbt_co_await）；native 调用线程有界等待其结束。
// 返回 false 表示超时或任务登记失败——调用方必须把它当失败，不得当成完成。
inline bool RunInCoTask(std::function<void()> fn,
                        std::chrono::milliseconds timeout) {
    EnsureRuntime();
    auto box = std::make_shared<detail::Latch>();
    bool registered = false;
    g_scheduler->RegistCoroutineTask(
        [box, fn = std::move(fn)] {
            try {
                fn();
                box->Settle(fw::result<void>::ok());
            } catch (const std::exception& e) {
                box->Settle(fw::result<void>::err(fw::MakeError(
                    fw::ErrorCode::InternalError, e.what())));
            } catch (...) {
                box->Settle(fw::result<void>::err(fw::MakeError(
                    fw::ErrorCode::InternalError, "co task threw")));
            }
        },
        registered);
    if (!registered)
        return false;
    return box->Wait(timeout);
}

// 在受管协程内求值一个 result<void> 表达式并把结果交回 native 调用方。
inline fw::result<void> CoEvalVoid(std::function<fw::result<void>()> fn,
                                   std::chrono::milliseconds timeout = 30s) {
    fw::result<void> out = fw::result<void>::err(fw::MakeError(
        fw::ErrorCode::RuntimeUnavailable, "co eval not run"));
    const bool ran = RunInCoTask([&] { out = fn(); }, timeout);
    if (!ran)
        return fw::result<void>::err(fw::MakeError(
            fw::ErrorCode::TimedOut, "co eval did not settle in test budget"));
    return out;
}

// v2::CoApp 的唯一创建入口是 coroutine 内的静态工厂：本辅助在同一受管协程内
// 求值 factory 并把 shared_ptr 交回 native 调用方（owner 在堆上，无栈引用）。
inline std::shared_ptr<fw::v2::CoApp> CreateV2App(
    std::function<std::shared_ptr<fw::v2::CoApp>()> fn,
    std::chrono::milliseconds timeout = 30s) {
    auto out = std::make_shared<std::shared_ptr<fw::v2::CoApp>>();
    RunInCoTask([&] { *out = fn(); }, timeout);
    return *out;
}

// 在受管协程内只调用 CoStart（启动失败回滚用例用）。
inline fw::result<void> CoStart(fw::v2::CoApp& app,
                                std::chrono::milliseconds timeout = 30s) {
    return CoEvalVoid([&] { return bbt_co_await app.CoStart(); }, timeout);
}

// 在受管协程内只调用 CoStop（显式关闭预算/重复收口用例用）。
inline fw::result<void> CoStop(fw::v2::CoApp& app,
                               std::chrono::milliseconds timeout = 30s) {
    return CoEvalVoid([&] { return bbt_co_await app.CoStop(); }, timeout);
}

// 在受管协程内调用 CoStop(CloseOptions)。
inline fw::result<void> CoStop(fw::v2::CoApp& app,
                               const bbt::infra::CloseOptions& options,
                               std::chrono::milliseconds timeout = 30s) {
    return CoEvalVoid(
        [&] { return bbt_co_await app.CoStop(options); }, timeout);
}

// 在受管协程内跑完整应用生命周期：CoStart → 等关闭请求 → CoStop。
// 返回启动或收口的真实结果（启动失败原错误、收口 TimedOut/失败原错误）。
// native 侧有界等待：超时返回 RuntimeUnavailable，绝不把「等待超时」当成
// 生命周期成功。
inline fw::result<void> RunAppUntilStopped(
    fw::v2::CoApp& app, std::chrono::milliseconds timeout = 120s) {
    EnsureRuntime();
    auto box = std::make_shared<detail::Latch>();
    bool registered = false;
    g_scheduler->RegistCoroutineTask(
        [box, app_ptr = &app] {
            auto started = bbt_co_await app_ptr->CoStart();
            if (!started) {
                box->Settle(started);
                return;
            }
            // 无期限：等待唯一的关闭请求槽（持久位，先请求后等待不丢）。
            auto requested = bbt_co_await app_ptr->CoWaitShutdownRequest();
            if (!requested) {
                box->Settle(requested);
                return;
            }
            box->Settle(bbt_co_await app_ptr->CoStop());
        },
        registered);
    if (!registered)
        return fw::result<void>::err(fw::MakeError(
            fw::ErrorCode::RuntimeUnavailable, "co app task not registered"));
    if (!box->Wait(timeout))
        return fw::result<void>::err(fw::MakeError(
            fw::ErrorCode::TimedOut, "co app task did not settle in test budget"));
    std::lock_guard<std::mutex> lk(box->mtx);
    return box->rc;
}

// 测试用等价入口 host hook：宿主自有 Engine 的 Run 只在入口 native 线程执行
// 一次。生产由 bbtco_main 的 host hook 调用；测试线程在此扮演该角色（不进入
// 产品）。返回 false 表示宿主尚未 Create/无 Engine。
inline bool StartEngineRun(fw::InfraHttpHost& host) {
    auto engine = host.egress_engine();
    if (!engine)
        return false;
    const auto config = host.engine_config();
    // detached：Run 的收口由 Engine Phase + WaitPhysicalReleased 证明；测试进程
    // 不依赖 join 结果，避免 joinable std::thread 在异常早退时 terminate。
    std::thread([engine, config] { (void)engine->Run(config); }).detach();
    return true;
}

// 应用根持有的共享 Engine：与 v2 产品路径同一形态（runtime_progress=
// Independent、num_event_loop=1、监听地址来自 AppNetworkOptions::ListenAddress
// ——与 InfraHttpHost::Create 写进 engine_config 的值逐项相同）。
// 生产由入口 host hook 经 HostGate 的发布执行；测试用等价原生线程扮演。
// 返回 true 表示 Run 已在原生线程发起（返回时机在 Engine Ready 之前或之后
// 不保证——就绪由宿主 Start 的 CoWaitRunning 观察）。
inline bool RunSharedEngine(
    const std::shared_ptr<net::Engine>& engine,
    const inf::ListenAddress& listen) {
    if (!engine)
        return false;
    std::error_code ec;
    auto address = net::Address::Ip(listen.host, listen.port, ec);
    if (ec)
        return false;
    net::EngineConfig config;
    config.runtime_progress = net::RuntimeProgress::Independent;
    config.options.num_event_loop = 1;
    config.listen_addresses = {address};
    std::thread([engine, config] { (void)engine->Run(config); }).detach();
    return true;
}

// 等价入口 host hook 的共享 Engine 收口：Run 返回由 Engine Phase 证明。
inline void CloseSharedEngine(const std::shared_ptr<net::Engine>& engine) {
    if (engine)
        engine->RequestClose();
}

// 测试自有辅助宿主（client/egress/peer）：Create 同步，Start / Close /
// WaitPhysicalReleased 必须在受管协程内——测试线程经本辅助驱动。
inline void StartSeamHostEngine(fw::INetworkHost& host) {
    if (auto* real = dynamic_cast<fw::InfraHttpHost*>(&host))
        (void)StartEngineRun(*real);
}

// 服务实例的受管指针：业务对象只能经 CoApp 装配（ConstructKey 为私有 passkey，
// 测试同样不能自行构造）。RegistryForTest 是本仓已声明的测试观察口。
inline std::shared_ptr<fw::v2::CoServiceBase> ServiceInstance(
    fw::v2::CoApp& app, std::string_view name) {
    auto h = app.FindService(name);
    if (!h)
        return {};
    auto reg = app.RegistryForTest();
    std::lock_guard<std::mutex> lk(reg->Mutex());
    auto* e = reg->FindEntryLocked(h.value().Id());
    if (!e || !e->object)
        return {};
    return std::static_pointer_cast<fw::v2::CoServiceBase>(e->object);
}

// seam 宿主的等价入口 host hook：宿主自有 Engine 在 host->Create() 内就位
// （Create 在受管协程的 CoStart 内执行）；native 测试线程有界轮询 engine 就位
// 后发起唯一一次 Run。返回 false 表示期限内未等到 Engine。
inline bool RunSeamHostEngineWhenReady(fw::InfraHttpHost& host,
                                       std::chrono::milliseconds wait = 15s) {
    const auto until = Clock::now() + wait;
    while (Clock::now() < until) {
        if (host.egress_engine())
            return StartEngineRun(host);
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    return false;
}

// 应用根的共享 Engine 的等价入口 host hook：CoApp 的宿主 Create 在受管协程
// 内装配 client/server（该相位 Engine 必须仍 Idle），Create 完成后经
// HostGate::Publish 发布 {engine, config, wake_main}——Pending() 为真即是
// 「Create 已装配、可 Run」的准确标记。native 测试线程有界轮询 Pending 后
// 发起唯一一次 Run（生产由 bbtco_main_host_hook 承担；CoAppSeam 的
// start_host_engine 字段当前无消费者）。
// 返回 false 表示期限内未见发布（CoStart 未到达或已失败）。
inline bool RunAppEngineWhenPublished(
    const std::shared_ptr<net::Engine>& engine,
    const inf::ListenAddress& listen,
    std::chrono::milliseconds wait = 15s) {
    (void)listen;
    const auto until = Clock::now() + wait;
    while (Clock::now() < until) {
        // 多 App/进程内多测试时槽位可能仍留有上一个 App 的发布；只认当前
        // App 的 Engine（CoStart 的 Publish 会替换未被取走的槽位），避免把
        // 新 Engine 提前 Run。
        auto publication = fw::HostGate::Peek();
        if (publication && publication->engine == engine) {
            auto config = publication->config;
            std::thread([engine, config] { (void)engine->Run(config); }).detach();
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    return false;
}

inline fw::result<void> HostCreateStart(
    fw::InfraHttpHost& host, std::chrono::milliseconds timeout = 30s) {
    EnsureRuntime();
    if (auto created = host.Create(); !created)
        return created;
    if (!StartEngineRun(host))
        return fw::result<void>::err(fw::MakeError(
            fw::ErrorCode::RuntimeUnavailable, "host engine missing"));
    fw::result<void> out = fw::result<void>::err(fw::MakeError(
        fw::ErrorCode::RuntimeUnavailable, "host start not run"));
    const bool ran = RunInCoTask(
        [&] {
            bbt::coroutine::WaitOptions wait;
            wait.deadline = Clock::now() + timeout;
            out = bbt_co_await host.Start(wait);
        },
        timeout);
    if (!ran)
        return fw::result<void>::err(fw::MakeError(
            fw::ErrorCode::TimedOut, "host start did not settle in test budget"));
    return out;
}

// 辅助宿主收口：逻辑封口（Graceful，同一预算）→ 可选等物理释放。
inline fw::result<void> HostClose(fw::InfraHttpHost& host,
                                  std::chrono::milliseconds budget = 5s,
                                  bool wait_physical = true) {
    EnsureRuntime();
    fw::result<void> out = fw::result<void>::err(fw::MakeError(
        fw::ErrorCode::RuntimeUnavailable, "host close not run"));
    const bool ran = RunInCoTask(
        [&] {
            auto closed = bbt_co_await host.Close(
                bbt::infra::CloseOptions::Graceful(Clock::now() + budget));
            if (!closed) {
                out = closed;
                return;
            }
            if (!wait_physical)
                return;
            bbt::coroutine::WaitOptions wait;
            wait.deadline = Clock::now() + budget;
            out = bbt_co_await host.WaitPhysicalReleased(wait);
        },
        budget + 5s);
    if (!ran)
        return fw::result<void>::err(fw::MakeError(
            fw::ErrorCode::TimedOut, "host close did not settle in test budget"));
    return out;
}

// 直接驱动宿主的排空等待（Co 面）并返回真实结果。
inline fw::result<void> HostWaitDrain(fw::InfraHttpHost& host,
                                      bbt::coroutine::Deadline deadline) {
    return CoEvalVoid([&host, deadline] {
        bbt::coroutine::WaitOptions wait;
        wait.deadline = deadline;
        return bbt_co_await host.WaitHandlersDone(wait);
    });
}

} // namespace framework_test
