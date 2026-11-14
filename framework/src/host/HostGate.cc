#include <bbt/framework/HostGate.hpp>

#include <condition_variable>
#include <mutex>
#include <optional>
#include <utility>

namespace bbt::framework {

namespace {

// Run 失败时给入口的退出码：runtime 以 HostHookFailed 上报非零值。
constexpr int kHostGateRunFailed = 1;

// 进程单槽位：只承载一次发布与一次取走。故意不提供重置/队列/注册表。
struct GateState {
    std::mutex mtx;
    std::condition_variable cv;
    std::optional<HostGate::Publication> slot;
    bool taken{false};
    // 主任务完成通知已兑现（runtime 侧 OnMainTaskDone 已调用）。
    bool main_done{false};
    // Run 期间的强引用：主任务先结束后 App 可能释放最后一个引用，本闸门保证
    // Engine 活到 Run 返回之后。
    std::shared_ptr<bbt::infra::net::Engine> running;
};

GateState& StateForProcess() noexcept {
    // 函数内静态：进程寿命，供 native host hook 与主协程共用。
    static GateState state;
    return state;
}

} // namespace

void HostGate::Publish(Publication publication) noexcept {
    GateState& state = StateForProcess();
    {
        std::lock_guard<std::mutex> lk(state.mtx);
        if (state.taken)
            return; // 同一进程已执行过 Run：忽略后一次发布
        state.slot = std::move(publication);
    }
    state.cv.notify_all();
}

bool HostGate::Pending() noexcept {
    GateState& state = StateForProcess();
    std::lock_guard<std::mutex> lk(state.mtx);
    return state.slot.has_value();
}

std::optional<HostGate::Publication> HostGate::Peek() noexcept {
    GateState& state = StateForProcess();
    std::lock_guard<std::mutex> lk(state.mtx);
    return state.slot;
}

int HostGate::RunHostGate(bbt::coroutine::HostHookContext& ctx) noexcept {
    GateState& state = StateForProcess();
    // 先登记：主任务可能在本函数取到发布之前就结束，登记保证那条路径也能唤醒
    // 发布等待，不会因为「等发布」与「主任务已结束」互相阻塞。
    ctx.OnMainTaskDone([&state] {
        std::shared_ptr<bbt::infra::net::Engine> engine;
        {
            std::lock_guard<std::mutex> lk(state.mtx);
            state.main_done = true;
            engine = state.running;
        }
        if (engine)
            engine->RequestClose();
        state.cv.notify_all();
    });

    Publication publication;
    {
        std::unique_lock<std::mutex> lk(state.mtx);
        state.cv.wait(lk, [&state] {
            return state.slot.has_value() || state.main_done;
        });
        if (state.main_done) {
            // 主任务在发布前结束：没有需要运行的网络代，不启动 Run。
            state.slot.reset();
            return 0;
        }
        publication = std::move(*state.slot);
        state.slot.reset();
        state.taken = true;
        state.running = publication.engine;
    }

    const std::error_code error = publication.engine->Run(publication.config);
    {
        std::lock_guard<std::mutex> lk(state.mtx);
        state.running.reset();
    }
    if (error) {
        // Run 提前失败：主任务可能仍挂在关闭请求等待上，唤醒它自行收口。
        if (publication.wake_main)
            publication.wake_main();
        return kHostGateRunFailed;
    }
    return 0;
}

void HostGateWakeLink::Attach(std::function<void()> wake) noexcept {
    std::lock_guard<std::mutex> lk(m_mtx);
    m_wake = std::move(wake);
}

void HostGateWakeLink::Wake() noexcept {
    // 持锁调用：Detach 返回后本函数必然是空操作，App 不会在析构后被唤醒。
    std::lock_guard<std::mutex> lk(m_mtx);
    if (m_wake)
        m_wake();
}

void HostGateWakeLink::Detach() noexcept {
    std::lock_guard<std::mutex> lk(m_mtx);
    m_wake = {};
}

} // namespace bbt::framework
