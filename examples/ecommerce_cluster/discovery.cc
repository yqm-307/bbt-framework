// Consul 发现组件实现（应用侧组装，v2::CoComponent）。
//
// 阻塞查询循环的规则（与 docs/architecture §5.2 的 revision/epoch 语义一致）：
//   - 首轮 index=0 取一次完整快照；
//   - 之后带上一轮的 X-Consul-Index 走阻塞查询（wait 由 adapter 附加）；
//   - 返回 index 小于上一轮 → 视为 agent epoch 重置，下一轮退回 index=0 重取
//     完整权威快照，不跨 epoch 比较；
//   - 返回 index=0 → 下一轮用 1，避免 busy loop；
//   - 相同 index 也可能带回不同视图：始终比较 endpoint 集合，只有真的变了才
//     记 changed=1；
//   - 每次成功查询都 Replace（刷新新鲜度），查询失败什么都不发布、也不刷新。
//
// 生命周期规则：
//   - 共享 Engine 的 Run/收口归应用根与入口 host hook；本组件不建线程、
//     不 join、不在 Co 域使用 native CV/sleep，也不关闭共享 Engine；
//   - watcher 协程只持有共享状态（State），不捕获本对象地址；组件在超时/销毁
//     之后仍可安全退出；
//   - OnStop 先停止 watcher 与视图发布，再按同一 CloseOptions 封口 consul；
//     任一步超时都保留记录，下一次显式收口可重试。

#include "discovery.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <memory>
#include <utility>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>
#include <bbt/infra/http/Client.hpp>

namespace ecommerce {

namespace fw = bbt::framework;
namespace v2 = bbt::framework::v2;

namespace {

using bbt::infra::ErrorCode;
using bbt::infra::MakeError;
using bbt::infra::result;

using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

// 观测行：由 Python 驱动器与人工读同一份文本证据。
void Journal(const std::string& line) {
    std::printf("%s\n", line.c_str());
    std::fflush(stdout);
}

std::string EndpointOf(const bbt::infra::consul::Instance& instance) {
    return instance.address + ":" + std::to_string(instance.port);
}

} // namespace

struct ConsulDiscoveryComponent::Watch {
    std::string service;
    std::uint64_t index{0}; // 下一轮请求使用的 Consul index
    std::uint64_t revision{0};
    std::vector<std::string> endpoints; // 上一次发布的 endpoint 串
    bool first_done{false};
    bool first_ok{false};
    std::string first_error;
};

// watcher 与组件共享的运行状态：watcher 只持有本结构的 shared_ptr，
// 组件销毁后仍在途的查询循环可以安全走完并退出。
struct ConsulDiscoveryComponent::State {
    std::mutex mtx; // 保护 watches/last_error/registered_*/watchers_active
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> changed =
        bbt::coroutine::sync::CoWaiter::Create();
    std::vector<Watch> watches;
    std::size_t watchers_active{0};
    bool watchers_submitted{false};
    std::string last_error;
    std::string registered_id;
    std::string registered_service;
    std::atomic<bool> stop{false};
    std::shared_ptr<bbt::infra::consul::Client> consul;
    std::shared_ptr<bbt::framework::DynamicRouteTable> table;
};

ConsulDiscoveryComponent::ConsulDiscoveryComponent(
    bbt::framework::detail::ConstructKey key, Options options)
    : v2::CoComponent(key),
      m_options(std::move(options)),
      m_state(std::make_shared<State>()) {
    m_state->table = m_options.table;
    m_state->watches.resize(m_options.watch_services.size());
    for (std::size_t i = 0; i < m_options.watch_services.size(); ++i)
        m_state->watches[i].service = m_options.watch_services[i];
}

ConsulDiscoveryComponent::~ConsulDiscoveryComponent() noexcept {
    s_current.store(nullptr, std::memory_order_release);
}

ConsulDiscoveryComponent* ConsulDiscoveryComponent::Current() noexcept {
    return s_current.load(std::memory_order_acquire);
}
std::atomic<ConsulDiscoveryComponent*>
    ConsulDiscoveryComponent::s_current{nullptr};

fw::result<void> ConsulDiscoveryComponent::OnInstall() {
    if (!m_options.engine)
        return fw::result<void>::err(fw::MakeError(
            fw::ErrorCode::InvalidArgument,
            "consul discovery: shared engine is required"));
    if (!m_options.table)
        return fw::result<void>::err(fw::MakeError(
            fw::ErrorCode::InvalidArgument,
            "consul discovery: route table must not be null"));
    if (m_options.agent_url.empty())
        return fw::result<void>::err(fw::MakeError(
            fw::ErrorCode::InvalidArgument,
            "consul discovery: agent_url is empty"));
    for (const auto& service : m_options.watch_services) {
        if (service.empty())
            return fw::result<void>::err(fw::MakeError(
                fw::ErrorCode::InvalidArgument,
                "consul discovery: empty watch service name"));
    }
    // adapter 的阻塞查询读档要容纳 wait，wait 必须严格小于总期限，否则阻塞
    // 返回会被总 deadline 打断。
    if (m_options.query_wait >= m_options.request_timeout)
        return fw::result<void>::err(fw::MakeError(
            fw::ErrorCode::InvalidArgument,
            "consul discovery: query_wait must be smaller than request_timeout"));

    // Idle 装配只创建专属 http/consul client；共享 Engine 由应用根持有，
    // 本组件不 Run、不建线程、不拨号。
    bbt::infra::http::ClientOptions http_options;
    http_options.limits.max_connections = 8;
    http_options.limits.max_keepalive_connections = 4;
    http_options.timeout = bbt::infra::http::Timeout::Uniform(
        bbt::infra::http::PhaseTimeout::Milliseconds(
            std::chrono::milliseconds{5000}));
    auto http = bbt::infra::http::Client::Create(m_options.engine,
                                               http_options);
    if (!http)
        return fw::result<void>::err(std::move(http.error()));

    bbt::infra::consul::Options consul_options;
    consul_options.agent_url = m_options.agent_url;
    consul_options.request_timeout = m_options.request_timeout;
    consul_options.query_wait = m_options.query_wait;
    auto consul = bbt::infra::consul::Client::Create(
        std::move(http).value(), consul_options);
    if (!consul)
        return fw::result<void>::err(std::move(consul.error()));

    m_state->consul = std::move(consul).value();
    s_current.store(this, std::memory_order_release);
    return fw::result<void>::ok();
}

fw::result<void> ConsulDiscoveryComponent::OnStart() {
    if (m_sealed.load(std::memory_order_acquire))
        return fw::result<void>::err(
            fw::MakeError(fw::ErrorCode::Closed,
                          "consul discovery: component closed"));
    if (!m_state->consul)
        return fw::result<void>::err(fw::MakeError(
            fw::ErrorCode::RuntimeUnavailable,
            "consul discovery: consul client not installed"));

    const auto state = m_state;
    if (state->watches.empty())
        return fw::result<void>::ok();

    bool register_now = false;
    {
        std::lock_guard<std::mutex> lk(state->mtx);
        if (!state->watchers_submitted) {
            state->watchers_submitted = true;
            register_now = true;
        }
    }
    if (!register_now)
        return fw::result<void>::ok();

    for (std::size_t i = 0; i < state->watches.size(); ++i) {
        // 先计数再登记：新任务可能立即在其它 worker 上开始并在失败
        // 路径递减；登记失败时回滚，计数只保留真正登记成功的任务。
        {
            std::lock_guard<std::mutex> lk(state->mtx);
            ++state->watchers_active;
        }
        bool submitted = false;
        // 这里只登记观察协程（不是启停 runtime）：run 已由业务入口负责；
        // 查询路径含 JSON 解析，协程栈显式放大，不用默认 12KB。
        g_scheduler->RegistCoroutineTask(
            256 * 1024,
            [state, i] { bbt_co_await _WatchLoop(state, i); },
            submitted);
        if (!submitted) {
            {
                std::lock_guard<std::mutex> lk(state->mtx);
                if (state->watchers_active > 0)
                    --state->watchers_active;
            }
            state->stop.store(true, std::memory_order_release);
            state->changed->Notify();
            _SetLastError(state,
                          "consul discovery: watcher coroutine "
                          "registration failed");
            return fw::result<void>::err(fw::MakeError(
                fw::ErrorCode::RuntimeUnavailable,
                "consul discovery: watcher coroutine registration failed"));
        }
    }
    return fw::result<void>::ok();
}

bbt_co_async(bbt::infra::result<void>)
ConsulDiscoveryComponent::WaitFirstSnapshots(
    bbt::coroutine::Deadline deadline) {
    const auto state = m_state;
    if (state->watches.empty())
        return result<void>::ok();
    auto first =
        bbt_co_await _WaitFor(state, WaitKind::FirstSnapshots, deadline);
    if (!first)
        return result<void>::err(std::move(first.error()));
    std::string failed_service;
    std::string failed_error;
    {
        std::lock_guard<std::mutex> lk(state->mtx);
        for (const auto& watch : state->watches) {
            if (!watch.first_ok) {
                failed_service = watch.service;
                failed_error = watch.first_error;
                break;
            }
        }
    }
    if (!failed_service.empty())
        return result<void>::err(MakeError(
            ErrorCode::RuntimeUnavailable,
            "consul discovery: first snapshot for '" + failed_service +
                "' failed: " + failed_error));
    return result<void>::ok();
}

fw::result<void> ConsulDiscoveryComponent::OnStop(
    const bbt::infra::CloseOptions& options) {
    if (options.mode == bbt::infra::CloseMode::Graceful &&
        options.deadline == bbt::coroutine::Deadline::max())
        return fw::result<void>::err(fw::MakeError(
            fw::ErrorCode::InvalidArgument,
            "consul discovery: graceful close requires a finite deadline"));

    m_sealed.store(true, std::memory_order_release);
    const auto state = m_state;
    state->stop.store(true, std::memory_order_release);
    state->changed->Notify();

    // 视图不再由本进程维护：立即标记不可用，残留 gateway 不会继续打到旧地址。
    for (const auto& watch : state->watches)
        state->table->MarkUnavailable(watch.service);

    // Close() 立即封口并唤醒在途查询，再按 mode/deadline 等待排空；
    // 受管协程内以 50ms 分片协作挂起（不阻塞 worker），超时/取消如实上抛。
    // 该入口幂等，超时后可再次调用继续收口。调用方 CloseOptions 原样消费，
    // 不在组件侧另造第二套关闭模式或期限。
    if (state->consul) {
        auto closed = state->consul->Close(options);
        if (!closed)
            return result<void>::err(std::move(closed.error()));
    }

    // watcher 是内部责任，不是用户可见业务：封口后必须退出，否则共享 Engine
    // 收口后仍会有协程持有 adapter。Graceful 用同一绝对期限；Immediate 不读
    // deadline，用 adapter 单次请求上界给内部退出定界。
    const auto drain_deadline =
        options.mode == bbt::infra::CloseMode::Graceful
            ? options.deadline
            : Clock::now() + m_options.request_timeout + 1s;
    auto drained =
        bbt_co_await _WaitFor(state, WaitKind::WatchersDrained, drain_deadline);
    if (!drained)
        return result<void>::err(std::move(drained.error()));

    return fw::result<void>::ok();
}

void ConsulDiscoveryComponent::OnUninstall() {
    s_current.store(nullptr, std::memory_order_release);
}

bbt_co_async(void) ConsulDiscoveryComponent::_WatchLoop(
    std::shared_ptr<State> state, std::size_t index) {
    // watches 在 Create 之后不再增删，引用稳定。
    Watch& watch = state->watches[index];
    std::uint64_t last_index = 0;
    while (!state->stop.load(std::memory_order_acquire)) {
        std::uint64_t request_index = 0;
        {
            std::lock_guard<std::mutex> lk(state->mtx);
            request_index = watch.index;
        }
        auto snapshot =
            bbt_co_await state->consul->HealthyInstances(watch.service,
                                                         request_index);
        // 关闭唤醒在途查询后先看 stop：正常收口不把「取消」记成查询失败。
        if (state->stop.load(std::memory_order_acquire))
            break;
        if (!snapshot) {
            _SetLastError(state, "query '" + watch.service + "' failed: " +
                                     snapshot.error().message);
            Journal("event=query_failed service=" + watch.service +
                    " code=" +
                    std::to_string(static_cast<int>(snapshot.error().code)));
            {
                std::lock_guard<std::mutex> lk(state->mtx);
                if (!watch.first_done) {
                    watch.first_done = true;
                    watch.first_ok = false;
                    watch.first_error = snapshot.error().message;
                }
            }
            state->changed->Notify();
            if (state->stop.load(std::memory_order_acquire))
                break;
            bbtco_sleep(200);
            continue;
        }

        std::vector<std::string> endpoints;
        endpoints.reserve(snapshot.value().instances.size());
        for (const auto& instance : snapshot.value().instances)
            endpoints.push_back(EndpointOf(instance));
        std::vector<std::string> endpoint_names = endpoints;

        auto replaced = state->table->Replace(watch.service,
                                              snapshot.value().index,
                                              std::move(endpoints));
        if (!replaced) {
            _SetLastError(state, "publish '" + watch.service + "' failed: " +
                                     replaced.error().message);
        } else {
            _SetLastError(state, std::string{});
        }
        // 关闭可能在上面的 Replace 期间完成标记：这里再封一次，保证关闭后
        // 路由表最终停在不可用而不是最后一份快照。
        if (state->stop.load(std::memory_order_acquire)) {
            state->table->MarkUnavailable(watch.service);
            break;
        }

        // 下一轮 index：epoch 回退退回 0 重取完整快照；0 用 1 避免 busy loop。
        std::uint64_t next_index = snapshot.value().index;
        if (snapshot.value().index < last_index)
            next_index = 0;
        else if (snapshot.value().index == 0)
            next_index = 1;
        last_index = snapshot.value().index;

        bool changed = false;
        std::size_t endpoint_count = 0;
        {
            std::lock_guard<std::mutex> lk(state->mtx);
            changed = endpoint_names != watch.endpoints;
            endpoint_count = endpoint_names.size();
            watch.endpoints = std::move(endpoint_names);
            watch.revision = snapshot.value().index;
            watch.index = next_index;
            if (!watch.first_done) {
                watch.first_done = true;
                watch.first_ok = bool(replaced);
                if (!replaced)
                    watch.first_error = replaced.error().message;
            }
        }
        state->changed->Notify();
        Journal("event=view service=" + watch.service +
                " revision=" + std::to_string(snapshot.value().index) +
                " endpoints=" + std::to_string(endpoint_count) +
                " changed=" + (changed ? "1" : "0") +
                " ok=" + (replaced ? "1" : "0"));
    }

    {
        std::lock_guard<std::mutex> lk(state->mtx);
        if (state->watchers_active > 0)
            --state->watchers_active;
    }
    state->changed->Notify();
    return {};
}

bbt_co_async(bbt::infra::result<void>) ConsulDiscoveryComponent::_WaitFor(
    const std::shared_ptr<State>& state, WaitKind kind,
    bbt::coroutine::Deadline deadline) {
    for (;;) {
        bool done = false;
        bool stopped = false;
        {
            std::lock_guard<std::mutex> lk(state->mtx);
            stopped = state->stop.load(std::memory_order_acquire);
            if (kind == WaitKind::FirstSnapshots) {
                done = std::all_of(state->watches.begin(), state->watches.end(),
                                   [](const Watch& watch) {
                                       return watch.first_done;
                                   });
            } else {
                done = state->watchers_active == 0;
            }
        }
        if (kind == WaitKind::FirstSnapshots && stopped)
            return result<void>::err(MakeError(
                ErrorCode::Closed,
                "consul discovery: closed before the first snapshot"));
        if (done)
            return result<void>::ok();

        const auto now = Clock::now();
        if (deadline != bbt::coroutine::Deadline::max() && now >= deadline)
            return result<void>::err(MakeError(
                ErrorCode::TimedOut,
                kind == WaitKind::FirstSnapshots
                    ? "consul discovery: first snapshot deadline reached"
                    : "consul discovery: watchers did not stop before "
                      "deadline"));
        const auto until =
            deadline == bbt::coroutine::Deadline::max()
                ? now + 50ms
                : std::min(deadline, now + 50ms);
        bbt::coroutine::WaitOptions wait;
        wait.deadline = until;
        switch (state->changed->Wait(wait)) {
        case bbt::coroutine::WaitStatus::Completed:
        case bbt::coroutine::WaitStatus::TimedOut:
            break;
        case bbt::coroutine::WaitStatus::AlreadyWaiting:
            bbtco_sleep(5);
            break;
        case bbt::coroutine::WaitStatus::Cancelled:
            return result<void>::err(MakeError(
                ErrorCode::Cancelled, "consul discovery: wait cancelled"));
        case bbt::coroutine::WaitStatus::InvalidContext:
            return result<void>::err(MakeError(
                ErrorCode::InvalidContext,
                "consul discovery: wait requires a managed coroutine context"));
        case bbt::coroutine::WaitStatus::RuntimeUnavailable:
            return result<void>::err(MakeError(
                ErrorCode::RuntimeUnavailable,
                "consul discovery: wait infrastructure unavailable"));
        }
    }
}

void ConsulDiscoveryComponent::_SetLastError(
    const std::shared_ptr<State>& state, std::string message) {
    std::lock_guard<std::mutex> lk(state->mtx);
    state->last_error = std::move(message);
}

bbt_co_async(bbt::infra::result<void>) ConsulDiscoveryComponent::RegisterSelf(
    std::string service, std::string instance_id, std::string address,
    std::uint16_t port) {
    if (m_sealed.load(std::memory_order_acquire))
        return result<void>::err(
            MakeError(ErrorCode::Closed, "consul discovery: component closed"));
    if (service.empty() || instance_id.empty() || address.empty() || port == 0)
        return result<void>::err(MakeError(
            ErrorCode::InvalidArgument,
            "consul discovery: register requires service/instance/address/port"));

    bbt::infra::consul::ServiceRegistration registration;
    registration.id = std::move(instance_id);
    registration.name = std::move(service);
    registration.address = std::move(address);
    registration.port = port;
    registration.tags = {"ecommerce-cluster", "issue70"};
    const std::string logged_id = registration.id;
    const std::string logged_service = registration.name;

    auto registered =
        bbt_co_await m_state->consul->Register(std::move(registration));
    if (!registered) {
        _SetLastError(m_state, "register '" + logged_service + "' failed: " +
                                   registered.error().message);
        return result<void>::err(std::move(registered.error()));
    }
    {
        std::lock_guard<std::mutex> lk(m_state->mtx);
        m_state->registered_id = logged_id;
        m_state->registered_service = logged_service;
    }
    Journal("event=registered service=" + logged_service + " instance=" +
            logged_id + " port=" + std::to_string(port));
    return result<void>::ok();
}

bbt_co_async(bbt::infra::result<void>) ConsulDiscoveryComponent::DeregisterSelf() {
    std::string id;
    {
        std::lock_guard<std::mutex> lk(m_state->mtx);
        id = m_state->registered_id;
    }
    if (id.empty())
        return result<void>::ok();
    if (m_sealed.load(std::memory_order_acquire))
        return result<void>::err(
            MakeError(ErrorCode::Closed, "consul discovery: component closed"));

    auto deregistered = bbt_co_await m_state->consul->Deregister(id);
    if (!deregistered) {
        _SetLastError(m_state, "deregister '" + id + "' failed: " +
                                   deregistered.error().message);
        return result<void>::err(std::move(deregistered.error()));
    }
    {
        std::lock_guard<std::mutex> lk(m_state->mtx);
        m_state->registered_id.clear();
    }
    Journal("event=deregistered instance=" + id);
    return result<void>::ok();
}

std::size_t ConsulDiscoveryComponent::watchers_active() const noexcept {
    std::lock_guard<std::mutex> lk(m_state->mtx);
    return m_state->watchers_active;
}

std::uint64_t ConsulDiscoveryComponent::Revision(
    std::string_view service) const {
    std::lock_guard<std::mutex> lk(m_state->mtx);
    for (const auto& watch : m_state->watches) {
        if (watch.service == service)
            return watch.revision;
    }
    return 0;
}

std::string ConsulDiscoveryComponent::LastError() const {
    std::lock_guard<std::mutex> lk(m_state->mtx);
    return m_state->last_error;
}

} // namespace ecommerce
