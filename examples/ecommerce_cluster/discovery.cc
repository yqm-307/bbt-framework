// Consul 发现组件实现（应用侧组装）。
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

#include "discovery.hpp"

#include <algorithm>
#include <cstdio>
#include <system_error>
#include <utility>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>

namespace ecommerce {

namespace {

using bbt::infra::Error;
using bbt::infra::ErrorCode;
using bbt::infra::MakeError;
using bbt::infra::result;

// 观测行：由 Python 驱动器与人工读同一份文本证据。
void Journal(const std::string& line) {
    std::printf("%s\n", line.c_str());
    std::fflush(stdout);
}

std::string EndpointOf(const bbt::infra::consul::Instance& instance) {
    return instance.address + ":" + std::to_string(instance.port);
}

} // namespace

ConsulDiscoveryComponent::ConsulDiscoveryComponent(
    Options options, std::shared_ptr<bbt::framework::DynamicRouteTable> table)
    : m_options(std::move(options)), m_table(std::move(table)) {
    m_watches.resize(m_options.watch_services.size());
    for (std::size_t i = 0; i < m_options.watch_services.size(); ++i)
        m_watches[i].service = m_options.watch_services[i];
}

ConsulDiscoveryComponent::~ConsulDiscoveryComponent() noexcept { Close(); }

result<std::shared_ptr<ConsulDiscoveryComponent>> ConsulDiscoveryComponent::Create(
    Options options, std::shared_ptr<bbt::framework::DynamicRouteTable> table) {
    if (!table)
        return result<std::shared_ptr<ConsulDiscoveryComponent>>::err(MakeError(
            ErrorCode::InvalidArgument,
            "consul discovery: route table must not be null"));
    if (options.agent_url.empty())
        return result<std::shared_ptr<ConsulDiscoveryComponent>>::err(MakeError(
            ErrorCode::InvalidArgument, "consul discovery: agent_url is empty"));
    for (const auto& service : options.watch_services) {
        if (service.empty())
            return result<std::shared_ptr<ConsulDiscoveryComponent>>::err(
                MakeError(ErrorCode::InvalidArgument,
                          "consul discovery: empty watch service name"));
    }
    // adapter 的阻塞查询读档要容纳 wait，wait 必须严格小于总期限，否则阻塞
    // 返回会被总 deadline 打断。
    if (options.query_wait >= options.request_timeout)
        return result<std::shared_ptr<ConsulDiscoveryComponent>>::err(MakeError(
            ErrorCode::InvalidArgument,
            "consul discovery: query_wait must be smaller than request_timeout"));

    auto self = std::shared_ptr<ConsulDiscoveryComponent>(
        new ConsulDiscoveryComponent(std::move(options), std::move(table)));
    if (auto started = self->_StartEngine(); !started) {
        self->Close();
        return result<std::shared_ptr<ConsulDiscoveryComponent>>::err(
            std::move(started.error()));
    }
    return result<std::shared_ptr<ConsulDiscoveryComponent>>::ok(std::move(self));
}

result<void> ConsulDiscoveryComponent::_StartEngine() {
    m_engine = bbt::infra::net::Engine::Create(
        std::shared_ptr<bbt::infra::net::RawHandler>{});
    if (!m_engine)
        return result<void>::err(MakeError(
            ErrorCode::InternalError, "consul discovery: engine create failed"));

    bbt::infra::net::EngineConfig config;
    config.runtime_progress = bbt::infra::net::RuntimeProgress::Independent;
    config.options.num_event_loop = 1;
    m_engine_thread = std::thread([this, config] {
        m_engine_error = m_engine->Run(config);
        m_engine_returned.store(true, std::memory_order_release);
    });
    for (int i = 0; i < 500; ++i) {
        if (m_engine->Phase() == bbt::infra::net::EnginePhase::Running) break;
        if (m_engine_returned.load(std::memory_order_acquire)) break;
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }
    if (m_engine->Phase() != bbt::infra::net::EnginePhase::Running)
        return result<void>::err(MakeError(
            ErrorCode::RuntimeUnavailable,
            "consul discovery: engine did not reach Running" +
                (m_engine_returned.load(std::memory_order_acquire)
                     ? std::string(": ") + m_engine_error.message()
                     : std::string{})));

    bbt::infra::http::ClientOptions http_options;
    http_options.limits.max_connections = 8;
    http_options.limits.max_keepalive_connections = 4;
    http_options.timeout = bbt::infra::http::Timeout::Uniform(
        bbt::infra::http::PhaseTimeout::Milliseconds(
            std::chrono::milliseconds{5000}));
    auto http = bbt::infra::http::Client::Create(m_engine, http_options);
    if (!http)
        return result<void>::err(std::move(http.error()));
    m_http = std::move(http).value();

    bbt::infra::consul::Options consul_options;
    consul_options.agent_url         = m_options.agent_url;
    consul_options.request_timeout   = m_options.request_timeout;
    consul_options.query_wait        = m_options.query_wait;
    auto consul = bbt::infra::consul::Client::Create(m_http, consul_options);
    if (!consul)
        return result<void>::err(std::move(consul.error()));
    m_consul = std::move(consul).value();
    return result<void>::ok();
}

result<void> ConsulDiscoveryComponent::Start() {
    if (m_closed.load())
        return result<void>::err(
            MakeError(ErrorCode::Closed, "consul discovery: component closed"));
    if (m_started.exchange(true))
        return result<void>::err(MakeError(
            ErrorCode::InvalidArgument, "consul discovery: Start called twice"));
    if (m_watches.empty()) return result<void>::ok();

    {
        std::lock_guard<std::mutex> lk(m_mtx);
        m_watchers_active = m_watches.size();
    }
    for (std::size_t i = 0; i < m_watches.size(); ++i) {
        bool submitted = false;
        // 查询路径含 JSON 解析：协程栈显式放大，不用默认 12KB。
        g_scheduler->RegistCoroutineTask(
            256 * 1024, [this, i] { bbt_co_await _WatchLoop(i); }, submitted);
        if (!submitted)
            return result<void>::err(MakeError(
                ErrorCode::RuntimeUnavailable,
                "consul discovery: watcher coroutine registration failed"));
    }

    // 首个权威快照决定 App 能否启动：失败即启动失败，不留「静默无视图」。
    const auto deadline = std::chrono::steady_clock::now() +
                          m_options.request_timeout * 2 +
                          std::chrono::milliseconds{1000};
    std::unique_lock<std::mutex> lk(m_mtx);
    m_cv.wait_until(lk, deadline, [this] {
        return std::all_of(m_watches.begin(), m_watches.end(),
                           [](const Watch& w) { return w.first_done; });
    });
    for (const auto& watch : m_watches) {
        if (!watch.first_done)
            return result<void>::err(MakeError(
                ErrorCode::TimedOut,
                "consul discovery: first snapshot for '" + watch.service +
                    "' did not arrive before deadline"));
        if (!watch.first_ok)
            return result<void>::err(MakeError(
                ErrorCode::RuntimeUnavailable,
                "consul discovery: first snapshot for '" + watch.service +
                    "' failed: " + watch.first_error));
    }
    return result<void>::ok();
}

bbt_co_async(void) ConsulDiscoveryComponent::_WatchLoop(std::size_t index) {
    // m_watches 在 Start 之后不再增删，引用稳定。
    Watch& watch = m_watches[index];
    std::uint64_t last_index = 0;
    while (!m_stop.load(std::memory_order_acquire)) {
        std::uint64_t request_index = 0;
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            request_index = watch.index;
        }
        auto snapshot = bbt_co_await m_consul->HealthyInstances(
            watch.service, request_index);
        if (!snapshot) {
            _SetLastError("query '" + watch.service + "' failed: " +
                          snapshot.error().message);
            Journal("event=query_failed service=" + watch.service +
                    " code=" + std::to_string(static_cast<int>(snapshot.error().code)));
            {
                std::lock_guard<std::mutex> lk(m_mtx);
                if (!watch.first_done) {
                    watch.first_done   = true;
                    watch.first_ok     = false;
                    watch.first_error  = snapshot.error().message;
                }
            }
            m_cv.notify_all();
            _SleepWhileActive(std::chrono::milliseconds{200});
            continue;
        }
        if (m_stop.load(std::memory_order_acquire)) break;

        std::vector<std::string> endpoints;
        std::vector<std::string> endpoint_names;
        endpoints.reserve(snapshot.value().instances.size());
        endpoint_names.reserve(snapshot.value().instances.size());
        for (const auto& instance : snapshot.value().instances) {
            const std::string endpoint = EndpointOf(instance);
            endpoints.push_back(endpoint);
            endpoint_names.push_back(endpoint);
        }

        auto replaced = m_table->Replace(watch.service, snapshot.value().index,
                                         std::move(endpoints));
        if (!replaced) {
            _SetLastError("publish '" + watch.service + "' failed: " +
                          replaced.error().message);
        } else {
            _SetLastError(std::string{});
        }

        // 下一轮 index：epoch 回退退回 0 重取完整快照；0 用 1 避免 busy loop。
        std::uint64_t next_index = snapshot.value().index;
        if (snapshot.value().index < last_index) next_index = 0;
        else if (snapshot.value().index == 0) next_index = 1;
        last_index = snapshot.value().index;

        bool changed = false;
        std::size_t endpoint_count = 0;
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            changed            = endpoint_names != watch.endpoints;
            endpoint_count     = endpoint_names.size();
            watch.endpoints    = std::move(endpoint_names);
            watch.revision     = snapshot.value().index;
            watch.index        = next_index;
            if (!watch.first_done) {
                watch.first_done = true;
                watch.first_ok   = bool(replaced);
                if (!replaced) watch.first_error = replaced.error().message;
            }
        }
        m_cv.notify_all();
        Journal("event=view service=" + watch.service +
                " revision=" + std::to_string(snapshot.value().index) +
                " endpoints=" + std::to_string(endpoint_count) +
                " changed=" + (changed ? "1" : "0") +
                " ok=" + (replaced ? "1" : "0"));
    }

    {
        std::lock_guard<std::mutex> lk(m_mtx);
        if (m_watchers_active > 0) --m_watchers_active;
    }
    m_cv.notify_all();
    return {};
}

bbt_co_async(bbt::infra::result<void>) ConsulDiscoveryComponent::RegisterSelf(
    std::string service, std::string instance_id, std::string address,
    std::uint16_t port) {
    if (m_closed.load())
        return result<void>::err(
            MakeError(ErrorCode::Closed, "consul discovery: component closed"));
    if (service.empty() || instance_id.empty() || address.empty() || port == 0)
        return result<void>::err(MakeError(
            ErrorCode::InvalidArgument,
            "consul discovery: register requires service/instance/address/port"));

    bbt::infra::consul::ServiceRegistration registration;
    registration.id      = std::move(instance_id);
    registration.name    = std::move(service);
    registration.address = std::move(address);
    registration.port    = port;
    registration.tags    = {"ecommerce-cluster", "issue70"};
    const std::string logged_id = registration.id;
    const std::string logged_service = registration.name;

    auto registered = bbt_co_await m_consul->Register(std::move(registration));
    if (!registered) {
        _SetLastError("register '" + logged_service + "' failed: " +
                      registered.error().message);
        return result<void>::err(std::move(registered.error()));
    }
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        m_registered_id      = logged_id;
        m_registered_service = logged_service;
    }
    Journal("event=registered service=" + logged_service + " instance=" +
            logged_id + " port=" + std::to_string(port));
    return result<void>::ok();
}

bbt_co_async(bbt::infra::result<void>) ConsulDiscoveryComponent::DeregisterSelf() {
    std::string id;
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        id = m_registered_id;
    }
    if (id.empty()) return result<void>::ok();
    if (m_closed.load())
        return result<void>::err(
            MakeError(ErrorCode::Closed, "consul discovery: component closed"));

    auto deregistered = bbt_co_await m_consul->Deregister(id);
    if (!deregistered) {
        _SetLastError("deregister '" + id + "' failed: " +
                      deregistered.error().message);
        return result<void>::err(std::move(deregistered.error()));
    }
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        m_registered_id.clear();
    }
    Journal("event=deregistered instance=" + id);
    return result<void>::ok();
}

void ConsulDiscoveryComponent::Close() noexcept {
    if (m_closed.exchange(true)) return;
    m_stop.store(true, std::memory_order_release);

    // 视图不再由本进程维护：立即标记不可用，残留 gateway 不会继续打到旧地址。
    for (const auto& watch : m_watches) m_table->MarkUnavailable(watch.service);
    m_cv.notify_all();

    {
        std::unique_lock<std::mutex> lk(m_mtx);
        m_cv.wait_for(lk,
                      m_options.request_timeout + std::chrono::milliseconds{1000},
                      [this] { return m_watchers_active == 0; });
    }
    if (m_consul) m_consul->Close();
    if (m_engine) {
        m_engine->RequestClose();
        if (m_engine_thread.joinable()) m_engine_thread.join();
    }
    m_consul.reset();
    m_http.reset();
}

bool ConsulDiscoveryComponent::IsClosed() const noexcept { return m_closed.load(); }

std::uint64_t ConsulDiscoveryComponent::Revision(
    std::string_view service) const {
    std::lock_guard<std::mutex> lk(m_mtx);
    for (const auto& watch : m_watches) {
        if (watch.service == service) return watch.revision;
    }
    return 0;
}

std::string ConsulDiscoveryComponent::LastError() const {
    std::lock_guard<std::mutex> lk(m_mtx);
    return m_last_error;
}

void ConsulDiscoveryComponent::_SetLastError(std::string message) {
    std::lock_guard<std::mutex> lk(m_mtx);
    m_last_error = std::move(message);
}

void ConsulDiscoveryComponent::_SleepWhileActive(
    std::chrono::milliseconds budget) {
    // 协程域睡眠：让出 worker，且 stop 后最多再等 budget 就退出循环。
    bbtco_sleep(static_cast<int>(budget.count()));
}

} // namespace ecommerce
