#include <bbt/framework/AppOptions.hpp>

#include <charconv>
#include <cstdint>
#include <system_error>

namespace bbt::framework {

AppOptions& AppOptions::Listen(std::string uri) {
    m_listen = std::move(uri);
    m_listen_addr = {};
    const auto colon = m_listen->rfind(':');
    if (colon == std::string::npos || colon == 0)
        return *this;

    const char* first = m_listen->data() + colon + 1;
    const char* last = m_listen->data() + m_listen->size();
    std::uint16_t port = 0;
    const auto parsed = std::from_chars(first, last, port);
    if (parsed.ec == std::errc{} && parsed.ptr == last)
        m_listen_addr = {m_listen->substr(0, colon), port};
    return *this;
}

AppOptions& AppOptions::SetAppLimits(AppLimits limits) {
    m_max_objects = limits.max_objects;
    m_max_bindings = limits.max_bindings;
    m_max_observers = limits.max_observers;
    m_observer_queue_capacity = limits.observer_queue_capacity;
    return *this;
}

result<void> AppOptions::Validate() const {
    if (m_max_objects == 0 || m_max_bindings == 0 || m_max_observers == 0 ||
        m_observer_queue_capacity == 0)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                           "AppOptions: SetLimits requires all four limits to be positive"));
    if (m_shutdown_budget.count() <= 0)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                           "AppOptions: SetShutdownBudget must be a finite positive value"));
    if (m_lifecycle_budget.count() <= 0)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                           "AppOptions: SetLifecycleBudget must be a finite positive value"));
    return result<void>::ok();
}

} // namespace bbt::framework
