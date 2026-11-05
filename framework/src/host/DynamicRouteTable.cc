// DynamicRouteTable 实现：锁内整体替换不可变快照，数据面只读。
//
// 不在此层发任何注册中心请求，也不缓存「失败」：provider 只在拿到权威结果时
// Replace，查询失败什么都不做——last-known-good 仍按 observed_at 在
// stale_limit 内可用，失败不刷新新鲜度。

#include <bbt/framework/DynamicRouteTable.hpp>

#include <algorithm>
#include <utility>

namespace bbt::framework {

namespace {

std::string RouteKey(std::string_view service) { return std::string(service); }

bool SameEndpoint(const bbt::infra::RpcAddress& a,
                  const bbt::infra::RpcAddress& b) {
    return a.transport == b.transport && a.endpoint == b.endpoint;
}

} // namespace

DynamicRouteTable::DynamicRouteTable(std::chrono::milliseconds stale_limit)
    : m_stale_limit(stale_limit) {}

result<void> DynamicRouteTable::Replace(
    std::string service, std::uint64_t revision,
    std::vector<bbt::infra::RpcAddress> endpoints) {
    if (service.empty())
        return result<void>::err(
            MakeError(ErrorCode::InvalidArgument,
                      "DynamicRouteTable::Replace: empty service name"));
    for (const auto& ep : endpoints) {
        if (ep.transport.empty() || ep.endpoint.empty())
            return result<void>::err(MakeError(
                ErrorCode::InvalidArgument,
                "DynamicRouteTable::Replace: service '" + service +
                    "' has endpoint with empty transport/endpoint"));
    }
    // 去重保序：同一 endpoint 重复出现只保留一份，否则 round-robin 会偏向它。
    std::vector<bbt::infra::RpcAddress> unique;
    unique.reserve(endpoints.size());
    for (auto& ep : endpoints) {
        const bool seen = std::any_of(
            unique.begin(), unique.end(),
            [&ep](const bbt::infra::RpcAddress& known) {
                return SameEndpoint(known, ep);
            });
        if (!seen)
            unique.push_back(std::move(ep));
    }

    Entry fresh;
    fresh.snapshot.revision    = revision;
    fresh.snapshot.observed_at = std::chrono::steady_clock::now();
    fresh.snapshot.endpoints   = std::move(unique);

    std::lock_guard<std::mutex> lk(m_mtx);
    auto& entry = m_entries[std::move(service)];
    const bool same_view = entry.snapshot.endpoints.size() == fresh.snapshot.endpoints.size() &&
        std::equal(entry.snapshot.endpoints.begin(), entry.snapshot.endpoints.end(),
                   fresh.snapshot.endpoints.begin(), SameEndpoint);
    entry.snapshot    = std::move(fresh.snapshot);
    if (!same_view)
        entry.next = 0;
    entry.unavailable = false;
    return result<void>::ok();
}

result<bbt::infra::RpcAddress> DynamicRouteTable::Resolve(
    std::string_view service) const {
    const std::string key = RouteKey(service);
    std::lock_guard<std::mutex> lk(m_mtx);
    const auto it = m_entries.find(key);
    if (it == m_entries.end())
        return result<bbt::infra::RpcAddress>::err(MakeError(
            ErrorCode::NotFound,
            "DynamicRouteTable::Resolve: service '" + key +
                "' is not published by any provider"));
    Entry& entry = it->second;
    if (entry.unavailable)
        return result<bbt::infra::RpcAddress>::err(MakeError(
            ErrorCode::RuntimeUnavailable,
            "DynamicRouteTable::Resolve: service '" + key +
                "' view was marked unavailable"));
    if (entry.snapshot.endpoints.empty())
        return result<bbt::infra::RpcAddress>::err(MakeError(
            ErrorCode::NotFound,
            "DynamicRouteTable::Resolve: service '" + key +
                "' currently has no healthy instance"));
    const auto age =
        std::chrono::steady_clock::now() - entry.snapshot.observed_at;
    if (age > m_stale_limit)
        return result<bbt::infra::RpcAddress>::err(MakeError(
            ErrorCode::RuntimeUnavailable,
            "DynamicRouteTable::Resolve: service '" + key +
                "' view is stale (no successful discovery within " +
                std::to_string(m_stale_limit.count()) + "ms)"));
    const std::size_t index = entry.next++ % entry.snapshot.endpoints.size();
    return result<bbt::infra::RpcAddress>::ok(
        entry.snapshot.endpoints[index]);
}

void DynamicRouteTable::MarkUnavailable(std::string_view service) noexcept {
    const std::string key = RouteKey(service);
    std::lock_guard<std::mutex> lk(m_mtx);
    const auto it = m_entries.find(key);
    if (it == m_entries.end())
        return;
    it->second.unavailable = true;
    it->second.snapshot.endpoints.clear();
}

bool DynamicRouteTable::HasService(std::string_view service) const noexcept {
    const std::string key = RouteKey(service);
    std::lock_guard<std::mutex> lk(m_mtx);
    return m_entries.count(key) != 0;
}

} // namespace bbt::framework
