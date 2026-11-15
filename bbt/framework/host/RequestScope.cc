#include <bbt/framework/internal/RequestScope.hpp>

#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>

namespace bbt::framework {
namespace {

struct ScopeEntry {
    const void*                           owner;  // RequestScope 身份
    std::shared_ptr<const RequestContext> ctx;
};

std::mutex g_scope_mtx;
// 协程内受管上下文：键 = 协程 id（协程跨 worker 迁移时 id 不变）。
std::unordered_map<std::uint64_t, std::vector<ScopeEntry>>   g_co_scopes;
// 非协程执行（宿主启动代码）：键 = 线程 id。
std::unordered_map<std::thread::id, std::vector<ScopeEntry>> g_thread_scopes;

template <class Map, class Key>
void EraseScopeEntry(Map& map, const Key& key, const void* owner) {
    auto it = map.find(key);
    if (it == map.end()) return;
    auto& stack = it->second;
    for (auto e = stack.rbegin(); e != stack.rend(); ++e) {
        if (e->owner == owner) {
            stack.erase(std::next(e).base());
            break;
        }
    }
    if (stack.empty()) map.erase(it);
}

} // namespace

RequestScope::RequestScope(std::shared_ptr<const RequestContext> ctx)
    : ctx_(std::move(ctx)) {
    if (!ctx_) return;
    co_id_     = bbt::coroutine::GetLocalCoroutineId();
    co_bound_  = (co_id_ != 0);
    thread_id_ = std::this_thread::get_id();
    std::lock_guard<std::mutex> lk(g_scope_mtx);
    if (co_bound_)
        g_co_scopes[co_id_].push_back(ScopeEntry{this, ctx_});
    else
        g_thread_scopes[thread_id_].push_back(ScopeEntry{this, ctx_});
}

RequestScope::~RequestScope() {
    if (!ctx_) return;
    std::lock_guard<std::mutex> lk(g_scope_mtx);
    if (co_bound_)
        EraseScopeEntry(g_co_scopes, co_id_, this);
    else
        EraseScopeEntry(g_thread_scopes, thread_id_, this);
}

result<std::shared_ptr<const RequestContext>> CurrentRequestContext() {
    const auto co_id = bbt::coroutine::GetLocalCoroutineId();
    std::lock_guard<std::mutex> lk(g_scope_mtx);
    if (co_id != 0) {
        auto it = g_co_scopes.find(co_id);
        if (it != g_co_scopes.end() && !it->second.empty())
            return result<std::shared_ptr<const RequestContext>>::ok(
                it->second.back().ctx);
    } else {
        auto it = g_thread_scopes.find(std::this_thread::get_id());
        if (it != g_thread_scopes.end() && !it->second.empty())
            return result<std::shared_ptr<const RequestContext>>::ok(
                it->second.back().ctx);
    }
    return result<std::shared_ptr<const RequestContext>>::err(MakeError(
        ErrorCode::InvalidContext,
        "no managed request context bound to current coroutine/thread"));
}

} // namespace bbt::framework
