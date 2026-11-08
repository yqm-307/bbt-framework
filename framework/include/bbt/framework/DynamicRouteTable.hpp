#pragma once

/**
 * @file DynamicRouteTable.hpp
 * @brief 出站目标的动态路由表：由发现提供方发布权威快照，数据面只读解析。
 *
 * 职责边界：
 *   - 只保存「逻辑服务名 → 一组 endpoint + 观测时间 + revision」，不联网、
 *     不重试、不感知具体注册中心后端；provider（如 Consul 阻塞查询循环）在
 *     应用侧组装，通过 Replace 发布完整权威快照。
 *   - 数据面（Resolve）只读内存，绝不在调用路径发起注册中心请求。
 *
 * 语义（与 docs/architecture/distributed-framework-baseline.md §5.2 一致）：
 *   - 每次 Replace 以不可变值整体替换该服务的视图，不合并增量；
 *   - 同一服务的 endpoint 集合按 round-robin 解析；空集合 → NotFound；
 *   - 成功查询即使视图未变也必须 Replace（刷新 observed_at），否则健康的稳定
 *     服务会在 stale_limit 后被误判陈旧；
 *   - revision 回退时照装调用方给出的完整快照：revision 只在同一 provider 的
 *     epoch 内可比，比较与重置由 provider 负责；
 *   - 超过 stale_limit 未刷新 → RuntimeUnavailable，禁止返回陈旧 endpoint。
 */

#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>


#include <bbt/framework/Result.hpp>

namespace bbt::framework {

/// @brief 一次权威发现结果的不可变视图（provider 发布、数据面消费）。
struct DynamicRouteSnapshot {
    std::uint64_t revision{0};   ///< 注册中心 revision；不跨 provider 比较
    std::chrono::steady_clock::time_point observed_at{};
    std::vector<std::string> endpoints;
};

/**
 * @brief 线程安全的动态路由表。
 *
 * 可在任意线程 Replace（provider）与 Resolve（业务出站），两者互不阻塞网络。
 */
class DynamicRouteTable {
public:
    /**
     * @param stale_limit 快照最长可用时间（本机 steady clock）；超过后
     *                    Resolve 返回 RuntimeUnavailable。默认 5s 为框架
     *                    约定的陈旧边界。
     */
    explicit DynamicRouteTable(
        std::chrono::milliseconds stale_limit = std::chrono::seconds{5});

    DynamicRouteTable(const DynamicRouteTable&) = delete;
    DynamicRouteTable& operator=(const DynamicRouteTable&) = delete;

    /**
     * @brief 用一次完整权威查询结果整体替换服务视图并刷新观测时间。
     *
     * @param service   逻辑服务名，非空。
     * @param revision  本次查询的注册中心 revision（0 表示未知）。
     * @param endpoints 完整 endpoint 集合；重复项按首次出现去重。
     * @return 空服务名/空 transport/空 endpoint → InvalidArgument。
     */
    result<void> Replace(std::string service,
                         std::uint64_t revision,
                         std::vector<std::string> endpoints);

    /**
     * @brief 解析一次出站目标（round-robin）。
     *
     * @return 未配置过该服务或集合为空 → NotFound；显式 MarkUnavailable 或
     *         超过 stale_limit 未刷新 → RuntimeUnavailable。
     */
    result<std::string> Resolve(std::string_view service) const;

    /// @brief 标记该服务视图不可用（保留已配置事实），Resolve 立即失败。
    void MarkUnavailable(std::string_view service) noexcept;

    /// @brief 该服务是否曾被 provider 配置（决定是否回退静态路由）。
    bool HasService(std::string_view service) const noexcept;

private:
    struct Entry {
        DynamicRouteSnapshot snapshot;
        std::size_t          next{0};
        bool                 unavailable{false};
    };

    const std::chrono::milliseconds m_stale_limit;
    mutable std::mutex              m_mtx;
    // round-robin 游标随解析推进，故 const Resolve 仍需可变表。
    mutable std::unordered_map<std::string, Entry> m_entries;
};

} // namespace bbt::framework
