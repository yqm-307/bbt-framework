#pragma once

/**
 * @file DynamicRouteTable.hpp
 * @brief 在内存中发布完整服务路由快照，由出站调用轮转解析。
 *
 * 发现提供方负责联网和比较 revision。成功查询应刷新快照，即使目标没变。
 * 查询失败保留旧快照的原观测时间，到期后解析失败。
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

/**
 * @brief 一次权威发现结果的不可变视图（provider 发布、数据面消费）。
 */
struct DynamicRouteSnapshot {
    /**
     * 注册中心 revision；不跨 provider 比较
     */
    std::uint64_t revision{0};
    std::chrono::steady_clock::time_point observed_at{};
    std::vector<std::string> endpoints;
};

/**
 * @brief 线程安全的动态路由表。
 *
 * 可在任意线程 Replace（provider）与 Resolve（业务出站），两个入口都不执行网络 I/O。
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
     * @param endpoints 完整 endpoint 集合；按首次出现去重。端点序列未变时保留轮转游标。
     * @return 空服务名或空 endpoint → InvalidArgument。
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

    /**
     * @brief 标记该服务视图不可用（保留已配置事实），Resolve 立即失败。
     */
    void MarkUnavailable(std::string_view service) noexcept;

    /**
     * @brief 该服务是否曾被 provider 配置（决定是否回退静态路由）。
     */
    bool HasService(std::string_view service) const noexcept;

private:
    struct Entry {
        DynamicRouteSnapshot snapshot;
        std::size_t next{0};
        bool unavailable{false};
    };

    const std::chrono::milliseconds m_stale_limit;
    mutable std::mutex m_mtx;
    /**
     * round-robin 游标随解析推进，故 const Resolve 仍需可变表。
     */
    mutable std::unordered_map<std::string, Entry> m_entries;
};

} // namespace bbt::framework
