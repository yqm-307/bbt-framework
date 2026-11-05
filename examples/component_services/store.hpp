#pragma once
// 电商商品/购物车示例的复用业务模块。
//
// 业务故事：PriceBook 组件保存价目与购物车行，CatalogService 在启动时写入
// 目录价，CartService 在绑定变化时写入购物车行；两个服务绑定同一个 App
// scope 的 "pricing" 实例，因此互相看得到对方写入的状态。
//
// 本头只演示公开 v2 面：CoComponent/CoService、声明式
// ServiceOptions::BindComponent、ComponentHandle::Borrow、生命周期与绑定
// hook。同一份头被主例程与独立 node 进程各自装配——进程内共享的是组件实例，
// 跨进程只共享代码，不共享对象。

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/framework/V2.hpp>

namespace shop {

// 只在本业务命名空间内收窄框架名字：这些是 public v2 面，写全限定名会淹没
// 业务语义。不做全局 using namespace，消费者不受影响。
namespace v2 = bbt::framework::v2;
using bbt::framework::result;
using bbt::framework::ErrorCode;
using bbt::framework::MakeError;
using bbt::framework::BindingEvent;
using bbt::framework::detail::ConstructKey;

/// @brief 两个服务共同绑定的组件逻辑名。
inline constexpr std::string_view kPricingSlot = "pricing";

/// @brief 示例 journal 行输出口；由 main 注入（写入 journal 文件并回显）。
using JournalFn = std::function<void(const std::string&)>;

inline std::mutex& JournalMutex() {
    static std::mutex m;
    return m;
}

inline JournalFn& JournalSink() {
    static JournalFn sink;
    return sink;
}

/// @brief 注入 journal 输出口；只在启动阶段调用一次。
inline void SetJournal(JournalFn sink) {
    std::lock_guard<std::mutex> lk(JournalMutex());
    JournalSink() = std::move(sink);
}

/// @brief 输出一行 `event=... k=v`；值不得含空格（驱动器按空格分词）。
inline void Emit(const std::string& line) {
    JournalFn sink;
    {
        std::lock_guard<std::mutex> lk(JournalMutex());
        sink = JournalSink();
    }
    if (sink) sink(line);
}

/// @brief 共享的价格/购物车组件。只能经 CoApp::CoInstallComponent 创建。
class PriceBook final : public v2::CoComponent {
public:
    /// @brief 装配参数：版本、目录单价、停止时挂起毫秒、是否让 OnStart 失败。
    struct Options {
        int  version          = 1;
        int  unit_price_cents = 0;
        int  halt_ms          = 0;
        bool fail_start       = false;
    };

    explicit PriceBook(ConstructKey key, Options o)
        : v2::CoComponent(key),
          m_version(o.version),
          m_unit_price(o.unit_price_cents),
          m_halt_ms(o.halt_ms),
          m_fail_start(o.fail_start),
          m_instance_seq(++s_instance_seq) {
        ++s_constructed;
        ++s_live;
    }

    int Version() const noexcept { return m_version; }
    int UnitPriceCents() const { return Locked([this] { return m_unit_price; }); }
    int InstanceSeq() const noexcept { return m_instance_seq; }
    int CatalogSize() const { return Locked([this] { return int(m_catalog.size()); }); }

    /// @brief 写入一条目录价；由 CatalogService 在启动时调用。
    void AddCatalogItem(const std::string& sku, int price_cents) {
        LockedVoid([&] { m_catalog[sku] = price_cents; });
    }
    /// @brief 追加购物车行（同 sku 数量累加）；由 CartService 在绑定变化时调用。
    void AddCartLine(const std::string& sku, int qty, int unit_price_cents) {
        LockedVoid([&] {
            m_cart_qty[sku] += qty;
            m_cart_total_cents += qty * unit_price_cents;
        });
    }
    int CartQuantity(const std::string& sku) const {
        return Locked([&] {
            auto it = m_cart_qty.find(sku);
            return it == m_cart_qty.end() ? 0 : it->second;
        });
    }
    int CartTotalCents() const { return Locked([this] { return m_cart_total_cents; }); }

    // 跨实例的阶段计数：用于断言"只创建一次/只存活一个"，不参与业务。
    inline static std::atomic<int> s_constructed{0};
    inline static std::atomic<int> s_live{0};
    inline static std::atomic<int> s_install{0};
    inline static std::atomic<int> s_start{0};
    inline static std::atomic<int> s_stop{0};
    inline static std::atomic<int> s_uninstall{0};

protected:
    ~PriceBook() noexcept override {
        --s_live;
        // noexcept 析构里不外传 journal 的分配失败。
        try {
            Emit("event=component_destroy name=" + std::string(kPricingSlot) +
                 " version=" + std::to_string(m_version) +
                 " instance_seq=" + std::to_string(m_instance_seq));
        } catch (...) {
        }
    }

    result<void> OnInstall() override {
        ++s_install;
        Emit("event=component_hook name=" + std::string(kPricingSlot) +
             " phase=install version=" + std::to_string(m_version) +
             " instance_seq=" + std::to_string(m_instance_seq));
        return result<void>::ok();
    }

    result<void> OnStart() override {
        ++s_start;
        Emit("event=component_hook name=" + std::string(kPricingSlot) +
             " phase=start version=" + std::to_string(m_version) +
             " instance_seq=" + std::to_string(m_instance_seq));
        if (m_fail_start)
            return result<void>::err(MakeError(
                ErrorCode::InternalError,
                "PriceBook: start refused (demo)"));
        return result<void>::ok();
    }

    result<void> OnStop() override {
        ++s_stop;
        // 真实挂起：证明 hook 在受管协程中执行，有限 deadline 覆盖它。
        if (m_halt_ms > 0) bbtco_sleep(m_halt_ms);
        Emit("event=component_hook name=" + std::string(kPricingSlot) +
             " phase=stop version=" + std::to_string(m_version) +
             " instance_seq=" + std::to_string(m_instance_seq));
        return result<void>::ok();
    }

    void OnUninstall() override {
        ++s_uninstall;
        Emit("event=component_hook name=" + std::string(kPricingSlot) +
             " phase=uninstall version=" + std::to_string(m_version) +
             " instance_seq=" + std::to_string(m_instance_seq));
    }

private:
    template <class F>
    int Locked(F&& f) const {
        std::lock_guard<std::mutex> lk(m_mtx);
        return f();
    }
    template <class F>
    void LockedVoid(F&& f) {
        std::lock_guard<std::mutex> lk(m_mtx);
        f();
    }

    const int  m_version;
    const int  m_unit_price;
    const int  m_halt_ms;
    const bool m_fail_start;
    const int  m_instance_seq;

    mutable std::mutex     m_mtx;
    std::map<std::string, int> m_catalog;
    std::map<std::string, int> m_cart_qty;
    int                    m_cart_total_cents = 0;

    inline static std::atomic<int> s_instance_seq{0};
    friend class bbt::framework::detail::Registry;
};

/// @brief 目录服务：安装时声明式绑定 "pricing"，启动时写入目录价。
class CatalogService final
    : public v2::CoService<PriceBook> {
public:
    static constexpr std::string_view kServiceName = "catalog";

    explicit CatalogService(ConstructKey key)
        : v2::CoService<PriceBook>(key) {}

protected:
    ~CatalogService() noexcept override = default;

    result<void> OnInstall() override {
        Emit("event=service_hook service=catalog phase=install");
        return result<void>::ok();
    }

    result<void> OnStart() override {
        Emit("event=service_hook service=catalog phase=start");
        // 声明式绑定在安装时建立；此时组件已 Running，可借用并写共享状态。
        auto h = GetComponent<PriceBook>(kPricingSlot);
        if (!h) {
            Emit("event=catalog_start error=get_component code=" +
                 std::to_string(int(h.error().code)));
            return result<void>::err(std::move(h.error()));
        }
        auto b = h.value().Borrow();
        if (!b) {
            Emit("event=catalog_start error=borrow code=" +
                 std::to_string(int(b.error().code)));
            return result<void>::err(std::move(b.error()));
        }
        PriceBook& book = b.value().Get();
        book.AddCatalogItem("sku-widget", book.UnitPriceCents());
        Emit("event=catalog_start instance_seq=" +
             std::to_string(book.InstanceSeq()) + " unit_price=" +
             std::to_string(book.UnitPriceCents()) + " catalog_items=" +
             std::to_string(book.CatalogSize()));
        return result<void>::ok();
    }

    result<void> OnStop() override {
        Emit("event=service_hook service=catalog phase=stop");
        return result<void>::ok();
    }

    void OnUninstall() override {
        Emit("event=service_hook service=catalog phase=uninstall");
    }

    void OnBindingChanged(const BindingEvent& ev) override {
        // 安装期绑定发生在服务 Installing 的装配窗口内，Registry 允许借
        // Installed 依赖做纯内存装配；运行期重绑同样可借。这里只记录事实。
        std::string line = "event=binding_hook service=catalog bound=";
        line += ev.bound ? "1" : "0";
        auto h = GetComponent<PriceBook>(kPricingSlot);
        if (h) {
            auto b = h.value().Borrow();
            line += " borrow=";
            line += b ? "1" : "0";
            line += " instance_seq=";
            line += b ? std::to_string(b.value().Get().InstanceSeq()) : "-";
        } else {
            line += " borrow=0 instance_seq=-";
        }
        Emit(line);
    }

    friend class bbt::framework::detail::Registry;
};

/// @brief 购物车服务：运行期被显式绑定后，在绑定 hook 中写购物车行。
class CartService final : public v2::CoService<PriceBook> {
public:
    static constexpr std::string_view kServiceName = "cart";
    /// @brief 绑定 hook 每次写入的行数。
    static constexpr int kQtyPerBinding = 2;

    explicit CartService(ConstructKey key)
        : v2::CoService<PriceBook>(key) {}

protected:
    ~CartService() noexcept override = default;

    result<void> OnInstall() override {
        Emit("event=service_hook service=cart phase=install");
        return result<void>::ok();
    }

    result<void> OnStart() override {
        Emit("event=service_hook service=cart phase=start");
        return result<void>::ok();
    }

    result<void> OnStop() override {
        Emit("event=service_hook service=cart phase=stop");
        return result<void>::ok();
    }

    void OnUninstall() override {
        Emit("event=service_hook service=cart phase=uninstall");
    }

    void OnBindingChanged(const BindingEvent& ev) override {
        if (!ev.bound) {
            Emit("event=binding_hook service=cart bound=0");
            return;
        }
        auto h = GetComponent<PriceBook>(kPricingSlot);
        if (!h) {
            Emit("event=cart_binding error=not_bound");
            return;
        }
        auto b = h.value().Borrow();
        if (!b) {
            Emit("event=cart_binding error=borrow code=" +
                 std::to_string(int(b.error().code)));
            return;
        }
        PriceBook& book = b.value().Get();
        book.AddCartLine("sku-widget", kQtyPerBinding, book.UnitPriceCents());
        Emit("event=cart_binding bound=1 instance_seq=" +
             std::to_string(book.InstanceSeq()) + " unit_price=" +
             std::to_string(book.UnitPriceCents()) + " cart_total=" +
             std::to_string(book.CartTotalCents()));
    }

    friend class bbt::framework::detail::Registry;
};

} // namespace shop
