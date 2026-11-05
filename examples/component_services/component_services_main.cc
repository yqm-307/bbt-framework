// bbt-framework 组件式服务示例：共享组件、运行期热拔插、hook 与观察者。
//
// 形态：普通 main 启动 coroutine runtime，再把场景投递进受管协程；场景内部
// 只使用公开 v2 面（v2::CoApp::Create、CoInstall/CoUninstall/CoBind/CoUnbind、
// CoStart/CoStop/CoObserve、ComponentHandle::Borrow）。不使用 internal/ 缝、
// 不启网络、不依赖 RPC/protobuf。
//
// 模式：
//   component_services shared --journal <path>
//     一个 App 内：安装 PriceBook("pricing")；CatalogService 安装时声明式绑定，
//     CartService 运行期显式绑定；两个服务写同一个组件实例。随后解绑、卸载、
//     用同一逻辑名重装新实例并重新绑定；断言旧 handle 失效、缺失槽位报
//     NotFound、ObserverToken 生效/撤销。
//     再启动一个独立 App，让组件 OnStart 失败，断言 Failed 状态、错误相位与
//     App::LifecycleFailures 记录，最后有限 deadline 收口。
//   component_services node --journal <path> --price <cents>
//     独立进程装配同一业务模块（store.hpp）与自己的组件实例；由 Python
//     驱动器以不同价格并发启动两个进程，断言 PID 不同、各自总额独立。
//
// 边界（不宣称）：进程内热拔插换的是受管组件实例，不是 dlopen/代码热更新；
// 跨进程只复用代码/协议，不共享 C++ 对象；没有注册中心或动态发现。

#include <atomic>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <unistd.h>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/framework/V2.hpp>

#include "store.hpp"

namespace fw = bbt::framework;
namespace v2 = bbt::framework::v2;

namespace {

using shop::Emit;

std::mutex    g_journal_mtx;
std::ofstream g_journal;
int           g_checks   = 0;
int           g_failures = 0;

void Journal(const std::string& line) {
    std::lock_guard<std::mutex> lk(g_journal_mtx);
    if (g_journal) {
        g_journal << line << '\n';
        g_journal.flush();
    }
}

// detail 必须无空格：Python 驱动器按空格切分 `k=v`。
void Check(bool ok, const std::string& name, const std::string& detail) {
    ++g_checks;
    if (!ok) ++g_failures;
    Journal("event=check name=" + name + " ok=" + (ok ? "1" : "0") +
            " detail=" + (detail.empty() ? "none" : detail));
    std::printf("%s %-38s %s\n", ok ? "PASS" : "FAIL", name.c_str(),
                detail.c_str());
}

std::string Int(long long v) { return std::to_string(v); }

// 稳定错误码的短名不逐个枚举：示例只关心比较 code，名字用于人读细节。
std::string Err(const fw::Error& e) {
    return "code_" + Int(int(e.code));
}

std::string Pid() { return Int(::getpid()); }

fw::AppOptions MakeAppOptions() {
    return fw::AppOptions{}
        .SetLimits(64, 64, 64, 64)
        .SetShutdownBudget(std::chrono::milliseconds{2000})
        .SetLifecycleBudget(std::chrono::milliseconds{2000});
}

fw::LifecycleOptions DeadlineMs(int ms) {
    return fw::LifecycleOptions{}.SetDeadline(
        std::chrono::steady_clock::now() + std::chrono::milliseconds{ms});
}

fw::ServiceOptions ServiceOpts() {
    return fw::ServiceOptions{}.SetMaxInflight(4);
}

// ---- 场景 1：同进程共享 + 热拔插 + hook/observer ----
void RunHookFailureScenario();

void RunSharedScenario() {
    Emit("event=scenario_start mode=shared pid=" + Pid());

    auto app = v2::CoApp::Create(MakeAppOptions());
    if (!app) {
        Check(false, "create_app", "null_owner");
        return;
    }

    shop::PriceBook::Options v1;
    v1.version = 1;
    v1.unit_price_cents = 1000;
    v1.halt_ms = 20;
    auto installed =
        bbt_co_await app->CoInstallComponent<shop::PriceBook>(
            std::string(shop::kPricingSlot), v1);
    Check(bool(installed), "install_pricing_v1",
          installed ? "ok" : Err(installed.error()));

    auto comp = app->FindComponent<shop::PriceBook>(
        std::string(shop::kPricingSlot));
    Check(bool(comp), "find_pricing", comp ? "ok" : Err(comp.error()));
    if (!comp) return;
    const fw::ObjectId    old_id  = comp.value().Id();
    const std::uint64_t   old_gen = comp.value().Generation();

    std::vector<fw::ObjectState> observed;
    auto token = bbt_co_await app->CoObserve(
        app->Handle(), comp.value(),
        [&observed](const fw::ObjectStateEvent& ev) {
            observed.push_back(ev.state);
        });
    Check(bool(token) && token.value().IsActive(), "observer_token_active",
          token ? "ok" : Err(token.error()));

    auto catalog = bbt_co_await app->CoInstallService<shop::CatalogService>(
        ServiceOpts().BindComponent<shop::PriceBook>(
            std::string(shop::kPricingSlot)));
    Check(bool(catalog), "install_catalog_with_declared_binding",
          catalog ? "ok" : Err(catalog.error()));

    auto cart = bbt_co_await app->CoInstallService<shop::CartService>(
        ServiceOpts());
    Check(bool(cart), "install_cart", cart ? "ok" : Err(cart.error()));

    auto started = bbt_co_await app->CoStart(DeadlineMs(2000));
    Check(bool(started), "co_start", started ? "ok" : Err(started.error()));
    Check(app->State() == fw::AppState::Running, "app_running",
          "state=" + Int(int(app->State())));
    Check(shop::PriceBook::s_constructed.load() == 1,
          "shared_single_instance",
          "constructed=" + Int(shop::PriceBook::s_constructed.load()));
    Check(shop::PriceBook::s_install.load() >= 1 &&
              shop::PriceBook::s_start.load() >= 1,
          "component_hooks_install_start",
          "install=" + Int(shop::PriceBook::s_install.load()) +
              " start=" + Int(shop::PriceBook::s_start.load()));

    auto catalog_h = app->FindService("catalog");
    auto cart_h    = app->FindService("cart");
    Check(bool(catalog_h) && bool(cart_h), "find_services",
          bool(catalog_h) && bool(cart_h) ? "ok" : "missing");

    // 运行期显式绑定：CartService 在 OnBindingChanged 里借用同一实例写购物车。
    auto cart_binding = bbt_co_await app->CoBindComponent<shop::PriceBook>(
        cart_h.value(), std::string(shop::kPricingSlot));
    Check(bool(cart_binding), "runtime_bind_cart",
          cart_binding ? "ok" : Err(cart_binding.error()));

    if (cart_binding) {
        auto borrow = cart_binding.value().Borrow();
        Check(bool(borrow), "cart_binding_borrowable",
              borrow ? "ok" : Err(borrow.error()));
        if (borrow) {
            shop::PriceBook& book = borrow.value().Get();
            Check(book.InstanceSeq() == 1 && book.CatalogSize() == 1 &&
                      book.CartQuantity("sku-widget") ==
                          shop::CartService::kQtyPerBinding,
                  "two_services_share_one_instance",
                  "instance_seq=" + Int(book.InstanceSeq()) +
                      " catalog_items=" + Int(book.CatalogSize()) +
                      " cart_qty=" + Int(book.CartQuantity("sku-widget")));
            Check(book.CartTotalCents() == 2000, "cart_hook_write_visible",
                  "cart_total=" + Int(book.CartTotalCents()));
        }
    }

    Check(!observed.empty() &&
              observed.back() == fw::ObjectState::Running,
          "observer_saw_running", "events=" + Int(observed.size()));

    auto missing = bbt_co_await app->CoBindComponent<shop::PriceBook>(
        cart_h.value(), "missing-slot");
    Check(!missing && missing.error().code == fw::ErrorCode::NotFound,
          "missing_slot_not_found",
          "code=" + Int(int(missing.error().code)));

    // ---- 热拔插：解绑两个 subject → 卸载旧实例 → 同名重装 ----
    auto unbind_catalog = bbt_co_await app->CoUnbindComponent<shop::PriceBook>(
        catalog_h.value(), std::string(shop::kPricingSlot));
    auto unbind_cart = bbt_co_await app->CoUnbindComponent<shop::PriceBook>(
        cart_h.value(), std::string(shop::kPricingSlot));
    Check(bool(unbind_catalog) && bool(unbind_cart), "unbind_both_subjects",
          bool(unbind_catalog) && bool(unbind_cart) ? "ok" : "failed");

    auto stale = cart_binding ? cart_binding.value().Borrow()
                              : fw::result<fw::ComponentBorrow<shop::PriceBook>>::
                                    err(fw::MakeError(fw::ErrorCode::Closed,
                                                      "no binding"));
    Check(!stale && stale.error().code == fw::ErrorCode::Closed,
          "old_handle_closed", "code=" + Int(int(stale.error().code)));

    auto uninstalled = bbt_co_await app->CoUninstallComponent<shop::PriceBook>(
        std::string(shop::kPricingSlot), fw::ComponentOptions{},
        DeadlineMs(2000));
    Check(bool(uninstalled), "uninstall_v1",
          uninstalled ? "ok" : Err(uninstalled.error()));
    Check(shop::PriceBook::s_stop.load() >= 1 &&
              shop::PriceBook::s_uninstall.load() >= 1,
          "component_hooks_stop_uninstall",
          "stop=" + Int(shop::PriceBook::s_stop.load()) +
              " uninstall=" + Int(shop::PriceBook::s_uninstall.load()));
    Check(shop::PriceBook::s_live.load() == 0, "old_instance_destroyed",
          "live=" + Int(shop::PriceBook::s_live.load()));
    const bool saw_destroyed =
        std::find(observed.begin(), observed.end(), fw::ObjectState::Destroyed) !=
        observed.end();
    Check(saw_destroyed, "observer_saw_destroyed",
          "events=" + Int(observed.size()));
    // token 的生命周期由观察者持有：显式 Revoke 后 IsActive 为 false。
    if (token) token.value().Revoke();
    Check(token ? !token.value().IsActive() : false,
          "observer_token_revoke", token ? "ok" : "no_token");

    auto gone = app->FindComponent<shop::PriceBook>(
        std::string(shop::kPricingSlot));
    Check(!gone && gone.error().code == fw::ErrorCode::NotFound,
          "find_after_uninstall_not_found",
          "code=" + Int(int(gone.error().code)));

    auto bind_gone = bbt_co_await app->CoBindComponent<shop::PriceBook>(
        cart_h.value(), std::string(shop::kPricingSlot));
    Check(!bind_gone && bind_gone.error().code == fw::ErrorCode::NotFound,
          "bind_after_uninstall_not_found",
          "code=" + Int(int(bind_gone.error().code)));

    shop::PriceBook::Options v2opts;
    v2opts.version = 2;
    v2opts.unit_price_cents = 1500;
    v2opts.halt_ms = 20;
    auto reinstalled =
        bbt_co_await app->CoInstallComponent<shop::PriceBook>(
            std::string(shop::kPricingSlot), v2opts);
    Check(bool(reinstalled), "install_pricing_v2_same_slot",
          reinstalled ? "ok" : Err(reinstalled.error()));
    Check(shop::PriceBook::s_constructed.load() == 2 &&
              shop::PriceBook::s_live.load() == 1,
          "single_live_instance_after_swap",
          "constructed=" + Int(shop::PriceBook::s_constructed.load()) +
              " live=" + Int(shop::PriceBook::s_live.load()));

    auto comp2 = app->FindComponent<shop::PriceBook>(
        std::string(shop::kPricingSlot));
    Check(bool(comp2) && (comp2.value().Id() != old_id ||
                          comp2.value().Generation() != old_gen),
          "reinstalled_identity_changed",
          comp2 ? "id=" + comp2.value().Id() +
                      " gen=" + Int(comp2.value().Generation())
                : Err(comp2.error()));
    if (comp2) {
        auto info = comp2.value().Info();
        Check(bool(info) && info.value().state == fw::ObjectState::Running,
              "reinstalled_running_while_app_running",
              info ? "state=" + Int(int(info.value().state)) : "no_info");
    }

    auto rebind_catalog = bbt_co_await app->CoBindComponent<shop::PriceBook>(
        catalog_h.value(), std::string(shop::kPricingSlot));
    auto rebind_cart = bbt_co_await app->CoBindComponent<shop::PriceBook>(
        cart_h.value(), std::string(shop::kPricingSlot));
    Check(bool(rebind_catalog) && bool(rebind_cart),
          "rebind_both_subjects_new_instance",
          bool(rebind_catalog) && bool(rebind_cart) ? "ok" : "failed");

    if (rebind_cart) {
        auto borrow2 = rebind_cart.value().Borrow();
        Check(bool(borrow2), "new_binding_borrowable",
              borrow2 ? "ok" : Err(borrow2.error()));
        if (borrow2) {
            shop::PriceBook& book = borrow2.value().Get();
            Check(book.InstanceSeq() == 2 && book.Version() == 2 &&
                      book.CatalogSize() == 0 && book.CartTotalCents() == 3000,
                  "new_instance_fresh_state",
                  "instance_seq=" + Int(book.InstanceSeq()) +
                      " catalog_items=" + Int(book.CatalogSize()) +
                      " cart_total=" + Int(book.CartTotalCents()));
        }
    }

    // 显式撤销 + 收口：token 撤销后不再派发，CoStop 有限 deadline 干净收尾。
    int svc_events = 0;
    auto cart_token = bbt_co_await app->CoObserve(
        app->Handle(), cart_h.value(),
        [&svc_events](const fw::ObjectStateEvent&) { ++svc_events; });
    Check(bool(cart_token), "observer_service_token",
          cart_token ? "ok" : Err(cart_token.error()));
    if (cart_token) cart_token.value().Revoke();
    Check(cart_token ? !cart_token.value().IsActive() : false,
          "observer_token_explicit_revoke", cart_token ? "ok" : "no_token");
    const int before_stop_events = svc_events;

    auto stopped = bbt_co_await app->CoStop(DeadlineMs(2000));
    Check(bool(stopped), "co_stop", stopped ? "ok" : Err(stopped.error()));
    Check(app->State() == fw::AppState::Stopped, "app_stopped",
          "state=" + Int(int(app->State())));
    Check(app->PendingCleanup().empty(), "no_pending_cleanup",
          "pending=" + Int(app->PendingCleanup().size()));
    Check(app->LifecycleFailures().empty(), "no_lifecycle_failures",
          "failures=" + Int(app->LifecycleFailures().size()));
    Check(svc_events == before_stop_events, "no_events_after_revoke",
          "before=" + Int(before_stop_events) +
              " after=" + Int(svc_events));
    Check(shop::PriceBook::s_live.load() == 0, "all_instances_destroyed",
          "live=" + Int(shop::PriceBook::s_live.load()));
    Check(shop::PriceBook::s_stop.load() >= 2 &&
              shop::PriceBook::s_uninstall.load() >= 2,
          "both_versions_stopped",
          "stop=" + Int(shop::PriceBook::s_stop.load()) +
              " uninstall=" + Int(shop::PriceBook::s_uninstall.load()));

    // 释放第一个 App 后再跑失败 hook 段落：两个 App 不同时活跃。
    app.reset();
    RunHookFailureScenario();

    Emit("event=scenario_end mode=shared pid=" + Pid() +
         " checks=" + Int(g_checks) + " failures=" + Int(g_failures));
}

// ---- 场景 2：hook 失败仍被记录，有限 deadline 收口 ----
void RunHookFailureScenario() {
    Emit("event=scenario_start mode=hook_failure pid=" + Pid());
    auto app = v2::CoApp::Create(MakeAppOptions());
    if (!app) {
        Check(false, "failure_create_app", "null_owner");
        return;
    }

    shop::PriceBook::Options bad;
    bad.version = 9;
    bad.unit_price_cents = 900;
    bad.fail_start = true;
    auto installed = bbt_co_await app->CoInstallComponent<shop::PriceBook>(
        "audit-pricing", bad);
    Check(bool(installed), "install_failing_component",
          installed ? "ok" : Err(installed.error()));

    auto comp = app->FindComponent<shop::PriceBook>("audit-pricing");
    if (!comp) {
        Check(false, "find_failing_component", Err(comp.error()));
        return;
    }

    auto started = bbt_co_await app->CoStart(DeadlineMs(1000));
    Check(!started && started.error().code == fw::ErrorCode::InternalError,
          "hook_failure_surfaces_error",
          started ? "unexpected_ok" : Err(started.error()));
    Check(app->State() == fw::AppState::Running,
          "partial_start_reports_running",
          "state=" + Int(int(app->State())));

    auto info = comp.value().Info();
    Check(bool(info) && info.value().state == fw::ObjectState::Failed &&
              info.value().last_error.has_value() &&
              info.value().last_error->phase == "start",
          "hook_failure_state_failed",
          info ? "state=" + Int(int(info.value().state)) +
                     " phase=" +
                     (info.value().last_error
                          ? info.value().last_error->phase
                          : "none")
               : "no_info");

    bool recorded = false;
    for (const auto& f : app->LifecycleFailures()) {
        if (f.object_id == comp.value().Id() && f.operation == "start" &&
            f.phase == "start") {
            recorded = true;
        }
    }
    Check(recorded, "hook_failure_recorded",
          "failures=" + Int(app->LifecycleFailures().size()));
    Emit("event=hook_failure_recorded pid=" + Pid() +
         " object=" + comp.value().Id());

    auto stopped = bbt_co_await app->CoStop(DeadlineMs(2000));
    Check(bool(stopped) && app->State() == fw::AppState::Stopped &&
              app->PendingCleanup().empty(),
          "failure_app_clean_stop",
          stopped ? "state=" + Int(int(app->State())) +
                        " pending=" + Int(app->PendingCleanup().size())
                  : Err(stopped.error()));
}

// ---- 场景 3：独立进程装配同一业务模块 ----
void RunNodeScenario(int price_cents) {
    Emit("event=scenario_start mode=node pid=" + Pid() +
         " price=" + Int(price_cents));
    auto app = v2::CoApp::Create(MakeAppOptions());
    if (!app) {
        Check(false, "node_create_app", "null_owner");
        return;
    }

    shop::PriceBook::Options opts;
    opts.version = 1;
    opts.unit_price_cents = price_cents;
    auto installed = bbt_co_await app->CoInstallComponent<shop::PriceBook>(
        std::string(shop::kPricingSlot), opts);
    Check(bool(installed), "node_install_component",
          installed ? "ok" : Err(installed.error()));
    auto catalog = bbt_co_await app->CoInstallService<shop::CatalogService>(
        ServiceOpts().BindComponent<shop::PriceBook>(
            std::string(shop::kPricingSlot)));
    auto cart = bbt_co_await app->CoInstallService<shop::CartService>(
        ServiceOpts());
    Check(bool(catalog) && bool(cart), "node_install_services",
          bool(catalog) && bool(cart) ? "ok" : "failed");

    auto started = bbt_co_await app->CoStart(DeadlineMs(2000));
    Check(bool(started), "node_co_start",
          started ? "ok" : Err(started.error()));

    auto cart_h = app->FindService("cart");
    auto binding = bbt_co_await app->CoBindComponent<shop::PriceBook>(
        cart_h.value(), std::string(shop::kPricingSlot));
    Check(bool(binding), "node_bind_cart",
          binding ? "ok" : Err(binding.error()));
    int total = -1;
    int seq   = -1;
    int items = -1;
    if (binding) {
        auto borrow = binding.value().Borrow();
        if (borrow) {
            shop::PriceBook& book = borrow.value().Get();
            total = book.CartTotalCents();
            seq   = book.InstanceSeq();
            items = book.CatalogSize();
        }
    }
    Check(total == shop::CartService::kQtyPerBinding * price_cents &&
              items == 1 && seq == 1,
          "node_independent_state",
          "price=" + Int(price_cents) + " total=" + Int(total) +
              " catalog_items=" + Int(items) + " instance_seq=" + Int(seq));
    Emit("event=cross_process_node pid=" + Pid() +
         " price=" + Int(price_cents) + " total=" + Int(total) +
         " instance_seq=" + Int(seq) + " catalog_items=" + Int(items));

    auto stopped = bbt_co_await app->CoStop(DeadlineMs(2000));
    Check(bool(stopped) && app->State() == fw::AppState::Stopped,
          "node_clean_stop",
          stopped ? "state=" + Int(int(app->State()))
                  : Err(stopped.error()));
    Emit("event=scenario_end mode=node pid=" + Pid() +
         " checks=" + Int(g_checks) + " failures=" + Int(g_failures));
}

// 把场景投递进受管协程，宿主线程用限时 CV 等它结束（不阻塞 worker）。
bool RunInManagedCoroutine(const std::function<void()>& fn,
                           std::chrono::milliseconds budget) {
    struct Slot {
        std::mutex              mtx;
        std::condition_variable cv;
        bool                    done = false;
    };
    auto slot = std::make_shared<Slot>();
    bool submitted = false;
    g_scheduler->RegistCoroutineTask(
        [slot, fn]() {
            try {
                fn();
            } catch (const std::exception& e) {
                Journal(std::string("event=exception what=") + e.what());
                ++g_failures;
            } catch (...) {
                Journal("event=exception what=unknown");
                ++g_failures;
            }
            std::lock_guard<std::mutex> lk(slot->mtx);
            slot->done = true;
            slot->cv.notify_all();
        },
        submitted);
    if (!submitted) return false;
    std::unique_lock<std::mutex> lk(slot->mtx);
    return slot->cv.wait_for(lk, budget, [&] { return slot->done; });
}

struct Args {
    std::string mode = "shared";
    std::string journal;
    int         price = 0;
};

bool ParseArgs(int argc, char* argv[], Args* out) {
    if (argc < 2) return false;
    out->mode = argv[1];
    for (int i = 2; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "--journal" && i + 1 < argc) {
            out->journal = argv[++i];
        } else if (a == "--price" && i + 1 < argc) {
            const char* text = argv[++i];
            char* end = nullptr;
            errno = 0;
            const long value = std::strtol(text, &end, 10);
            if (text == end || *end != '\0' || errno == ERANGE || value < 0 ||
                value > INT_MAX / shop::CartService::kQtyPerBinding) {
                return false;
            }
            out->price = static_cast<int>(value);
        } else {
            return false;
        }
    }
    if (out->journal.empty()) return false;
    return out->mode == "shared" || out->mode == "node";
}

} // namespace

int main(int argc, char* argv[]) {
    Args args;
    if (!ParseArgs(argc, argv, &args)) {
        std::fprintf(stderr,
                     "usage: %s shared|node --journal <path> [--price <cents>]\n",
                     argv[0]);
        return 90;   // EX_USAGE
    }

    g_journal.open(args.journal, std::ios::out | std::ios::trunc);
    if (!g_journal) {
        std::fprintf(stderr, "component_services: cannot open journal %s\n",
                     args.journal.c_str());
        return 6;
    }
    shop::SetJournal(&Journal);

    // 普通 main + 协程入口：先配置并启动 runtime，再把场景投递进受管协程。
    // hook 内含字符串/容器操作，默认 12KB 栈偏紧，显式放大。
    g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
    g_bbt_coroutine_config->m_cfg_stack_size        = 256 * 1024;
    g_scheduler->Start();

    bool finished = false;
    if (args.mode == "shared") {
        finished = RunInManagedCoroutine(RunSharedScenario,
                                         std::chrono::seconds{60});
    } else {
        finished = RunInManagedCoroutine(
            [price = args.price] { RunNodeScenario(price); },
            std::chrono::seconds{60});
    }
    if (!finished) {
        Journal("event=timeout mode=" + args.mode);
        std::fprintf(stderr, "component_services: scenario timed out\n");
        return 3;
    }

    const int rc = g_failures == 0 ? 0 : 1;
    Journal("event=run_end mode=" + args.mode + " pid=" + Pid() +
            " checks=" + Int(g_checks) + " failures=" + Int(g_failures) +
            " rc=" + Int(rc));
    std::fprintf(stderr,
                 "component_services mode=%s checks=%d failures=%d rc=%d\n",
                 args.mode.c_str(), g_checks, g_failures, rc);
    return rc;
}
