// examples/dual_service/driver_main.cc — driver：演示客户端进程。
//
// 独立进程：自起 Scheduler + net::Engine + infra 公开的 V2 会话客户端
// (bbt::infra::rpc::CoRpcClient)，向 gateway 发真实 RPC。只依赖 infra 公开头
// （net/Engine、rpc/CoRpcClient、rpc/Request、rpc/Response）与 framework 公开面
// （CoRpc 位置参数 codec）——不引用 framework internal 头，也不手写第二套
// SessionFrame/u32be/Hello 编解码：wire 的唯一实现是 infra 的 V2 会话。
//
// V2 会话的预算语义：CallOptions.deadline 同时是客户端等待上限与 wire
// remaining_budget_ms 的来源，服务端据此把入站预算收敛为
// min(传输硬看门, now+budget) 并在 handler 可见前扣掉回包预留。见场景 5。
//
// 场景（按序执行，各自打印 PASS/FAIL + 实际结果）：
//   1. store+fetch 正常回路（经 gateway → storage 真实跨进程）
//   2. fetch 未写入的 key     → 业务错误 NotFound 透传
//   3. explode                → 业务错误 InternalError（gateway 本地）
//   4. missing                → 未路由服务名 → NotFound（无 I/O）
//   5. slowcall               → gateway 给 storage 的子预算到期，**服务端**如实
//                              TimedOut（客户端预算远大于子预算，故不是客户端超时）
//   6. del 本轮 key + 复核 fetch → NotFound（只清本轮命名空间，证明两端已无残留）
//   7. storage 未启动/端口不可达时 fetch → 传输层错误（连接失败）
//      （run_demo.sh 起 storage 时本场景自动跳过并说明）
//
// key 命名空间：所有写入键带 BBT_DEMO_KEY_PREFIX 前缀（每轮唯一），只碰本轮
// 自己的键，不覆盖共享后端的固定键。未设前缀时退化为 "color"/"absent"
// （仅手工调试，run_demo.sh 总是显式给前缀）。
//
// 用法: driver <gateway_endpoint host:port> [--expect-storage-down]
//   BBT_DEMO_KEY_PREFIX  本轮 key 前缀（可选）

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <system_error>
#include <thread>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/rpc/CoRpcClient.hpp>
#include <bbt/infra/rpc/Options.hpp>
#include <bbt/infra/rpc/Request.hpp>
#include <bbt/infra/rpc/Response.hpp>

#include <bbt/framework/Framework.hpp> // CoRpcReq/位置参数 codec（公开面）

namespace fw = bbt::framework;
namespace co = bbt::coroutine;
namespace inf = bbt::infra;
namespace net = bbt::infra::net;
namespace rpc = bbt::infra::rpc;
using Clock = std::chrono::steady_clock;

namespace {

std::string g_key_prefix; // 本轮 key 命名空间（见 main）

std::string Key(std::string_view bare) {
    return g_key_prefix + std::string(bare);
}

struct Reply {
    bool         ok = false;
    fw::Error    error{};                 // ok=false 时有效
    std::vector<std::uint8_t> payload;    // ok=true 时有效
    std::chrono::milliseconds elapsed{0}; // 本次调用墙钟耗时
};

const char* CodeName(fw::ErrorCode c) {
    switch (c) {
    case fw::ErrorCode::InvalidArgument:    return "InvalidArgument";
    case fw::ErrorCode::InvalidContext:     return "InvalidContext";
    case fw::ErrorCode::RuntimeUnavailable: return "RuntimeUnavailable";
    case fw::ErrorCode::Closed:             return "Closed";
    case fw::ErrorCode::Cancelled:          return "Cancelled";
    case fw::ErrorCode::TimedOut:           return "TimedOut";
    case fw::ErrorCode::Overloaded:         return "Overloaded";
    case fw::ErrorCode::Unavailable:        return "Unavailable";
    case fw::ErrorCode::TransportError:     return "TransportError";
    case fw::ErrorCode::ProtocolError:      return "ProtocolError";
    case fw::ErrorCode::NotFound:           return "NotFound";
    case fw::ErrorCode::TypeMismatch:       return "TypeMismatch";
    case fw::ErrorCode::UnsupportedRoute:   return "UnsupportedRoute";
    case fw::ErrorCode::OutcomeUnknown:     return "OutcomeUnknown";
    case fw::ErrorCode::RemoteError:        return "RemoteError";
    case fw::ErrorCode::InternalError:      return "InternalError";
    }
    return "?";
}

// 全局运行态：main 建立；协程里执行 Call。
// 必须是可显式释放的全局：若留在静态析构阶段，Engine/Client 的析构会发生在
// g_scheduler->Stop() 之后，访问已停调度器导致崩溃。main 返回前 reset()。
struct DriverRuntime {
    std::shared_ptr<net::Engine> engine;
    std::shared_ptr<rpc::CoRpcClient> client;
    std::thread runner;
    std::shared_future<std::error_code> finished;
};
std::shared_ptr<DriverRuntime> g_runtime;
std::string g_endpoint;
std::uint32_t g_seq = 0;

// 启动 client-only Engine（本代网络资源）并装配 V2 会话客户端。
bool StartRuntime() {
    auto rt = std::make_shared<DriverRuntime>();
    rt->engine = net::Engine::Create(std::shared_ptr<net::RawHandler>{});
    if (!rt->engine)
        return false;

    net::EngineConfig config;
    config.runtime_progress = net::RuntimeProgress::Independent;
    config.options.num_event_loop = 1;
    auto done = std::make_shared<std::promise<std::error_code>>();
    rt->finished = done->get_future().share();
    rt->runner = std::thread([engine = rt->engine, config, done]() mutable {
        try {
            done->set_value(engine->Run(config));
        } catch (...) {
            done->set_value(std::make_error_code(std::errc::io_error));
        }
    });

    const auto until = Clock::now() + std::chrono::seconds{5};
    while (rt->engine->Phase() != net::EnginePhase::Running) {
        if (rt->finished.wait_for(std::chrono::milliseconds{0}) ==
            std::future_status::ready) {
            std::fprintf(stderr, "[driver] engine Run failed before Running\n");
            rt->runner.join();
            return false;
        }
        if (Clock::now() >= until) {
            std::fprintf(stderr, "[driver] engine startup timed out\n");
            rt->engine->RequestClose();
            rt->runner.join();
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }

    rpc::ClientOptions options;
    options.SetLocalIdentity("dual-service-driver")
        .SetTrustedNetworkPlaintext(true)
        .SetMaxSessions(8)
        .SetMaxFrameBytes(65536);
    auto client = rpc::CoRpcClient::Create(rt->engine, std::move(options));
    if (!client) {
        std::fprintf(stderr, "[driver] rpc client create failed: %s\n",
                     client.error().message.c_str());
        rt->engine->RequestClose();
        rt->runner.join();
        return false;
    }
    rt->client = std::move(client).value();
    g_runtime = std::move(rt);
    return true;
}

void StopRuntime() {
    if (!g_runtime)
        return;
    // 控制线程不在协程域：统一关闭入口的协程 Close 不可直接消费，用同步
    // Shutdown 立即封口；连接物理回收由 Engine RequestClose 后 Run 返回收口。
    if (g_runtime->client)
        g_runtime->client->Shutdown();
    if (g_runtime->engine)
        g_runtime->engine->RequestClose();
    if (g_runtime->runner.joinable())
        g_runtime->runner.join();
    g_runtime->client.reset();
    g_runtime->engine.reset();
    g_runtime.reset();
}

Reply Send(const std::string& service, const std::string& method,
           const fw::CoRpcReq& req, std::chrono::milliseconds budget) {
    Reply out;
    const auto t0 = Clock::now();

    // V2 请求：五关联字段 + Required/Service；预算只经 CallOptions.deadline
    // 承载（同时是客户端等待上限与 wire remaining_budget_ms 来源）。
    rpc::Request request;
    request.SetService(service)
        .SetMethod(method)
        .SetRequestId("drv-" + std::to_string(++g_seq))
        .SetRequestSchema(std::string(fw::kCoRpcPositionalSchema))
        .SetResponseSchema(std::string(fw::kCoRpcPositionalSchema))
        .SetReply(rpc::ReplyMode::Required)
        .SetTraffic(rpc::TrafficClass::Service)
        .SetPayload(req.payload());
    const std::string want_request_id = request.RequestId();

    // Client::Call 只在协程内合法；控制线程这边用 latch（mutex+CV）收协程结果。
    struct Slot {
        std::mutex              m;
        std::condition_variable cv;
        bool                    done = false;
        Reply                   reply;
    };
    auto slot = std::make_shared<Slot>();
    bbtco[request, slot, budget, want_request_id]() mutable {
        inf::CallOptions o;
        o.deadline = Clock::now() + budget;
        auto res = bbt_co_await g_runtime->client->Call(
            "tcp://" + g_endpoint, std::move(request), o);
        Reply r;
        if (!res) {
            r.ok    = false;
            r.error = std::move(res.error());
        } else if (res.value().RequestId() != want_request_id) {
            r.error = fw::MakeError(fw::ErrorCode::ProtocolError,
                                    "v2 reply: request_id mismatch");
        } else {
            r.ok = true;
            r.payload = res.value().Payload();
        }
        std::lock_guard<std::mutex> lk(slot->m);
        slot->reply = std::move(r);
        slot->done  = true;
        slot->cv.notify_all();
    };
    std::unique_lock<std::mutex> lk(slot->m);
    // 有界等待：协程未在预算+5s 内落定即记 InternalError（不无限等）。
    if (!slot->cv.wait_for(lk, budget + std::chrono::seconds{5},
                           [&] { return slot->done; })) {
        out.error = fw::MakeError(fw::ErrorCode::InternalError,
                                  "driver: send coroutine did not finish");
    } else {
        out = slot->reply;
    }
    out.elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        Clock::now() - t0);
    return out;
}

template <class T>
std::optional<T> Decode(const Reply& r) {
    if (!r.ok) return std::nullopt;
    auto v = fw::CoRpcReq(std::vector<std::uint8_t>(r.payload)).Parse<T>();
    if (!v) return std::nullopt;
    return v.value();
}

int g_pass = 0, g_fail = 0;

void Check(bool cond, const std::string& name, const std::string& detail) {
    if (cond) {
        ++g_pass;
        std::printf("  PASS %-34s %s\n", name.c_str(), detail.c_str());
    } else {
        ++g_fail;
        std::printf("  FAIL %-34s %s\n", name.c_str(), detail.c_str());
    }
}

std::string ErrDetail(const Reply& r) {
    return "err=" + std::string(CodeName(r.error.code)) + " \"" +
        r.error.message + "\" elapsed=" + std::to_string(r.elapsed.count()) +
        "ms";
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 2 || argc > 3) {
        std::fprintf(stderr,
            "usage: driver <gateway_endpoint host:port> "
            "[--expect-storage-down]\n");
        return 64;
    }
    g_endpoint = argv[1];
    const bool storage_down =
        (argc == 3 && std::strcmp(argv[2], "--expect-storage-down") == 0);
    if (const char* p = std::getenv("BBT_DEMO_KEY_PREFIX"))
        g_key_prefix = p;

    g_bbt_coroutine_config->m_cfg_static_thread_num = 3;
    g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
    g_scheduler->Start();

    if (!StartRuntime())
        return 70;

    std::printf("[driver] gateway=%s key_prefix=\"%s\"\n\n",
                g_endpoint.c_str(), g_key_prefix.c_str());

    if (!storage_down) {
        // ── 1. 正常回路：store 经 gateway 转发 storage.put；fetch → get ──
        {
            auto req = fw::CoRpcReq::From(std::tuple{
                Key("color"), std::string("blue")});
            auto r = Send("gateway", "store", req.value(),
                          std::chrono::milliseconds{8000});
            auto v = Decode<std::string>(r);
            Check(r.ok && v && *v == "blue",
                  "store color=blue -> ok", r.ok ? "reply ok" : ErrDetail(r));
        }
        {
            auto req = fw::CoRpcReq::From(Key("color"));
            auto r = Send("gateway", "fetch", req.value(),
                          std::chrono::milliseconds{8000});
            auto v = Decode<std::string>(r);
            Check(r.ok && v && *v == "blue",
                  "fetch color -> \"blue\" (storage.get via gateway)",
                  r.ok ? (v ? "value=" + *v : "decode failed") : ErrDetail(r));
        }
        // ── 2. 业务错误透传：未写入的 key → NotFound ──
        {
            auto req = fw::CoRpcReq::From(Key("absent"));
            auto r = Send("gateway", "fetch", req.value(),
                          std::chrono::milliseconds{8000});
            Check(!r.ok && r.error.code == fw::ErrorCode::NotFound,
                  "fetch absent -> NotFound", ErrDetail(r));
        }
        // ── 3. gateway 本地业务错误 ──
        {
            auto req = fw::CoRpcReq::From(std::string("x"));
            auto r = Send("gateway", "explode", req.value(),
                          std::chrono::milliseconds{8000});
            Check(!r.ok && r.error.code == fw::ErrorCode::InternalError,
                  "explode -> InternalError", ErrDetail(r));
        }
        // ── 4. 未路由服务名 → find_route NotFound，无 I/O ──
        {
            auto req = fw::CoRpcReq::From(std::string("x"));
            auto r = Send("gateway", "missing", req.value(),
                          std::chrono::milliseconds{8000});
            Check(!r.ok && r.error.code == fw::ErrorCode::NotFound,
                  "missing -> NotFound (unrouted service)", ErrDetail(r));
        }
        // ── 5. 服务端预算继承：gateway 给 storage 的子预算到期，服务端 TimedOut ──
        // 客户端预算 8000ms 远大于 gateway 子预算（见 gateway_main.cc
        // kSlowCallSubBudgetMs=600ms）。若预算只在客户端生效，本调用要么跑满
        // storage 的传输 incoming_timeout（30s，被客户端 8s 先掐断），要么在
        // 8s 处由客户端超时——两种都不该出现「服务端自产 TimedOut」。这里断言：
        //   - 错误码 TimedOut；
        //   - 错误文本来自 storage handler（含 "sleep_ms"）→ 服务端确实收到并
        //     继承子预算后自行中止，不是客户端掐断；
        //   - 耗时 ≈ 子预算（< 3s）→ 既不是 storage 睡满 5000ms，也不是客户端 8s。
        {
            auto req = fw::CoRpcReq::From(std::int32_t{5000});
            auto r = Send("gateway", "slowcall", req.value(),
                          std::chrono::milliseconds{8000});
            const bool server_origin =
                r.error.message.find("sleep_ms") != std::string::npos;
            Check(!r.ok && r.error.code == fw::ErrorCode::TimedOut &&
                      server_origin &&
                      r.elapsed < std::chrono::milliseconds{3000},
                  "slowcall 子预算 600ms -> 服务端 TimedOut", ErrDetail(r));
        }
        // ── 6. 本轮 key 清理：只删自己写入的键，并复核已从两端删除 ──
        // del 走真实 storage（Redis DEL + Mongo DeleteOne）；随后 fetch 必须
        // NotFound，证明两端都无残留。清理失败在此暴露为 FAIL → 进程非零退出，
        // run_demo.sh 据此不得宣称完成。
        {
            auto dreq = fw::CoRpcReq::From(Key("color"));
            auto dr = Send("gateway", "del", dreq.value(),
                           std::chrono::milliseconds{8000});
            auto dv = Decode<std::int32_t>(dr);
            auto freq = fw::CoRpcReq::From(Key("color"));
            auto fr = Send("gateway", "fetch", freq.value(),
                           std::chrono::milliseconds{8000});
            Check(dr.ok && dv && *dv == 1 && !fr.ok &&
                      fr.error.code == fw::ErrorCode::NotFound,
                  "cleanup: del 本轮 key -> 复核 NotFound",
                  "del=" + (dr.ok ? std::to_string(dv ? *dv : -1)
                                  : ErrDetail(dr)) +
                      " fetch=" + ErrDetail(fr));
        }
    } else {
        // ── 7. 失败路径：storage 不可达 → fetch 传输层/可用性错误 ──
        // 严格断言实际契约取值：gateway 调 storage 建连失败，如实返回
        // TransportError/Unavailable/OutcomeUnknown 之一，不得是 NotFound
        // 这类业务语义，也不得被 InternalError 兜底吞掉（过弱断言曾把
        // 任何非 NotFound 都算通过）。
        {
            auto req = fw::CoRpcReq::From(Key("color"));
            auto r = Send("gateway", "fetch", req.value(),
                          std::chrono::milliseconds{8000});
            const bool transport_like =
                r.error.code == fw::ErrorCode::TransportError ||
                r.error.code == fw::ErrorCode::Unavailable ||
                r.error.code == fw::ErrorCode::OutcomeUnknown;
            Check(!r.ok && transport_like,
                  "storage down -> Transport/Unavailable/OutcomeUnknown",
                  ErrDetail(r));
        }
    }

    std::printf("\n[driver] %d passed, %d failed\n", g_pass, g_fail);
    std::fflush(stdout);

    // 收口：Client Shutdown 后 Engine RequestClose，Run 返回才表示本代网络资源
    // 收口；显式 reset 后于 Close 即可（coroutine Scheduler 无 Stop）。
    StopRuntime();
    return g_fail == 0 ? 0 : 1;
}
