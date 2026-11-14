// examples/dual_service/driver_main.cc — driver：演示客户端进程。
//
// 独立进程：业务主协程（runtime 的生成式 `bbtco_main`）里装配 net::Engine +
// infra 公开的 V2 会话客户端 (bbt::infra::rpc::CoRpcClient)，向 gateway 发真实
// RPC；共享 Engine 的唯一一次 `Engine::Run` 经 framework `HostGate` 发布给
// 入口 `bbtco_main_host_hook` 在主线程执行，业务侧不建线程、不 join、不等待
// future。只依赖 infra 公开头（net/Engine、rpc/CoRpcClient、rpc/Request、
// rpc/Response）与 framework 公开面（HostGate、CoRpc 位置参数 codec）——
// 不引用 framework internal 头，也不手写第二套 SessionFrame/u32be/Hello
// 编解码：wire 的唯一实现是 infra 的 V2 会话。
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
// 参数（生成式入口不暴露 argv，参数经环境变量传入）：
//   BBT_EXAMPLE_GATEWAY_ENDPOINT=<host:port>（必填）
//   BBT_EXAMPLE_EXPECT_STORAGE_DOWN=1（可选：只跑 storage 不可达的失败路径）
//   BBT_DEMO_KEY_PREFIX  本轮 key 前缀（可选）

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <system_error>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/rpc/CoRpcClient.hpp>
#include <bbt/infra/rpc/Options.hpp>
#include <bbt/infra/rpc/Request.hpp>
#include <bbt/infra/rpc/Response.hpp>

#include <bbt/framework/Framework.hpp> // CoRpcReq/位置参数 codec（公开面）
#include <bbt/framework/HostGate.hpp>

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

// 全局运行态：bbtco_main 装配，场景协程里执行 Call。
// 必须是可显式释放的全局：静态析构阶段销毁 Engine/Client 会发生在业务收口之后
// 的任意时刻。主协程返回前 reset()。
struct DriverRuntime {
    std::shared_ptr<net::Engine> engine;
    std::shared_ptr<rpc::CoRpcClient> client;
};
std::shared_ptr<DriverRuntime> g_runtime;
std::string g_endpoint;
std::uint32_t g_seq = 0;

// 启动 client-only Engine（本代网络资源）并装配 V2 会话客户端。
// 与生产 App 同一形态：Idle 装配完成后经 HostGate 发布 {engine, config}，由
// 入口 host hook 在主线程执行一次 Run；本协程经 CoWaitRunning 观察 Ready。
// Run 由 bbtco_main_host_hook 执行——见文件尾；不建 runner 线程、不 join。
bool StartRuntime() {
    auto rt = std::make_shared<DriverRuntime>();
    rt->engine = net::Engine::Create(std::shared_ptr<net::RawHandler>{});
    if (!rt->engine)
        return false;

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
        return false;
    }
    rt->client = std::move(client).value();

    // Idle 装配完成 → 发布给入口 host hook；client-only 无 listen_addresses。
    bbt::framework::HostGate::Publication publication;
    publication.engine = rt->engine;
    publication.config.runtime_progress = net::RuntimeProgress::Independent;
    publication.config.options.num_event_loop = 1;
    bbt::framework::HostGate::Publish(std::move(publication));

    bbt::coroutine::WaitOptions wait;
    wait.deadline = Clock::now() + std::chrono::seconds{5};
    auto ready = rt->engine->CoWaitRunning(wait);
    if (!ready) {
        std::fprintf(stderr, "[driver] engine not running: %s\n",
                     ready.error().message.c_str());
        auto closed = bbt_co_await rt->client->Close(
            bbt::infra::CloseOptions::Graceful(Clock::now() +
                                               std::chrono::seconds{5}));
        (void)closed;
        rt->engine->RequestClose();
        return false;
    }
    g_runtime = std::move(rt);
    return true;
}

void StopRuntime() {
    if (!g_runtime)
        return;
    if (g_runtime->client) {
        // 客户端自己的会话责任先收：统一关闭入口在 Co 域 await。
        auto closed = bbt_co_await g_runtime->client->Close(
            bbt::infra::CloseOptions::Graceful(Clock::now() +
                                               std::chrono::seconds{5}));
        (void)closed;
    }
    if (g_runtime->engine) {
        // Engine 的物理凭据只有「入口 host hook 的 Run 已返回」；主任务结束时
        // hook 经 OnMainTaskDone 调一次 RequestClose。这里显式同值收口，使
        // 主协程返回前 Run 必然已在收口路径上（幂等）。
        g_runtime->engine->RequestClose();
    }
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

    // Client::Call 只在协程内合法：Send 本身就在业务主协程里，直接 await，
    // 不再投递协程 + 用 CV 把结果搬回 native 线程。预算已由 CallOptions 承载，
    // 客户端等待上限与 wire remaining_budget_ms 共用同一个 deadline。
    inf::CallOptions o;
    o.deadline = Clock::now() + budget;
    auto res = bbt_co_await g_runtime->client->Call(
        "tcp://" + g_endpoint, std::move(request), o);
    if (!res) {
        out.ok = false;
        out.error = std::move(res.error());
    } else if (res.value().RequestId() != want_request_id) {
        out.error = fw::MakeError(fw::ErrorCode::ProtocolError,
                                  "v2 reply: request_id mismatch");
    } else {
        out.ok = true;
        out.payload = res.value().Payload();
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

// 客户端命令面是单一协程流，Engine 的 Run 由入口 host hook 承担、
// CoWaitRunning 与 Client Close 都要 await，故用 runtime 默认栈位即可；
// 这里只钉 worker 数（业务 1 + Engine 2 槽）。
void setup_runtime() {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 3;
    g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
}

int bbtco_main() {
    // 参数（生成式入口不暴露 argv）：BBT_EXAMPLE_GATEWAY_ENDPOINT 必填，
    // BBT_EXAMPLE_EXPECT_STORAGE_DOWN=1 时只跑 storage 不可达的失败路径。
    const char* endpoint = std::getenv("BBT_EXAMPLE_GATEWAY_ENDPOINT");
    if (endpoint == nullptr || *endpoint == '\0') {
        std::fprintf(stderr,
                     "usage: driver with env BBT_EXAMPLE_GATEWAY_ENDPOINT=<host:port> "
                     "[BBT_EXAMPLE_EXPECT_STORAGE_DOWN=1]\n");
        return 64;
    }
    g_endpoint = endpoint;
    const char* down_flag = std::getenv("BBT_EXAMPLE_EXPECT_STORAGE_DOWN");
    const bool storage_down = down_flag != nullptr && *down_flag != '\0' &&
                              std::strcmp(down_flag, "0") != 0;
    if (const char* p = std::getenv("BBT_DEMO_KEY_PREFIX"))
        g_key_prefix = p;

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

    // 收口：Client Close → Engine RequestClose（入口 host hook 的 Run 在主任务
    // 结束后必然返回；Run 返回即物理凭据）。收口后显式 reset 全局运行态。
    StopRuntime();
    return g_fail == 0 ? 0 : 1;
}

// 入口 native host hook：共享 Engine 的 Engine::Run 在主线程执行一次。
int bbtco_main_host_hook(bbt::coroutine::HostHookContext& ctx) {
    return bbt::framework::HostGate::RunHostGate(ctx);
}

BBTCO_MAIN_WITH_SETUP_AND_HOST_HOOK();
