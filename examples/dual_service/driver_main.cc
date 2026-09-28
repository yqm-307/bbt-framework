// examples/dual_service/driver_main.cc — driver：演示客户端进程。
//
// 独立进程：自起 Scheduler + NetworkRuntime + HttpClient，按公开线格式
// （x-bbt-* header + CoRpc 位置参数负载编码）向 gateway 发真实 RPC。
// 不引用 framework internal 头——x-bbt-* 映射在本地重新实现（~30 行），
// 与 internal/RpcHttpBridge 同源字段但口径对外可见。
//
// 场景（按序执行，各自打印 PASS/FAIL + 实际结果）：
//   1. store+fetch 正常回路（经 gateway → storage 真实跨进程）
//   2. fetch 未写入的 key     → 业务错误 NotFound 透传
//   3. explode                → 业务错误 InternalError（gateway 本地）
//   4. missing                → 未路由服务名 → NotFound（无 I/O）
//   5. slowcall(ms=5000)，客户端预算 300ms → TimedOut
//   6. storage 未启动/端口不可达时 fetch → 传输层错误（连接失败）
//      （run_demo.sh 起 storage 时本场景自动跳过并说明）
//
// 用法: driver <gateway_endpoint host:port> [--expect-storage-down]

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/sync/CompletionSignal.hpp>

#include <bbt/infra/HttpClient.hpp>
#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/NetworkTypes.hpp>

#include <bbt/framework/Framework.hpp>   // CoRpcReq/位置参数 codec + schema 名

namespace fw  = bbt::framework;
namespace co  = bbt::coroutine;
namespace inf = bbt::infra;
using Clock = std::chrono::steady_clock;

namespace {

// ── x-bbt-* ↔ envelope 映射（客户端侧自实现，framework 公共面之外）──

const char* HeaderOf(
    const std::vector<std::pair<std::string, std::string>>& hs,
    const char* name) {
    for (const auto& [k, v] : hs)
        if (k == name) return v.c_str();
    return nullptr;
}

inf::HttpRequest ToHttp(const inf::RpcEnvelope& env, const std::string& ep) {
    inf::HttpRequest r;
    r.method = "POST";
    r.url    = "http://" + ep + "/rpc";
    r.headers = {
        {"x-bbt-service",         env.service},
        {"x-bbt-method",          env.method},
        {"x-bbt-request-id",      env.request_id},
        {"x-bbt-request-schema",  env.request_schema},
        {"x-bbt-response-schema", env.response_schema},
    };
    r.body.assign(env.payload.begin(), env.payload.end());
    return r;
}

struct Reply {
    bool        ok = false;
    fw::Error   error{};                 // ok=false 时有效
    std::vector<std::uint8_t> payload;   // ok=true 时有效
};

Reply FromHttp(const inf::HttpResponse& res) {
    Reply out;
    if (res.status == 200) {
        out.ok = true;
        out.payload.assign(res.body.begin(), res.body.end());
        return out;
    }
    out.ok = false;
    if (const char* c = HeaderOf(res.headers, "x-bbt-err-code"))
        out.error.code = static_cast<fw::ErrorCode>(std::atoi(c));
    else
        out.error.code = fw::ErrorCode::ProtocolError;
    if (const char* m = HeaderOf(res.headers, "x-bbt-err-message"))
        out.error.message = m;
    if (const char* d = HeaderOf(res.headers, "x-bbt-err-domain"))
        out.error.domain = d;
    return out;
}

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

// 全局运行态：main 建立；协程里执行 Send。
// 必须是可显式释放的全局：若留在静态析构阶段，HttpClient/NetworkRuntime
// 的析构会发生在 g_scheduler->Stop() 之后，访问已停调度器导致崩溃。
// main 返回前 reset()（见 main 收尾）。
std::shared_ptr<inf::NetworkRuntime> g_rt;
std::shared_ptr<inf::HttpClient>     g_client;
std::string                          g_endpoint;
int                                  g_seq = 0;

Reply Send(const std::string& service, const std::string& method,
           const fw::CoRpcReq& req, std::chrono::milliseconds budget) {
    inf::RpcEnvelope env;
    env.service         = service;
    env.method          = method;
    env.request_id      = "drv-" + std::to_string(++g_seq);
    env.request_schema  = std::string(fw::kCoRpcPositionalSchema);
    env.response_schema = std::string(fw::kCoRpcPositionalSchema);
    env.payload         = req.payload();

    // client->Request 与 CompletionSignal::Wait 都只在协程内合法；
    // 控制线程这边用 latch（mutex+CV）收协程结果。
    struct Slot {
        std::mutex              m;
        std::condition_variable cv;
        bool                    done = false;
        Reply                   reply;
    };
    auto slot = std::make_shared<Slot>();
    bbtco [env = std::move(env), slot, budget]() mutable {
        inf::CallOptions o;
        o.deadline = Clock::now() + budget;
        o.cancel   = {};
        auto res = g_client->Request(ToHttp(env, g_endpoint), o);
        {
            std::lock_guard<std::mutex> lk(slot->m);
            if (!res) {
                slot->reply.ok    = false;
                slot->reply.error = std::move(res.error());
            } else {
                slot->reply = FromHttp(res.value());
            }
            slot->done = true;
        }
        slot->cv.notify_all();
    };
    std::unique_lock<std::mutex> lk(slot->m);
    if (!slot->cv.wait_for(lk, budget + std::chrono::seconds{5},
                           [&] { return slot->done; })) {
        Reply r;
        r.error = fw::MakeError(fw::ErrorCode::InternalError,
                                "driver: send coroutine did not finish");
        return r;
    }
    return slot->reply;
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

    g_bbt_coroutine_config->m_cfg_static_thread_num = 1;
    g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
    g_scheduler->Start();

    auto rt = inf::NetworkRuntime::Create(inf::NetworkLimits{
        /*max_connections*/ 32, /*max_inflight*/ 32,
        /*max_header_bytes*/ 16384, /*max_body_bytes*/ 65536,
        /*incoming_timeout*/ std::chrono::milliseconds{5000}});
    if (!rt) {
        std::fprintf(stderr, "[driver] NetworkRuntime::Create failed: %s\n",
                     rt.error().message.c_str());
        g_scheduler->Stop();
        return 70;
    }
    g_rt = rt.value();
    if (auto r = g_rt->Start(); !r) {
        std::fprintf(stderr, "[driver] runtime start failed: %s\n",
                     r.error().message.c_str());
        g_scheduler->Stop();
        return 70;
    }
    auto cl = g_rt->CreateHttpClient();
    if (!cl) {
        std::fprintf(stderr, "[driver] CreateHttpClient failed\n");
        g_rt->RequestClose();
        g_scheduler->Stop();
        return 70;
    }
    g_client = cl.value();

    std::printf("[driver] gateway=%s\n\n", g_endpoint.c_str());

    if (!storage_down) {
        // ── 1. 正常回路：store 经 gateway 转发 storage.put；fetch → get ──
        {
            auto req = fw::CoRpcReq::From(std::tuple{
                std::string("color"), std::string("blue")});
            auto r = Send("gateway", "store", req.value(),
                          std::chrono::milliseconds{5000});
            auto v = Decode<std::string>(r);
            Check(r.ok && v && *v == "blue",
                  "store color=blue -> ok", r.ok ? "reply ok" :
                  "err=" + std::string(CodeName(r.error.code)) + " " +
                      r.error.message);
        }
        {
            auto req = fw::CoRpcReq::From(std::string("color"));
            auto r = Send("gateway", "fetch", req.value(),
                          std::chrono::milliseconds{5000});
            auto v = Decode<std::string>(r);
            Check(r.ok && v && *v == "blue",
                  "fetch color -> \"blue\" (storage.get via gateway)",
                  r.ok ? (v ? "value=" + *v : "decode failed")
                       : "err=" + std::string(CodeName(r.error.code)) +
                             " " + r.error.message);
        }
        // ── 2. 业务错误透传：未写入的 key → NotFound ──
        {
            auto req = fw::CoRpcReq::From(std::string("absent"));
            auto r = Send("gateway", "fetch", req.value(),
                          std::chrono::milliseconds{5000});
            Check(!r.ok && r.error.code == fw::ErrorCode::NotFound,
                  "fetch absent -> NotFound",
                  "err=" + std::string(CodeName(r.error.code)) + " " +
                      r.error.message);
        }
        // ── 3. gateway 本地业务错误 ──
        {
            auto req = fw::CoRpcReq::From(std::string("x"));
            auto r = Send("gateway", "explode", req.value(),
                          std::chrono::milliseconds{5000});
            Check(!r.ok && r.error.code == fw::ErrorCode::InternalError,
                  "explode -> InternalError",
                  "err=" + std::string(CodeName(r.error.code)) + " " +
                      r.error.message);
        }
        // ── 4. 未路由服务名 → find_route NotFound，无 I/O ──
        {
            auto req = fw::CoRpcReq::From(std::string("x"));
            auto r = Send("gateway", "missing", req.value(),
                          std::chrono::milliseconds{5000});
            Check(!r.ok && r.error.code == fw::ErrorCode::NotFound,
                  "missing -> NotFound (unrouted service)",
                  "err=" + std::string(CodeName(r.error.code)) + " " +
                      r.error.message);
        }
        // ── 5. 超时：storage.sleep_ms(5000)，客户端预算 300ms ──
        {
            auto req = fw::CoRpcReq::From(std::int32_t{5000});
            auto r = Send("gateway", "slowcall", req.value(),
                          std::chrono::milliseconds{300});
            Check(!r.ok && (r.error.code == fw::ErrorCode::TimedOut ||
                            r.error.code == fw::ErrorCode::Cancelled),
                  "slowcall 5000ms with 300ms budget -> TimedOut",
                  "err=" + std::string(CodeName(r.error.code)) + " " +
                      r.error.message);
        }
    } else {
        // ── 6. 失败路径：storage 不可达 → fetch 传输层/可用性错误 ──
        {
            auto req = fw::CoRpcReq::From(std::string("color"));
            auto r = Send("gateway", "fetch", req.value(),
                          std::chrono::milliseconds{3000});
            Check(!r.ok && r.error.code != fw::ErrorCode::NotFound,
                  "fetch with storage down -> transport/unavailable error",
                  "err=" + std::string(CodeName(r.error.code)) + " " +
                      r.error.message);
        }
    }

    std::printf("\n[driver] %d passed, %d failed\n", g_pass, g_fail);
    std::fflush(stdout);

    g_client->RequestClose();
    g_rt->RequestClose();
    const auto until = Clock::now() + std::chrono::seconds{10};
    while (!g_rt->IsClosed() && Clock::now() < until)
        std::this_thread::yield();
    // 显式释放：infra 对象析构依赖运行中的调度器代际；静态析构
    // 会晚于 g_scheduler->Stop()，在那里访问已停调度器。
    g_client.reset();
    g_rt.reset();
    g_scheduler->Stop();
    return g_fail == 0 ? 0 : 1;
}
