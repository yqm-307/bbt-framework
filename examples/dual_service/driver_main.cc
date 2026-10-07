// examples/dual_service/driver_main.cc — driver：演示客户端进程。
//
// 独立进程：自起 Scheduler + NetworkRuntime + HttpClient，按 infra 正式
// RpcWire（ProtoWireV1，POST /rpc + application/x-protobuf）向 gateway 发真实
// RPC。只依赖 infra 公开头 <bbt/infra/rpc/RpcWire.hpp> 与 framework 公开面
// （CoRpc 位置参数 codec）——不引用 framework internal 头，也不读 x-bbt-* header。
//
// 为什么必须是 ProtoWireV1 而非迁移期 header 桥：只有正式 wire body 携带
// remaining_budget_ms，服务端才能在 handler 可见前把入站预算收敛为
// min(传输硬看门, now+budget)；header 桥没有任何预算字段，服务端只能吃
// 传输层 incoming_timeout，做不到真正继承调用方预算。见场景 5 的断言口径。
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
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/infra/HttpClient.hpp>
#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/rpc/RpcWire.hpp>       // 公开 wire profile 契约（非 framework internal）

#include <bbt/framework/Framework.hpp>     // CoRpcReq/位置参数 codec

namespace fw  = bbt::framework;
namespace co  = bbt::coroutine;
namespace inf = bbt::infra;
namespace rpcw = bbt::infra::rpc;
using Clock = std::chrono::steady_clock;

namespace {

std::string g_key_prefix;   // 本轮 key 命名空间（见 main）

std::string Key(std::string_view bare) {
    return g_key_prefix + std::string(bare);
}

struct Reply {
    bool         ok = false;
    fw::Error    error{};                 // ok=false 时有效
    std::vector<std::uint8_t> payload;    // ok=true 时有效
    std::uint32_t remaining_ms = 0;       // 服务端回填的生效剩余预算（wire）
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

// 全局运行态：main 建立；协程里执行 Send。
// 必须是可显式释放的全局：若留在静态析构阶段，HttpClient/NetworkRuntime
// 的析构会发生在 g_scheduler->Stop() 之后，访问已停调度器导致崩溃。
// main 返回前 reset()（见 main 收尾）。
std::shared_ptr<inf::NetworkRuntime> g_rt;
std::shared_ptr<inf::HttpClient>     g_client;
std::string                          g_endpoint;
std::uint32_t                        g_seq = 0;

Reply Send(const std::string& service, const std::string& method,
           const fw::CoRpcReq& req, std::chrono::milliseconds budget) {
    Reply out;
    const auto t0 = Clock::now();

    // 组正式 wire 信封：remaining_budget_ms 由本地预算换算并 clamp 到 wire 合法
    // 区间（[1, kMaxRemainingBudgetMs]）；request_id 用于回显关联校验。
    rpcw::RpcWireEnvelope w;
    w.profile_version     = rpcw::kRpcWireProfileVersion;
    w.request_id          = "drv-" + std::to_string(++g_seq);
    w.service             = service;
    w.method              = method;
    const auto budget_ms  = static_cast<long long>(budget.count());
    w.remaining_budget_ms = static_cast<std::uint32_t>(std::clamp<long long>(
        budget_ms, 1, static_cast<long long>(rpcw::kMaxRemainingBudgetMs)));
    w.request_schema      = std::string(fw::kCoRpcPositionalSchema);
    w.response_schema     = std::string(fw::kCoRpcPositionalSchema);
    w.payload             = req.payload();
    w.success             = true;

    auto http_req = rpcw::MakeRpcWireHttpRequest(w, "http://" + g_endpoint);
    if (!http_req) {
        out.error = std::move(http_req.error());
        out.elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            Clock::now() - t0);
        return out;
    }
    const std::string want_request_id = w.request_id;

    // client->Request 只在协程内合法；控制线程这边用 latch（mutex+CV）
    // 收协程结果。
    struct Slot {
        std::mutex              m;
        std::condition_variable cv;
        bool                    done = false;
        Reply                   reply;
    };
    auto slot = std::make_shared<Slot>();
    bbtco [http_req = std::move(http_req.value()), slot, budget,
           want_request_id]() mutable {
        inf::CallOptions o;
        o.deadline = Clock::now() + budget;
        auto res = g_client->Request(http_req, o);
        Reply r;
        if (!res) {
            r.ok    = false;
            r.error = std::move(res.error());
        } else {
            auto parsed = rpcw::ParseRpcWireHttpResponse(res.value());
            if (!parsed) {
                r.error = std::move(parsed.error());   // 非法 wire body
            } else {
                const auto& w2 = parsed.value();
                if (w2.request_id != want_request_id) {
                    r.error = fw::MakeError(
                        fw::ErrorCode::ProtocolError,
                        "wire reply: request_id mismatch");
                } else if (!w2.success) {
                    r.ok    = false;
                    r.error = w2.error;                 // 远端（含服务端自产）错误
                } else {
                    r.ok           = true;
                    r.payload      = w2.payload;
                    r.remaining_ms = w2.remaining_budget_ms;
                }
            }
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
        return 70;
    }
    g_rt = rt.value();
    if (auto r = g_rt->Start(); !r) {
        std::fprintf(stderr, "[driver] runtime start failed: %s\n",
                     r.error().message.c_str());
        g_rt->Close();
        return 70;
    }
    auto cl = g_rt->CreateHttpClient();
    if (!cl) {
        std::fprintf(stderr, "[driver] CreateHttpClient failed\n");
        g_rt->Close();
        return 70;
    }
    g_client = cl.value();

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

    // 收口：ICoCloseable::Close() 同步幂等（返回即物理释放）；先关客户端再关
    // runtime。显式 reset 后于 Close 即可——不再依赖已删除的调度器停机入口
    //（coroutine Scheduler 无 Stop，进程寿命单例）。
    g_client->Close();
    g_rt->Close();
    g_client.reset();
    g_rt.reset();
    return g_fail == 0 ? 0 : 1;
}
