// framework #8 F8-T3 业务 Service 实现——独立 TU，只 include 公开头 + stdlib。
// 本文件的 include 闭包必须不含 bbt/framework/internal/* 与第三方 driver；
// runner 以编译器 -H 记录并断言（logs/service-include-closure.txt）。
#include "framework_resource_service.hpp"

#include <chrono>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace fw8test {

namespace {

using Clock = std::chrono::steady_clock;

// ── BSON 裸字节编解码（MongoDocument 是 owning bytes；只处理本业务用到的
//    最小元素：string 与 int32，含一层 $set 嵌入文档）──
void PutI32(std::vector<std::uint8_t>& b, std::int32_t v) {
    for (int i = 0; i < 4; ++i)
        b.push_back(static_cast<std::uint8_t>((v >> (8 * i)) & 0xFF));
}
std::int32_t ReadI32(const std::vector<std::uint8_t>& b, std::size_t pos) {
    return static_cast<std::int32_t>(
        static_cast<std::uint32_t>(b[pos]) |
        (static_cast<std::uint32_t>(b[pos + 1]) << 8) |
        (static_cast<std::uint32_t>(b[pos + 2]) << 16) |
        (static_cast<std::uint32_t>(b[pos + 3]) << 24));
}

struct BsonBuilder {
    std::vector<std::uint8_t> m_body;
    void _Key(std::string_view k) {
        m_body.insert(m_body.end(), k.begin(), k.end());
        m_body.push_back(0x00);
    }
    void Str(std::string_view k, std::string_view v) {
        m_body.push_back(0x02);                 // string
        _Key(k);
        PutI32(m_body, static_cast<std::int32_t>(v.size() + 1));
        m_body.insert(m_body.end(), v.begin(), v.end());
        m_body.push_back(0x00);
    }
    void Doc(std::string_view k, const BsonBuilder& sub) {
        m_body.push_back(0x03);                 // embedded document
        _Key(k);
        const auto d = sub.Finish();
        m_body.insert(m_body.end(), d.bytes.begin(), d.bytes.end());
    }
    inf::MongoDocument Finish() const {
        inf::MongoDocument d;
        PutI32(d.bytes, static_cast<std::int32_t>(m_body.size() + 5));
        d.bytes.insert(d.bytes.end(), m_body.begin(), m_body.end());
        d.bytes.push_back(0x00);
        return d;
    }
};

inf::MongoDocument DocId(std::string_view id) {
    BsonBuilder b;
    b.Str("_id", id);
    return b.Finish();
}
inf::MongoDocument DocIdV(std::string_view id, std::string_view v) {
    BsonBuilder b;
    b.Str("_id", id);
    b.Str("v", v);
    return b.Finish();
}
inf::MongoDocument DocSetV(std::string_view v) {
    BsonBuilder inner;
    inner.Str("v", v);
    BsonBuilder b;
    b.Doc("$set", inner);
    return b.Finish();
}

// 读取顶层 string 字段（本业务只写入 string/int32；其他类型不出现）。
std::optional<std::string> ReadStrField(
    const inf::MongoDocument& d, std::string_view key) {
    const auto& b = d.bytes;
    if (b.size() < 5) return std::nullopt;
    std::size_t pos = 4;
    while (pos < b.size() && b[pos] != 0x00) {
        const std::uint8_t type = b[pos++];
        std::string name;
        while (pos < b.size() && b[pos] != 0x00)
            name.push_back(static_cast<char>(b[pos++]));
        if (pos >= b.size()) return std::nullopt;
        ++pos;                                  // skip name NUL
        if (type == 0x02) {
            if (pos + 4 > b.size()) return std::nullopt;
            const std::int32_t len = ReadI32(b, pos);
            pos += 4;
            if (len < 1 || pos + static_cast<std::size_t>(len) > b.size())
                return std::nullopt;
            std::string val(reinterpret_cast<const char*>(b.data() + pos),
                            static_cast<std::size_t>(len - 1));
            pos += static_cast<std::size_t>(len);
            if (name == key) return val;
        } else if (type == 0x10) {
            pos += 4;
        } else {
            return std::nullopt;                // 未使用的元素类型
        }
    }
    return std::nullopt;
}

} // namespace

fw::CoRpcResp KvSvc::_Reply(std::int32_t status, std::string value) {
    return fw::CoRpcResp::From(std::tuple{status, std::move(value)});
}

fw::CoRpcResp KvSvc::_Missing(const char* name) {
    return fw::CoRpcResp::Error(fw::MakeError(
        fw::ErrorCode::Unavailable,
        std::string("kv: resource '") + name + "' missing"));
}

KvSvc::ResolvedOrError KvSvc::_Res() {
    ResolvedOrError out;
    auto ctx = context().request();
    if (!ctx) { out.resp = fw::CoRpcResp::Error(ctx.error()); return out; }
    out.res.cache = context().resource<inf::CoRedisCli>("cache");
    out.res.docs  = context().resource<inf::CoMongoCli>("docs");
    out.res.deadline = ctx.value()->deadline;
    if (!out.res.cache) { out.resp = _Missing("cache"); return out; }
    if (!out.res.docs)  { out.resp = _Missing("docs");  return out; }
    out.ok = true;
    return out;
}

fw::CoRpcResp KvSvc::Get(fw::CoRpcReq req) {
    auto id = req.Parse<std::string>();
    if (!id) return fw::CoRpcResp::Error(id.error());
    auto base = _Res();
    if (!base.ok) return base.resp;
    inf::CallOptions o;
    o.deadline = base.res.deadline;
    const std::string key = "kv:" + id.value();
    auto c = base.res.cache->Get(key, o);
    if (!c) return fw::CoRpcResp::Error(c.error());
    if (c.value().has_value())
        return _Reply(0, c.value().value());          // cache hit
    mongo_reads.fetch_add(1, std::memory_order_acq_rel);
    auto f = base.res.docs->FindOne(DocId(id.value()), o);
    if (!f) return fw::CoRpcResp::Error(f.error());
    if (!f.value().has_value())
        return _Reply(1, "");                          // not-found
    auto v = ReadStrField(f.value().value(), "v");
    if (!v) return fw::CoRpcResp::Error(fw::MakeError(
        fw::ErrorCode::ProtocolError, "kv: document missing 'v'"));
    auto w = base.res.cache->Set(key, *v, o);              // 写回 cache
    if (!w) return fw::CoRpcResp::Error(w.error());
    return _Reply(0, *v);
}

fw::CoRpcResp KvSvc::Insert(fw::CoRpcReq req) {
    auto args = req.Parse<std::string, std::string>();
    if (!args) return fw::CoRpcResp::Error(args.error());
    auto base = _Res();
    if (!base.ok) return base.resp;
    inf::CallOptions o;
    o.deadline = base.res.deadline;
    const auto& [id, v] = args.value();
    auto r = base.res.docs->InsertOne(DocIdV(id, v), o);
    if (!r) return fw::CoRpcResp::Error(r.error());    // 原样传播
    return _Reply(0, v);
}

// 更新：先落 Mongo，再失效并刷新 Redis（非强一致 cache 承诺）。
fw::CoRpcResp KvSvc::Update(fw::CoRpcReq req) {
    auto args = req.Parse<std::string, std::string>();
    if (!args) return fw::CoRpcResp::Error(args.error());
    auto base = _Res();
    if (!base.ok) return base.resp;
    inf::CallOptions o;
    o.deadline = base.res.deadline;
    const auto& [id, v] = args.value();
    const std::string key = "kv:" + id;
    auto u = base.res.docs->UpdateOne(DocId(id), DocSetV(v), o);
    if (!u) return fw::CoRpcResp::Error(u.error());
    auto d = base.res.cache->Delete({key}, o);             // 失效
    if (!d) return fw::CoRpcResp::Error(d.error());
    auto w = base.res.cache->Set(key, v, o);               // 刷新
    if (!w) return fw::CoRpcResp::Error(w.error());
    return _Reply(static_cast<std::int32_t>(u.value().matched), v);
}

fw::CoRpcResp KvSvc::Exists(fw::CoRpcReq req) {
    auto id = req.Parse<std::string>();
    if (!id) return fw::CoRpcResp::Error(id.error());
    auto base = _Res();
    if (!base.ok) return base.resp;
    inf::CallOptions o;
    o.deadline = base.res.deadline;
    auto e = base.res.cache->Exists("kv:" + id.value(), o);
    if (!e) return fw::CoRpcResp::Error(e.error());
    return _Reply(e.value() ? 1 : 0, "");
}

fw::CoRpcResp KvSvc::Ping(fw::CoRpcReq req) {
    auto id = req.Parse<std::string>();   // 占位参数，保持位置 codec 一致
    if (!id) return fw::CoRpcResp::Error(id.error());
    auto base = _Res();
    if (!base.ok) return base.resp;
    inf::CallOptions o;
    o.deadline = base.res.deadline;
    auto r = base.res.cache->Ping(o);
    if (!r) return fw::CoRpcResp::Error(r.error());
    return _Reply(0, "PONG");
}

// 有限 deadline 探针：把 context().request()->deadline 的剩余预算回传。
fw::CoRpcResp KvSvc::ProbeDeadline(fw::CoRpcReq req) {
    auto id = req.Parse<std::string>();   // 占位参数，保持位置 codec 一致
    if (!id) return fw::CoRpcResp::Error(id.error());
    auto ctx = context().request();
    if (!ctx) return fw::CoRpcResp::Error(ctx.error());
    const auto d = ctx.value()->deadline;
    if (d == co::Deadline::max())
        return _Reply(-1, "unbounded");
    const auto remain = std::chrono::duration_cast<std::chrono::milliseconds>(
        d - Clock::now()).count();
    return _Reply(static_cast<std::int32_t>(remain), "");
}

// 过期请求路径：等父预算真实到期后，用父 deadline 调资源。handler 若把
// Deadline::max() 下传则此处会成功、键被写入；正确传播则 infra 在发起
// I/O 前返回 TimedOut，键不出现——两者结果不同，断言可区分。
fw::CoRpcResp KvSvc::ExpiredSet(fw::CoRpcReq req) {
    auto args = req.Parse<std::string, std::string>();
    if (!args) return fw::CoRpcResp::Error(args.error());
    auto ctx = context().request();
    if (!ctx) return fw::CoRpcResp::Error(ctx.error());
    auto cache = context().resource<inf::CoRedisCli>("cache");
    if (!cache) return _Missing("cache");
    inf::CallOptions o;
    o.deadline = ctx.value()->deadline;
    // 等到父预算真实到期（无 sleep）：CoWaiter 在 deadline 处落定。
    auto waiter = co::sync::CoWaiter::Create();
    co::WaitOptions wo;
    wo.deadline = o.deadline;
    (void)waiter->Wait(wo);
    const auto& [id, v] = args.value();
    auto s = cache->Set("kv:" + id, v, o);   // 期望 TimedOut（无 I/O）
    if (!s) return fw::CoRpcResp::Error(s.error());
    return _Reply(0, "SET");
}

// 关闭中在途：停在宿主注入的闸门上直到放行（真实 HTTP handler 在途）。
fw::CoRpcResp KvSvc::Hold(fw::CoRpcReq req) {
    auto id = req.Parse<std::string>();
    if (!id) return fw::CoRpcResp::Error(id.error());
    auto ctx = context().request();
    if (!ctx) return fw::CoRpcResp::Error(ctx.error());
    if (!hold_gate) return fw::CoRpcResp::Error(fw::MakeError(
        fw::ErrorCode::RuntimeUnavailable, "kv: hold gate absent"));
    hold_gate();
    return _Reply(0, "HELD");
}

} // namespace fw8test
