#pragma once
// service-actor/v2：业务消息声明与框架生成的通用 codec。
//
// 业务侧只写字段声明与 schema 名：
//   struct EchoReq { std::int32_t v = 0; std::string text; };
//   BBT_MESSAGE_FIELDS(EchoReq, "test.EchoReq/v1", v, text);
// 框架据 MessageTraits<T>::Fields() 的成员指针生成编解码：
//  - 线格式与对象布局/ABI/sizeof(struct)/memcpy(struct) 无关：消息头
//    u32 count，随后逐字段 [u16 tag][u8 type][u32 len][payload]，tag 为
//    声明序下标，数值一律小端；
//  - 解码严格：未知 tag / 重复 tag / 类型不符 / 截断 / count 超界 /
//    尾随字节（trailing garbage）→ ProtocolError，不进 handler；
//  - 缺失字段保留类型内默认值（向后兼容加字段）；
//  - 支持字段类型：bool、定宽整型、float/double、std::string、
//    std::vector<std::uint8_t>，以及已声明的子消息（嵌套）；
//  - schema/版本元数据即 SchemaId() 字符串，方法表按它做 schema 校验。
// 宏只是「字段名 → 成员指针」的薄糖衣，不含逻辑；不用宏也可手写
// MessageTraits 特化获得同等行为。

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <bbt/framework/Result.hpp>

namespace bbt::framework {

// 消息元数据 traits：主模板默认「未声明」，BBT_MESSAGE_FIELDS 给出特化。
// 特化必须提供：
//   static constexpr bool kDefined = true;
//   static constexpr std::string_view SchemaId() noexcept;
//   static constexpr auto Fields();  // std::tuple<FieldDef<Owner, M>...>
template <class T>
struct MessageTraits {
    static constexpr bool kDefined = false;
};

template <class T>
inline constexpr bool has_message_traits_v = MessageTraits<T>::kDefined;

namespace msg {

// 字段类型码（线格式 type 字节）。Message = 嵌套消息（长度前缀块内
// 递归使用同一格式）。
enum class WireType : std::uint8_t {
    Bool    = 1,
    Int     = 2,    // 有符号定宽整型，len = sizeof(M) ∈ {1,2,4,8}，小端
    Uint    = 3,    // 无符号定宽整型，len = sizeof(M)，小端
    Float   = 4,    // len = sizeof(M) ∈ {4,8}，IEEE754 位型小端
    String  = 5,
    Bytes   = 6,
    Message = 7,
};

// 字段描述：name 仅用于可读性/调试，线格式只认声明序下标 tag。
template <class Owner, class M>
struct FieldDef {
    const char* name;
    M Owner::*  member;
};

template <class Owner, class M>
constexpr FieldDef<Owner, M> Field(const char* name, M Owner::* member) {
    return FieldDef<Owner, M>{name, member};
}

// ---- 字段类型 → WireType 映射（编译期） ----

template <class M, class = void>
struct WireTypeOf {
    static constexpr WireType value = WireType::Message;
};

template <class M>
inline constexpr WireType wire_type_v = WireTypeOf<M>::value;

template <>
struct WireTypeOf<bool> {
    static constexpr WireType value = WireType::Bool;
};
template <class M>
struct WireTypeOf<M, std::enable_if_t<
        std::is_integral_v<M> && std::is_signed_v<M> &&
        !std::is_same_v<M, bool>>> {
    static constexpr WireType value = WireType::Int;
};
template <class M>
struct WireTypeOf<M, std::enable_if_t<
        std::is_integral_v<M> && std::is_unsigned_v<M> &&
        !std::is_same_v<M, bool>>> {
    static constexpr WireType value = WireType::Uint;
};
template <class M>
struct WireTypeOf<M, std::enable_if_t<std::is_floating_point_v<M>>> {
    static constexpr WireType value = WireType::Float;
};
template <>
struct WireTypeOf<std::string> {
    static constexpr WireType value = WireType::String;
};
template <>
struct WireTypeOf<std::vector<std::uint8_t>> {
    static constexpr WireType value = WireType::Bytes;
};

// 不支持的字段类型在编码路径 static_assert 暴露（编译期错误）。

template <class M>
inline constexpr bool wire_type_supported_v =
    wire_type_v<M> != WireType::Message || MessageTraits<M>::kDefined;

// ---- 小端写字 ----

inline void WriteU16(std::vector<std::uint8_t>& out, std::uint16_t v) {
    out.push_back(static_cast<std::uint8_t>(v));
    out.push_back(static_cast<std::uint8_t>(v >> 8));
}
inline void WriteU32(std::vector<std::uint8_t>& out, std::uint32_t v) {
    for (int i = 0; i < 4; ++i)
        out.push_back(static_cast<std::uint8_t>(v >> (8 * i)));
}

// ---- 读游标：所有越界访问在读取前检查，不依赖输入可信 ----

struct Reader {
    const std::uint8_t* p   = nullptr;
    std::size_t         len = 0;
    std::size_t         pos = 0;

    bool Take(std::size_t n, const std::uint8_t*& dst) {
        if (len - pos < n) return false;
        dst = p + pos;
        pos += n;
        return true;
    }
    bool U8(std::uint8_t& v) {
        const std::uint8_t* d;
        if (!Take(1, d)) return false;
        v = *d;
        return true;
    }
    bool U16(std::uint16_t& v) {
        const std::uint8_t* d;
        if (!Take(2, d)) return false;
        v = static_cast<std::uint16_t>(d[0]) |
            (static_cast<std::uint16_t>(d[1]) << 8);
        return true;
    }
    bool U32(std::uint32_t& v) {
        const std::uint8_t* d;
        if (!Take(4, d)) return false;
        v = static_cast<std::uint32_t>(d[0]) |
            (static_cast<std::uint32_t>(d[1]) << 8) |
            (static_cast<std::uint32_t>(d[2]) << 16) |
            (static_cast<std::uint32_t>(d[3]) << 24);
        return true;
    }
    bool U64(std::uint64_t& v) {
        const std::uint8_t* d;
        if (!Take(8, d)) return false;
        v = 0;
        for (int i = 0; i < 8; ++i)
            v |= static_cast<std::uint64_t>(d[i]) << (8 * i);
        return true;
    }
};

inline Error Malformed(const char* what) {
    return MakeError(ErrorCode::ProtocolError,
                     std::string("message decode: ") + what);
}

// ---- 逐字段编码/解码 ----

template <class T>
result<std::vector<std::uint8_t>> EncodeMessage(const T& obj);
template <class T>
result<T> DecodeMessage(const std::uint8_t* data, std::size_t len);
template <class T>
result<T> DecodeMessage(const std::vector<std::uint8_t>& bytes) {
    return DecodeMessage<T>(bytes.data(), bytes.size());
}

// 标量字段体编码（不含 tag/type/len 头）。Message 由外层递归处理。
template <class M>
void EncodeScalar(const M& v, std::vector<std::uint8_t>& out) {
    constexpr WireType wt = wire_type_v<M>;
    static_assert(wt != WireType::Message,
        "EncodeScalar: Message 字段走递归编码");
    if constexpr (wt == WireType::Bool) {
        out.push_back(v ? 1 : 0);
    } else if constexpr (wt == WireType::Int) {
        // 符号扩展到 64 位后按字段实际宽度取低 len 字节（小端）。
        const std::uint64_t u =
            static_cast<std::uint64_t>(static_cast<std::int64_t>(v));
        for (std::size_t i = 0; i < sizeof(M); ++i)
            out.push_back(static_cast<std::uint8_t>(u >> (8 * i)));
    } else if constexpr (wt == WireType::Uint) {
        const std::uint64_t u = static_cast<std::uint64_t>(v);
        for (std::size_t i = 0; i < sizeof(M); ++i)
            out.push_back(static_cast<std::uint8_t>(u >> (8 * i)));
    } else if constexpr (wt == WireType::Float) {
        // 位型搬运针对单字段（IEEE754 宽度显式），不是对象布局拷贝。
        if constexpr (sizeof(M) == 4) {
            std::uint32_t u;
            std::memcpy(&u, &v, 4);
            WriteU32(out, u);
        } else {
            std::uint64_t u;
            std::memcpy(&u, &v, 8);
            for (int i = 0; i < 8; ++i)
                out.push_back(static_cast<std::uint8_t>(u >> (8 * i)));
        }
    } else if constexpr (wt == WireType::String ||
                         wt == WireType::Bytes) {
        out.insert(out.end(), v.begin(), v.end());
    }
}

// 标量字段体解码；false = 类型/长度/截断不符。
template <class M>
bool DecodeScalar(Reader& r, std::uint32_t len, M& dst) {
    constexpr WireType wt = wire_type_v<M>;
    static_assert(wt != WireType::Message,
        "DecodeScalar: Message 字段走递归解码");
    if constexpr (wt == WireType::Bool) {
        if (len != 1) return false;
        std::uint8_t b;
        if (!r.U8(b)) return false;
        if (b > 1) return false;    // 线上 bool 只接受 0/1
        dst = (b != 0);
    } else if constexpr (wt == WireType::Int ||
                         wt == WireType::Uint) {
        if (len != sizeof(M)) return false;
        const std::uint8_t* d;
        if (!r.Take(len, d)) return false;
        std::uint64_t u = 0;
        for (std::size_t i = 0; i < len; ++i)
            u |= static_cast<std::uint64_t>(d[i]) << (8 * i);
        if constexpr (wt == WireType::Int) {
            // 字段宽度符号扩展（len < 8 时最高位是符号位）。
            if (len < 8 && (u & (std::uint64_t{1} << (len * 8 - 1))))
                u |= ~std::uint64_t{0} << (len * 8);
            dst = static_cast<M>(static_cast<std::int64_t>(u));
        } else {
            dst = static_cast<M>(u);
        }
    } else if constexpr (wt == WireType::Float) {
        if (len != sizeof(M)) return false;
        if constexpr (sizeof(M) == 4) {
            std::uint32_t u;
            if (!r.U32(u)) return false;
            std::memcpy(&dst, &u, 4);
        } else {
            std::uint64_t u;
            if (!r.U64(u)) return false;
            std::memcpy(&dst, &u, 8);
        }
    } else if constexpr (wt == WireType::String ||
                         wt == WireType::Bytes) {
        const std::uint8_t* d;
        if (!r.Take(len, d)) return false;
        dst.assign(d, d + len);
    }
    return true;
}

template <class T, class Tuple, std::size_t... I>
result<void> EncodeFields(const T& obj, const Tuple& fields,
                          std::index_sequence<I...>,
                          std::vector<std::uint8_t>& out) {
    result<void> res = result<void>::ok();
    (
        [&] {
            if (!res) return;
            const auto& f = std::get<I>(fields);
            const auto& v = obj.*(f.member);
            using M = std::decay_t<decltype(v)>;
            static_assert(wire_type_supported_v<M>,
                "BBT_MESSAGE_FIELDS: 不支持的字段类型（允许 bool/定宽整型/"
                "浮点/std::string/std::vector<uint8_t>/已声明消息）");
            WriteU16(out, static_cast<std::uint16_t>(I));
            out.push_back(static_cast<std::uint8_t>(wire_type_v<M>));
            const auto len_at = out.size();
            WriteU32(out, 0);                 // len 占位，写体后回填
            if constexpr (wire_type_v<M> == WireType::Message) {
                auto inner = EncodeMessage<M>(v);
                if (!inner) {
                    res = result<void>::err(std::move(inner.error()));
                    return;
                }
                out.insert(out.end(), inner.value().begin(),
                           inner.value().end());
            } else {
                EncodeScalar(v, out);
            }
            const std::uint32_t flen = static_cast<std::uint32_t>(
                out.size() - len_at - 4);
            out[len_at]     = static_cast<std::uint8_t>(flen);
            out[len_at + 1] = static_cast<std::uint8_t>(flen >> 8);
            out[len_at + 2] = static_cast<std::uint8_t>(flen >> 16);
            out[len_at + 3] = static_cast<std::uint8_t>(flen >> 24);
        }(),
        ...);
    return res;
}

template <class T>
result<std::vector<std::uint8_t>> EncodeMessage(const T& obj) {
    static_assert(MessageTraits<T>::kDefined,
        "EncodeMessage<T>: T 缺少 MessageTraits（用 BBT_MESSAGE_FIELDS 声明）");
    const auto fields = MessageTraits<T>::Fields();
    constexpr std::size_t kCount =
        std::tuple_size_v<std::decay_t<decltype(fields)>>;
    static_assert(kCount > 0 && kCount <= 0xFFFF,
        "EncodeMessage<T>: 字段数须在 1..65535");

    std::vector<std::uint8_t> out;
    WriteU32(out, static_cast<std::uint32_t>(kCount));
    auto er = EncodeFields(obj, fields,
                           std::make_index_sequence<kCount>{}, out);
    if (!er)
        return result<std::vector<std::uint8_t>>::err(
            std::move(er.error()));
    return result<std::vector<std::uint8_t>>::ok(std::move(out));
}

// 单个字段解码：tag 经编译期下标逐个匹配。
template <class T, class Tuple, std::size_t... I>
result<void> DecodeFieldByTag(std::uint16_t tag, std::uint8_t type,
                              std::uint32_t flen, Reader& r, T& obj,
                              const Tuple& fields,
                              std::index_sequence<I...>) {
    result<void> out = result<void>::err(Malformed("unknown field tag"));
    bool found = false;
    (
        [&] {
            if (found || tag != static_cast<std::uint16_t>(I)) return;
            found = true;
            auto member = std::get<I>(fields).member;
            using M = std::decay_t<decltype(obj.*member)>;
            if (type != static_cast<std::uint8_t>(wire_type_v<M>)) {
                out = result<void>::err(Malformed("field type mismatch"));
                return;
            }
            if constexpr (wire_type_v<M> == WireType::Message) {
                const std::uint8_t* d;
                if (!r.Take(flen, d)) {
                    out = result<void>::err(Malformed("truncated field"));
                    return;
                }
                auto inner = DecodeMessage<M>(d, flen);
                if (!inner) {
                    out = result<void>::err(std::move(inner.error()));
                    return;
                }
                obj.*member = std::move(inner.value());
            } else {
                M tmp{};
                if (!DecodeScalar(r, flen, tmp)) {
                    out = result<void>::err(
                        Malformed("field payload malformed"));
                    return;
                }
                obj.*member = std::move(tmp);
            }
            out = result<void>::ok();
        }(),
        ...);
    return out;
}

template <class T>
result<T> DecodeMessage(const std::uint8_t* data, std::size_t len) {
    static_assert(MessageTraits<T>::kDefined,
        "DecodeMessage<T>: T 缺少 MessageTraits（用 BBT_MESSAGE_FIELDS 声明）");
    static_assert(std::is_default_constructible_v<T>,
        "DecodeMessage<T>: 消息类型须默认可构造");
    const auto fields = MessageTraits<T>::Fields();
    constexpr std::size_t kCount =
        std::tuple_size_v<std::decay_t<decltype(fields)>>;

    Reader r{data, len, 0};
    std::uint32_t count = 0;
    if (!r.U32(count))
        return result<T>::err(Malformed("header truncated"));
    if (count > kCount)
        return result<T>::err(Malformed("field count exceeds schema"));

    T obj{};
    std::vector<char> seen(kCount, 0);
    for (std::uint32_t i = 0; i < count; ++i) {
        std::uint16_t tag = 0;
        std::uint8_t  type = 0;
        std::uint32_t flen = 0;
        if (!r.U16(tag) || !r.U8(type) || !r.U32(flen))
            return result<T>::err(Malformed("field header truncated"));
        if (tag >= kCount)
            return result<T>::err(Malformed("unknown field tag"));
        if (seen[tag])
            return result<T>::err(Malformed("duplicate field tag"));
        seen[tag] = 1;
        auto dr = DecodeFieldByTag(tag, type, flen, r, obj, fields,
                                   std::make_index_sequence<kCount>{});
        if (!dr)
            return result<T>::err(std::move(dr.error()));
    }
    // 尾随字节一律拒绝：声明字段之后不允许存在未消费数据。
    if (r.pos != r.len)
        return result<T>::err(Malformed("trailing garbage"));
    return result<T>::ok(std::move(obj));
}

} // namespace msg

// ---- 框架通用 codec：与 bbt::infra::Codec<T> 同签名；方法表与出站 ----
// 调用按 has_message_traits_v 自动选用，业务无需任何 codec 代码。

template <class T>
class MessageCodec {
public:
    static std::string_view SchemaId() noexcept {
        return MessageTraits<T>::SchemaId();
    }
    static result<std::vector<std::uint8_t>> Encode(const T& obj) {
        return msg::EncodeMessage(obj);
    }
    static result<T> Decode(const std::vector<std::uint8_t>& payload) {
        return msg::DecodeMessage<T>(payload);
    }
};

} // namespace bbt::framework

// ---- 字段列表宏（薄糖衣）：产生 MessageTraits<T> 特化 ----
// 用法（命名空间作用域、类型声明之后）：
//   BBT_MESSAGE_FIELDS(EchoReq, "test.EchoReq/v1", v, text)
// 最多 16 个字段；字段名展开为 msg::Field("v", &EchoReq::v)。

#define BBT_MSG_DETAIL_CAT_(a, b) a##b
#define BBT_MSG_DETAIL_CAT(a, b) BBT_MSG_DETAIL_CAT_(a, b)
#define BBT_MSG_DETAIL_EXPAND(...) __VA_ARGS__

#define BBT_MSG_DETAIL_NARG_( \
    _1,_2,_3,_4,_5,_6,_7,_8,_9,_10,_11,_12,_13,_14,_15,_16,N,...) N
#define BBT_MSG_DETAIL_NARG(...) \
    BBT_MSG_DETAIL_EXPAND(BBT_MSG_DETAIL_NARG_(__VA_ARGS__, \
        16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1))

#define BBT_MSG_DETAIL_FIELD(TYPE, NAME) \
    ::bbt::framework::msg::Field(#NAME, &TYPE::NAME)

#define BBT_MSG_DETAIL_FE_1(T,X,...)  BBT_MSG_DETAIL_FIELD(T,X)
#define BBT_MSG_DETAIL_FE_2(T,X,...)  BBT_MSG_DETAIL_FIELD(T,X), \
    BBT_MSG_DETAIL_EXPAND(BBT_MSG_DETAIL_FE_1(T,__VA_ARGS__))
#define BBT_MSG_DETAIL_FE_3(T,X,...)  BBT_MSG_DETAIL_FIELD(T,X), \
    BBT_MSG_DETAIL_EXPAND(BBT_MSG_DETAIL_FE_2(T,__VA_ARGS__))
#define BBT_MSG_DETAIL_FE_4(T,X,...)  BBT_MSG_DETAIL_FIELD(T,X), \
    BBT_MSG_DETAIL_EXPAND(BBT_MSG_DETAIL_FE_3(T,__VA_ARGS__))
#define BBT_MSG_DETAIL_FE_5(T,X,...)  BBT_MSG_DETAIL_FIELD(T,X), \
    BBT_MSG_DETAIL_EXPAND(BBT_MSG_DETAIL_FE_4(T,__VA_ARGS__))
#define BBT_MSG_DETAIL_FE_6(T,X,...)  BBT_MSG_DETAIL_FIELD(T,X), \
    BBT_MSG_DETAIL_EXPAND(BBT_MSG_DETAIL_FE_5(T,__VA_ARGS__))
#define BBT_MSG_DETAIL_FE_7(T,X,...)  BBT_MSG_DETAIL_FIELD(T,X), \
    BBT_MSG_DETAIL_EXPAND(BBT_MSG_DETAIL_FE_6(T,__VA_ARGS__))
#define BBT_MSG_DETAIL_FE_8(T,X,...)  BBT_MSG_DETAIL_FIELD(T,X), \
    BBT_MSG_DETAIL_EXPAND(BBT_MSG_DETAIL_FE_7(T,__VA_ARGS__))
#define BBT_MSG_DETAIL_FE_9(T,X,...)  BBT_MSG_DETAIL_FIELD(T,X), \
    BBT_MSG_DETAIL_EXPAND(BBT_MSG_DETAIL_FE_8(T,__VA_ARGS__))
#define BBT_MSG_DETAIL_FE_10(T,X,...) BBT_MSG_DETAIL_FIELD(T,X), \
    BBT_MSG_DETAIL_EXPAND(BBT_MSG_DETAIL_FE_9(T,__VA_ARGS__))
#define BBT_MSG_DETAIL_FE_11(T,X,...) BBT_MSG_DETAIL_FIELD(T,X), \
    BBT_MSG_DETAIL_EXPAND(BBT_MSG_DETAIL_FE_10(T,__VA_ARGS__))
#define BBT_MSG_DETAIL_FE_12(T,X,...) BBT_MSG_DETAIL_FIELD(T,X), \
    BBT_MSG_DETAIL_EXPAND(BBT_MSG_DETAIL_FE_11(T,__VA_ARGS__))
#define BBT_MSG_DETAIL_FE_13(T,X,...) BBT_MSG_DETAIL_FIELD(T,X), \
    BBT_MSG_DETAIL_EXPAND(BBT_MSG_DETAIL_FE_12(T,__VA_ARGS__))
#define BBT_MSG_DETAIL_FE_14(T,X,...) BBT_MSG_DETAIL_FIELD(T,X), \
    BBT_MSG_DETAIL_EXPAND(BBT_MSG_DETAIL_FE_13(T,__VA_ARGS__))
#define BBT_MSG_DETAIL_FE_15(T,X,...) BBT_MSG_DETAIL_FIELD(T,X), \
    BBT_MSG_DETAIL_EXPAND(BBT_MSG_DETAIL_FE_14(T,__VA_ARGS__))
#define BBT_MSG_DETAIL_FE_16(T,X,...) BBT_MSG_DETAIL_FIELD(T,X), \
    BBT_MSG_DETAIL_EXPAND(BBT_MSG_DETAIL_FE_15(T,__VA_ARGS__))

#define BBT_MSG_DETAIL_FOREACH(T, ...) \
    BBT_MSG_DETAIL_EXPAND(BBT_MSG_DETAIL_CAT(BBT_MSG_DETAIL_FE_, \
        BBT_MSG_DETAIL_NARG(__VA_ARGS__))(T, __VA_ARGS__))

#define BBT_MESSAGE_FIELDS(TYPE, SCHEMA_ID, ...)                          \
    template <>                                                           \
    struct bbt::framework::MessageTraits<TYPE> {                          \
        static constexpr bool kDefined = true;                            \
        static constexpr std::string_view SchemaId() noexcept {           \
            return SCHEMA_ID;                                             \
        }                                                                 \
        static constexpr auto Fields() {                                  \
            return std::make_tuple(                                       \
                BBT_MSG_DETAIL_FOREACH(TYPE, __VA_ARGS__));               \
        }                                                                 \
    };
