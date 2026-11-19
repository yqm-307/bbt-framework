#pragma once
/**
 * @brief 服务和 Actor 的地址、邮箱地址及消息内容。
 * MailBox 可以复制，但只是地址：它不持有 Actor，也不保证消息能送达。
 */

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <bbt/framework/Result.hpp>

namespace bbt::framework {

/**
 * @brief 拥有消息的原始字节。按类型编解码请使用 CoRpcReq 或 CoRpcResp。
 */
class Payload {
public:
    Payload() = default;
    /**
     * @brief 以字节向量构造（移动语义）。
     */
    explicit Payload(std::vector<std::uint8_t> bytes)
        : m_bytes(std::move(bytes)) {}
    /**
     * @brief 以字符串内容构造（拷贝为字节）。
     */
    explicit Payload(std::string_view sv)
        : m_bytes(sv.begin(), sv.end()) {}

    /**
     * @brief 载荷字节。
     */
    const std::vector<std::uint8_t>& Bytes() const noexcept { return m_bytes; }
    /**
     * @brief 载荷字节数。
     */
    std::size_t Size() const noexcept { return m_bytes.size(); }
    /**
     * @brief 载荷是否为空。
     */
    bool Empty() const noexcept { return m_bytes.empty(); }

private:
    std::vector<std::uint8_t> m_bytes;
};

/**
 * @brief 可公开的目标地址值。
 *
 * @par 职责
 * 表达 Service（name）或 Actor（type+key）目标及可选目标 app 逻辑 id。
 *
 * @par 约束
 * 值语义可复制；只是地址，不授予解引用权、不保证路由可达。
 */
class AppObjectAddress {
public:
    /**
     * @brief 目标类别。
     */
    enum class Kind {
        Service,
        Actor
    };

    /**
     * @brief 构造 Service 地址。
     * @param name 服务名（Validate 要求非空）。
     * @param app 目标 app 逻辑 id；空 = 本 app。
     */
    static AppObjectAddress Service(std::string name, std::string app = {}) {
        AppObjectAddress a;
        a.m_kind = Kind::Service;
        a.m_type = std::move(name);
        a.m_app = std::move(app);
        return a;
    }
    /**
     * @brief 构造 Actor 地址。
     * @param type 非空的 Actor 类型名。
     * @param key 非空的实例标识。
     * @param app 目标应用标识；为空表示本应用。
     */
    static AppObjectAddress Actor(std::string type, std::string key,
                                  std::string app = {}) {
        AppObjectAddress a;
        a.m_kind = Kind::Actor;
        a.m_type = std::move(type);
        a.m_key = std::move(key);
        a.m_app = std::move(app);
        return a;
    }

    /**
     * @brief 目标类别。
     */
    Kind ObjectKind() const noexcept { return m_kind; }
    /**
     * @brief Service：服务名；Actor：actor type。
     */
    const std::string& Type() const noexcept { return m_type; }
    /**
     * @brief Actor key；Service 恒为空串。
     */
    const std::string& Key() const noexcept { return m_key; }
    /**
     * @brief 目标 app 逻辑 id；空 = 本 app。
     */
    const std::string& App() const noexcept { return m_app; }

    /**
     * @brief 检查服务名或 Actor 的类型和 key 是否为空，不检查目标是否可达。
     * @return 必填字段为空返回 InvalidArgument，否则返回 ok。
     */
    result<void> Validate() const;

private:
    Kind m_kind = Kind::Service;
    std::string m_type;
    std::string m_key;
    std::string m_app;
};

/**
 * @brief 可复制的邮箱地址，不是消息队列或 Actor 引用。
 */
class MailBox {
public:
    MailBox() = default;
    /**
     * @brief 以地址值构造。
     */
    explicit MailBox(AppObjectAddress address)
        : m_address(std::move(address)) {}

    /**
     * @brief 地址值。
     */
    const AppObjectAddress& Address() const noexcept { return m_address; }

private:
    AppObjectAddress m_address;
};

/**
 * @brief 消息流量类别。
 */
enum class AppTrafficClass : std::uint8_t {
    /**
     * infra 生成的控制帧。
     */
    System = 0,
    /**
     * Service 请求流量。
     */
    Service = 1,
    /**
     * Actor 消息流量。
     */
    Actor = 2
};
/**
 * @brief 回复要求。
 */
enum class ReplyMode : std::uint8_t {
    /**
     * Tell（无回复）。
     */
    None = 0,
    /**
     * Call（要求回复）。
     */
    Required = 1
};

/**
 * @brief 应用层消息值（owning）。
 */
struct AppMessage {
    AppObjectAddress source;
    AppObjectAddress target;
    std::string method;
    std::string request_id;
    std::string message_id;
    ReplyMode reply = ReplyMode::None;
    std::uint64_t sequence = 0;
    Payload payload;
};

} // namespace bbt::framework
