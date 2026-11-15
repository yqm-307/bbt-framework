#pragma once
// framework v2 唯一地址/消息值层（契约 §5.1 最小落地）。
//
// AppObjectAddress 是可公开的目标地址值；MailBox 是它的可复制包装
// （地址值，不是队列或 Actor 引用）。本地发送路径本切片不接业务消息
// 分发——CoCall/CoTell 由 Service/Actor 受管入口在后续切片接入网络；
// 本切片只保证地址语义、校验与 Service/Actor handle 失效观察。

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <bbt/framework/Result.hpp>

namespace bbt::framework {

/**
 * @brief 消息载荷（owning 字节容器）。
 *
 * 本切片的最小形态——位置参数/proto 编解码由既有 CoRpc.hpp/rpc_detail 承载
 * （From/FromProto/Parse* 在需要 typed 调用时直接使用）；这里只提供原始字节
 * 构造/访问，不暴露 transport owner。
 */
class Payload {
public:
    Payload() = default;
    /// @brief 以字节向量构造（移动语义）。
    explicit Payload(std::vector<std::uint8_t> bytes)
        : m_bytes(std::move(bytes)) {}
    /// @brief 以字符串内容构造（拷贝为字节）。
    explicit Payload(std::string_view sv)
        : m_bytes(sv.begin(), sv.end()) {}

    /// @brief 载荷字节。
    const std::vector<std::uint8_t>& Bytes() const noexcept { return m_bytes; }
    /// @brief 载荷字节数。
    std::size_t Size() const noexcept { return m_bytes.size(); }
    /// @brief 载荷是否为空。
    bool        Empty() const noexcept { return m_bytes.empty(); }

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
    /// @brief 目标类别。
    enum class Kind { Service, Actor };

    /**
     * @brief 构造 Service 地址。
     * @param name 服务名（Validate 要求非空）。
     * @param app 目标 app 逻辑 id；空 = 本 app。
     */
    static AppObjectAddress Service(std::string name, std::string app = {}) {
        AppObjectAddress a;
        a.m_kind = Kind::Service;
        a.m_type = std::move(name);
        a.m_app  = std::move(app);
        return a;
    }
    /**
     * @brief 构造 Actor 地址。
     * @param type Actor 类型（Validate 要求非空）。
     * @param key Actor 实例 key（Validate 要求非空；空 key 仅 CoCreateActor
     *        生成侧允许，地址本身不合法）。
     * @param app 目标 app 逻辑 id；空 = 本 app。
     */
    static AppObjectAddress Actor(std::string type, std::string key,
                                  std::string app = {}) {
        AppObjectAddress a;
        a.m_kind = Kind::Actor;
        a.m_type = std::move(type);
        a.m_key  = std::move(key);
        a.m_app  = std::move(app);
        return a;
    }

    /// @brief 目标类别。
    Kind ObjectKind() const noexcept { return m_kind; }
    /// @brief Service：服务名；Actor：actor type。
    const std::string& Type() const noexcept { return m_type; }
    /// @brief Actor key；Service 恒为空串。
    const std::string& Key() const noexcept { return m_key; }
    /// @brief 目标 app 逻辑 id；空 = 本 app。
    const std::string& App() const noexcept { return m_app; }

    /**
     * @brief 装配期校验：Service 名非空；Actor type/key 均非空。
     *
     * 仅字符集/长度检查，不做路由可达性判断。
     * @return 合法返回 ok；缺字段返回 InvalidArgument。
     */
    result<void> Validate() const;

private:
    Kind        m_kind = Kind::Service;
    std::string m_type;
    std::string m_key;
    std::string m_app;
};

/**
 * @brief 可复制地址值包装。
 *
 * 不是队列、不是 Actor 引用、不授予解引用权。
 */
class MailBox {
public:
    MailBox() = default;
    /// @brief 以地址值构造。
    explicit MailBox(AppObjectAddress address)
        : m_address(std::move(address)) {}

    /// @brief 地址值。
    const AppObjectAddress& Address() const noexcept { return m_address; }

private:
    AppObjectAddress m_address;
};

/// @brief 消息流量类别。
enum class AppTrafficClass : std::uint8_t {
    System = 0,  ///< infra 生成的控制帧。
    Service = 1, ///< Service 请求流量。
    Actor = 2    ///< Actor 消息流量。
};
/// @brief 回复要求。
enum class ReplyMode : std::uint8_t {
    None = 0,     ///< Tell（无回复）。
    Required = 1  ///< Call（要求回复）。
};

/// @brief 应用层消息值（owning）。
struct AppMessage {
    AppObjectAddress source;      ///< 源地址。
    AppObjectAddress target;      ///< 目标地址。
    std::string      method;      ///< 显式方法名。
    std::string      request_id;  ///< 请求关联 id。
    std::string      message_id;  ///< 消息 id。
    ReplyMode        reply = ReplyMode::None; ///< 回复要求。
    std::uint64_t    sequence = 0;///< 提交序号。
    Payload          payload;     ///< 载荷。
};

} // namespace bbt::framework
