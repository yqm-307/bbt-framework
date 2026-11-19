#include <bbt/framework/internal/MethodTable.hpp>

namespace bbt::framework {

const RpcMethod* RpcMethodTable::find(std::string_view name) const noexcept {
    for (const auto& m : m_methods)
        if (m.name == name)
            return &m;
    return nullptr;
}

void RpcMethodTable::Add(RpcMethod m) {
    if (m.name.empty())
        throw std::invalid_argument("method table: empty method name");
    if (find(m.name) != nullptr)
        throw std::invalid_argument(
            "method table: duplicate method '" + m.name + "'");
    m_methods.push_back(std::move(m));
}

result<void> ValidateActorKeying(const RpcMethodTable& table,
                                 ExecutionPolicy execution) {
    if (execution != ExecutionPolicy::ActorSerial)
        return result<void>::ok();
    for (const auto& m : table.methods()) {
        if (m.extract_key == nullptr)
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                               "ActorSerial method '" + m.name +
                                                   "' has no key extractor; declare via fw::ActorMethod"));
    }
    return result<void>::ok();
}

} // namespace bbt::framework
