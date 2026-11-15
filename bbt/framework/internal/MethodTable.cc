#include <bbt/framework/internal/MethodTable.hpp>

namespace bbt::framework {

result<void> ValidateActorKeying(const RpcMethodTable& table,
                                 ExecutionPolicy execution) {
    if (execution != ExecutionPolicy::ActorSerial)
        return result<void>::ok();
    for (const auto& m : table.methods()) {
        if (!m.actor_keyed)
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                "ActorSerial method '" + m.name +
                    "' has no key extractor; declare via fw::ActorMethod"));
    }
    return result<void>::ok();
}

} // namespace bbt::framework
