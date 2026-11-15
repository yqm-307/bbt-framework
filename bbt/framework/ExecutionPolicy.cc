#include <bbt/framework/ExecutionPolicy.hpp>

#include <string>

namespace bbt::framework {

namespace {

result<void> FailField(const char* field, const char* why) {
    return result<void>::err(MakeError(ErrorCode::InvalidArgument,
        "ServiceOptions." + std::string(field) + ": " + why));
}

} // namespace

result<void> ValidateServiceOptions(const ServiceOptions& options) {
    if (options.max_inflight == 0)
        return FailField("max_inflight", "must be > 0");

    switch (options.execution) {
    case ExecutionPolicy::ActorSerial:
        if (options.mailbox_capacity == 0)
            return FailField("mailbox_capacity",
                             "ActorSerial requires mailbox_capacity > 0");
        if (options.max_actors == 0)
            return FailField("max_actors",
                             "ActorSerial requires max_actors > 0");
        break;
    case ExecutionPolicy::Concurrent:
        if (options.mailbox_capacity != 0)
            return FailField("mailbox_capacity",
                             "Concurrent requires mailbox_capacity == 0");
        if (options.max_actors != 0)
            return FailField("max_actors",
                             "Concurrent requires max_actors == 0");
        if (options.ordered_ingress)
            return FailField("ordered_ingress",
                             "ordered_ingress is only allowed for ActorSerial");
        break;
    }

    if (options.ordered_ingress) {
        if (options.max_ordered_streams == 0)
            return FailField("max_ordered_streams",
                             "ordered_ingress requires > 0");
        if (options.max_cached_results == 0)
            return FailField("max_cached_results",
                             "ordered_ingress requires > 0");
        if (options.max_cached_result_bytes == 0)
            return FailField("max_cached_result_bytes",
                             "ordered_ingress requires > 0");
    } else {
        if (options.max_ordered_streams != 0)
            return FailField("max_ordered_streams",
                             "must be 0 when ordered_ingress is disabled");
        if (options.max_cached_results != 0)
            return FailField("max_cached_results",
                             "must be 0 when ordered_ingress is disabled");
        if (options.max_cached_result_bytes != 0)
            return FailField("max_cached_result_bytes",
                             "must be 0 when ordered_ingress is disabled");
    }
    return result<void>::ok();
}

} // namespace bbt::framework
