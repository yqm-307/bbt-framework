#include <bbt/framework/ExecutionPolicy.hpp>
#include <bbt/framework/Object.hpp>

#include <string>

namespace bbt::framework {

namespace {

result<void> FailField(const char* field, const char* why) {
    return result<void>::err(MakeError(ErrorCode::InvalidArgument,
        "ServiceOptions." + std::string(field) + ": " + why));
}

} // namespace

result<void> ValidateServiceOptions(const ServiceOptions& options) {
    if (options.MaxInflight() == 0)
        return FailField("max_inflight", "must be > 0");

    switch (options.Execution()) {
    case ExecutionPolicy::ActorSerial:
        if (options.MailboxCapacity() == 0)
            return FailField("mailbox_capacity",
                             "ActorSerial requires mailbox_capacity > 0");
        if (options.MaxActors() == 0)
            return FailField("max_actors",
                             "ActorSerial requires max_actors > 0");
        break;
    case ExecutionPolicy::Concurrent:
        if (options.SerialQueueCapacity() != 0)
            return FailField("mailbox_capacity",
                             "Concurrent requires mailbox_capacity == 0");
        if (options.MaxSerialKeys() != 0)
            return FailField("max_actors",
                             "Concurrent requires max_actors == 0");
        if (options.OrderedIngress())
            return FailField("ordered_ingress",
                             "ordered_ingress is only allowed for ActorSerial");
        break;
    }

    if (options.OrderedIngress()) {
        if (options.MaxOrderedStreams() == 0)
            return FailField("max_ordered_streams",
                             "ordered_ingress requires > 0");
        if (options.MaxCachedResults() == 0)
            return FailField("max_cached_results",
                             "ordered_ingress requires > 0");
        if (options.MaxCachedResultBytes() == 0)
            return FailField("max_cached_result_bytes",
                             "ordered_ingress requires > 0");
    } else {
        if (options.MaxOrderedStreams() != 0)
            return FailField("max_ordered_streams",
                             "must be 0 when ordered_ingress is disabled");
        if (options.MaxCachedResults() != 0)
            return FailField("max_cached_results",
                             "must be 0 when ordered_ingress is disabled");
        if (options.MaxCachedResultBytes() != 0)
            return FailField("max_cached_result_bytes",
                             "must be 0 when ordered_ingress is disabled");
    }
    return result<void>::ok();
}

} // namespace bbt::framework
