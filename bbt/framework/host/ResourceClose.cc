#include <bbt/framework/internal/ResourceClose.hpp>

namespace bbt::framework::internal {

void CloseResource(
    const std::shared_ptr<bbt::infra::ICoCloseable>& closeable) noexcept {
    if (closeable)
        closeable->Close();
}

} // namespace bbt::framework::internal
