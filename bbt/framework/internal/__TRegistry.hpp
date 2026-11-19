#pragma once

namespace bbt::framework::detail {

template <class Pred>
bool Registry::MutexCvWait(std::unique_lock<std::mutex>& lk,
                           bbt::coroutine::Deadline deadline, Pred&& pred) {
    return m_cv.wait_until(lk, deadline, std::forward<Pred>(pred));
}

template <class T>
T* Registry::NewManaged(ConstructKey k) {
    return new T(k);
}

template <class T, class Opt>
T* Registry::NewManaged(ConstructKey k, Opt&& options) {
    return new T(k, std::forward<Opt>(options));
}

template <class T>
void Registry::DeleteManaged(T* p) noexcept {
    delete p;
}

} // namespace bbt::framework::detail
