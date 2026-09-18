#pragma once
// co-service-actor/v1 F1-a：受管请求上下文的登记/取出（契约第 139 行）。
// this->co_rpc_call 从当前受管请求取得隐式上下文；非受管请求协程调用返回
// InvalidContext。启动代码要调用远端，必须由宿主显式建立受管请求上下文
// （构造 RequestScope）。
//
// 绑定键设计（不是 thread_local）：
//  - 协程内登记：键 = 协程 id（运行时下单调分配、存活期不重用）。协程
//    挂起后恢复即使换了 worker，协程 id 不变，上下文仍可取——缓存随
//    协程走，满足「禁止 thread_local 跨挂起缓存」。
//  - 非协程执行（宿主启动代码）：键 = 线程 id；无线程迁移问题。
// RequestScope 为 RAII：构造时压栈登记、析构时退出，允许嵌套（内层遮蔽
// 外层）。受管作用域必须在所属协程/逻辑请求结束前析构。

#include <cstdint>
#include <memory>
#include <thread>

#include <bbt/framework/RequestContext.hpp>
#include <bbt/framework/Result.hpp>

namespace bbt::framework {

class RequestScope {
public:
    // ctx 为 null 时不登记（空作用域）。
    explicit RequestScope(std::shared_ptr<const RequestContext> ctx);
    ~RequestScope();
    RequestScope(const RequestScope&) = delete;
    RequestScope& operator=(const RequestScope&) = delete;
private:
    std::shared_ptr<const RequestContext> ctx_;
    bool            co_bound_ = false;    // 登记键类型：协程 id or 线程 id
    std::uint64_t   co_id_ = 0;
    std::thread::id thread_id_;
};

} // namespace bbt::framework
