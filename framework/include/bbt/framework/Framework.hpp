#pragma once
// service-actor/v2：业务面公共头汇总（消息/服务/RPC 声明）。
// 单独包含任一子头亦可自给编译。宿主 CoApp 请另含 CoApp.hpp——
// add_service 需展开方法表，不经本头传递 internal/。
// 机器面头文件在 internal/ 下（分发器、线桥、测试缝等）。

#include <bbt/framework/Result.hpp>
#include <bbt/framework/Route.hpp>
#include <bbt/framework/CallOptions.hpp>
#include <bbt/framework/OrderedTypes.hpp>
#include <bbt/framework/OrderedSession.hpp>
#include <bbt/framework/RequestContext.hpp>
#include <bbt/framework/ServiceContext.hpp>
#include <bbt/framework/ExecutionPolicy.hpp>
#include <bbt/framework/ShutdownState.hpp>
#include <bbt/framework/RpcMethods.hpp>
#include <bbt/framework/ICoService.hpp>
#include <bbt/framework/CoRpc.hpp>
#include <bbt/framework/CoService.hpp>
