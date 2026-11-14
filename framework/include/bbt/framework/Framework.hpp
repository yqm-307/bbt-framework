#pragma once
// v2 业务面公共头汇总（对象、组件、服务、RPC 与装配根）。
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
#include <bbt/framework/RpcPolicy.hpp>
#include <bbt/framework/CoRpc.hpp>
#include <bbt/framework/V2.hpp>
