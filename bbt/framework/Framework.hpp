#pragma once
/**
 * 业务面公共头汇总（受管对象/宿主/RPC 声明与值层）。
 * 单独包含任一子头亦可自给编译；本头只做聚合包含，不用聚合入口掩盖
 * 单头不能独立包含的问题。机器面头文件在 internal/ 下
 * （分发器、线桥、测试缝等）。
 */

#include <bbt/framework/Result.hpp>
#include <bbt/framework/Address.hpp>
#include <bbt/framework/Object.hpp>
#include <bbt/framework/Component.hpp>
#include <bbt/framework/Actor.hpp>
#include <bbt/framework/CoService.hpp>
#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/Route.hpp>
#include <bbt/framework/CallOptions.hpp>
#include <bbt/framework/OrderedTypes.hpp>
#include <bbt/framework/OrderedSession.hpp>
#include <bbt/framework/RequestContext.hpp>
#include <bbt/framework/ExecutionPolicy.hpp>
#include <bbt/framework/RpcMethods.hpp>
#include <bbt/framework/RpcPolicy.hpp>
#include <bbt/framework/CoRpc.hpp>
#include <bbt/framework/DynamicRouteTable.hpp>
