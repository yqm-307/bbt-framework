#pragma once

/**
 * @file V2.hpp
 * @brief v2 唯一类型面的聚合入口：Co-only 对象与装配根。
 *
 * v2 命名空间下是唯一的 Co 生命周期面（普通 main + bbtco 注册，宿主不启停
 * runtime）。本头只做聚合包含，具体契约在各类型自己的头里：
 *
 *   v2::CoApp        装配根（Create 唯一入口，走 coroutine 创建观测）
 *   v2::CoService<C…> 类型化服务基类（ServiceBase 是它的 internal 实现名）
 *   v2::CoComponent  能力组件基类
 *   v2::CoActor      Actor 基类
 *
 * 公共 Co-only 类型由 `bbt::framework::v2` 提供；本头只聚合这些类型，
 * 不另设别名或转发入口。
 */

#include <bbt/framework/Address.hpp>
#include <bbt/framework/App.hpp>
#include <bbt/framework/Object.hpp>
#include <bbt/framework/Service.hpp>
