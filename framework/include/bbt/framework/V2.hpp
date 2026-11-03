#pragma once
// Co-only 对象与装配入口；与成熟旧框架的同名 CoService/CoApp 隔离——
// v2 命名空间下是唯一 Co 生命周期面（普通 main + bbtco 注册，宿主不启停 runtime）。
//
// R3 唯一类型面：
//   v2::CoApp        —— 装配根（Create 唯一入口，coroutine 创建观测）
//   v2::CoService<C…>—— 类型化服务基类（ServiceBase 为 internal 实现名）
//   v2::CoComponent  —— 能力组件基类
//   v2::CoActor      —— Actor 基类
//
// 旧业务类型 Component/Service/Actor 与旧宿主 framework::CoApp 均已迁移：
// 公共面不再存在第二套同名类型，命名归属收口到本头聚合包含。

#include <bbt/framework/Address.hpp>
#include <bbt/framework/App.hpp>
#include <bbt/framework/Object.hpp>
#include <bbt/framework/Service.hpp>
