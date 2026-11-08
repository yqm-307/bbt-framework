// framework 侧 V1 面缺席的负向检查（framework.v1_absent）。
//
// 判据分两层，均不依赖「跑起来能连上」：
//  1) 编译层正向对照：本 TU 只 include V2 公开面并实际使用 Request/Response/
//     RpcEgress/InstallRpcAdmission 类型——证明 V2 面在场且可编译；
//  2) 文本层缺席：扫描 framework/ 的产品源码与 CMake，断言旧 RPC 面
//     （LegacyHeaders/x-bbt 信封头族/RpcEnvelope/RpcAddress/RpcInboundBridge/
//     RpcEgressProfile/HttpEgress/双桥安装入口/旧能力宏）零命中。
//
// 扫描范围刻意排除本文件自身与 tests/ 目录：本测试的判据字符串本身必须
// 出现，否则无法表达「什么不该存在」。裸 RpcHandler 不是残留证据
// （CoRpcServer::RpcHandler 是保留的 V2 路由别名，见 ADR 0022 §5）。

#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/internal/RpcBridge.hpp>
#include <bbt/infra/rpc/Request.hpp>
#include <bbt/infra/rpc/Response.hpp>

namespace fw = bbt::framework;
namespace inf = bbt::infra;
namespace fs = std::filesystem;

namespace {

// 产品源码根：由 tests/CMakeLists.txt 以编译定义注入（绝对路径）。
constexpr const char* kSourceRoot = BBT_FRAMEWORK_SOURCE_DIR;

struct Forbidden {
    const char* label;
    const char* needle;
};

// 旧 RPC 面的文本指纹。只列「出现即证明旧面仍在」的精确 token：
//  - x-bbt 信封头族（x-bbt-err-* 是 HTTP 错误映射，保留，故不列）；
//  - 旧值类型/旧桥/旧 profile 枚举/旧能力宏。
constexpr Forbidden kForbidden[] = {
    {"x-bbt-service", "x-bbt-service"},
    {"x-bbt-method", "x-bbt-method"},
    {"x-bbt-request-id", "x-bbt-request-id"},
    {"x-bbt-request-schema", "x-bbt-request-schema"},
    {"x-bbt-response-schema", "x-bbt-response-schema"},
    {"x-bbt-meta-", "x-bbt-meta-"},
    {"RpcEnvelope", "RpcEnvelope"},
    {"RpcAddress", "RpcAddress"},
    {"RpcWireEnvelope", "RpcWireEnvelope"},
    {"IRpcTransport", "IRpcTransport"},
    {"IRpcInboundTransport", "IRpcInboundTransport"},
    {"RpcInboundBridge", "RpcInboundBridge"},
    {"RpcEgressProfile", "RpcEgressProfile"},
    {"HttpEgress", "HttpEgress"},
    {"InstallRpcHttpBridge", "InstallRpcHttpBridge"},
    {"InstallRpcWireBridge", "InstallRpcWireBridge"},
    {"RpcHttpBridge.hpp", "RpcHttpBridge.hpp"},
    {"RpcWireBridge.hpp", "RpcWireBridge.hpp"},
    {"RpcWire.hpp", "rpc/RpcWire.hpp"},
    {"HttpTransport.hpp", "rpc/HttpTransport.hpp"},
    {"BBT_FRAMEWORK_HAS_RPC_WIRE", "BBT_FRAMEWORK_HAS_RPC_WIRE"},
    {"BBT_FRAMEWORK_HAS_RPC_TCP", "BBT_FRAMEWORK_HAS_RPC_TCP"},
};

bool IsScannedFile(const fs::path& p) {
    const auto ext = p.extension().string();
    if (ext == ".hpp" || ext == ".h" || ext == ".cc" || ext == ".cpp")
        return true;
    return p.filename() == "CMakeLists.txt";
}

std::vector<fs::path> CollectFiles() {
    std::vector<fs::path> files;
    for (const char* rel : {"include", "src"}) {
        const fs::path root = fs::path(kSourceRoot) / "framework" / rel;
        if (!fs::exists(root))
            continue;
        for (const auto& entry : fs::recursive_directory_iterator(root)) {
            if (entry.is_regular_file() && IsScannedFile(entry.path()))
                files.push_back(entry.path());
        }
    }
    const fs::path top_cmake = fs::path(kSourceRoot) / "framework" / "CMakeLists.txt";
    if (fs::exists(top_cmake))
        files.push_back(top_cmake);
    return files;
}

std::string ReadAll(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

} // namespace

BOOST_AUTO_TEST_SUITE(framework_v1_absent)

// 正向对照：V2 公开面可编译、可用（本 TU 已 include 并实例化）。
BOOST_AUTO_TEST_CASE(v2_surface_compiles_and_is_usable) {
    inf::rpc::Request request("svc.echo", "Echo");
    request.SetRequestId("rid-1")
        .SetReply(inf::rpc::ReplyMode::Required)
        .SetTraffic(inf::rpc::TrafficClass::Service)
        .SetRequestSchema("bbt.echo.EchoReq/v1")
        .SetResponseSchema("bbt.echo.EchoResp/v1")
        .SetPayload({0x08, 0x01});
    auto response = inf::rpc::Response::FromRequest(request);
    BOOST_CHECK_EQUAL(response.RequestId(), "rid-1");
    BOOST_CHECK_EQUAL(response.Service(), "svc.echo");
    BOOST_CHECK(response.Payload().empty());

    // 安装入口与出站入口都是 V2 名字（类型可用即证明头已就位）。
    using AdmissionFn = void (*)(fw::InfraHttpHost&, fw::CoApp&);
    AdmissionFn admission = static_cast<AdmissionFn>(&fw::InstallRpcAdmission);
    BOOST_CHECK(admission != nullptr);
    BOOST_CHECK(sizeof(fw::RpcEgress) > 0);
}

// 旧桥文件必须已删除（存在即失败）。
BOOST_AUTO_TEST_CASE(legacy_bridge_headers_are_gone) {
    const fs::path root(kSourceRoot);
    BOOST_CHECK(!fs::exists(root / "framework/include/bbt/framework/internal/RpcHttpBridge.hpp"));
    BOOST_CHECK(!fs::exists(root / "framework/include/bbt/framework/internal/RpcWireBridge.hpp"));
    BOOST_CHECK(!fs::exists(root / "framework/src/host/RpcHttpBridge.cc"));
    BOOST_CHECK(!fs::exists(root / "framework/src/host/RpcWireBridge.cc"));
    // 新接线在场。
    BOOST_CHECK(fs::exists(root / "framework/include/bbt/framework/internal/RpcBridge.hpp"));
    BOOST_CHECK(fs::exists(root / "framework/src/host/RpcBridge.cc"));
}

// 产品源码/CMake 里不得再出现旧 RPC 面 token。
BOOST_AUTO_TEST_CASE(legacy_tokens_absent_from_product_sources) {
    const auto files = CollectFiles();
    BOOST_REQUIRE_MESSAGE(!files.empty(),
                          "no framework sources collected under " << kSourceRoot);
    for (const auto& file : files) {
        const std::string text = ReadAll(file);
        for (const auto& f : kForbidden) {
            const auto hit = text.find(f.needle);
            BOOST_CHECK_MESSAGE(hit == std::string::npos,
                                file.string() << " still contains " << f.label);
        }
    }
}

BOOST_AUTO_TEST_SUITE_END()
