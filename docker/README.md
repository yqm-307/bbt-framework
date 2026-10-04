# CI 工具链镜像（Issue #37）

本目录的镜像配方只提供**工具链**，不烘焙业务/bbt 库：

- `bbtools-common-image`：Debian 13 + g++ + cmake + ninja + 源码构建的 Boost 1.90。
- `bbtools-runner`：官方 actions-runner 基座 + 上述工具 + 锁定 protobuf 3.21.12
  隔离前缀 `/opt/protobuf-3.21.12`。**刻意不装 bbtools-core，也不写 `/usr/local`**
  ——旧配方把 core@master 编进 `/usr/local` 会被运行时抢绑（与本工单 R4/S5 相悖）。

业务与 bbt 库在 job 内按 `deps.lock` 固定 SHA 隔离构建，镜像本身不携带。

## 固定输入（唯一真源 `docker/toolchain.lock`）

| 项 | 值 |
|---|---|
| runner 基座 | `ghcr.io/actions/actions-runner:2.333.0@sha256:1ad98353…` |
| common 基座 | `debian:trixie-slim@sha256:a99cfc51…` |
| Boost | `1.90.0`，归档 sha256 `5e93d582…` |
| protobuf | `3.21.12`（复用 `scripts/prepare_protobuf.sh` 的 Ubuntu amd64 锁定包集 + `libprotobuf.a` sha256） |

digest 由只读 registry 解析得到（多架构 index digest）；Boost/protobuf checksum
固定自上游归档与 `#30` 的 `protobuf-3.21.12.provenance.md`。**不重写** #30 已交付的
protobuf 配方，镜像构建只是 COPY 并运行同一个 `prepare_protobuf.sh`。

## 构建与验证

```bash
# 构建（只构建，不部署/不 import/不切换 runner）。默认 noProxy：仅在需要时
# 显式给代理，例如 BBT_HTTP_PROXY=http://127.0.0.1:7890 docker/build_toolchain.sh
docker/build_toolchain.sh
# 产物身份（imageID / RepoDigest / 所用 common imageID）写到
# build-toolchain/toolchain-build-manifest.txt；默认自有 tag 为
# bbt-p0a-ci-7c74c41d85c2-common:v2 与 bbt-p0a-ci-7c74c41d85c2-runner:v2，
# 不覆盖共享 bbtools-common-image:v1 / bbtools-runner:v1（旧产物留作对照）。

# 工具链身份探针，三种模式：
scripts/verify_toolchain.sh --in-image                  # 镜像内自检（构建期/CI 复用）
scripts/verify_toolchain.sh --prefix <pb-prefix>        # 校验 prepare_protobuf.sh 前缀
scripts/verify_toolchain.sh --image <docker-ref>        # 宿主机对镜像 docker run 自检
# --lock <file> 可显式指定契约 lock；缺省顺序为
#   显式 --lock > 仓内 docker/toolchain.lock > 镜像内置 /usr/local/share/bbt/toolchain.lock
# 存在仓内 checkout 时优先本仓 lock；--image 模式把它只读挂进容器再判定。
```

探针 fail-closed：protoc 版本不符、`libprotobuf.a` checksum 不符、Boost 成品缺失、
或存在 `/usr/local/lib/libbbt_*`（旧产物）任一即非零退出，**禁止系统 protoc 回退**。

## 本机候选 vs 远端待部署

- 本目录配方、`toolchain.lock`、探针与 `build_toolchain.sh` 是**本机候选产物**，
  已在本地 Docker daemon 构建并跑通探针（见 scratch 证据索引）。
- 远端 `arc-s4-framework` runner 是否已切到本镜像**不由本仓证明**。切换属共享
  runner/ARC/containerd 操作，需获准运维执行；未切换时 `CI-T3` / R4 / S4 记
  `BLOCKED`，不报通过、不绿色 skipped。
- `.github/workflows/ci.yml` 里 `toolchain-verify` job 仅 `workflow_dispatch` 触发，
  不进 required checks；运维切换后可把它并入 build job 成为 P0-A 强制前置。
