# 分布式服务端框架基调：证据索引

本文件记录基调设计引用的源码、仓库文档和外部资料。源码条目固定到研究时读取的完整 commit；“已读取”不等于已完成构建、运行或性能验证。

## 本地仓库源码与文档

| ID | 仓库 | 固定 commit | 路径与范围 | 读取结论 |
|---|---|---|---|---|
| L01 | `yqm-307/bbt-framework` | `0f7873740450cf4715150cc71b903962ad3debfe` | `README.md:1-55` | —；parent_read; working file equals git object; no build or runtime test |
| L02 | `yqm-307/bbt-framework` | `0f7873740450cf4715150cc71b903962ad3debfe` | `AGENTS.md:1-58` | —；parent_read; working file equals git object; no build or runtime test |
| L03 | `yqm-307/bbt-framework` | `0f7873740450cf4715150cc71b903962ad3debfe` | `framework/src/host/CoApp.cc:1-200` | CoApp::CoApp, CoApp::run, CoApp::find_route, CoApp::grant_ordered_stream；parent_read; working file equals git object; no build or runtime test |
| L04 | `yqm-307/bbt-framework` | `0f7873740450cf4715150cc71b903962ad3debfe` | `framework/src/host/CallOptionsAdapter.cc:1-90` | AdaptCallOptions；parent_read; working file equals git object; no build or runtime test |
| L05 | `yqm-307/bbt-framework` | `0f7873740450cf4715150cc71b903962ad3debfe` | `framework/src/host/RequestScope.cc:1-81` | RequestScope::RequestScope, CurrentRequestContext；parent_read; working file equals git object; no build or runtime test |
| L06 | `yqm-307/bbt-framework` | `0f7873740450cf4715150cc71b903962ad3debfe` | `framework/src/actor/ActorMailbox.cc:1-103` | ActorMailbox::TryEnqueue, ActorMailbox::_Drain；parent_read; working file equals git object; no build or runtime test |
| L07 | `yqm-307/bbt-framework` | `0f7873740450cf4715150cc71b903962ad3debfe` | `framework/src/actor/OrderedIngress.cc:1-352` | OrderedIngress::GrantStream, OrderedIngress::Admit, OrderedIngress::Complete；parent_read; working file equals git object; no build or runtime test |
| L08 | `yqm-307/bbt-framework` | `0f7873740450cf4715150cc71b903962ad3debfe` | `framework/include/bbt/framework/OrderedTypes.hpp:1-129` | OrderedGrant, OrderedTicket::BindOrCheck；parent_read; working file equals git object; no build or runtime test |
| L09 | `yqm-307/bbtools-infra` | `3e6da6205cd02387a9685f746244a3d75277265d` | `include/bbt/infra/Result.hpp:1-197` | ErrorCode, Error；parent_read; working file equals git object; no build or runtime test |
| L10 | `yqm-307/bbtools-infra` | `3e6da6205cd02387a9685f746244a3d75277265d` | `docs/decisions/0006-infra-foundation-and-dynamic-config.md:1-178` | —；parent_read; working file equals git object; no build or runtime test |
| L11 | `yqm-307/bbtools-infra` | `3e6da6205cd02387a9685f746244a3d75277265d` | `include/bbt/infra/NetworkTypes.hpp:1-108` | IncomingCallContext, RpcEnvelope；parent_read; working file equals git object; no build or runtime test |
| L12 | `yqm-307/bbt-framework` | `0f7873740450cf4715150cc71b903962ad3debfe` | `framework/include/bbt/framework/ExecutionPolicy.hpp:1-36` | ExecutionPolicy, ServiceOptions；parent_read; working file equals git object; no build or runtime test |
| L13 | `yqm-307/bbt-framework` | `0f7873740450cf4715150cc71b903962ad3debfe` | `framework/src/host/HostLifecycle.cc:1-217` | HostLifecycle::_ShutDown, HostLifecycle::_WaitHandlersDone, HostLifecycle::_WaitClosed；parent_read; working file equals git object; no build or runtime test |
| L14 | `yqm-307/bbt-framework` | `0f7873740450cf4715150cc71b903962ad3debfe` | `framework/src/host/RpcHttpBridge.cc:1-186` | ToHttpRequest, ToHttpResponse, EnvelopeFromResponse, HttpEgress::Send；parent_read; working file equals git object; no build or runtime test |
| L15 | `yqm-307/bbt-framework` | `0f7873740450cf4715150cc71b903962ad3debfe` | `framework/src/host/CoServiceCall.cc:1-125` | ICoService::_CallSend；parent_read; working file equals git object; no build or runtime test |

## 外部官方文档与原始论文

| ID | 标题 | URL | 使用范围与限制 |
|---|---|---|---|
| E01 | gRPC Deadlines | <https://grpc.io/docs/guides/deadlines/> | Deadline Propagation and server cancellation; opened and read 2026-09-24 |
| E02 | gRPC Retry | <https://grpc.io/docs/guides/retry/> | Transparent retry, commit point, retry policy, backoff and throttling; opened and read |
| E03 | etcd v3.6 API guarantees | <https://etcd.io/docs/v3.6/learning/api_guarantees/> | KV/Watch/Lease guarantees including unbounded watch delay and non-linearizable watch; versioned docs, not a latest-release claim |
| E04 | Orleans: Distributed Virtual Actors for Programmability and Scalability | <https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/Orleans-MSR-TR-2014-41.pdf> | 2014 technical report; primary paper opened; used for actor lifecycle/placement architecture, not current implementation version |
| E05 | Spanner: Google’s Globally-Distributed Database | <https://storage.googleapis.com/gweb-research2023-media/pubtools/1974.pdf> | Primary-author paper, 2013 ACM TOCS version (the PDF is not the 2012 conference layout); opened and read for transaction/time assumptions, not present-day product limits |
| E06 | In Search of an Understandable Consensus Algorithm (Extended Version) | <https://raft.github.io/raft.pdf> | Section 8 client interaction read, especially client deduplication and linearizable reads; primary paper, not a claim of full-paper or implementation verification |
| E07 | Envoy Overload manager | <https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/operations/overload_manager> | Unversioned latest docs observed 1.40.0-dev-e0f419; only conceptual separation from circuit breaking used, not release recommendation |
| E08 | FoundationDB: A Distributed Unbundled Transactional Key Value Store | <https://www.foundationdb.org/files/fdb-paper.pdf> | Section 4 deterministic simulation and section 7 limits of model checking; primary paper read |

## 证据边界

- `[L…]` 引用本地仓库源码或仓库文档；`[E…]` 引用外部文档或论文。
- 本索引不表示这些仓库当前分支已经合入设计，也不替代对应仓库的构建、测试和发布门禁。
- 研究缓存、子代理中间日志和未完成专题报告留在外部研究工作目录，不作为仓库真源。
