# v0.3 功能验收与模块 UT

## 执行与判定

前置：启动 PD、TiKV、SeaweedFS（README 中的三个启动脚本），安装 Python requirements.lock.txt。执行 `bash scripts/acceptance.sh`。

脚本依次运行独立 UT、race detector、ANN 召回率门禁、真实后端集成、HTTP 进程崩溃恢复、Python UT、真实 S3 测试。任一步失败即停止；总结果以本次 `reports/acceptance.status` 为准。历史单项 JSON 不能代替总结果。Go UT 不依赖 TiKV/SeaweedFS；S3 SDK 适配器 UT 使用本机临时 HTTP 端口。

以下覆盖当前已实现功能，不代表商用能力已经齐备。异常注入 UT 证明应用分支正确，真实 TiKV 集成测试证明事务与并发行为，两者不可互相替代。

## 用例矩阵

每行操作都对应可执行测试。Go 名称位于 internal/vector，HTTP 名称位于 cmd/vector-server。

| ID | 操作/输入 | 预期结果 | 自动化入口 |
|---|---|---|---|
| AC-01 | 空 namespace、非法维度；创建及重新打开 schema | 非法配置拒绝；相同配置成功；不同配置拒绝 | TestRepositorySchema |
| AC-02 | 创建、读取、更新、删除、重新创建同一 ID | revision 递增；墓碑无向量；重建继续递增 | TestRepositoryLifecycle |
| AC-03 | 同一请求重试；请求 ID 换内容或换记录 | 原始结果不重复提交；内容不匹配冲突 | TestRepositoryLifecycle、TestTiKVSeaweedIntegration |
| AC-04 | 写入记录、事件、head 和凭证期间逐一注入失败、提交失败 | 不产生部分已提交记录 | TestRepositoryMutationFailures、TestLogFailureAndBounds；真实 rollback_record_and_outbox |
| AC-05 | 提交实际成功但确认丢失，再用相同 ID 重试 | 返回原始版本，不二次修改 | TestRepositoryUnknownCommitRetry（故障注入） |
| AC-06 | 两个写者以相同 expected_revision 修改 | 恰好一次成功，事件数量正确 | concurrent_expected_revision（真实 TiKV） |
| AC-07 | 扫描失败、途中失败、非法 JSON、超上限、取消 | 不返回部分结果；等于上限可以读取；不跨键范围 | TestRepositoryScanFailures |
| AC-08 | 清单首次发布、旧代际发布、两个并发发布者 | 首次成功，旧代际拒绝，并发只成功一次 | TestRepositoryManifest、concurrent_manifest_publish |
| AC-09 | 空索引、非法维度/NaN/Inf/数值/K/token | 空数组或明确错误，不崩溃 | TestIndexBoundaries、TestValidationAndMissingFilter |
| AC-10 | 已知距离、距离相同的 ID、K 小于数据量 | squared-L2 正确，按 ID 稳定排序，正确截断 | TestIndexDistanceTieAndCopies、TestReplayDoesNotResurrectAndFiltersBeforeTopK |
| AC-11 | 属性过滤，字段缺失与空值，过滤后 Top-K | 先过滤再截断；字段缺失不匹配空字符串 | TestValidationAndMissingFilter、TestReplayDoesNotResurrectAndFiltersBeforeTopK |
| AC-12 | 新版本后应用旧版本/相同版本；删除后重放，再创建 | 不倒退、不复活；更高版本可重新创建 | TestReplayDoesNotResurrectAndFiltersBeforeTopK、TestIndexDistanceTieAndCopies |
| AC-13 | 修改输入、Snapshot 和 Search 返回的 map/slice | 不改变内部查询视图 | TestIndexDistanceTieAndCopies |
| AC-14 | 桶已存在、不存在、查询/创建失败 | 正确创建或传播失败 | TestObjectsEnsure |
| AC-15 | 快照上传回读、重复生成、墓碑序列化 | 校验一致、对象名不同、保留墓碑 | TestObjectsRoundTrip |
| AC-16 | 负数/过大长度、截断、多余字节、哈希错、坏 JSON、下载错误 | 拒绝快照并关闭读取流 | TestObjectsFailures |
| AC-17 | S3 SDK HEAD/PUT/GET | SDK 适配正确；真实后端另行验证 | TestObjectsS3Adapter、verify.py |
| AC-18 | 启动时桶、清单、快照、维度、记录或扫描异常 | 不产生可用 Service | TestServiceStartupFailures |
| AC-19 | 旧快照加载后重放后续删除 | 旧记录不复活，token 可达 | TestServiceRecoveryAndCheckpoint、snapshot_plus_later_delete_replay |
| AC-20 | checkpoint 各阶段失败 | 未校验成功不发布；CAS 失败不覆盖 | TestServiceFailureNeverPublishes |
| AC-21 | 批次含坏记录、扫描失败、并发同步与查询 | 整批拒绝，原视图保留，无数据竞争 | TestServiceSyncAtomicAndConcurrent |
| AC-22 | 所有接口缺认证、错误方法、错误路径 | 401/405/404；不调用业务后端 | TestHandlerAuthAndMethods |
| AC-23 | 坏 JSON、多 JSON、未知字段、超大 body、错误字段类型 | 400，拒绝写入 | TestHandlerDecodeAndMutationValidation |
| AC-24 | GET/PUT/DELETE/search/checkpoint/health 成功路径 | JSON 结果、token、过滤结果、非部分结果正确 | TestHandlerSuccess |
| AC-25 | 缺记录、冲突、后端故障、未到达 token | 分别 404/409/503；明确错误 | TestHandlerErrorMapping |
| AC-26 | 真实 HTTP 写入→过滤搜索→更新→检查旧向量 | 最新向量参与排序，读己之写成功 | scripts/verify_api.py |
| AC-27 | checkpoint 后删除，kill 服务进程并重启，再重试旧请求 | 删除不复活，旧请求不覆盖新版本 | scripts/verify_api.py |
| AC-28 | Python 清单 CAS、重开 SQLite；下载校验失败 | CAS 正确，原文件保留，临时文件清理 | test_storage.py |
| AC-29 | 17 MiB 分段上传、Range、四路恢复、错误校验和 | 回读一致，范围正确，旧文件保留 | verify.py |
| AC-30 | SeaweedFS 重启后只运行 restore-only | 不上传即可恢复 | 手动重启后运行 verify.py --restore-only；不在默认一键验收中 |

## 模块 UT 文件

- repository_test.go：事务、schema、幂等、扫描、清单、故障注入。
- index_test.go：距离、过滤、版本、边界、复制隔离。
- objects_test.go：对象校验、桶操作、SDK HTTP 适配、异常关闭。
- service_test.go：初始化、同步、恢复、发布顺序、并发。
- handler_test.go：所有接口、认证、解码、状态码、环境变量。
- test_storage.py：保留的 Python S3 原型及 SQLite 清单。

main 的进程启动/退出编排通过真实 HTTP 子进程验收，不用 UT 启动 main 或模拟 os.Exit。启动脚本只负责环境，不把 shell 覆盖率算作功能完成率。

## 不纳入当前通过范围

生产真实数据的 HNSW 召回率、多分片、副本仲裁、整机掉电、可用区容灾、完整生命周期 GC、多分片日志截断、背压、生产性能和多租户身份映射尚未实现，状态为“待开发/待验收”，不得标记通过。AC-30 为单独的服务重启验收；AC-27 只强杀向量服务，不强杀 TiKV。

覆盖率输出 reports/coverage.txt 和 reports/unit.cover；覆盖率是辅助证据，异常路径、业务断言和真实后端测试共同决定验收结果。

## 本轮索引验收

| ID | 操作/输入 | 预期结果 | 自动化入口 |
|---|---|---|---|
| AC-31 | 不同 K、随机向量的堆与旧全排序对照 | 精确结果逐条一致 | TestHeapMatchesFullSort |
| AC-32 | 10,000 × 128 维；50 个独立随机查询 | Recall@10 ≥ 95%，距离评估数低于全扫描 | TestHNSWRecallAcceptance；一键验收启用 ANN_ACCEPTANCE=1 |
| AC-33 | 小过滤集、字段缺失、空值、多字段 AND | 只评分匹配集，正确处理空值 | TestPayloadIntersection、TestHNSWFilterVersionsAndRebuild |
| AC-34 | 大过滤集经 ANN，更新属性和删除后再查 | 返回够 K 条有效候选，不泄漏旧版本 | TestHNSWFilteredANNAndRestoredSearch |
| AC-35 | 候选预算不足、非法参数、坏批次 | 精确兜底；无效输入拒绝 | TestHNSWFallbackAndValidation |
| AC-36 | 图导出、恢复、追加节点；格式/图/映射损坏 | 保留图和结果；坏快照拒绝 | TestHNSWGraphSnapshot、TestHNSWFilteredANNAndRestoredSearch |
| AC-37 | 重建、取消、发布冲突、并发读写 | 版本不倒退，墓碑保留，旧节点清理 | TestHNSWFilterVersionsAndRebuild、TestHNSWConcurrentSearchApplyRebuild |
| AC-38 | 过期节点阈值、恢复后重放删除 | 正确触发重建且不复活 | TestHNSWAutomaticRebuildEligibility、TestGraphObjectAndServiceRecovery |
| AC-39 | 原生图建图、搜索、保存加载、参数边界 | 邻居一致，无空 slice 崩溃 | internal/ann/graph_test.go |
| AC-40 | rebuild 的认证/方法/冲突/失败/成功 | 状态码正确，重建成功后才 checkpoint | TestHandlerRebuild；verify_api.py 真实图快照发布 |

新增模块 UT：`internal/ann/graph_test.go` 和 `internal/vector/hnsw_test.go`（含属性倒排、图快照、重建）。原生 C++ 不计入 Go 覆盖率；Go race 不替代 C++ 内存检测。性能基准执行 `bash scripts/go.sh test -run '^$' -bench BenchmarkSearch -benchtime=1s ./internal/vector`，耗时不作为跨机器硬门槛。完整结果及限制见 HNSW.md。

## v0.3 增量日志及并发验收

| ID | 操作 | 预期 | 自动化入口 |
|---|---|---|---|
| AC-41 | 600 事件分批消费、幂等重试 | 连续序号，无重复追加；空轮询不读历史 | TestOrderedLogBatchesAndRetry |
| AC-42 | 第二批失败后重试 | cursor 停在第 256 条，从下一条恢复，只读取新前缀 | TestIncrementalRetryDoesNotRescanHistory |
| AC-43 | 序号缺口、损坏、计数溢出、第四次写失败 | 拒绝跳读，无部分提交 | TestLogFailureAndBounds |
| AC-44 | 未 checkpoint、已 checkpoint、快照后删除 | 仅清理已持久化前缀，删除事件保留 | TestCheckpointCursorPruneAndRecovery |
| AC-45 | 分批删除失败、floor 写入失败 | 删除和 floor 原子提交，不推进假位点 | TestPruneFailureDoesNotAdvanceFloor |
| AC-46 | 清理后恢复、落后消费者恢复、旧请求重试 | 无删除复活；从同一记录快照与 head 重新追赶 | TestCheckpointCursorPruneAndRecovery、TestIncrementalLogTiKV |
| AC-47 | 旧 schema 与随机 Outbox | 离线升级后从权威记录建立基线，避免随机 ID 游标 | TestSchemaOfflineUpgrade、TestLegacyBootstrapAndIncrementalSync |
| AC-48 | 8 个真实 TiKV 写者提交 96 条记录 | 无缺口，96 条可清理，后续删除可恢复 | TestIncrementalLogTiKV |
| AC-49 | 不同 ef 并发查询 | 结果和请求局部计数不互相干扰 | TestConcurrentQueryLocalState；native_stress.cc + TSan |
| AC-50 | 建图时更新和删除 | 重建追赶并保留全部最新版本 | TestRebuildCatchesConcurrentWrites |
| AC-51 | 清理 HTTP 接口认证、方法、错误及成功 | 401/405/503/200；不支持适配器为 501 | TestHandlerPrune |
| AC-52 | checkpoint → prune → 后续删除 → kill/restart | 从快照位点恢复、重放删除 | scripts/verify_api.py |
| AC-53 | 真实后端写入并每 500 ms 增量同步 | 报告写入和可见延迟，token 不提前可见 | TestWriteVisibilityLatencyTiKV |
| AC-54 | 1 万／5 万条 128 维，50 个查询，4 路搜索，持续写入重建 | Recall@10 ≥95%，记录吞吐、P95/P99、重建耗时及峰值 RSS | TestCapacityAcceptance（扩展验收） |

`EXTENDED_ACCEPTANCE=1 bash scripts/acceptance.sh` 包含容量测试和原生 TSan。普通一键验收不跑容量测试，避免把硬件耗时变成 UT 门槛。新增模块 `log.go` 的 UT 在 `log_test.go`。容量配置与原始报告见 INCREMENTAL.md；真实业务数据未提供前不得把合成测试标记为生产数据验收。

## 有规律的数据验收

`STRUCTURED_ACCEPTANCE=1 bash scripts/go.sh test -count=1 -run TestStructured -v ./internal/vector`，也已加入扩展一键验收。

- AC-55 / TestStructuredGenerator：固定种子可复现；16 个主题簇、头部簇约 50%、5% 近重复、约 1% 离群点；128 维由 8 个潜在因子产生相关性。
- AC-56 / TestStructuredAcceptance：1 万和 5 万条；主题内独立查询、主题边界、近重复、独立离群查询、主题过滤、跨租户过滤、稀有属性 AND、空过滤共 8 类，每类 40 次查询。
- AC-57 / 同一测试：分别在初始数据、修改 5% / 删除 5% 后、图快照恢复并重建后三个阶段验收；每个非空场景 Recall@10 ≥ 95%，结果数量、属性、revision、距离和删除状态必须正确。

真值来自当前记录的精确搜索。查询噪声使用独立种子；近重复场景有意从相邻样本扰动生成，其他场景不使用库内向量直接自查。空过滤单独检查零返回，报告中的 recall=1 只是约定，不作为非空召回证据。数据生成器和测试在 internal/vector/structured_test.go，报告和可阅读样本位于 reports/structured.json、reports/structured-sample-10000.json、reports/structured-sample-50000.json。

## HNSW 原生图进程崩溃验收

AC-58：`.venv/bin/python scripts/verify_hnsw_crash.py`，已加入普通一键验收。前置为最新 `.tools/vector-server`、真实 TiKV/SeaweedFS，使用随机 namespace 和独立端口 18092，仅强杀自己的子进程。

1. 写入 600 条 32 维有主题分布的向量，先同步再更新一条，使快照包含 601 个图节点（其中一个旧版本节点）。
2. checkpoint 并清理被覆盖日志。SIGKILL 后重启，比较 ANN 结果、完整记录、节点映射和原生图 SHA-256；图字节必须完全相同，旧节点仍存在，证明直接恢复了图，而非只重建最新记录。
3. 快照之后提交更新、删除和新增，不再 checkpoint，再次 SIGKILL。重启逐条校验 600 条存活记录的 ID、版本、属性和距离；检查删除墓碑及幂等重试。
4. 验证消费位点从 601 到 604、图节点从 601 到 603：原图保留，追加两个新版本节点。

本次两次强杀均通过，重启至 ready 约 0.31 秒（小数据本机测试，含 readiness 轮询）。报告在 reports/hnsw-crash.json；服务日志在 reports/hnsw-crash-server.log。只模拟向量服务崩溃，未同时强杀 TiKV/SeaweedFS，也不代表整机掉电或多节点故障验收。


## 两层分区验收

| ID | 操作 | 预期 | 测试 |
|---|---|---|---|
| AC-59 | ID hash、数据→索引绑定、重开 layout | hash 固定；绑定持久化；分区数修改拒绝 | TestPartitionRouterAndLayout |
| AC-60 | 多分区精确查询、过滤、token 和距离相同的记录 | 全局 Top-K 与单索引真值逐条相同；token 只检查所属分区 | TestPartitionedLifecycleAndRecovery |
| AC-61 | 独立快照与清理后更新删除，再恢复 | 各片 offset 正确；索引快照与数据路径独立；不丢更新 | TestPartitionedLifecycleAndRecovery |
| AC-62 | 不同 hash 分区重复使用 request_id | namespace 级冲突，不产生第二条写入 | TestPartitionedLifecycleAndRecovery、verify_partitions.py |
| AC-63 | 一个索引分区失败或查询超时 | 不返回部分 hits；HTTP 503 | TestPartitionFailureAndDeadline、TestHandlerPartitionAPI |
| AC-64 | 旧单分区记录及快照经新入口打开 | 原路径和接口响应兼容 | TestSinglePartitionCompatibility；原 verify_api.py / verify_hnsw_crash.py |
| AC-65 | 四分区真实后端写入 800 条、快照、prune、SIGKILL、恢复、重建 | 分区恢复正确；全局精确排序及过滤正确；错误配置拒绝启动 | scripts/verify_partitions.py |

`partitions.go` 对应 UT 为 `partitions_test.go`。四分区 HTTP 验收已纳入普通一键脚本，报告在 reports/partitions-http.json。当前固定分区不等于多机器部署，也不提供跨分区原子快照或动态迁移；详细边界见 PARTITIONS.md。


## Master / Gateway / Worker 验收

| ID | 场景 | 验收点 | 入口 |
|---|---|---|---|
| AC-66 | 两个 Master 候选并发选主 | 只有一个事务成功；leader epoch 递增 | TestMasterElectionAndFenceTiKV |
| AC-67 | 旧主先检查，再由新主接管，旧事务最后提交 | fencing 写冲突；旧调度不能提交 | TestMasterElectionAndFenceTiKV、TestMasterElectionScheduleAndStaleLeader |
| AC-68 | Worker 非目标、计划变化、Master 暂时缺席 | 不自主抢分区；计划变化后停止续租；主缺席时已有计划可续租 | TestRolesAndAssignmentEnforcement、TestWorkerOnlyFollowsMasterAssignment |
| AC-69 | ownership 过期、接管、旧发布/清理在途事务 | 拒绝旧 epoch；不推进旧清单或删除日志 | TestOwnershipLifecycleAndFencing、TestLeaseFenceTransactionConflictTiKV |
| AC-70 | 角色配置、业务 HTTP 与内部 RPC | 无效配置拒绝；非 Gateway 不能提供业务 API；内部 token 独立 | TestOpenClusterRejectsInvalidConfigBeforeStorage、TestRoleHTTPGate、TestInternalRPCAuthenticationAndFencing |
| AC-71 | 分区退休时仍有本地读操作 | 等待读者退出后释放图 | TestRetirementWaitsForLocalReaders |
| AC-72 | 两个组合角色进程 | 路由、快照、主/Worker 接管、旧 epoch 拒绝 | scripts/verify_cluster.py |
| AC-73 | 两个独立 Master、两个 Worker、一个 Gateway | 主暂停/接管期间查询持续；全 Master 暂停仍可查询；Worker 暂停/强杀后恢复 | scripts/verify_roles.py |

普通一键验收已包含 AC-72/73，完整运行可能需数分钟。多进程报告在 reports/cluster-http.json、reports/roles-http.json。所有进程仍在同一台机器，测试不能代替跨主机网络隔离、TiKV 仲裁丢失或可用区故障验收。Master 负责固定分配，不代表已完成按负载调度或聚类迁移。

## 多 Worker 扩缩容验收

| ID | 场景 | 验收点 | 入口 |
|---|---|---|---|
| AC-74 | 本机多进程启动与收敛 | 配置校验；启动失败回收；终止超时强杀；注册完成但 Master 未调度时不能误报成功 | scripts/test_cluster_local.py |
| AC-75 | 2 → 3 个 Worker | 新 Worker 获得分区；未迁移分区 epoch 不变；全量精确结果和 HNSW 查询正确 | scripts/verify_multiworker.py |
| AC-76 | 快照后更新/删除，3 → 1 个 Worker | 恢复所有分区及增量尾部；无丢失、重复、旧属性或旧版本 | scripts/verify_multiworker.py |
| AC-77 | 1 → 0 → 2 个 Worker | 无 Worker 查询返回 503；期间写入仍持久化；全部新进程恢复后结果一致；验收退出不遗留子进程 | scripts/verify_multiworker.py |

扩缩容固定 8 个数据/索引分区，不验证在线改变分区数，也不承诺迁移期间查询不中断。测试使用同机进程和真实后端，报告为 reports/multiworker-http.json。

## 命令行验收

| ID | 场景 | 验收点 | 入口 |
|---|---|---|---|
| AC-78 | 状态与记录命令参数 | 正确 HTTP 方法、认证、ID 编码、整数精度、非法参数拒绝、HTTP 错误非零退出、不跟随重定向 | cmd/vectorctl/main_test.go |
| AC-79 | 本机控制协议 | 无效/超长输入和未知字段不触发变更；扩容失败明确返回；停止先确认再回收 | scripts/test_cluster_local.py |
| AC-80 | 编译后的 CLI 插入/更新/读取/删除 | 插入冲突、请求幂等、版本 CAS、墓碑和读取不存在 ID | scripts/verify_cli.py |
| AC-81 | 查询与维护 | 文件向量输入、全局 Top-N、属性过滤、写后可见性 token、快照/清理/重建 | scripts/verify_cli.py |
| AC-82 | 跨终端集群管理 | socket 0600；全部状态命令；2 → 3 → 1 Workers；停止回收所有子进程并清理 socket | scripts/verify_cli.py |

## 向量列表验收

| ID | 场景 | 验收点 | 入口 |
|---|---|---|---|
| AC-83 | 单片/多片分页、前缀 ID、Unicode ID | 静态数据完整遍历，无重复遗漏；返回向量；游标不能跨 namespace/布局或切换墓碑选项 | TestListPaginationPartitionsAndScope |
| AC-84 | 大量墓碑、存储失败、取消与非法参数 | 每次最多扫描 1000 条；空页仍可继续；错误不伪装为完整结果 | TestListTombstoneBudgetAndErrors、TestListEndpoint |
| AC-85 | CLI 与真实集群列表 | 正确传递分页参数；分页结果完整；默认隐藏墓碑；无 Worker 仍可列出 TiKV 数据 | TestListCommand、scripts/verify_cli.py |

## gRPC 与 SDK 验收

| ID | 场景 | 验收点 | 入口 |
|---|---|---|---|
| AC-86 | gRPC 参数、认证及角色 | 维度/k/ID 校验；无 token 拒绝；非 Gateway 拒绝业务；deadline 生效 | internal/rpcserver/server_test.go |
| AC-87 | typed proto / Go SDK | optional 版本 CAS；uint64 epoch 不丢精度；维护状态转换；TLS 受信任连接 | TestSDKAndGRPC、TestSDKWithTLS |
| AC-88 | Python SDK | token 和 deadline 传递；insert 的版本 0 与 upsert 的无条件字段区分；空页带游标继续遍历 | sdk/python/tests/test_client.py |
| AC-89 | 真实多进程双语言调用 | Python 写、Go 读/改、Python 再读；幂等、CAS、分页、过滤、可见性、快照/重建/清理、删除；与 HTTP 数据一致 | scripts/verify_rpc.py、TestSDKRealCluster |
| AC-90 | onebox RPC 监听 | CLI 传递 grpc-port；仅 Gateway 绑定独立端口；停机清理子进程 | scripts/verify_cli.py、scripts/verify_rpc.py |

完整验收需要先按 RPC.md 安装 Python gRPC 依赖。协议生成产物已提交到工作目录，正常编译不要求安装 protoc。两套 SDK 尚未发布到公共包仓库。

## 双副本与排空迁移

| ID | 场景 | 验收点 | 入口 |
|---|---|---|---|
| AC-91 | 双副本计划、发布权限、迁移 | 恢复前保留旧副本；仅 publisher 可发布；移除后不能续租 | TestReplicaMigrationAndPublisherFence |
| AC-92 | 慢副本/恢复副本与协议隔离 | 清理不越过最慢消费位点；禁止旧协议混用 | TestReplicaRetentionAndProtocolIsolation |
| AC-93 | 旧 publisher 在途事务遇到计划切换 | TiKV 写冲突阻止旧事务提交，marker 不存在 | TestReplicaPublisherPlanConflictTiKV |
| AC-94 | 持续查询、并发写入与 Worker 故障/迁移 | SIGSTOP/SIGKILL、扩容、排空缩容无查询错误、缺片、重复；token 不回退 | scripts/verify_replicas.py |

## 32 分片与负载均衡

| ID | 场景 | 验收点 | 入口 |
|---|---|---|---|
| AC-95 | 初始 32 分片双副本、容量权重 | 四个同容量 Worker 各 16 副本；容量更大者承担更多；同片不共址 | TestBalancedInitialPlacement、TestWorkerCapacityWeights |
| AC-96 | 热点成本、冷却、未完成迁移 | 更重副本参与成本计算；单次最多移动一个；持久化冷却与迁移门禁 | TestBalanceHotShardCooldownAndMigrationGate |
| AC-97 | 调度配置和协议隔离 | 拒绝不同协议或冷却配置混跑 | TestSchedulerProtocolAndConfiguration |
| AC-98 | 真实 32 分片 3 → 4 Worker | 持续查询、目标负载收敛、一个主动迁移、真实遥测、无反复搬动 | scripts/verify_balance.py |

## 单进程业务与运维流水线

| ID | 场景 | 验收点 | 入口 |
|---|---|---|---|
| AC-99 | 单进程 2 Gateway + 2 Master + 3 Worker | 32 分片双副本；真实 HTTP/TiKV/S3；跨 Gateway 读写、幂等/CAS、检索、分页、快照清理、删除、主切换、排空、重建、Gateway 退出和 Worker 重启串行贯通 | TestOneProcessPipeline、scripts/acceptance-onebox.sh |

已加入统一验收脚本；阶段报告与边界见 PIPELINE.md。七个服务实例共用一个 OS 进程，底层 TiKV/PD/SeaweedFS 是外部依赖。

### AC100–AC105：持久迁移任务与可观测调度

- AC100：Pending/Restoring/CatchingUp/Retiring 的 Master、源、目标故障矩阵；Master 重启保持任务 ID，切换前保留源，切换后不回滚；旧版本和旧 epoch 上报拒绝。见 migration_test.go。
- AC101：任务与计划提交失败无部分写入；真实 TiKV 下迟到上报与计划替换冲突。见 TestMigrationAtomicFailureAndTaskPagination、TestMigrationReportPlanConflictTiKV。
- AC102：在线源必须等待读者/加载器退出后确认；确认失败可重试；超时不强制切换。见 TestRetirementWaitsForLocalReaders、TestRetirementWaitsForLoaderAndRetriesAcknowledgement、TestMigrationStaleReportTimeoutAndReplacement。
- AC103：只读预览不产生事务提交、不消费冷却；预算不足不分配，策略不修改输入；任务分页、错误传播和旧协议拒绝。见 migration_test.go。
- AC104：HTTP 鉴权/方法/参数与 CLI 路径、转义、dry-run 必填测试；七服务流水线新增任务完成、explain、查询与只读预览。
- AC105：32 分片扩容验收经真实 vectorctl 验证 tasks 分页、task、explain、balance --dry-run，最终没有非终态任务；持续验证并发查询无错误。主动迁移现在还等待源资源释放，收敛超时上限调整为 360 秒，未放宽负载均衡和正确性断言。

以上纳入 scripts/acceptance.sh；EXTENDED_ACCEPTANCE=1 继续包含容量、有规律数据和原生 TSan。

2026-11-13 最终执行：EXTENDED_ACCEPTANCE=1 bash scripts/acceptance.sh 全部通过；新增容量不足保留源、切换时重新捕获源 epoch、初始恢复与日志同步阶段边界的定向 race 回归也通过。32 分片从 3 扩至 4 Worker，64 副本分布为 16/17/16/15，1569 次查询零错误，历史任务均处于 Completed/Superseded，测试子进程全部清理。汇总见 reports/migration-acceptance.json。

## 异步图复制验收

- AC106：原生图变更生成后完全回退；Follower 应用后图字节、映射、记录、RNG 与 Leader 相同；覆盖容量扩展、更新、删除、重复/截断变更及快照恢复后接任构图。
- AC107：提交失败与提交结果不确定均不泄露未发布状态；重试不丢失、不重复提交；旧 Publisher 无法提交/发布。
- AC108：暂停 Follower，Leader 仍发布；同一位置副本 ANN 结果一致；真实 SIGKILL Leader 后 B 接管并继续消费数据日志。
- AC109：接管期间原始数据读取/写入正常，存活副本可以查询旧图；新增副本恢复、Leader 重建、checkpoint/prune 后结果一致。
- AC110：图复制模式运行 2 Gateway、2 Master、3 Worker、32 分片的单进程流水线，覆盖 CRUD、幂等、过滤、分页、主节点切换、迁移、重建、重启。

运行 `bash scripts/acceptance-graph.sh`，依赖本地 PD/TiKV/SeaweedFS。报告保存在 reports/graph-*.log/json；多进程测试仅使用一台物理机的不同 host 标签。

## 对外变更订阅验收

- AC111：数据事件分页序号连续；更新和删除完整保留；空尾页不改变游标；非法分片、流、页大小及未来游标被拒绝。
- AC112：已提交未发布的图事件不可订阅；发布后可重复读取相同位置；图对象包含可读的分层邻接表；重建产生明确 reset 事件。
- AC113：日志清理后的旧游标返回 reset_required 与 checkpoint，不静默跳过；Worker 下线不影响已有日志读取。
- AC114：HTTP 鉴权、RPC Gateway 角色限制、Go/Python SDK、vectorctl 断点及大于 2^53 的整数序号不丢精度。

模块 UT：subscription_test.go、patch_links_test.go、subscription_handler_test.go，以及 RPC/CLI/Python SDK 测试。端到端：`.venv/bin/python scripts/verify_subscriptions.py`，依赖已构建的 .tools/vector-server/vectorctl、本地 PD/TiKV/SeaweedFS 和 Python gRPC/boto3。报告为 reports/subscriptions.json。

## 消费保留与回收验收

- AC115：有效 data/graph 消费租约阻止清理；ACK 单调且不越过 head；释放、过期、新 session/epoch 与旧会话隔离符合协议。
- AC116：真实 TiKV 验证并发 acquire 与清理双向冲突，过期位置无法通过旧事务重新注册；对象采用与回收标记也产生事务冲突。
- AC117：图日志由检查点覆盖后才能清理，S3 对象随清理被删除，检查点保留；无租约旧游标收到 reset_required 和 checkpoint_position。
- AC118：落后副本跨图日志 floor 从快照恢复，继续追增量，并在 Leader 被 SIGKILL 后接管写服务。
- AC119：上传失败留下登记意图；孤儿意图过期后回收；删除失败可重试；迟到 PUT 不能重新被采用，后续 sweep 再次删除。
- AC120：启用保留模式的 32 分片七服务流水线覆盖 CRUD、迁移、主备切换、checkpoint/prune、重建和重启；读取类请求在路由切换时有界刷新，维护操作不自动重试。

执行 `bash scripts/acceptance-retention.sh`。该脚本包含全量 Go race/真实 TiKV 集成、vet、七服务流水线、Python SDK 和真实 S3/进程故障验收。报告位于 reports/retention-*.log/json，测试子进程负责清理。仍未覆盖物理多主机掉电或 TiKV/SeaweedFS 自身高可用故障。

## 聚类路由与来源完成水位

运行 `bash scripts/acceptance-dispatch.sh`；配置及报告见 [CLUSTER_ROUTING.md](CLUSTER_ROUTING.md)。

| Case | 预期与覆盖 |
|---|---|
| AC-121 | 4 数据分区到 3 聚类索引，稳定选簇、平局规则、拒绝原地修改布局 |
| AC-122 | 全部 Worker 暂停时原始写入仍返回，恢复后有界窗口追赶 |
| AC-123 | 在途重复与已提交重投不重复构图；提交失败不推进来源水位 |
| AC-124 | 空洞阻止连续前缀推进，无关目标不阻塞；来源重启跳过已提交窗口 |
| AC-125 | 真实 TiKV 中来源租约撤销使在途图提交冲突，图与来源水位无部分成功 |
| AC-126 | 跨簇更新、删除、旧位置清理、回迁新版本保护、主从图一致 |
| AC-127 | data/graph 独立订阅和 ACK、独立数据快照、GC 后来源与索引接管 |
| AC-128 | 七服务单进程覆盖双 Gateway、主备 Master、Worker 排空/重启及重建 |
