# Review、故障验收与质量门禁

最新聚类投递实现的 review 覆盖：来源窗口恢复、连续完成位点、稀疏投递去重、入队/构图/事务提交边界、主备接管、快照与 GC、HTTP/gRPC 状态、验收脚本自身的失败处理。

## 本次修复

| 问题 | 影响 | 修复及自动化证据 |
|---|---|---|
| 完成位点事务实际提交，但客户端收到错误，内存窗口未裁剪 | 下次 tick 读到新 completed，但窗口仍停在旧事件，后续日志不再投递 | 每轮按持久化 completed 清理缓存，断续缓存从日志重建；`TestDispatchRecoversUncertainCompletedCommit` 注入“提交成功但返回错误”，修复前稳定失败 |
| 接收端只检查事件递增，没有验证目标范围完整 | 漏掉同一目标的早期事件、改写载荷或伪造尾部事件也能入队，推进 source commit 后可能掩盖遗漏 | 入队时与同一 TiKV 快照中的有界来源日志比对，按固定路由重算目标子集；`TestRoutedRejectsIncompleteOrChangedDelivery` 三种变体修复前全部失败 |
| 队列持有调用方的切片/属性引用，取出时重新编码计算大小 | 入队后内容可能改变，且持锁重复 JSON 编码 | 入队取得记录所有权并缓存编码大小；测试修改原请求、重试与字节计数归零 |
| 流水线把 source completed 当作全部副本可查询 | Follower 允许落后，造成全量结果断言偶发失败 | 全量 oracle 前检查所有来源完成、committed=visible、两份副本的完整 GraphPosition 一致；Python harness 单测覆盖缺副本、落后、未发布和过期 owner |
| 状态中保留旧 owner，不能仅用 owner 非空判断可用 | 故障时监控可能把过期 dispatcher 当成可用 | 增加 HTTP/gRPC `owner_available`，来源于租约解析结果；指标区分历史 epoch 与当前可用性 |
| 原有消费租约测试用 acquire 时的旧到期时间模拟 ACK 后过期 | ACK 已续租，测试受毫秒级耗时影响而偶发失败 | 使用最新 ACK 返回的持久化到期时间；故障在完整回归中被捕获，修复后针对测试重复 30 次通过 |

日志校验只在接收端入队时执行，一次最多 32 条来源日志、2 MiB；构图前与最终提交仍检查 source epoch、Index Leader epoch 和 commit 边界。去重已提交批次无需重新读取已 GC 的日志，不引入每事件路由表或额外 Index 输入日志。成功返回给业务方的条件仍然是原始数据事务持久化。

## 运行入口

```bash
# 不需要 TiKV/S3；需要 Go/C++ 工具链、Python SDK 依赖。
.venv/bin/python scripts/quality.py --lane unit

# 完整 smoke：使用已经启动的本地 PD/TiKV/SeaweedFS。
bash scripts/acceptance-dispatch.sh
```

`unit` 包含全部 Go UT + race + coverage、vet、Python SDK 和验收脚本 UT。`smoke` 在此基础上运行真实存储集成、四种模式的七服务流水线、独立进程聚类故障矩阵和旧模式订阅/GC 回归。旧的 `scripts/acceptance.sh` 仍保留全项目扩展验收入口。

每次产生独立的 `reports/quality/<UTC时间>-<随机ID>/report.json`、阶段日志、coverage 和本次验收 JSON 副本；`reports/quality/latest.json` 指向最近一次结果内容。报告记录 commit、dirty 标记、命令、耗时、退出码和失败原因。门禁会删除阶段声明的旧报告，缺少新报告、非 passed、清理失败、子命令失败、超时都不能算通过。工作区文件锁阻止两个 quality runner 互相覆盖共享二进制和旧脚本报告。

每个阶段运行在独立进程组；完成、失败、超时或 SIGINT/SIGTERM 后清理该组内的子孙进程，包括被 SIGSTOP 暂停的 Worker。外部 SIGKILL 无法被捕获；此时 `running` 报告不代表通过。不要同时运行其他直接调用旧验收脚本的任务，它们不遵守 quality runner 的工作区锁。

## 故障矩阵

需要连 PD/TiKV/SeaweedFS 一起启动、暂停和强杀时，使用新增的 [整套进程 onebox 验收](ONEBOX_E2E.md)：`bash scripts/acceptance-onebox-e2e.sh`。它独立于默认 smoke，默认包含 10 分钟常稳、6 轮连续故障和原目录全栈冷恢复；以独立端口和数据目录运行。

| 层级 | 场景 | 必须满足的结果 |
|---|---|---|
| Go UT | 完成位点不确定提交；失败事务；重复批次；源 epoch 失效；乱序/缺失/篡改事件；缓存丢失 | 不跳过日志、不错误推进进度、可重试恢复、图构造不重复 |
| Go UT | 穷举不同来源目标 ACK 组合，包含广播删除 | 只推进连续完成前缀，不能跨过最早未完成事件 |
| 真实 TiKV | 已接受来源 epoch 后撤销全局租约；更高 epoch 在图提交快照之后持久化 | 租约撤销本身不阻塞已接受批次；Index 接受新 epoch 后旧快照写冲突，重试返回结构化 epoch 拒绝 |
| 单 PID 七服务 | 2 Gateway、2 Master、3 Worker；CRUD/CAS/分页/过滤、主切换、排空、重建、Gateway/Worker 重启 | 无结果遗漏/重复，版本、距离、属性正确；非 Worker 不加载 HNSW |
| 单 PID 七服务聚类模式 | 4 Data → 3 Index；两轮全 Worker 停止，每轮 144 条结构化写入；跨簇移动→删除→恢复；快照/GC 后再故障 | 原始读写继续，跨批次恢复；幂等凭证仍返回原结果；383 条最终记录逐条核对；积压和副本滞后归零 |
| 多进程 | 所有 Worker SIGSTOP 后业务写入；旧 owner 长暂停→租约失效→新 owner 接管→恢复旧 owner | 写入不等待构图；旧 epoch 无法覆盖新状态；来源位点不倒退 |
| 多进程 | 三轮 Master 与 Worker 同时 SIGKILL，其中一轮两个 Worker 同时故障；4 路并发写入、同 ID CAS 竞争、重试、跨簇更新/删除/恢复；每轮先快照/GC | 原始数据读写成功；恰好一个 CAS 成功、一个冲突；每轮恢复后完整 oracle；接管 epoch 增长；精确结果与 ANN recall 检查 |
| 多进程旧模式 | 消费 lease pin/ACK/release、图对象 GC、落后副本快照恢复、GC 后 Leader 故障 | 不清理被租约保护的日志；reset 可恢复；图副本可继续构建 |
| 观测 | 鉴权、指标格式与数值、位点、队列、错误/去重计数、故障积压及恢复收敛 | 故障不能显示为零积压的假健康；指标能被 Prometheus parser 读取；RPC uint64 不经 float64 |
| 时延诊断 | Histogram 并发采样和失败归属、重投不重置队列年龄、清空后年龄归零、实时应用位点、TiKV 失败时局部指标仍可读 | UT 校验桶/计数/累计值及接口鉴权；onebox 记录停写后图提交、发布、内存应用、心跳上报和完成位点的时间线 |
| 事件消费 | 空队列与入队竞态、通知合并、积压连续处理、错误退避、Follower 转 Leader、持续输入取消、慢分片隔离、等待中的分片退休、不确定提交后主动恢复回放 | UT/race 验证无丢失唤醒；聚类七服务禁用旧定时 SyncOwned；真实 onebox 断言事件消费者有执行且没有后台 worker_sync 样本 |
| 来源唤醒与合批 | 多来源通知合并、鉴权、窗口完成后继续取数、空来源不自旋；有界取批与来源轮转 | 通知不推进水位；跨来源合批只有一条图日志，所有来源同时提交或全部失败；Leader/Follower 图结构相同 |
| 对象启动就绪 | PUT/GET/校验/清理失败、取消、多副本并发启动 | 桶存在不能绕过数据路径检查；失败不缓存就绪、成功检查只执行一次、probe 不触及业务对象 |
| 并发图提交 | 确定写冲突的有限重试、取消、对象复用、重试期间 Index Leader 撤销；TiKV 原子提交快速路径及强制 2PC | 只有确定未提交的冲突可重试；每次重新检查 fencing；被撤销 Index Leader 不得提交；来源 completed_lsn 仍校验来源租约 |
| Index 来源 epoch | 控制日志、已完成批次升级、未知提交、入队/构图中拒绝、检查点/清理/切主；真实 TiKV 持有共享来源锁和 GC 竞争 | 两个 Index 并发提交不依赖来源锁；新屏障或 GC 后旧图事务失败；屏障不推进数据完成位点，不改 HNSW，完整恢复后不回退 |
| 查询路由长尾 | 真实 TiKV secondary prewrite 锁、计划读取超时、长期闲置提示/计划失效、并发刷新、owner 被撤销、调用者取消 | 最近计划只能辅助选副本；仍重新校验 owner；维护调用不能使用缓存；底层超时和回退次数均可观测；锁清理后刷新到新计划 |
| 脚本自测 | 非零退出、超时、缺报告、旧成功报告、cleanup=false、依赖启动失败 | 所有失败都向上传播，不能误报 passed |
| 受限容量探索 | 固定种子高维簇数据、精确距离/召回 oracle、资源停止线、采样失败、仅停止本轮进程、仅删除标记目录 | 44 个 harness UT 包含资源/清理保护；每档真实启动独立服务，任一一秒目标失败即不再扩档；只记录已测试容量范围 |
| 分桶可见性与错误归因 | 稳定互斥 ID 桶、超过 1,000 条的全量覆盖、空桶/删除残留、revision/属性/距离错误、额外副本、前后成员变化、禁止跨轮拼接结果、真实 HTTP 状态/响应体及关联日志、计划移除与本地退休分开 | 新增 7 个 harness UT；保持服务 K≤1000、请求 3 秒超时和最后 ACK 起一秒门槛；未知或当前成员错误不可由重试成功掩盖 |
| 探针开销与诊断边界 | 并发日志完整性、单句柄有界缓冲、写/刷新失败、刷新纳入完成时钟、晚到的目标发现、位点已达标但查询仍错误、早期指标失败后恢复、完整 uint64 位点、指标 HTTP 错误证据 | 新增 8 个 harness UT，总计 59 个；完整查询与最终成员检查始终保留，辅助位点不能替代一秒门禁，完整直方图仍做前后差分 |
| 数据事务合批 | 同 ID 多 revision、批内重复 request_id/跨 ID 冲突、CAS 隔离、读写失败、未知提交重试、取消、关闭、提交前不得 ACK | UT 验证一批只提交一次、LSN 连续、无部分持久化；真实 TiKV 模拟两个 Gateway 并发 CAS/幂等/不同 ID 写入 |
| 批量图回放与发布 | 元数据批读、记录数/字节上限、乱序下载、顺序 apply、下载错误保留连续前缀、取消 join、损坏 hash/缺日志/错误目标、Leader delta 复用 | UT/race 验证完整图/记录/RNG 一致；图增量先验证，持久化后即可回放；未知提交可恢复、失效 owner 不可追加；同分片 32 请求突发场景保留原一秒门槛 |
| 持久化即回放 | 单条尾日志在 Leader apply 前由 Follower 查询、无额外 head/ACK 写入、非法增量不 ACK、未知提交、全量 GC 后继续追加、来源水位与日志 hash 绑定、旧协议隔离 | UT/race；真实 TiKV 验证反向尾读的 MVCC 快照、并发追加唯一胜者及提交期间撤销 Leader；RPC 验证 sources 的大 uint64 不丢精度 |
| 调度与上报并发 | 多分片 drain 时 Master 计划事务与 Worker task fence 上报持续写冲突 | 最多 4 次确定写冲突重试、短随机退避、新事务重读节点/计划/fence；未知提交、语义错误和已撤销 Master 不重试 |

七服务各实例通过真实 HTTP 通信；TiKV/S3 是外部服务。独立进程场景才使用 OS SIGSTOP/SIGKILL。数据按固定簇中心、渐变偏移、跨簇更新构造，不是纯随机向量。

专项时延验收可运行 `bash scripts/acceptance-onebox-e2e.sh --latency-only --steady-seconds 600`。报告使用负载前后逐进程直方图差分，拒绝混入重启计数和缺失采样，详见 [PIPELINE_LATENCY.md](PIPELINE_LATENCY.md)。

一秒可见性门禁：`bash scripts/acceptance-onebox-freshness.sh --freshness-rounds 20 --load-seconds 5`。它单独启动真实进程，等待存储、来源 owner、查询副本和构图 Publisher 就绪，检查 640 条初始化批量和 20 次停写；每轮最后都包含插入、更新、删除。启动等待单独记录，业务 t0 为最后一次持久化写 ACK；要求所有 Index 发布目标来源水位、全部在役副本在当前 owner epoch 下实际应用，且每份副本和两个 Gateway 的普通查询均通过全量 ID/revision/属性/距离 oracle。查询不带隐式同步参数，心跳进度不作为完成条件。任何轮次超过 1 秒，报告和退出码均失败；即使之后追平，也不能改算通过。各轮保留原始轨迹、来源 LSN 目标、图目标、阶段耗时、硬件/负载条件和 N/P50/P95/P99/max。故障恢复另由完整 onebox 评估，不与健康停写样本混合。

## CI 与边界

2026-11-17 持久化即回放协议：完整 smoke `20261117T115000Z-2a8015f7` **10/10 通过**。覆盖全项目 UT/race、vet、61 个 harness UT、5 个 SDK UT、真实 TiKV/SeaweedFS、四种七服务模式、独立进程投递和保留故障，以及 server/vectorctl 构建；所有测试实例均已清理。针对新持久化/观测语义的补充 race 复核也通过。首次 smoke `20261117T114627Z-82db5b21` 因本机存储未运行/未就绪而失败，失败报告保留；启动本轮专用存储后重新运行全部门禁。协议和兼容边界见 [GRAPH_JOURNAL.md](GRAPH_JOURNAL.md)。同参数的原生 2,000 条／64 维、三轮并发突发复测通过，完整可见性 605 ms（初始化）与 908/962/564 ms（常稳），业务和探针零错误；内部冲突重试如实保留。记录见 [PIPELINE_LATENCY.md](PIPELINE_LATENCY.md)。

2026-11-17 日志合批/并发回放：完整 smoke `20261117T103038Z-b5a3b23c` **10/10 通过**，包括所有 Go UT/race、vet、61 个 harness UT、5 个 SDK UT、真实存储、四种七服务模式、两个独立进程故障验收及 server/vectorctl 构建。新增数据队列背压/取消测试随后单独 race 重复 10 次通过，生产代码与 smoke 二进制相同。真实 640 条小档和 2,000 条/64 维、三轮同分片 32 并发请求的对照通过；突发写及读回中位耗时约 185→110 ms，完整可见性为 860/935/562 ms，仍在原一秒门槛内，但可见性 P50 没有改善。首次 smoke 的 drain 超时、旧版本对照第三轮超一秒及内部重试均保留，详见 [PIPELINE_LATENCY.md](PIPELINE_LATENCY.md#合批版本的性能与验收结果)。

2026-11-17 探针 profile 优化：59 个 Python harness UT 通过。先跑 640 条小档，再跑 2,000 条 / 64 维短冒烟，最后在同容量、20 次/秒下完成五轮各 8 秒的并发读写复测；初始化核验 598 ms，常稳 P50 553 ms、最大 601 ms，全部通过原一秒门槛。日志刷新、全部桶/副本/Gateway 查询与最终成员检查仍共用同一个 t0。服务二进制未变，四次本阶段测试实例和临时数据均已清理；完整故障矩阵与更大容量未重测。原始证据、测量开销及小样本边界见 [PIPELINE_LATENCY.md](PIPELINE_LATENCY.md#优化后的真实进程复测)。

此前的分桶/诊断验证：51 个 Python harness UT、`internal/vector` 模块 Go race 回归及 vet 通过。640 条 / 64 维小档和独立退休诊断通过；2,000 条 / 64 维初始化完整核验正确但耗时 1.191 s，按一秒门槛判定失败，后续稳态轮次未执行。失败报告保留，全部临时实例及数据清理完成。该轮生产改动仅增加可选的内部搜索错误关联日志，没有重跑完整故障矩阵。

此前同日的受限容量探索仅改验收工具，生产 Go 代码和二进制未变；当时 44 个 Python harness UT 通过，五个正式档位共 25 轮健康停写检查通过。该轮没有重跑完整故障矩阵或 Go 门禁；以下保留更早的完整验收结果。

事件消费最终回归：`20261116T150019Z-d62292a0`，完整 smoke **10/10 阶段通过**。包括全项目 Go UT/race/coverage、vet、35 个 Python harness UT、5 个 Python SDK UT、真实 TiKV/S3 集成、四种模式的七服务流水线、服务端/CLI 构建、独立进程投递故障和订阅/GC 故障。聚类七服务最终核对 383 条记录，多进程投递故障核对 246 条；所有测试实例和子进程已清理。最终 smoke 构建的二进制 SHA-256 与正式可见性和完整 onebox 使用的二进制一致。

独立真实进程验收也已完成：`20261116T223954-201993cf` 的初始批量和 20 轮稳态可见性全部在 1 秒内；`20261116T224624-a0ac0ab9` 的 10 分钟常稳、故障矩阵、6 轮连续故障、原目录冷恢复和恢复后常稳共 12 组 case 全部通过。运行条件、分位数、历史失败记录及尚未覆盖的大规模/跨机边界见 [PIPELINE_LATENCY.md](PIPELINE_LATENCY.md) 和 [ONEBOX_E2E.md](ONEBOX_E2E.md)。这些报告对应提交前的工作区，原始 commit/dirty 字段保持不改写。

以下为事件消费优化前的历史验收记录：

2026-11-16 本地执行 `bash scripts/acceptance-dispatch.sh`，10 个阶段全部通过；报告运行 ID 为 `20261116T103659Z-263fda42`（本地路径 `reports/quality/<运行ID>/report.json`）。四种七服务模式全部通过；聚类单 PID 场景最终核对 383 条记录，多进程反复故障场景最终核对 246 条记录；18 个 Python harness UT、5 个 Python SDK UT 通过，测试启动的全部服务/子进程已停止。RPC 生成物可重复生成且无差异，CI YAML 与 Compose 配置通过本地解析校验。

`.github/workflows/quality.yml` 在 push main、PR 和手工触发时分别运行 unit/smoke，并检查 Go 格式与生成的 RPC 代码一致性；Linux smoke 用 `compose.ci.yaml` 启动独立 PD/TiKV 8.5.8、SeaweedFS 4.53。失败也上传日志和报告、停止容器。分支保护是否将这些检查设为必需，需要在 GitHub 仓库设置中配置；仓库文件本身不能强制禁止绕过合并。

本轮本地验证使用真实 macOS TiKV/SeaweedFS；GitHub Ubuntu 容器任务的执行结果应以远端 Actions 为准。它们不是跨机网络分区、时钟漂移、磁盘损坏、对象存储宕机、TiKV 多节点失去多数派或长时间生产压测。故障恢复期间最近邻查询可以暂时失败；本轮保证恢复后的完整性，不声称同时失去多个副本仍能无中断查询。

引用日志与自动检查点验收：`scripts/verify_reference_checkpoint.py` 已纳入 smoke，启动 2 Master、2 Gateway、3 Worker，分别用小字节阈值和短年龄阈值验证自动检查点、精确来源覆盖、graph 租约连带保护数据日志、自动 GC、空闲不重复快照，以及全部 Worker 被杀后的完整快照恢复和尾部写入。生产默认 30 MB / 3 分钟由常量/阈值单测验证；集成缩短阈值以保持流水线有界。

本轮专门回归：异步上传后检查点发布与构图 fencing 的写冲突，允许有限重试但只能上传一次；提交结果未知不能重算或重复追加；空闲检查点复用仍检查 Publisher 权限。真实 TiKV 集成验证图条目与跨多个 256 KiB 载荷块原子提交，以及竞争追加/旧 Leader fencing。

2026-11-17 引用日志验收汇总：`reports/reference-graph-20261117/validation.json`。包含最终全包 race / vet、真实存储、四种七服务流水线、多进程故障/租约回收、最终自动检查点两种触发方式以及同参数并发性能复测。早期失败报告保持原样；验收脚本修正和 GC 读取优化后的专项通过记录独立保存，不覆盖历史失败。


2026-11-18 v13 来源 epoch 实现的完整 smoke（11 阶段）通过：`reports/quality/20261118T034655Z-97d6a675/report.json`。最终 race/vet、真实 TiKV epoch/GC 竞争、重复性能试验及清理证据汇总在 `reports/source-epoch-20261118/validation.json`；停写追平比较见 [PIPELINE_LATENCY.md](PIPELINE_LATENCY.md)。历史 v12 失败样本保留，未降低验收门槛。

## v14：租约内缓存控制状态

本轮 review 把 owner、Publisher 计划、图头和重复 Claim 从正常构图批次移到控制/恢复路径。Master 必须等待已发放 Publisher lease 到期才切主；主动迁移显式进入 lease_draining，普通续租不争用图提交 guard。

新增保护重点：未知旧提交与恢复通过同一个 TiKV guard 建立顺序；迟到请求在切主后被 storage fencing 拒绝；相同任期续租不等待本地构图锁，避免大批次或重建阻塞全 Worker 心跳；本地缓存只接受已确认提交，维护追加后重新恢复。精确数据日志校验、来源 epoch 屏障、重复投递去重、检查点覆盖和 GC 规则仍保留。

`index_lease_test.go` 和 `index_lease_integration_test.go` 自动进入现有门禁。热路径测试在 transaction 接口层禁止 owner/plan/head 读取与尾部反向扫描，并经过内部 RPC 和 consumer 入口；真实 TiKV 测试覆盖 1PC/2PC 迟到提交、续租/迁移竞争和未知提交恢复。可观测性测试要求 TiKV 断开时仍能返回本地 lease/cache 指标。升级协议拒绝 v13 namespace，不能直接修改 cluster-mode 绕过检查。
