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
| 真实 TiKV | 提交图引用和 source commit 前并发撤销来源租约 | 事务被 fencing，无半提交的图日志或完成凭证 |
| 单 PID 七服务 | 2 Gateway、2 Master、3 Worker；CRUD/CAS/分页/过滤、主切换、排空、重建、Gateway/Worker 重启 | 无结果遗漏/重复，版本、距离、属性正确；非 Worker 不加载 HNSW |
| 单 PID 七服务聚类模式 | 4 Data → 3 Index；两轮全 Worker 停止，每轮 144 条结构化写入；跨簇移动→删除→恢复；快照/GC 后再故障 | 原始读写继续，跨批次恢复；幂等凭证仍返回原结果；383 条最终记录逐条核对；积压和副本滞后归零 |
| 多进程 | 所有 Worker SIGSTOP 后业务写入；旧 owner 长暂停→租约失效→新 owner 接管→恢复旧 owner | 写入不等待构图；旧 epoch 无法覆盖新状态；来源位点不倒退 |
| 多进程 | 三轮 Master 与 Worker 同时 SIGKILL，其中一轮两个 Worker 同时故障；4 路并发写入、同 ID CAS 竞争、重试、跨簇更新/删除/恢复；每轮先快照/GC | 原始数据读写成功；恰好一个 CAS 成功、一个冲突；每轮恢复后完整 oracle；接管 epoch 增长；精确结果与 ANN recall 检查 |
| 多进程旧模式 | 消费 lease pin/ACK/release、图对象 GC、落后副本快照恢复、GC 后 Leader 故障 | 不清理被租约保护的日志；reset 可恢复；图副本可继续构建 |
| 观测 | 鉴权、指标格式与数值、位点、队列、错误/去重计数、故障积压及恢复收敛 | 故障不能显示为零积压的假健康；指标能被 Prometheus parser 读取；RPC uint64 不经 float64 |
| 时延诊断 | Histogram 并发采样和失败归属、重投不重置队列年龄、清空后年龄归零、实时应用位点、TiKV 失败时局部指标仍可读 | UT 校验桶/计数/累计值及接口鉴权；onebox 记录停写后图提交、发布、内存应用、心跳上报和完成位点的时间线 |
| 事件消费 | 空队列与入队竞态、通知合并、积压连续处理、错误退避、Follower 转 Leader、持续输入取消、慢分片隔离、等待中的分片退休、不确定提交后主动发布 | UT/race 验证无丢失唤醒；聚类七服务禁用旧定时 SyncOwned；真实 onebox 断言事件消费者有执行且没有后台 worker_sync 样本 |
| 来源唤醒与合批 | 多来源通知合并、鉴权、窗口完成后继续取数、空来源不自旋；有界取批与来源轮转 | 通知不推进水位；跨来源合批只有一条图日志，所有来源同时提交或全部失败；Leader/Follower 图结构相同 |
| 对象启动就绪 | PUT/GET/校验/清理失败、取消、多副本并发启动 | 桶存在不能绕过数据路径检查；失败不缓存就绪、成功检查只执行一次、probe 不触及业务对象 |
| 并发图提交 | 确定写冲突的有限重试、取消、对象复用、重试期间来源撤销；TiKV 原子提交快速路径及强制 2PC | 只有确定未提交的冲突可重试；每次重新检查 fencing；被撤销来源不得提交图日志或来源完成水位 |
| 来源提交锁 | 键排序/去重、原快照 forUpdateTS、NOWAIT 与有限重试；真实 TiKV 的锁竞争、并发撤销和陈旧快照 | 持锁期间撤销不能越过图事务；锁占用必须在图 prewrite 前返回；新事务拒绝失效 owner，旧快照不能通过新锁读取陈旧 fencing token |
| 查询路由长尾 | 真实 TiKV secondary prewrite 锁、计划读取超时、长期闲置提示/计划失效、并发刷新、owner 被撤销、调用者取消 | 最近计划只能辅助选副本；仍重新校验 owner；维护调用不能使用缓存；底层超时和回退次数均可观测；锁清理后刷新到新计划 |
| 脚本自测 | 非零退出、超时、缺报告、旧成功报告、cleanup=false、依赖启动失败 | 所有失败都向上传播，不能误报 passed |
| 受限容量探索 | 固定种子高维簇数据、精确距离/召回 oracle、资源停止线、采样失败、仅停止本轮进程、仅删除标记目录 | 44 个 harness UT 包含资源/清理保护；每档真实启动独立服务，任一一秒目标失败即不再扩档；只记录已测试容量范围 |

七服务各实例通过真实 HTTP 通信；TiKV/S3 是外部服务。独立进程场景才使用 OS SIGSTOP/SIGKILL。数据按固定簇中心、渐变偏移、跨簇更新构造，不是纯随机向量。

专项时延验收可运行 `bash scripts/acceptance-onebox-e2e.sh --latency-only --steady-seconds 600`。报告使用负载前后逐进程直方图差分，拒绝混入重启计数和缺失采样，详见 [PIPELINE_LATENCY.md](PIPELINE_LATENCY.md)。

一秒可见性门禁：`bash scripts/acceptance-onebox-freshness.sh --freshness-rounds 20 --load-seconds 5`。它单独启动真实进程，等待存储、来源 owner、查询副本和构图 Publisher 就绪，检查 640 条初始化批量和 20 次停写；每轮最后都包含插入、更新、删除。启动等待单独记录，业务 t0 为最后一次持久化写 ACK；要求所有 Index 发布目标来源水位、全部在役副本在当前 owner epoch 下实际应用，且每份副本和两个 Gateway 的普通查询均通过全量 ID/revision/属性/距离 oracle。查询不带隐式同步参数，心跳进度不作为完成条件。任何轮次超过 1 秒，报告和退出码均失败；即使之后追平，也不能改算通过。各轮保留原始轨迹、来源 LSN 目标、图目标、阶段耗时、硬件/负载条件和 N/P50/P95/P99/max。故障恢复另由完整 onebox 评估，不与健康停写样本混合。

## CI 与边界

2026-11-17 受限容量探索仅改验收工具，生产 Go 代码和二进制未变；全部 44 个 Python harness UT 通过，五个正式档位共 25 轮健康停写检查通过。没有在本轮重跑完整故障矩阵或 Go 门禁；下方保留生产二进制上一轮完整验收结果。具体资源预算、读写比例和小样本限制见 [PIPELINE_LATENCY.md](PIPELINE_LATENCY.md)。

事件消费最终回归：`20261116T150019Z-d62292a0`，完整 smoke **10/10 阶段通过**。包括全项目 Go UT/race/coverage、vet、35 个 Python harness UT、5 个 Python SDK UT、真实 TiKV/S3 集成、四种模式的七服务流水线、服务端/CLI 构建、独立进程投递故障和订阅/GC 故障。聚类七服务最终核对 383 条记录，多进程投递故障核对 246 条；所有测试实例和子进程已清理。最终 smoke 构建的二进制 SHA-256 与正式可见性和完整 onebox 使用的二进制一致。

独立真实进程验收也已完成：`20261116T223954-201993cf` 的初始批量和 20 轮稳态可见性全部在 1 秒内；`20261116T224624-a0ac0ab9` 的 10 分钟常稳、故障矩阵、6 轮连续故障、原目录冷恢复和恢复后常稳共 12 组 case 全部通过。运行条件、分位数、历史失败记录及尚未覆盖的大规模/跨机边界见 [PIPELINE_LATENCY.md](PIPELINE_LATENCY.md) 和 [ONEBOX_E2E.md](ONEBOX_E2E.md)。这些报告对应提交前的工作区，原始 commit/dirty 字段保持不改写。

以下为事件消费优化前的历史验收记录：

2026-11-16 本地执行 `bash scripts/acceptance-dispatch.sh`，10 个阶段全部通过；报告运行 ID 为 `20261116T103659Z-263fda42`（本地路径 `reports/quality/<运行ID>/report.json`）。四种七服务模式全部通过；聚类单 PID 场景最终核对 383 条记录，多进程反复故障场景最终核对 246 条记录；18 个 Python harness UT、5 个 Python SDK UT 通过，测试启动的全部服务/子进程已停止。RPC 生成物可重复生成且无差异，CI YAML 与 Compose 配置通过本地解析校验。

`.github/workflows/quality.yml` 在 push main、PR 和手工触发时分别运行 unit/smoke，并检查 Go 格式与生成的 RPC 代码一致性；Linux smoke 用 `compose.ci.yaml` 启动独立 PD/TiKV 8.5.8、SeaweedFS 4.53。失败也上传日志和报告、停止容器。分支保护是否将这些检查设为必需，需要在 GitHub 仓库设置中配置；仓库文件本身不能强制禁止绕过合并。

本轮本地验证使用真实 macOS TiKV/SeaweedFS；GitHub Ubuntu 容器任务的执行结果应以远端 Actions 为准。它们不是跨机网络分区、时钟漂移、磁盘损坏、对象存储宕机、TiKV 多节点失去多数派或长时间生产压测。故障恢复期间最近邻查询可以暂时失败；本轮保证恢复后的完整性，不声称同时失去多个副本仍能无中断查询。
