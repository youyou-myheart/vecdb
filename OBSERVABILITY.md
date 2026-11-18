# 聚类投递与图复制观测

启用 `CLUSTER_ROUTING_FILE` 时，数据按 ID hash 进入 DataPartition，向量按配置的最近簇中心进入 IndexPartition，二者数量独立。当前为不可变的固定中心路由，不是自动学习簇或在线 split/merge。默认模式仍使用一一对应布局。

## 获取状态

所有逻辑角色支持 `GET /metrics`，使用现有 API Token，输出 Prometheus text 0.0.4。单机非集群服务返回 501，状态读取失败返回 503；鉴权失败返回 401。指标不输出 token、向量 ID、向量内容或错误文本。

时延诊断新增 `GET /metrics?scope=local`，只读进程内指标，不依赖 TiKV。阶段 Histogram、最老队列年龄、内存实时应用位点以及 10 分钟 onebox 追平时间线见 [PIPELINE_LATENCY.md](PIPELINE_LATENCY.md)。各 Worker 必须单独采集，Gateway 不汇总 Worker 的进程内时延。

聚类 Index Leader 使用入队通知驱动连续消费，空队列阻塞等待。Follower 每轮成功回放使图位点前进后，立即继续读取最新图日志；直到一次读取没有推进位点才等待 100 ms，再轮询。`vecdb_index_consumer_cycles_total{index_partition,reason}` 中 `continue` 为 Leader 连续处理，`follower_continue` 为 Follower 回放后的立即重读，`follower_poll` 仅为空闲等待后的轮询，`retry` 为失败退避后的重试；另有启动与通知唤醒。`vecdb_index_consumer_waiting` 显示是否正在等待，`index_consumer_work` / `index_consumer_idle` 阶段记录工作与等待时间。正常空闲 Leader 不应持续产生周期消费；Follower 不会因残留的 Leader 输入队列而空转。

`vecdb_graph_commit_conflict_retries_total{index_partition}` 记录复用已持久化对象的确定写冲突重试。`graph_commit_attempt` 阶段的 error 表示单次事务冲突等错误，`graph_commit` 的 error 才表示整批提交最终失败；业务请求成功率另行观察，不能将这两种错误计数相加。

v13 聚类协议已删除共享来源锁，正常不再有 `graph_source_lock` 样本；旧报告中的该阶段保留用于比较。v13 的 `source_epoch_admission` 包含持久化 epoch 读取、请求校验和必要时的控制条目提交；v14 相同 epoch 从内存读取，整体成本看 `index_admission`。`dispatch_notify` 记录异步来源提示的 owner 解析与网络发送时间；失败只表示提示没送达，来源仍由持久化日志和控制巡检恢复。`vecdb_graph_local_owner_epoch` 必须与同份副本的 `vecdb_graph_local_applied_sequence` 配对，避免把旧 owner 的内存位点误认为新副本进度。

查询超时可以结合 `route_plan`、`route_owner` 和 `replica_rpc_search` 阶段定位：分别对应计划读取、owner 解析、单次 Worker 查询调用，避免把 TiKV 元数据锁等待误判为 HNSW 计算慢。

验收探针通过内部搜索请求的 `X-Vecdb-Probe-ID`（32 位十六进制）关联失败证据。Worker 在已鉴权、已解码的请求上回传该 ID，搜索失败时写 `partition_rpc_probe` 日志，包含分片、epoch、HTTP 状态和错误码，不写 token、向量或属性；分片名转义且限长，ID 不作为指标标签。探针在 `*-requests.jsonl` 保存相同 ID、时序、真实状态码及有界响应体，并在查询前后检查成员。单凭重试成功、成员变化或 HTTPError 类型不能认定是正常 fencing，历史缺失信息应明确标注；审计方法见 [PIPELINE_LATENCY.md](PIPELINE_LATENCY.md#探针错误审计与分桶核验)。

探针自身也有开销观测：`profiling.request_journal` 给出文件打开、编码、锁等待、写入和刷新；逐请求保留线程池排队及 HTTP/JSON 阶段，`scrape_profile` 给出各端点的指标采集和解析时间。计时窗口中的 `parse_mode=progress_only` 只解析实际应用位点和 epoch，完整阶段指标仍在负载前后采集。`first_frontier_observed_seconds` 是辅助位点观测上界，不能替代 `both_seconds` 的完整查询、成员核验及日志刷新门槛；所有这些时长使用单调墙钟。

`vecdb_route_plan_fallback_total{index_partition}` 记录图查询在读取计划超时后使用上次成功读取的路由提示的次数。该值上升即使业务零错误也说明元数据读取存在长尾；缓存只提供地址，仍逐次验证当前 owner 租约和 epoch，不能被视为跳过 fencing 的成功。

```bash
curl -H "Authorization: Bearer $API_TOKEN" http://127.0.0.1:18202/metrics
curl -H "Authorization: Bearer $API_TOKEN" http://127.0.0.1:18202/v1/cluster
.tools/vectorctl cluster status
```

地址随本地集群端口参数变化。Python `client.cluster()`、Go SDK GetCluster 和 vectorctl 的集群状态保留精确 uint64 位点，并新增 `data_progress[data-NNNN].owner_available`。`owner` 可能保留已过期 owner 的 epoch、错误和身份，是否可用必须看 `owner_available`。

## 指标语义

日志合批新增 `vecdb_pipeline_batch_items{stage,partition,outcome}` Histogram，`stage=data_commit` 表示每次数据组提交尝试的请求数（包含幂等和语义冲突请求），`source_heads` 表示一轮来源进度读取的 Index head 数，`graph_replay` 表示一个图回放窗口的日志数。`sum/count` 是平均批量，不能把请求数当成新增 LSN 数；与 `vecdb_index_batch_records` 一起看是否实际形成合批。

阶段计时新增 `data_group_queue`（已入队到开始处理，不包含入队前背压等待）、`data_group_commit`（包含批读、事务构造和确定写冲突重试）、`graph_replay_batch`（包含并发下载和顺序 apply）、`graph_prepare_delta`（持久化之前的 apply/rollback 验证）。`data_mutate` 仍覆盖业务写入全程。`graph_head_read` 记录控制锚点与有界尾读耗时；新协议没有 `graph_publish` 阶段，缺少该样本是正常的；阶段互相嵌套，不应相加。`vecdb_graph_object_loads_inflight{index_partition}` 给出本进程正在下载/解码图对象的并发数；每份副本最大 4 个，异常退出后归零。

性能脚本把新增批量直方图保存为 `batch_items_data_commit` / `batch_items_source_heads` / `batch_items_graph_replay`，与秒数直方图区分，持续保留前后差分和错误 outcome。

`master_schedule_attempt` 记录 Master 每次副本计划事务（含失败尝试）的耗时。确定写冲突会在同一 Tick 内最多尝试 4 次，因此不能把该阶段的 error 计数直接当成 drain 失败次数；最终任务状态仍看迁移任务接口。

| 指标 | 含义与用途 |
|---|---|
| `vecdb_data_head_lsn` / `completed_lsn` / `floor_lsn` | 每个 DataPartition 的写入头、连续完成前缀、保留底线；应满足 floor ≤ completed ≤ head |
| `vecdb_data_pending_events` | head − completed；持续大于零且 completed 不变表示投递或构图停滞 |
| `vecdb_data_retained_events` | head − floor；消费租约 pin 住 GC 时可能增大，不一定是投递异常 |
| `vecdb_data_owner_available` / `owner_epoch` / `dispatch_error` | 来源调度者的租约可用性、最近看到的 epoch、当前报告是否含错误；错误详情看状态中的 owner.last_error |
| `vecdb_index_source_epoch{index_partition,source}` | 该 Index 已持久化接受的来源 epoch，不等于 Coordinator 当前租约，也不表示副本已回放 |
| `vecdb_index_source_epoch_advances_total{index_partition,source}` | 本进程确认提交成功的 epoch 控制记录数；未知提交后来恢复的不补记，权威状态看 source_epoch gauge |
| `vecdb_index_source_epoch_rejections_total{index_partition,phase}` | `admission` / `commit` 阶段拒绝旧 epoch 的次数；与背压、一般 Index Leader fencing 分开 |
| `vecdb_index_source_commit_lsn{index_partition,source}` | Index 对每个来源的持久化覆盖位点；source 是十进制 DataPartition ID。稀疏路由下不能简单把所有 Index 的该值取 min 当作 Data completed |
| `vecdb_graph_committed_sequence` / `visible_sequence` / `floor_sequence` / `term` | 图日志持久化头、兼容的可回放位置（等于 committed）、图日志保留底线和 Leader term |
| `vecdb_graph_unpublished_entries` | 兼容指标，正常恒为 0；不用于判断各副本是否追平 |
| `vecdb_graph_replica_progress_known` / `applied_sequence` / `lag_entries` | 副本进度是否读取成功、已应用位置，以及 committed − applied；未知进度不输出假的 lag=0 |
| `vecdb_cluster_ready` / `fully_replicated` / `master_available` / `master_epoch` / `routing_version` | 查询副本可用性、配置副本数、Master 租约及布局版本；ready 不是全部来源完成或全部副本追平的承诺 |
| `vecdb_dispatch_acquisitions_total` | 本进程获得 DataPartition 执行权的次数，包含首次获取，不等于纯 failover 次数 |
| `vecdb_dispatch_ticks_total` / `dispatch_errors_total` / `dispatch_duration_seconds_total` | 本进程来源轮询次数、失败次数、累计耗时；包含空闲 tick |
| `vecdb_dispatch_wake_total{data_partition,reason}` | 来源提示次数，合并前计数；`write`、`graph_commit` 为发出提示，`received`、`window_drained`、`retry` 为本地调度原因；不代表持久化 ACK 或实际执行次数 |
| `vecdb_delivery_attempts_total` / `delivery_errors_total` | 本进程向 Index 投递的次数/失败次数，包含重试；成功只表示入队，不代表图已持久化 |
| `vecdb_index_admissions_total` / `duplicates_total` / `rejections_total` | 本进程接受批次、重复批次、拒绝批次；拒绝包括背压和 fencing，需要结合错误状态判断 |
| `vecdb_index_queue_batches` / `queue_bytes` | 当前本地 Index 输入队列大小；每来源最多一批，总编码预算 4 MiB，不包括正在构图的批次 |
| `vecdb_scrub_scanned_records_total` / `removed_records_total` | 后台错簇清理扫描的记录数、已提交删除的本地旧记录数 |
| `vecdb_search_stale_candidates_total` / `exact_fallbacks_total` | 查询候选权威校验剔除数，以及超过候选预算转为精确验证的次数；升高时检查跨簇移动积压与清理进度 |

表中省略重复前缀的列沿用同一前缀，例如 `vecdb_data_completed_lsn`、`vecdb_index_duplicates_total`。

全局 gauge 从 TiKV 状态读取，可由 Gateway 暴露；local counter/queue 只反映被抓取进程，Worker 应全部抓取。counter 重启归零，使用 `rate`/`increase`，不要当成持久化事件数。多 Gateway 和 Worker 都会重复暴露全局 gauge，汇总时使用 `max by (data_partition)` 等去重，不能直接相加。默认 `/metrics` 每次 scrape 会访问 TiKV，建议先采用 15 秒间隔；`scope=local` 不读取全局状态。

Graph committed 与 visible 在新协议中相等，均表示持久化可回放位置；replica applied 才是某份内存视图已经应用的位置；Data completed 也不承诺所有副本已应用。Source LSN 与图 sequence 不是同一计数空间。Prometheus 浮点样本不能精确表示大于 2^53 的整数，排障涉及精确位点时使用 HTTP/gRPC 状态。

## 初始告警建议

可按自身恢复目标调整以下时间，先作为测试集群阈值：

- `vecdb_data_owner_available == 0` 持续 30 秒：来源未恢复执行权。
- `vecdb_data_pending_events > 0` 且 `changes(vecdb_data_completed_lsn[5m]) == 0`：有积压但连续完成位点停滞。
- `vecdb_graph_unpublished_entries != 0`：协议或状态不一致，不能通过“等待发布”修复。
- `vecdb_graph_replica_lag_entries > 0` 持续 2 分钟：副本持续落后；同时检查 `progress_known` 与 `fully_replicated`，缺副本不能用没有 lag 样本表示健康。
- `rate(vecdb_dispatch_errors_total[5m]) > 0`：检查投递/存储错误与 fencing；短暂接管错误不一定需要立刻升级为严重告警。
- Prometheus 自身 `up == 0`：包括节点不可达、鉴权失败、TiKV 状态读取失败；不能把抓取失败当成 gauge=0。

目前没有自动配置 Prometheus、Grafana 或告警接收人。单 PID/多进程验收会检查故障积压出现、接管 epoch 增长、恢复后 pending 和 replica lag 归零；故障报告保存来源/图位点和最后一次集群状态。

## 引用日志与自动检查点（v12；聚类 epoch 扩展 v13）

- `vecdb_graph_log_bytes` / `checkpoint_bytes` / `checkpoint_pending_bytes`：累计图载荷字节、检查点覆盖字节和未覆盖差值。
- `vecdb_graph_checkpoint_sequence`：已发布检查点的位置；不会因后续追加而被抬高。
- `vecdb_index_source_checkpoint_lsn`：快照中的来源水位；`vecdb_index_source_retired_lsn`：已回收图条目的来源水位。后者决定数据引用何时可释放。
- pipeline 阶段 `graph_checkpoint`、`graph_gc`、`data_checkpoint_gc` 的 count/sum/bucket 与 outcome 用于观察上传、发布和 GC 错误。
- `graph_object_save` 现在仅用于检查点或重建 reset；普通 graph commit 不应伴随对象保存。`graph_payload_load`、`graph_data_load` 分别统计 TiKV 图载荷和历史数据读取，`graph_load` 包含读取与解码，旧的 graph_object_loads_inflight 名称保留兼容。

排查积压顺序：先看 pending bytes 是否到阈值，再看 checkpoint 阶段错误；检查点前进但 floor 不前进，检查 graph 租约 ACK；图 floor 前进但数据 floor 不前进，检查 floor_sources 连续前缀、data 租约和数据检查点。

## Publisher lease 与内存图头（v14）

正常构图不再读取 owner/plan/head；`graph_head_read` 仍包含 Follower、恢复和维护读取，不能全部算作 Leader 每批开销。`index_lease_renew` 统计控制循环申请/续期，`index_lease_recovery` 统计重新建立图头缓存并追平，`index_lease_fast_sync` 统计有效缓存上的处理循环。更高来源 epoch 的持久化是控制操作，仍在 `index_admission` 内计时。

- `vecdb_index_local_lease_valid{index_partition,replica}`：本地 Publisher lease 是否有效。
- `vecdb_index_local_lease_remaining_seconds{index_partition,replica}`：扣除安全余量后的本地剩余秒数；不是 PD 的绝对时间。
- `vecdb_index_local_head_ready{index_partition,replica}`：租约有效且已有确认的图头缓存。未知提交后降为 0，恢复成功后回到 1。
- `vecdb_index_publisher_lease_draining{index_partition}`：Master 正在等待旧 Publisher lease 到期。HTTP/gRPC `replica_plans[*].lease_draining` 与之对应。
- `index_queue_wait`：入队到取出；`index_queue_to_build`：入队到完成校验及版本合并、调用 HNSW GenerateDelta。后者包含前者，不能相加。直方图增加 0.25、0.75 ms 桶，保留原有 1 ms 门槛。

本地指标走 `/metrics?scope=local`，TiKV 不可用时仍可查询。Follower 的 Publisher lease 指标为 0 是正常状态。续期失败不会延长 lease；必须同时查看有效性、缓存可用性和恢复阶段错误。
