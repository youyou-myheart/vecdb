# 聚类投递与图复制观测

启用 `CLUSTER_ROUTING_FILE` 时，数据按 ID hash 进入 DataPartition，向量按配置的最近簇中心进入 IndexPartition，二者数量独立。当前为不可变的固定中心路由，不是自动学习簇或在线 split/merge。默认模式仍使用一一对应布局。

## 获取状态

所有逻辑角色支持 `GET /metrics`，使用现有 API Token，输出 Prometheus text 0.0.4。单机非集群服务返回 501，状态读取失败返回 503；鉴权失败返回 401。指标不输出 token、向量 ID、向量内容或错误文本。

```bash
curl -H "Authorization: Bearer $API_TOKEN" http://127.0.0.1:18202/metrics
curl -H "Authorization: Bearer $API_TOKEN" http://127.0.0.1:18202/v1/cluster
.tools/vectorctl cluster status
```

地址随本地集群端口参数变化。Python `client.cluster()`、Go SDK GetCluster 和 vectorctl 的集群状态保留精确 uint64 位点，并新增 `data_progress[data-NNNN].owner_available`。`owner` 可能保留已过期 owner 的 epoch、错误和身份，是否可用必须看 `owner_available`。

## 指标语义

| 指标 | 含义与用途 |
|---|---|
| `vecdb_data_head_lsn` / `completed_lsn` / `floor_lsn` | 每个 DataPartition 的写入头、连续完成前缀、保留底线；应满足 floor ≤ completed ≤ head |
| `vecdb_data_pending_events` | head − completed；持续大于零且 completed 不变表示投递或构图停滞 |
| `vecdb_data_retained_events` | head − floor；消费租约 pin 住 GC 时可能增大，不一定是投递异常 |
| `vecdb_data_owner_available` / `owner_epoch` / `dispatch_error` | 来源调度者的租约可用性、最近看到的 epoch、当前报告是否含错误；错误详情看状态中的 owner.last_error |
| `vecdb_index_source_commit_lsn{index_partition,source}` | Index 对每个来源的持久化覆盖位点；source 是十进制 DataPartition ID。稀疏路由下不能简单把所有 Index 的该值取 min 当作 Data completed |
| `vecdb_graph_committed_sequence` / `visible_sequence` / `floor_sequence` / `term` | 图日志持久化头、Leader 发布版本、图日志保留底线和 Leader term |
| `vecdb_graph_unpublished_entries` | committed − visible；有持久化图尚未发布，应在恢复/发布后归零 |
| `vecdb_graph_replica_progress_known` / `applied_sequence` / `lag_entries` | 副本进度是否读取成功、已应用位置，以及 committed − applied；未知进度不输出假的 lag=0 |
| `vecdb_cluster_ready` / `fully_replicated` / `master_available` / `master_epoch` / `routing_version` | 查询副本可用性、配置副本数、Master 租约及布局版本；ready 不是全部来源完成或全部副本追平的承诺 |
| `vecdb_dispatch_acquisitions_total` | 本进程获得 DataPartition 执行权的次数，包含首次获取，不等于纯 failover 次数 |
| `vecdb_dispatch_ticks_total` / `dispatch_errors_total` / `dispatch_duration_seconds_total` | 本进程来源轮询次数、失败次数、累计耗时；包含空闲 tick |
| `vecdb_delivery_attempts_total` / `delivery_errors_total` | 本进程向 Index 投递的次数/失败次数，包含重试；成功只表示入队，不代表图已持久化 |
| `vecdb_index_admissions_total` / `duplicates_total` / `rejections_total` | 本进程接受批次、重复批次、拒绝批次；拒绝包括背压和 fencing，需要结合错误状态判断 |
| `vecdb_index_queue_batches` / `queue_bytes` | 当前本地 Index 输入队列大小；每来源最多一批，总编码预算 4 MiB，不包括正在构图的批次 |
| `vecdb_scrub_scanned_records_total` / `removed_records_total` | 后台错簇清理扫描的记录数、已提交删除的本地旧记录数 |
| `vecdb_search_stale_candidates_total` / `exact_fallbacks_total` | 查询候选权威校验剔除数，以及超过候选预算转为精确验证的次数；升高时检查跨簇移动积压与清理进度 |

表中省略重复前缀的列沿用同一前缀，例如 `vecdb_data_completed_lsn`、`vecdb_index_duplicates_total`。

全局 gauge 从 TiKV 状态读取，可由 Gateway 暴露；local counter/queue 只反映被抓取进程，Worker 应全部抓取。counter 重启归零，使用 `rate`/`increase`，不要当成持久化事件数。多 Gateway 和 Worker 都会重复暴露全局 gauge，汇总时使用 `max by (data_partition)` 等去重，不能直接相加。每次 scrape 会访问 TiKV，建议先采用 15 秒间隔。

Graph committed、visible 和 replica applied 是不同含义：Leader 可以在 Follower 尚未追上时发布；Data completed 也不承诺所有副本已应用。Source LSN 与图 sequence 不是同一计数空间。Prometheus 浮点样本不能精确表示大于 2^53 的整数，排障涉及精确位点时使用 HTTP/gRPC 状态。

## 初始告警建议

可按自身恢复目标调整以下时间，先作为测试集群阈值：

- `vecdb_data_owner_available == 0` 持续 30 秒：来源未恢复执行权。
- `vecdb_data_pending_events > 0` 且 `changes(vecdb_data_completed_lsn[5m]) == 0`：有积压但连续完成位点停滞。
- `vecdb_graph_unpublished_entries > 0` 持续 1 分钟：持久化图尚未发布。
- `vecdb_graph_replica_lag_entries > 0` 持续 2 分钟：副本持续落后；同时检查 `progress_known` 与 `fully_replicated`，缺副本不能用没有 lag 样本表示健康。
- `rate(vecdb_dispatch_errors_total[5m]) > 0`：检查投递/存储错误与 fencing；短暂接管错误不一定需要立刻升级为严重告警。
- Prometheus 自身 `up == 0`：包括节点不可达、鉴权失败、TiKV 状态读取失败；不能把抓取失败当成 gauge=0。

目前没有自动配置 Prometheus、Grafana 或告警接收人。单 PID/多进程验收会检查故障积压出现、接管 epoch 增长、恢复后 pending 和 replica lag 归零；故障报告保存来源/图位点和最后一次集群状态。
