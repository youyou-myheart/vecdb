# 整套独立进程 onebox 验收

`scripts/onebox_e2e.py` 在本机启动自己的 PD、TiKV、SeaweedFS 和 7 个独立 `vector-server` 进程：2 Gateway、2 Master、3 Worker，总共 10 个 OS 进程。SeaweedFS 使用 `weed server` 在一个进程内运行 master、volume、filer 和 S3。它不复用或停止已有 12379/12160/18333 的存储进程。

默认布局为 16 个 DataPartition（ID hash）、8 个 IndexPartition（固定簇中心）、双索引副本、load 调度、16 维向量。768 条结构化向量包含稳定查询锚点、跨簇移动、递增版本、属性、删除和重新插入。两个 Gateway 都开放 HTTP 和 gRPC。

## 执行

需要已有本项目的 Go/C++ 工具链、`.venv` Python 依赖、`.tools/weed` 和 `.tools/tiup-home/components/{pd,tikv}/v8.5.8` 原生程序。执行：

```bash
# 默认：10 分钟并发常稳 + 全部故障 + 1 分钟恢复后常稳，结束清理所有本次进程。
bash scripts/acceptance-onebox-e2e.sh

# 验收通过后保留恢复后的健康 onebox，供手工运维；控制命令停止时统一清理。
bash scripts/acceptance-onebox-e2e.sh --keep-running

# 调整持续时间和连续故障轮数；短运行只能作为编排预检。
bash scripts/acceptance-onebox-e2e.sh --steady-seconds 1800 --rounds 12 --recovery-seconds 120

# 仅常稳 + 时延定位，跳过故障矩阵；仍启动全部 10 个进程并执行完整业务核验。
bash scripts/acceptance-onebox-e2e.sh --latency-only --steady-seconds 600 --keep-running

# 重复测量：最后一次写 ACK 后，全部副本应用且真实查询正确，必须 <= 1 秒。
# 不和上面的测试同时运行，它们使用相同默认端口与构建路径。
bash scripts/acceptance-onebox-freshness.sh --freshness-rounds 20 --load-seconds 5
```

默认端口范围由 `--base-port 25400` 决定：PD 25400、TiKV 25402、S3 25413；Gateway HTTP 25502/25535，gRPC 25580/25581。启动前检查全部监听端口，发现占用即失败，不抢占已有进程。旧运行产生的 TIME_WAIT 连接不算存活服务。

每次使用新 namespace、新 `.state/onebox-e2e/<run_id>/` 数据目录和 `reports/onebox-e2e/<run_id>/` 日志目录。故障恢复始终复用同次运行的存储目录和 namespace，不通过换空集群掩盖恢复问题。

## Case 顺序

| Case | 注入与断言 |
|---|---|
| 启动与初始化 | 10 个独立 PID；真实持久化 768 条向量；两个 HTTP/gRPC Gateway 的完整检索、过滤、ANN recall 与分页 |
| 常稳 | 4 个写线程、2 个读线程；默认持续 600 秒；每 ID 串行 CAS，跨 ID 并行；更新、跨簇移动、删除重插和幂等重试；常稳期间不允许请求重试错误 |
| Gateway/Master 单故障 | 杀掉一个 Gateway，客户端切换另一入口；杀掉当前 Master，记录备用主 epoch 增长耗时；持续读写后恢复 |
| 旧 Worker 恢复 | SIGSTOP 持有来源分片的 Worker，等待租约过期与新 epoch，然后 SIGCONT；旧进程不能使位点倒退 |
| S3 暂停 | 暂停 SeaweedFS 8 秒，确认进程暂停且数据继续持久化，记录索引积压（引用日志路径可能为 0）；恢复后全部副本追平 |
| TiKV 暂停 | 暂停 TiKV 8 秒，使客户端出现真实超时/服务错误；使用同一 request ID 重试不确定写入；恢复后无重复版本或丢失 |
| 叠加故障 | 同时强杀 2 Worker、当前 Master 和一个 Gateway；另一个 Gateway 仍接收业务写入；恢复后逐条校验 |
| 全 Worker 丢失 | 强杀全部 Worker，提交 512 次更新；确认无 owner 时没有成功 ACK，再恢复 Worker，等待全部请求重试成功并核验数据。当前 owner-routed 写入不能在所有数据 owner 停止时直接 ACK |
| 连续故障 | 默认 6 轮 Worker/Master 强杀与重启；各轮之间不等分片追平，业务线程持续运行；最后统一收敛检查 |
| 整套冷恢复 | 快照/GC 后再写 256 次尾部更新，SIGKILL 全部 7 个向量进程和 3 个存储进程；从原持久化目录启动并验证 |
| 恢复后常稳 | 默认再持续 60 秒读写，最后对全部记录和两份查询入口进行完整校验 |

故障阶段允许明确的瞬时连接/超时/429/5xx 错误，逐次记录且有总重试期限；4xx 业务错误、错误数据、重复结果和未收敛都直接失败。每次写入只有收到 durable token 才推进测试 oracle；不确定结果使用原 request ID 重试。测试没有把请求失败悄悄算成成功。

完整 oracle 在写线程退出后运行：校验全部记录（包括墓碑）的 ID、版本、向量、属性；两个 Gateway 的全量精确检索和排序无重复/遗漏；gRPC 属性过滤精确匹配，3 个稳定锚点 ANN recall@10 ≥ 0.9；重放写请求仍返回原 token。所有来源必须 completed=head，所有图必须 committed=visible，两份图副本的 GraphPosition 一致。每次最终验证还确认本次 10 个进程都存活。

每次追平还记录约 100 ms 间隔的位点轨迹，区分图提交、发布、副本内存应用、心跳上报和 source completed；常稳阶段保存各进程的 Histogram 差分。指标口径、报告文件和误差说明见 [PIPELINE_LATENCY.md](PIPELINE_LATENCY.md)。

`onebox_freshness.py` 使用独立计时口径：各写线程退出并完成尾部插入/更新/删除后，以实际最后一次 durable ACK 的单调时间为 t0，线程收尾和探针准备时间仍计入。它读取本轮 data log 来确定每个 Index 的来源 LSN，检查 published 和当前 epoch 的本地 applied，并对每份副本和两个 Gateway 做无 `min_visible` 的普通精确搜索。返回的 ID、revision、属性、距离必须与停写时 oracle 完全一致。每轮均保存 `<case>-visibility.jsonl`、`<case>-visibility.json`、`<case>-targets.json` 和阶段指标差分。初始化批量、常稳停写和故障恢复分别统计；探针本身的采样/查询开销包含在结果中。当前全量探针最多支持 1,000 条存活记录，不能据此宣称百万级容量下同样满足目标。

## 报告与运维

`reports/onebox-e2e-latest.json` 是最新结果，独立运行目录保留：

- `report.json`：各 case 成败与耗时、Master/来源接管时间、数据/图位点、每类操作 p50/p95/p99/max、错误与重试次数、PID 和地址。
- `oracle.json`：最新全量校验使用的期望状态；报告保存其 SHA-256。
- `observations.jsonl`：测试期间每 5 秒记录进程状态、RSS/CPU、Prometheus 指标或抓取错误。结束后停止采样，避免保留 onebox 时无限增长。
- `pd.log`、`tikv.log`、`seaweed.log`、`services/*.log`：各真实进程日志。

使用 `--keep-running` 且通过后：

```bash
source .state/onebox-e2e.env
.tools/vectorctl cluster status
.tools/vectorctl cluster local-status
curl -H "Authorization: Bearer $API_TOKEN" "$VECTOR_ENDPOINT/metrics"
.tools/vectorctl cluster stop
```

停止命令返回已接受后，启动器回收七个向量进程和三个存储进程，删除本次控制 socket，并更新报告中的 `all_children_stopped`。数据和日志目录保留便于排查；启动器只管理自身创建的进程。凭据文件权限为 0600，不提交到 Git。

这些是单机真实进程与本地持久化故障测试。10 分钟常稳不是 24/72 小时 soak，单 PD/单 TiKV 也不代表跨机器存储多数派或网络分区验收。冷恢复计时包含依赖启动、分片追平和完整结果验证，不等同于首次可读的精确 RTO。

## 2026-11-16 事件消费复测

运行 `20261116T224624-a0ac0ab9`：真实启动 PD、TiKV、SeaweedFS 和七个向量服务，12 组 case 全部通过，结束后本次启动的全部进程已停止。16 Data / 8 Index / 双副本；768 条结构化 16 维向量；二进制与 [一秒可见性正式验收](PIPELINE_LATENCY.md) 相同。

| 项目 | 结果 |
|---|---|
| 常稳 | 600.257 秒，业务请求错误/重试为 0；完整核验通过 |
| 常稳请求 | GET 记录 14,256、PUT 11,956、DELETE 1,024、搜索 2,831 次，含幂等重放与最终核验 |
| PUT / 搜索 | P95 为 5.94 / 32.58 ms，P99 为 50.18 / 47.27 ms |
| 常稳停写追平 | 108 ms 首次观察到图发布、所有副本实际应用与来源完成；982 ms 心跳上报也追平 |
| Master / 旧 Worker 来源接管 | 4.859 / 7.244 秒；恢复旧进程后完整 oracle 通过 |
| S3 / TiKV 暂停后追平 | 1.534 / 4.671 秒；原始持久化和不确定写重试均核验通过 |
| 两 Worker + Master + Gateway 叠加故障 | 恢复进程后 3.679 秒追平 |
| 全 Worker 丢失 | 512 次更新持久化成功、pending=512；恢复后 10.113 秒追平 |
| 连续故障 | 6 轮之间没有追平屏障，最终 6.071 秒追平 |
| 全栈原目录冷恢复 | 全部 10 个进程 SIGKILL，复用原目录启动并完成完整校验 36.082 秒 |
| 恢复后常稳 | 60 秒负载，完整 oracle 通过，无业务请求错误 |
| 最终数据 | 14,714 条持久化数据变更；768 条记录，764 条有效、4 条预期墓碑 |

故障阶段共有 193 次请求尝试失败，保留具体类型和重试结果；没有计入常稳零错误，也没有删除失败记录。服务日志未发现 panic/fatal/race 签名，运行服务为普通构建，race 检查由独立质量门禁执行。各故障追平时间从恢复后开始观测，与“最后 durable ACK 后一秒内真实查询可见”的专项口径不同；恢复时仍受租约和依赖启动影响。

证据位于 `reports/onebox-e2e/20261116T224624-a0ac0ab9/`：报告、全量 oracle、故障观测轨迹、指标差分和服务日志；`latency-analysis.json` 汇总事件消费者执行、排队与图提交阶段。优化前的失败实验和基线继续保留，见 [PIPELINE_LATENCY.md](PIPELINE_LATENCY.md)。

## 2026-11-16 优化前实测

北京时间 20:03–20:18，运行 `20261116T200354-147c2a3a` 的 12 组 case 全部通过，随后保留恢复后的 onebox。另有短预检通过；短预检不计入正式 10 分钟常稳时间。

| 项目 | 结果 |
|---|---|
| 正式常稳 | 持续 600.253 秒，HTTP 请求错误/重试为 0 |
| 常稳请求 | GET 记录 14,326 次、PUT 12,045 次、DELETE 1,024 次、搜索 2,812 次；包含幂等重放与阶段末校验，不等同于独立修改数 |
| PUT 延迟 | P95 6.31 ms，P99 13.16 ms，最大 1,266.51 ms |
| 搜索延迟 | P95 39.41 ms，P99 56.36 ms，最大 1,157.66 ms |
| 常稳积压 | 5 秒采样的来源 pending 峰值 270 条；停写后约 7.175 秒完成所有来源/图副本追平 |
| Master 接管 | 约 4.862 秒 |
| 旧 Worker 暂停后的来源接管 | 约 6.746 秒，恢复旧进程后仍通过完整 oracle |
| S3/TiKV 暂停后的追平 | 分别约 7.806 / 4.319 秒（恢复并停止负载后开始计时） |
| 两 Worker + Master + Gateway 叠加故障 | 恢复进程后约 9.382 秒追平，完整 oracle 通过 |
| 全 Worker 丢失 | 512 次原始更新持久化成功，pending=512，恢复 Worker 后约 20.033 秒追平 |
| 连续故障 | 6 轮之间没有追平屏障，最终约 8.407 秒追平 |
| 全栈冷恢复 | 10 个服务进程全部 SIGKILL，复用原目录启动并完成完整校验约 44.598 秒 |
| 恢复后常稳 | 再持续 60 秒负载，完整 oracle 通过 |
| 最终数据 | 14,840 条持久化数据变更；768 条记录，其中 764 条有效、4 条预期墓碑，无误删或遗漏 |

故障阶段共记录 258 次明确的请求尝试失败，按同一请求标识重试后恢复；这些失败没有计为常稳成功，也没有从报告中隐藏。日志未发现 panic/fatal/race 签名（运行服务使用正常构建，这不等于启用了运行时 race 检测）。保留集群的 CLI 状态/读取、两套 gRPC 入口、数据/图订阅 reset、consumer acquire/ACK/release 已再次通过检查。验收工具及原有脚本 23 个单测通过。

本次报告和观测文件位于 `reports/onebox-e2e/20261116T200354-147c2a3a/`；`handoff-checks.json` 保存最后的运维接口检查结果。报告聚合标签随后改为去除分页 cursor，另补了对应 UT；正式运行原始报告保留当时的数据。
