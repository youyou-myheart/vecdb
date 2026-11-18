# 数据分片异步投递与聚类索引

状态：已实现固定簇中心路由、数据与索引分区数量独立、来源完成水位、接管恢复和旧位置清理。split/merge、在线重新训练簇中心尚未实现。此模式使用 `v14-cluster-routing-lease-*`，必须使用新 namespace，不能将已有一一映射布局原地切换。

## 部署和数据流

```mermaid
flowchart LR
    D["TiKV 数据分区<br/>记录与有序数据日志"] --> P["数据分片处理器<br/>有界窗口、聚类路由"]
    P --> I["目标索引 Leader<br/>内存队列、串行批量构图"]
    I --> G["持久化图日志<br/>TiKV 图结构 + 数据日志引用"]
    G --> F["索引 Follower<br/>重放图变更"]
```

业务写入只等待原始记录、数据日志、幂等凭证的 TiKV 事务，不等待处理器、构图、进度轮询或 Follower。Gateway 可以在全部 Worker 暂停时继续写入和按 ID 读取。

DataPartition 仍按 ID hash 划分。每个数据分区新增一个可接管的处理器，由 Master 分配到 Worker，以 `data-NNNN` 的租约和 epoch 管理。它是异步日志处理者，不是权威数据写入的入口或主副本。IndexPartition 的副本计划、构图 Leader 和图复制仍独立管理。

## 固定聚类路由

启动参数 `CLUSTER_ROUTING_FILE` 指向 JSON：

```json
{"version":1,"centers":[[-10,0],[0,10],[10,0]]}
```

这个例子有三个二维索引簇：index-0000、index-0001、index-0002。选择 squared-L2 距离最近的中心，平局取较小索引编号。路由版本 1、中心坐标和索引编号持久化在布局中，不随进程故障或副本搬迁变化。此版本由配置提供中心，不自动训练或更新中心。

数据分区数量由 `PARTITIONS` 独立配置，索引数量由 centers 数量决定，两者各支持 1..64。所有角色必须提供相同配置，启动时校验持久化布局。恢复使用原事件向量计算目标，不读取最新记录替代历史事件，也不先持久化逐事件路由计划。

`GET /v1/layout` 中 `routing` 描述簇中心；`bindings` 此模式只枚举索引，`data_partition=-1` 表示没有一一绑定。数据分区集合由 data_partitions 定义。gRPC 同样暴露 routing，中心使用 Center.vector 消息表示。

## 投递窗口与完成水位

- 数据事务提交后，Gateway 异步通知对应来源处理器；图事务结束后，Index 也通知来源重新读取持久化水位。来源按实际目标推进连续完成前缀，一个窗口完成后立即读取下一窗口。通知合并且不承担 ACK，失败或丢失由 500 ms 控制巡检兜底；正常处理不等待该定时器。
- 每个来源窗口最多 32 条、2 MiB 的编码事件；达到限制停止加载，积压保留在 TiKV。
- 索引队列每个来源最多一批，排队编码载荷预算 4 MiB；另有当前正在处理的一批。处理批次按来源轮转。投递接口不等待构图；首次接受更高 epoch 时先写小型控制日志，普通同 epoch 请求校验后入队。
- 入队通过可合并通知唤醒该索引的独立消费者，Leader 连续处理到队列为空再阻塞等待；不再依赖 500 ms 或 10 ms 队列轮询。计划变更、失败重试和启动恢复同样可以触发处理。
- 一轮可合并队列中已经存在的多个来源批次，最多 128 条事件、约 2 MiB 编码输入；不等待凑批，单个合法大批次允许带协议头的有界余量。所有来源的 Index 本地 epoch、完成水位与同一个图引用原子校验和提交，任一来源 epoch 已被更高 epoch 替换则整批不提交。
- 每个图批次串行完成预校验、提交和本地 apply 再处理下一批。批次中同一个 ID 的多个版本折叠为最后的最高版本，再生成可回退图变更。
- 图增量先验证可回放，再去掉向量字节、保存历史数据引用；TiKV 图条目、载荷分块与累计 `GraphEntry.sources` 写在同一条日志中，事务提交即确认持久化。`GraphHead.sources` 从最新尾记录派生，不再逐批写 head。sources 的键是数据分区编号的十进制字符串，值是该来源的数据 LSN；它不是图 sequence。
- 图提交检查 Index Publisher 和每个来源的 Index 本地持久化 epoch，不再锁共享来源 fence。更高 epoch 通过图日志控制条目接受；旧请求在该 Index 接受新 epoch 后被拒绝。
- 同一来源可在多个 Index 独立提交，互不写来源锁。Index 内追加与 epoch 升级由本地追加锁和持久化 guard 串行化，切主等待 Publisher lease 到期并改变 guard；图 GC 保留检查点和来源水位约束；仅确定写冲突做有限重试，未知结果重新读取图日志恢复。
- 来源水位是持久化确认，不保证 Follower 已经应用，也不等待 Follower。

投递包含 `source`、来源 ownership、`route_version`、`previous`、`through` 和选中的原始事件。previous 是已覆盖边界：等于目标已有水位，或跳过到来源已持久化的 completed_lsn。跳过到尚未完成的位置会被拒绝。目标事件自身必须严格递增，来源 hash 和聚类目标均被校验。网络重试不能改变批次内容；授权的来源处理器负责完整选择范围内应投递的事件。

先校验 epoch；更高 epoch 即使全部事件已完成，也先持久化控制条目，再按提交水位去重。未提交的相同来源批次在有界队列中合并。构图中的重复批次即使重新排队，也会在下一次取出时按最新持久化水位跳过。进程退出丢失队列后，由来源重投，不需要恢复接收端内存。

## 连续完成位置与恢复

例：100 → I0，101 → I1，102 → I0。I0 确认 102 而 I1 尚未确认 101 时，来源 completed_lsn 只能到 100。I1 确认后才能越过空洞。不能取全部索引水位的最大值或最小值；没有收到事件的索引不参与该事件的完成条件。

处理器从窗口头逐条检查，遇到第一条未完成事件就停下。已完成前缀以来源租约 fencing 写入 TiKV，持久化后释放窗口前部。读取不同索引的进度不必是同一时刻，因为它们都是同一路由版本内单调的持久化完成事实。

来源进程故障后：

1. Master 分配新处理器，新节点领取递增 epoch。
2. 加载持久化 completed_lsn，查询目标来源水位。
3. 从 completed_lsn + 1 读取一个有界窗口，使用固定路由重算目标。
4. 索引已经完成的事件直接越过；其余重投，继续推进。

不会全量扫描数据，也不会重新构建 HNSW。恢复等待主要取决于租约检测、Master 调度和存储响应；不承诺固定秒数。可以恢复处理后逐步追赶积压，不必先把全部积压放进内存。来源故障不妨碍原始读写；若同一 Worker 还承载索引，索引故障走独立副本接管协议。

## 更新、删除、旧位置清理和查询

向量更新只投递到新向量所属的簇，不保存逐 ID 路由历史。删除事件没有向量，因此投递到全部索引，来源需要等待所有目标确认该删除。

索引 Leader 的后台维护按权威数据的分页扫描检查旧位置，每页最多 128 条返回记录（底层扫描另有界限）。当最新版本更高且已删除或属于其他簇时，用该版本生成本地索引墓碑，再通过图日志复制。它不修改原始数据，也不直接修改 Follower。检查和构图由本地同步锁串行化，版本比较保护重新返回该簇的新版本。扫描游标只在本页处理成功后推进，重启可重新扫描。当前后台维护沿用 30 秒周期，单轮检查一页，不承诺大数据量下的清理时延。

查询仍访问全部索引。每片候选使用 TiKV 权威记录校验 revision、删除状态和当前簇归属，排除旧位置与墓碑；候选不足时扩大到最多 1000 个，仍被旧记录占满则退回本地精确扫描和版本校验。该路径增加 TiKV 读取成本，不能当作容量优化已验收。普通查询仍可能因新版本尚未应用而漏掉新记录；不承诺跨片原子快照。

MinVisible 先等待一个有限的来源数据头完成投递与提交，再同步持久化图日志的本地应用，并对目标索引检查当前记录版本。删除依靠权威墓碑过滤。持续写入不会不断延长已捕获的等待目标。

## 快照、订阅、清理与状态

图快照保存完整 HNSW、记录/属性/墓碑、图位置，以及捕获时的 sources/source_epochs/log_bytes；每片 30 MB 或 3 分钟触发，上传期间可以继续追加。此模式下 GraphPosition.data_offset 是索引内部的单调构图批次进度，用于副本屏障；来源 LSN 必须读取 sources，不能与该字段比较。副本搬迁屏障取当前图提交位置，不再读取某个绑定的数据日志头。

数据快照单独从一个 TiKV 事务快照读取记录和 log-head，保存为 `data-records-v1` 的 JSON Record 数组。检查点响应包含 index-NNNN 图快照和 data-NNNN 数据快照。数据订阅 reset 使用自己的数据快照，不再借用某个 HNSW 快照。

- data 流：以 `data-NNNN` 寻址；graph 流：以 `index-NNNN` 寻址。
- 消费租约、ACK、cursor 继续按流独立管理。
- 数据 GC 受数据快照位置、持久化 completed_lsn、各实际目标的 floor_sources 和有效 data 消费者 ACK 限制；先回收图日志，才能回收它引用的数据日志。索引完成证明依赖保留的图日志/图快照恢复链。
- 图 GC 继续受图快照与 graph 消费者约束；已回收日志的副本从图快照恢复。累计来源水位、已接受 epoch 及已退休来源水位保留在图日志头，不随图条目清理而丢失。
- 后台自动运行有界清理；`cluster prune` 也可显式触发；本模式返回 data-NNNN 的 floor。图 floor 在 cluster status 中查看。
- 数据快照暂不回收；历史图快照和永久对象回收标记的既有保留边界不变。

`vectorctl cluster status`、HTTP `/v1/cluster` 和 gRPC GetCluster 提供 `data_progress`：每个数据分区的 head、completed_lsn、floor、处理器 ownership；`graph_heads[*].sources` 展示来源完成水位，`source_epochs` 展示该 Index 已接受的来源 epoch。来源 owner 的 offset 也上报 completed_lsn，last_error 记录处理错误。状态是观测信息，不是全局原子快照。

## 启动与验收

先启动 TiKV、SeaweedFS 并编译。使用仓库示例 `examples/cluster-routing.json`，或准备自己的 routing JSON，然后用新 namespace：

```bash
CLUSTER_ROUTING_FILE="$PWD/examples/cluster-routing.json" \
INDEX_REPLICATION=graph CONSUMER_RETENTION=1 \
  .tools/vectorctl --socket "$PWD/.state/cluster-routing.sock" \
  cluster start --namespace demo/cluster-routing-v1 \
  --partitions 4 --dimension 2 --workers 3 --replicas 2
```

该文件需要在各进程宿主上可读；集群 token 按 CLI.md 配置。不要在已启动的 namespace 内修改中心、数量或版本；split/merge 的存量迁移与路由切换协议尚未实现。

```bash
bash scripts/acceptance-dispatch.sh
```

覆盖全包 Go race、真实 TiKV Index 来源 epoch 屏障、独立提交及 GC 竞争、2 Gateway + 2 Master + 3 Worker 单进程流水线、真实多进程暂停/终止、业务写入不等待索引、4 个数据分区到 3 个簇、跨簇更新/删除、独立订阅与 GC、清理后接管，以及原一一映射图复制回归。报告见 reports/dispatch-acceptance.status、dispatch-all-tests.log、clustered-onebox-pipeline.json、dispatch-processes.json。所有测试只清理自身实例；未验证真实跨物理机故障和生产容量。

## 代码边界

- `cluster_routing.go`：不可变聚类布局、按向量选簇。
- `dispatch.go` / `source_signals.go`：来源租约、事件唤醒与控制巡检、有界窗口、连续完成前缀、投递。
- `routed_input.go`：有界接收队列、图提交、后台旧位置清理。
- `index_lease.go` / `index_writer.go`：Publisher 租约、迁移等待、内存控制状态、提交 guard 与未知提交恢复；正常构图不重复读 owner/plan/head。
- `index_consumer.go`：每索引独立消费、入队唤醒、连续处理、Follower 追赶、失败退避和退出。
- `index_commit.go`：复用持久化对象，只对确定的 TiKV 写冲突重试事务，每次重新校验 fencing。
- `route_plan.go`：图查询的计划读取超时回退；缓存只用于选副本，不取代 owner/epoch 校验或维护授权。
- `source_epoch.go`：持久化来源 epoch 控制记录、升级授权和旧 epoch 拒绝；协议细节见 [GRAPH_JOURNAL.md](GRAPH_JOURNAL.md)。
- `graph_journal.go`：图引用与来源水位的原子提交。
- `clustered_service.go`：显式同步、查询有效性、独立数据快照及回收。

split/merge 将来需要新旧索引并行准备、存量导入、增量屏障和查询路由切换。后台清理只能消除多余数据，不能替代新位置完整性检查；旧分片不能在新分片准备完成前退出查询。
