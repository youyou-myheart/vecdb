# 多分片与负载均衡

onebox 默认创建 **32 个固定数据/索引分片**，双副本对应 64 个索引副本。仍支持 1..64 分片；已有 namespace 的分片数不能直接修改，必须保留创建时的参数。独立 vector-server 的 PARTITIONS 默认仍为 1。

本阶段提供两层均衡：Gateway 对健康副本轮换查询，Master 根据副本成本和 Worker 容量逐步调整放置。副本迁移复用先恢复、达到 barrier 后再移除旧副本的协议。

## 启动

使用新 namespace，并确保底层 TiKV/PD/SeaweedFS 已运行：

```bash
export API_TOKEN=local-public-api-token
export CLUSTER_TOKEN=local-peer-rpc-secret
export VECTOR_CONTROL_SOCKET="$PWD/.state/balance-control.sock"
.tools/vectorctl cluster start --namespace demo/load-v1 \
  --workers 4 --partitions 32 --replicas 2 --scheduler load --grpc-port 18250
```

默认调度策略仍为 `hash`，显式 `--scheduler load` 才开启负载调度，且必须双副本。独立部署使用 `SCHEDULER=load REPLICAS=2 PARTITIONS=32`，所有角色配置保持一致。

负载协议使用 v7 schema 和 `cluster-mode=v4-load-2`，拒绝与旧调度协议混跑。现有 hash namespace 暂不支持原地切换；本轮测试使用独立 namespace，没有迁移旧数据。调度模式与冷却时间写入 TiKV，不一致的节点配置拒绝启动。

## 调度依据

Worker 随心跳上报 `nodes[].load[逻辑分片]`：

- `estimated_bytes`：按存活向量和 HNSW 节点数估算的向量/图存储成本，不包含全部 Go/C++ 开销和属性倒排数据，不是进程 RSS。
- `busy`：该分片查询耗时总和 / 采样间隔，使用 0.25 新样本 + 0.75 旧值的平滑。包含查询期间的同步和校验等待，不是 CPU 使用率；并发时可大于 1。
- `capacity`：Worker 容量权重，独立进程用 `WORKER_CAPACITY` 设置，默认 1，允许 0.1..100。它是调度比例，不是硬内存配额。

每个分片成本为：

```text
1 + estimated_bytes / 64MiB + 4 × busy
```

同一分片取存活 Worker 上报值中的最大成本；缺少样本时退化为成本 1，不把新 Worker 当作无限容量。节点负载是目标副本成本之和除以 capacity。使用实际副本成本可以避免仅按副本个数分配时把大分片、热分片聚在同一 Worker。

初始分配和故障补副本使用预计负载最低的节点，且同一片两个副本不能落在同一 Worker。已有正常分配保持不动，只有满足条件才迁移，避免每次心跳重新算整张分配表。

## 迁移保护

- 源节点归一化负载需超过目标的 1.2 倍，且移动后两者负载平方和确实下降，才做主动均衡。
- 一次最多主动迁移一个副本；所有目标副本 ready、追赶到 barrier，且上一次旧副本已从 serving 移除后，才考虑下一次。
- `BALANCE_COOLDOWN` 默认 10s，允许最小 1s；最后迁移时间持久化在 TiKV，Master 切换不会绕过冷却。
- 初次建副本、节点故障和主动排空优先修复可用性，不受上述“每次一个”的主动均衡限制，可能同时恢复多片。不要把它理解为全局恢复并发上限。

扩容是渐进过程。`scale N` 返回表示目标进程和副本就绪，不代表所有负载已平衡；新 Worker 可能先处于空闲，随后逐片接收副本。均衡允许一定差异，不追求频繁迁移来达到精确相等。

## 查询均衡

Gateway 对每个分片独立轮换副本，并跳过未 ready 或租约过期的副本。一次查询仍只取一个成功副本的结果。故障重试、原始请求 deadline 和 min_visible 检查保持不变。

这一步采用轮询，不是依据瞬时 CPU 动态选副本。Master 的放置调度会使用上报的查询耗时。两个层次分开，避免每次请求都改变持久化分配。

## 查看状态

```bash
.tools/vectorctl cluster nodes
.tools/vectorctl cluster partitions
.tools/vectorctl cluster status
```

`nodes[].capacity/load` 可看容量和采样值；`replica_plans[].desired` 是目标分配，`serving` 包含迁移中仍保留的副本。两者不一致表示迁移尚未收敛。`replica_owners` 给出实际 ready 副本及消费位点。

## 验收与限制

`scripts/verify_balance.py` 使用真实后端、32 分片、64 副本，验证 3 → 4 Worker 的分配收敛；持续比较全部记录，检查主动迁移最多一个、查询不断流、Worker 上报非空大小和耗时样本，以及平衡后的分配稳定。验收为加快运行使用 1s 冷却，生产默认 10s。

单测额外使用不均匀成本验证热点分片迁移、容量权重、冷却时间、迁移中禁止继续搬片，以及调度配置/协议隔离。真实进程验收主要覆盖扩容均衡，尚未进行大规模热点压测或跨主机资源干扰测试。

未实现在线拆分已有分片、自动采集 RSS/CPU、硬资源准入、基于向量聚类迁移或最优放置求解。更多分片会增加查询扇出和协调开销；32 分片验收通过也不等同于完成最大规模性能认证。
