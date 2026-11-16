# TiKV + SeaweedFS 向量存储 v0.5

已实现可运行的 Go HTTP 服务：真实 TiKV 事务写入记录/Outbox/幂等凭证，HNSW / 精确向量检索、属性倒排过滤，SeaweedFS 图快照，TiKV 清单 CAS，服务重启恢复。

先阅读 [当前架构与分区设计](ARCHITECTURE.md)：用独立图说明两层分区、Worker 部署与主备复制，并记录保留 DataPartition 的原因和未来聚类索引边界。

详细架构、协议与商用开发阶段见 [DESIGN.md](DESIGN.md)。索引实现和验收数据见 [HNSW.md](HNSW.md)。增量消费、升级步骤和并发优化见 [INCREMENTAL.md](INCREMENTAL.md)。两层分区见 [PARTITIONS.md](PARTITIONS.md)：数据按 ID hash 分区，索引分区暂与数据分区一一对应，支持固定多分区。多进程角色、TiKV 主备选举和故障接管见 [CLUSTER.md](CLUSTER.md)，尚非生产完成版。

本机多 Worker 可用 `python3 scripts/cluster_local.py --namespace demo/multiworker-v1 --workers 2 --partitions 8 --dimension 16` 启动（需先编译并设置 API_TOKEN / CLUSTER_TOKEN），运行中输入 `scale 3`、`status` 或 `quit`。完整步骤和迁移期间的可用性边界见 [CLUSTER.md](CLUSTER.md#一键运行多个-worker-与扩缩容)。

统一命令行 `vectorctl` 支持集群控制、状态查询、记录读写和 Top-N 检索，详见 [CLI.md](CLI.md)。

Gateway 另提供版本化 gRPC 接口和 Python/Go SDK，开启方式、示例和错误语义见 [RPC.md](RPC.md)。

Worker 双副本与先恢复后退出的迁移已提供，可用 `--replicas 2` 在新 namespace 启用；升级边界、状态和验收见 [REPLICAS.md](REPLICAS.md)。

onebox 默认分片数现为 32；负载放置调度使用 `--replicas 2 --scheduler load`，详见 [BALANCING.md](BALANCING.md)。

持久迁移任务、调度解释和只读预览已接入，可用 `vectorctl cluster tasks`、`task ID`、`explain PARTITION`、`balance --dry-run` 查看。新双副本任务协议需要新 namespace，暂不自动升级旧双副本集群；设计与故障边界见 [DESIGN.md](DESIGN.md)，用法见 [CLI.md](CLI.md)。

单进程七服务的业务与运维流水线可运行 `bash scripts/acceptance-onebox.sh`，详见 [PIPELINE.md](PIPELINE.md)。

支持可选异步图复制模式：`INDEX_REPLICATION=graph REPLICAS=2`。每分片单 Leader 构图，Follower 重放持久化的图变更，允许查询进度落后；详见 [设计](DESIGN.md#异步图复制index_replicationgraph) 和 [启动方式](CLI.md#异步-hnsw-图复制)。新增验收入口为 `bash scripts/acceptance-graph.sh`。

支持可选 [聚类索引与数据分片异步投递](CLUSTER_ROUTING.md)：数据按 ID hash，索引按固定簇中心路由，数量可独立配置；来源周期汇总持久化进度，支持接管恢复。通过 `CLUSTER_ROUTING_FILE` 在新 namespace 启用，验收入口 `bash scripts/acceptance-dispatch.sh`。

最新 review 修复、重复/叠加故障矩阵与自动化门禁见 [QUALITY.md](QUALITY.md)。该入口现在包含所有 Go race/UT、真实存储、四种七服务流水线和多进程故障回归。聚类投递与图复制的 `/metrics`、HTTP/gRPC 位点和诊断说明见 [OBSERVABILITY.md](OBSERVABILITY.md)。

需要拉起整套真实进程并做常稳、存储暂停、连续故障和全部进程冷恢复时，运行 `bash scripts/acceptance-onebox-e2e.sh`；加 `--keep-running` 可在验收通过后保留健康 onebox。拓扑、用例和运维命令见 [ONEBOX_E2E.md](ONEBOX_E2E.md)。

写入后索引追平的耗时拆解、队列等待、构图/存储 IO 指标和 10 分钟复测入口见 [PIPELINE_LATENCY.md](PIPELINE_LATENCY.md)。`/metrics?scope=local` 可在 TiKV 不可用时读取进程内诊断，需分别采集各 Worker。

## 启动基础组件

实测 PD/TiKV 8.5.8、SeaweedFS 4.53，macOS ARM64。安装包和 Go 缓存在 `.tools/`，数据在 `.state/`。已有本次安装包时，在三个终端分别启动，已运行的组件不要重复启动：

```bash
bash scripts/run-pd.sh
bash scripts/run-tikv.sh
bash run-local.sh
```

三个命令各自长期运行。`tikv-local.toml` 为 4 GiB 测试容量预算、512 MiB 预留，未关闭磁盘保护，不用于生产。

重新安装时，将官方 TiUP 放入 `.tools/tiup`，然后执行：

```bash
mkdir -p .tools/tiup-home/bin
curl -fsSL https://tiup-mirrors.pingcap.com/root.json -o .tools/tiup-home/bin/root.json
TIUP_HOME="$PWD/.tools/tiup-home" .tools/tiup install pd:v8.5.8 tikv:v8.5.8
```

SeaweedFS 从官方 4.53 Release 下载对应平台二进制放入 `.tools/weed`。本次下载为 macOS ARM64。另有 `compose.yaml`，已解析校验但未实际启动容器。SeaweedFS 本机和容器方式只能选一种，S3 端口相同。

## 编译与启动向量服务

升级已有 namespace 前先停止全部旧版写者，不支持混跑；详见 INCREMENTAL.md。

需要启用 CGO 和 C++14 编译器（macOS Command Line Tools，Linux g++）。hnswlib 0.8.0 已随源码固定版本；无需运行时下载。

```bash
bash scripts/go.sh build -o .tools/vector-server ./cmd/vector-server
API_TOKEN=local-vector-test-token .tools/vector-server
```

默认 API 为 `127.0.0.1:18090`，维度 3，namespace 为 `demo/default`，距离为 squared-L2。

新 namespace 可设置 `PARTITIONS=4` 启用四分区，默认 1；已持久化的分区数不可变，旧 namespace 保持 1。`GET /v1/layout` 可查看映射。

可配置：`PARTITIONS`、`API_TOKEN`、`LISTEN_ADDR`、`DIMENSION`、`NAMESPACE`、`PD_ENDPOINTS`、`S3_ENDPOINT`、`S3_ACCESS_KEY`、`S3_SECRET_KEY`、`S3_BUCKET`。Go 的 S3_ENDPOINT 使用 `host:port`，当前只支持本机 HTTP。namespace 维度不可变，不同维度使用新的 namespace。

演示 token 与 s3.json 是公开测试凭据，不用于生产。所有 HTTP 接口需 Bearer token。

## 多进程模式

设置 `CLUSTER_MODE=1`，并配置 `NODE_ID`、`ADVERTISE_URL`、`CLUSTER_TOKEN`。`ROLES` 默认为 `master,gateway,worker`，可分别部署；至少需要 Master 候选、Worker 和 Gateway。详细启动示例与升级边界见 CLUSTER.md。

## 写入与搜索

```bash
curl -s http://127.0.0.1:18090/v1/records/a \
  -X PUT -H 'Authorization: Bearer local-vector-test-token' \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"demo-a-1","vector":[1,0,0],"payload":{"category":"book"}}'

curl -s http://127.0.0.1:18090/v1/search \
  -H 'Authorization: Bearer local-vector-test-token' \
  -H 'Content-Type: application/json' \
  -d '{"vector":[1,0,0],"k":5,"filter":{"category":"book"},"min_visible":{"id":"a","revision":1}}'

curl -s http://127.0.0.1:18090/v1/checkpoint \
  -X POST -H 'Authorization: Bearer local-vector-test-token'
```

更新和删除每次使用新的 request_id；重试同一次操作使用相同 request_id 和相同内容。PUT 可携带 expected_revision。DELETE 的 JSON body 包含 request_id，也可携带 expected_revision。

搜索可设置 `"exact":true` 强制精确检索，或 `"ef_search":256` 调整 ANN 搜索预算（1..4096，0 使用默认值）。属性过滤支持字符串等值 AND。

`POST /v1/rebuild` 清理旧版本图节点并发布新图快照；重建期间会追赶写入；重复并发重建返回 409，可重试。所有接口仍使用同一 Bearer token。后台每 30 秒检查图，节点数至少 256 且过期比例至少 30% 时尝试内存重建，持久化由 checkpoint 完成。

## 验证

先启动三个基础组件：

```bash
INTEGRATION=1 bash scripts/go.sh test -race -v ./internal/vector
python3 scripts/verify_api.py
```

报告在 `reports/tikv-integration.json`、`reports/go-test.log`、`reports/http-integration.json`。HTTP 测试启动自己的 18091 端口子进程，强制终止后验证恢复，不停止基础组件。测试数据使用随机 namespace，暂时保留便于调试。

原 Python S3 层验证仍可运行：

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.lock.txt
.venv/bin/python verify.py
# 手动重启 SeaweedFS 后
.venv/bin/python verify.py --restore-only
```

Python 验证 17 MiB 分段上传、Range、SHA-256、四路恢复；SQLite 清单只用于此早期独立测试。新 Go 服务使用真实 TiKV 清单，不依赖 SQLite。

## 当前限制

- HNSW 是近似搜索；`exact:true` 使用精确 Top-K。原始 float64 向量之外还保留图及 float32 向量，降低查询分配不等于降低总常驻内存。
- 同一图支持并发查询；每个数据分区的事务序号仍可能是局部写热点。重建需要新旧图同时驻留，追赶跟不上写入时可能超时。
- Outbox 已改为有序增量日志，支持快照保护的分批清理；完整基线恢复仍限制为 100,000 条记录。已支持固定分区跨进程分配与故障接管，尚无负载调度、背压和完整容量管理。
- 快照最多 512 MiB，完整回读及恢复占用内存。
- 单 namespace、固定分区、字符串等值过滤，无跨租户身份管理。
- 独立 TiKV 的 GC、安全点、备份恢复与多节点故障测试尚未实现。
- 验证了服务进程崩溃恢复，没有验证整机掉电、Raft 仲裁故障或可用区容灾。
- 不可变对象使用新 UUID 命名；孤立对象与测试 namespace 暂不自动清理。

基础服务以前台进程运行，结束对应终端可停止。不要删除 `.state/` 或执行 `docker compose down -v`，除非明确要丢弃本项目测试数据。

## 一键验收与 UT

扩展运行：`EXTENDED_ACCEPTANCE=1 bash scripts/acceptance.sh`，含有规律的主题簇、近重复、离群点、属性过滤及更新删除后的召回测试。结构化数据和调参结果见 INCREMENTAL.md；样本在 reports/structured-sample-10000.json。

用例矩阵见 [ACCEPTANCE.md](ACCEPTANCE.md)。执行 `bash scripts/acceptance.sh`，本次总结果见 `reports/acceptance.status`。

消费 ACK、保留租约和有界图日志/对象清理可用 `CONSUMER_RETENTION=1` 在新 graph namespace 中启用，见 [CLI.md](CLI.md#消费保留与-gc)。验收入口：`bash scripts/acceptance-retention.sh`。
