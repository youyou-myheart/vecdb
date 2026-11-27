# TiKV + SeaweedFS 向量存储 v0.5

Go 服务提供向量读写、HNSW 和精确检索、属性过滤。TiKV 持久化分片日志与幂等凭证，SeaweedFS 保存图检查点。

先阅读 [当前架构与分区设计](ARCHITECTURE.md)：其中说明了两层分区、Worker 部署与图副本复制。DataPartition 和 IndexPartition 共用选主与日志接口，Dispatcher 按来源 LSN 续读并投递给目标索引。详见 [分片日志协议与数据流](CLUSTER_ROUTING.md#分片日志接口)。

详细架构、协议与商用开发阶段见 [DESIGN.md](DESIGN.md)。索引实现和验收数据见 [HNSW.md](HNSW.md)。增量消费、升级步骤和并发优化见 [INCREMENTAL.md](INCREMENTAL.md)。两层分区见 [PARTITIONS.md](PARTITIONS.md)：数据按 ID hash 分区，固定簇布局支持独立配置索引分区数，也支持一一映射布局。多进程角色、TiKV 主备选举和故障接管见 [CLUSTER.md](CLUSTER.md)，尚非生产完成版。

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

聚类 Index Leader 已采用事件消费：入队唤醒、连续处理到队列为空、阻塞等待新通知。各索引分片独立推进，取消该模式下每 500 ms 只处理一批的后台调度。

受限容量探索入口：`.venv/bin/python scripts/capacity_explore.py --series my-run --records 960 --dimension 64 --rate 80`。每次只跑一个档位，包含磁盘/内存/CPU 守卫和本轮数据清理；资源预算及已验证范围见 [PIPELINE_LATENCY.md](PIPELINE_LATENCY.md)。

探针支持最多 2,000 条的分桶全量核验，服务查询仍限制 `K ≤ 1000`。使用 `--records 2000 --dimension 64 --rate 20 --seed-rate 20 --rounds 2 --load-seconds 3` 运行短冒烟；全部在役副本、两个 Gateway、空桶、删除结果和查询后成员检查共用最后 durable ACK 的计时起点。初始写入默认限速 20 次/秒，不能与旧不限速初始化结果直接比较。错误诊断、运行边界和结果以时延文档为准。

探针 profile 优化后，2,000 条 / 64 维、20 次/秒的同容量五轮复测通过，完整停写核验最大 601 ms；单次样本与短轮次不代表生产 SLA。各阶段证据和仍未覆盖的范围见[复测记录](PIPELINE_LATENCY.md#优化后的真实进程复测)。

图协议与升级边界见 [GRAPH_JOURNAL.md](GRAPH_JOURNAL.md)；新协议需新 namespace，不能与旧发布协议混用。

日志路径现支持同一 DataPartition 的数据事务合批、批量读取来源水位、图日志持久化即允许回放（无额外发布事务），以及 Follower 的 TiKV 图载荷及数据引用最多 4 路预取与顺序 apply。用 `.venv/bin/python scripts/capacity_explore.py --series batch-check --records 2000 --dimension 64 --rate 20 --seed-rate 40 --rounds 3 --load-seconds 8 --burst-writes 32` 验证同分片并发突发写；批量上限、故障语义与测量口径见 [PIPELINE_LATENCY.md](PIPELINE_LATENCY.md)。

## 启动基础组件

服务统一使用 TiKV API v2，让普通日志写入和选主事务安全地共用 TiKV。服务端必须设置 `[storage]` 下的 `api-version = 2` 和 `enable-ttl = true`；仓库内的本机、CI 和压测配置已经启用。普通日志仍使用 RawKV `BatchPut`，不会因此增加事务。

RawKV 写入器只为尚未确认的请求暂存版本号和请求结果。确认后释放这份暂存数据，后续重试查询已有的日志历史，重启后也不再复制整份历史请求表。这不增加 TiKV 请求，也不缩短请求重试的有效期。

每个数据分区的小批次最多同时发送 2 批，后续请求在已有写入进行时自然合批；队列达到 32 条或估算 1 MiB 时，可以立即使用额外的发送位置，总上限仍为 16 批。空闲分区立即写入，不使用额外的等待计时器。每条写入仍是独立日志 key，多条通过一次普通 `BatchPut` 发送；只有前面的日志都确认成功，才按顺序返回 ACK。

RawKV 日志历史将较旧记录的内容转存到本机临时文件，内存中的近期内容以 4096 条、估算 8 MiB 为目标；单个正在加入的批次可以暂时超过字节目标。转存按块进行，成功写入后才释放旧内容。旧请求重试、旧版本读取和日志回放按位置读取文件，并校验内容；读写错误会明确返回，不会被当作记录不存在。版本号、记录 ID、请求 ID 和文件位置索引仍在内存中，且仍随数据量增长，因此这不是整个进程的 RSS 上限。

后台读取和恢复 RawKV 日志时，先用 `BatchGet` 读取 4 个 key，再按上一批最大的日志值估算后续批量，最多一次读取 8 个 key。大记录使用较小的批量；读取量以约 1 MiB 为估算目标，并非严格字节上限，后续记录突然变大时仍可能超出。解析和校验每组最多 4 个 key，保留旧格式中一个 key 含多条记录的兼容性。发现缺口仍从缺口重新确认，不跳过；发生错误时返回已经完成的读取位置，重试不会重复应用前面的记录。这不改变日志写入方式，也不增加事务。

临时文件使用系统临时目录（`TMPDIR`）及仅当前用户可读写的权限，在 Linux/macOS 上创建后立即移除目录项，文件句柄随历史缓存回收或进程退出而关闭。文件仍占用磁盘空间，应为临时目录预留容量。TiKV 的完整日志继续作为恢复依据；重启或切主重新读取日志并构建本机缓存，不依赖旧进程的临时文件。本次不删除 TiKV 日志，也没有缩短全量日志恢复的时间。

保存 HNSW 快照时，生产对象存储适配器将记录、节点和图数据分段编码到本机临时文件，再从文件上传到 SeaweedFS；上传后分段读取对象，核对总长度和 SHA-256。这样不再同时保留整份 JSON 编码缓冲和整份校验副本。现有快照格式、恢复读取方式和 TiKV 请求数量不变。临时文件只在保存期间占用磁盘，关闭或进程退出后释放；截取索引时的记录和图副本仍占内存，因此这不是整个 worker 的内存上限。

**旧数据升级：不能直接把已有 API v1 数据目录改成 v2，也不能只换 namespace。** 请保留原集群与备份，停止旧服务后，将数据迁移到单独部署的 API v2 集群，再切换服务。当前代码不会自动迁移、清空旧数据或退回 v1。已有日志和元数据都需要迁移，不能只复制 SeaweedFS 的图快照。此处的格式兼容要求见 [TiKV API v2 官方说明](https://tikv.org/docs/7.1/concepts/explore-tikv-features/api-v2/)。长期运行还需要配置 TiKV 的旧版本数据回收；短期独立测试在结束时清理本轮数据。

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
- Outbox 已改为有序增量日志，支持快照保护的分批清理；完整基线恢复仍限制为 100,000 条记录。已支持固定分区跨进程分配、故障接管、基础负载调度与有界队列背压，尚无完整容量管理。
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

图复制现使用 v12 数据引用日志：正常构图不上传 SeaweedFS 增量对象，每个索引分片累计 30 MB 或等待 3 分钟生成完整检查点，再按依赖顺序清理图日志与数据日志。配置 `GRAPH_CHECKPOINT_BYTES=30000000`、`GRAPH_CHECKPOINT_AGE=3m`；需要新 namespace，详细恢复/订阅/升级边界见 [GRAPH_JOURNAL.md](GRAPH_JOURNAL.md)。
