# vectorctl 命令行

`vectorctl` 提供集群状态、维护操作、记录读写及最近邻搜索。业务和状态命令通过 HTTP(S) 调用 Gateway，可用于已有远程集群；启动、扩缩容和停止通过本机 Unix socket 调用进程管理器，只管理该管理器启动的进程。

## 编译与配置

在仓库根目录执行：

```bash
bash scripts/go.sh build -o .tools/vector-server ./cmd/vector-server
bash scripts/go.sh build -o .tools/vectorctl ./cmd/vectorctl
export API_TOKEN=local-public-api-token
export CLUSTER_TOKEN=local-peer-rpc-secret
export VECTOR_ENDPOINT=http://127.0.0.1:18202
export VECTOR_CONTROL_SOCKET="$PWD/.state/control.sock"
.tools/vectorctl --help
```

API_TOKEN 用于业务 HTTP，CLUSTER_TOKEN 只由本机启动器传给服务进程；凭据不需要放进命令行参数。已有远程服务只需设置 VECTOR_ENDPOINT 和 API_TOKEN。

全局参数必须放在命令前：`--endpoint`、`--socket`、`--timeout`（默认 90s）。`cluster start` 是前台长期运行命令，不受请求超时限制。正常命令输出 JSON；错误写 stderr，退出码非零。启动命令输出进程管理器运行信息。

## 启动和控制

底层 TiKV、PD、SeaweedFS 需先启动。终端 A：

```bash
.tools/vectorctl cluster start --namespace demo/cli-v1 \
  --workers 2 --partitions 8 --dimension 3 --base-port 18200
```

这会启动 2 Master、1 Gateway 和 2 Worker，Gateway 在 18202。`--namespace` 和控制 socket 是必需配置。默认从二进制相邻的 `../scripts/cluster_local.py` 加载管理器；移动二进制后可以用 `cluster start --launcher /绝对路径/cluster_local.py` 指定。管理器需要 Python 3，并使用仓库 `.tools/vector-server`。

终端 B，设置相同的上述环境变量后：

```bash
.tools/vectorctl cluster local-status
.tools/vectorctl cluster scale --workers 3
.tools/vectorctl cluster scale --workers 1
.tools/vectorctl cluster stop
```

- `local-status` 包含实际集群状态、子进程 PID/退出码、期望 Worker 列表。
- 非零 `scale` 等待 Master 计划及实际 owner 收敛；支持 0..32 个 Worker。不会改变固定分区数。
- `stop` 返回 `stopping: true` 表示已接受停止命令；前台管理器完成子进程回收后退出并移除 socket。Ctrl-C/SIGTERM 也会回收自身子进程。
- socket 权限为 0600，仅同一操作系统用户可连接。已存在的 socket 不会自动覆盖；非正常退出遗留 socket 时，应确认管理器已停止再移除。
- 超时不意味着操作已回滚。扩缩容可能已生效，应先 `local-status` 确认。控制命令串行执行，正在扩缩容时其他控制请求会等待；HTTP 状态查询可独立执行。
- 缩容和故障恢复期间可能短暂缺片；当前无无中断迁移、远程进程管理或自动进程重启。

## 状态与维护

| 命令 | 内容 |
|---|---|
| `cluster status` | 完整集群状态 |
| `cluster nodes` | 存活节点、角色、session、地址及租期 |
| `cluster leader` | 当前主节点、epoch 和租约；主是否可用还应看 status 的 master_available |
| `cluster partitions` | Master 的 assignments、实际 owners 和 ready |
| `cluster layout` | 固定分区数量、数据/索引映射及 hash 协议 |
| `cluster health` | 目标 HTTP 进程状态，不代表所有分片已恢复 |
| `cluster checkpoint` | 为各分片生成快照 |
| `cluster rebuild` | 重建索引并 checkpoint |
| `cluster prune` | 按已发布快照安全清理增量日志 |

维护命令调用现有 Gateway 接口，执行范围为所有索引分片，不提供跨分片原子性。状态命令不包含尚未实现的 CPU、QPS、内存时序监控。

## 插入、更新、读取、删除

以下示例对应上面的 3 维集群：

```bash
.tools/vectorctl record insert --id item-1 --vector '[1,2,3]' \
  --payload '{"category":"book"}' --request-id create-item-1

.tools/vectorctl record get --id item-1

.tools/vectorctl record update --id item-1 --vector '[2,3,4]' \
  --payload '{"category":"book","status":"new"}' \
  --revision 1 --request-id update-item-1-v2

.tools/vectorctl record delete --id item-1 --revision 2 --request-id delete-item-1-v3
```

`insert` 发送 expected_revision=0，已有记录时返回 HTTP 409。`update` 和 `delete` 要求正整数 `--revision`，与当前版本不一致时返回 409，避免覆盖其他写者。更新替换整个向量和属性；省略 payload 会写入空属性，不是局部 PATCH。

`record upsert` 允许创建或覆盖，不要求 revision；也可显式指定 `--revision` 做条件写入。删除后 `get` 返回包含 `deleted: true` 的墓碑，搜索排除该记录；从未存在的 ID 返回 404。

每次逻辑写操作必须指定新的 `--request-id`；重试同一操作时保持 ID 和全部参数不变。该 ID 在 namespace 范围内唯一，不能跨记录重复使用。写入响应包含 `durable` 和 `{id, revision}` 可见性 token。

`--vector`、`--payload`、`--filter` 支持 `@文件路径`，例如 `--vector @query.json`。属性和过滤值必须是字符串，向量维度由服务端校验。

## 最近的 N 个向量

这里的“最近”指与查询向量距离最近，使用 squared-L2；不是按写入时间排序。

```bash
.tools/vectorctl search --vector '[1,2,3]' --k 10
.tools/vectorctl search --vector @query.json --k 5 \
  --filter '{"category":"book"}' --exact
```

`--k` 支持 1..1000，默认 10。默认使用 HNSW 和现有精确回退策略；`--exact` 强制精确检索，`--ef-search` 可设 1..4096 调整 HNSW 搜索预算（0 为服务默认）。Gateway 查询所有分片后合并全局 Top-K，输出 hits 中包含 ID、版本、属性和距离。

写入持久化不等同于索引立即可见。需要读到刚写入的版本时，使用写入响应中的 token：

```bash
.tools/vectorctl search --vector '[2,3,4]' --k 10 \
  --visible-id item-1 --visible-revision 2
```

这会先同步各分片，再检查指定记录版本。不能用 token 实现跨记录、跨分片的事务快照。

## 验收

`cmd/vectorctl/main_test.go` 覆盖请求方法、ID 编码、版本条件、认证、错误退出、参数校验和大整数 epoch。`scripts/test_cluster_local.py` 覆盖控制协议、无效请求不触发操作及扩缩容失败传播。

`python3 scripts/verify_cli.py` 用编译后的命令行启动真实集群，执行状态、插入、幂等重试、版本冲突、读取、文件输入、Top-N、过滤、维护、扩缩容、删除和停止。结果在 `reports/cli-http.json`，已加入统一验收脚本。

## 分页列出向量

```bash
.tools/vectorctl record list --limit 20
.tools/vectorctl record list --limit 20 --cursor '上一页返回的 next_cursor'
.tools/vectorctl record list --limit 20 --include-deleted
```

HTTP 接口为 `GET /v1/records?limit=20&cursor=...&include_deleted=false`，沿用 API_TOKEN 认证。返回 `records`（ID、revision、vector、payload、deleted）及可选 `next_cursor`。默认每页 100 条，支持 1..1000；默认排除删除墓碑。

按**数据分区编号、分区内 ID 字节序**遍历，不按写入时间或向量距离排序。直接读取 TiKV，不依赖 Worker 或索引同步；Master/Worker-only 节点仍禁止调用业务接口。

每次请求最多扫描 1000 条物理记录，因此大量墓碑时可能返回不足 limit 条，甚至空 records，但仍有 next_cursor。继续使用 next_cursor，直到响应不再包含它；不要用 records 数量判断结束。达到页大小时也可能多出最后一次空页请求。

游标绑定当前 namespace 的数据分区布局和 include_deleted 选项，分页中不能切换该选项；limit 可以调整。每页在一个 TiKV 事务读视图中读取，跨页不是固定快照：并发插入到已遍历位置的记录可能不会出现，更新和删除也会反映在后续页。需要稳定导出时应暂停写入；此接口不提供时间点备份语义。

升级后需要重启原先运行的 vector-server/Gateway 进程才能使用新接口。编译不会自动替换运行中的进程。

启用 Gateway gRPC 可在 `cluster start` 后添加 `--grpc-port 18250`；Python/Go SDK 用法见 [RPC.md](RPC.md)。

双副本 onebox 可添加 `--replicas 2`（新 namespace），此时非零缩容先排空再停止；详见 [REPLICAS.md](REPLICAS.md)。

## 迁移任务、调度解释和预览

双副本 tasks v1 集群支持：

```bash
.tools/vectorctl --endpoint http://127.0.0.1:18202 cluster tasks --limit 50
.tools/vectorctl --endpoint http://127.0.0.1:18202 cluster tasks --limit 50 --after '<next>'
.tools/vectorctl --endpoint http://127.0.0.1:18202 cluster task '<task-id>'
.tools/vectorctl --endpoint http://127.0.0.1:18202 cluster explain index-0000
.tools/vectorctl --endpoint http://127.0.0.1:18202 cluster balance --dry-run
```

tasks 按任务 ID 分页，limit 为 1..200，返回 next 时继续传入 after；plan 的 task_id 指向当前任务。阶段、blocked、timed_out、last_error、progress 中的 lag 和 history 可以区分尚未领取、恢复、追赶、资源退休等原因。任务的 Completed 表示目标已满足迁移屏障，在线源已确认资源释放或原源节点租约已失效。

balance 必须显式带 --dry-run，当前没有手工强制执行接口。它不会修改计划或冷却时间；结果只代表 snapshot_ts 对应的输入。explain 返回同样的全局调度原因及指定分片的计划、建议和任务信息。

对应鉴权 HTTP GET 接口：/v1/cluster/tasks?limit=50&after=...、/v1/cluster/task?id=...、/v1/cluster/explain?partition=...、/v1/cluster/balance。与 /v1/cluster 一样可从三种角色查询，仍需 API_TOKEN；这些运维接口暂不包含在业务 gRPC SDK 中。

容量估算准入可在 Worker 设置 WORKER_MEMORY_BUDGET_BYTES，需 load 调度，默认不限制。状态机、预算假设和新 namespace 升级限制见 DESIGN.md 的“分片迁移任务与调度控制面”。

## 异步 HNSW 图复制

使用全新 namespace，在所有 Master/Gateway/Worker 上一致设置 `INDEX_REPLICATION=graph`、`REPLICAS=2`。本地 onebox 可直接继承该环境变量：

```bash
INDEX_REPLICATION=graph .tools/vectorctl --socket .state/graph-control.sock cluster start --namespace graph-demo --replicas 2
.tools/vectorctl cluster status
```

多机器部署额外设置 `REPLICA_FAILURE_DOMAIN=host`，每台物理机器配置唯一且稳定的 `HOST_ID`；同机多个 Worker 使用相同 HOST_ID。全部角色必须使用相同的 failure-domain 策略。独立构图与图复制不能混用已有 namespace。

状态字段 `graph_heads[index].visible` 是该分区最新已发布位置，`graph_positions[replica]` 是副本已应用位置。两个副本进度不同是允许的；Follower 落后不阻塞 Leader 发布。普通查询允许陈旧结果，需要确认某次写入可见时使用原有 MinVisible token。
