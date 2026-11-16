# gRPC 与 Python / Go SDK

Gateway 同时支持 HTTP/JSON 和 gRPC/Protobuf，调用同一业务层，记录、版本、请求幂等、分片路由和索引可见性语义一致。协议定义在 `proto/vectorstore/v1/vector.proto`，服务全名为 `vectorstore.v1.VectorStore`。生成的 Go/Python 协议代码已随仓库提供。

## 启动

在仓库根目录先编译；已有集群需要重启进程才能加载新接口。不要改变已有 namespace 的维度和固定分区数量。

```bash
bash scripts/go.sh build -o .tools/vector-server ./cmd/vector-server
bash scripts/go.sh build -o .tools/vectorctl ./cmd/vectorctl
export API_TOKEN=local-public-api-token
export CLUSTER_TOKEN=local-peer-rpc-secret
export VECTOR_CONTROL_SOCKET="$PWD/.state/rpc-control.sock"
.tools/vectorctl cluster start --namespace demo/rpc-v1 \
  --workers 2 --partitions 8 --dimension 3 --grpc-port 18250
```

该示例需要 TiKV/PD/SeaweedFS 已启动：HTTP Gateway 在 18202，gRPC 在 18250。只给 Gateway 开启 gRPC，不给每个 Worker 绑定同一端口。省略 `--grpc-port` 时保持原 HTTP-only 启动方式。管理器预留 base-port 到 base-port+34 给 HTTP 节点，RPC 端口必须在该范围外。

独立启动服务可以设置 `GRPC_LISTEN_ADDR=127.0.0.1:18250`。同一进程若有 Master/Worker 但无 Gateway 角色，只允许 Health/GetLayout/GetCluster，拒绝业务及维护 RPC。Gateway 到 Worker 的内部通信目前仍是原有 HTTP RPC，本次只增加对外 gRPC。

## 接口与认证

| RPC | 功能 |
|---|---|
| Put / Get / Delete | 写入、读取、删除记录 |
| List | 分片顺序分页，默认隐藏墓碑 |
| Search | 全局 Top-K、属性过滤、HNSW 或精确搜索 |
| Health | 进程状态和 namespace，不等同于分片全部 ready |
| GetCluster / GetLayout | 主节点、节点列表、分片归属与布局 |
| Checkpoint / Rebuild / Prune | 快照、重建及安全日志清理 |

请求 metadata 必须包含 `authorization: Bearer <API_TOKEN>`，SDK 自动附加。CLUSTER_TOKEN 不用于外部 RPC。协议是明确类型的 Protobuf 消息，epoch、revision、offset 使用 uint64，不通过浮点数转换。

`Put.expected_revision` / `Delete.expected_revision` 是 optional：省略表示无版本条件，0 表示记录当前版本必须为 0，正值表示 CAS 更新/删除。SDK 的 insert 自动携带 0；update 要求正版本。每次逻辑写操作使用新的 request_id，重试保持 request_id 和所有请求参数不变。SDK 不主动重试写入；遇到超时，写入可能已提交，使用同一请求 ID 查询/重试，不能假定失败意味着没有写入。

List 的 limit=0 使用默认 100，最大 1000。可能出现空 records 但仍有 next_cursor，必须继续读取直到游标为空。每页是独立事务视图，跨页不是时间点快照，详见 CLI.md 的列表语义。

Search 的 k 必须为 1..1000。min_visible 使用写入响应 token，同步索引并检查指定记录版本；不是跨记录事务快照。

错误使用标准 gRPC code：

| Code | 含义 |
|---|---|
| UNAUTHENTICATED / PERMISSION_DENIED | token 无效 / 角色不允许 |
| INVALID_ARGUMENT | 维度、参数、游标不合法 |
| NOT_FOUND | 记录从未存在 |
| ABORTED | 版本或幂等内容冲突 |
| UNAVAILABLE | 分片不可用、索引尚不可见或存储失败 |
| DEADLINE_EXCEEDED / CANCELLED | 请求超时或取消 |
| RESOURCE_EXHAUSTED | 消息超过 gRPC 大小限制 |
| UNIMPLEMENTED | 单机模式未提供集群状态 |

服务端每次 RPC 最长 30 秒，客户端更短的 deadline 优先生效。SDK 默认连接/调用超时 30 秒；Go 使用 context 取消，Python 同步调用可通过每次调用的 timeout 设置截止时间。超过 RPC 超时不保证底层原生 HNSW 计算立即停止。请求上限 1 MiB，响应上限 64 MiB，列表体积过大时减小 limit。

## Python SDK

安装仓库中的包（尚未发布到 PyPI）：

```bash
.venv/bin/python -m pip install ./sdk/python
```

示例对应 3 维集群：

```python
import os
from vectorstore import Client

with Client("127.0.0.1:18250", os.environ["API_TOKEN"]) as client:
    print(client.cluster())
    result = client.insert("example-1", [1, 2, 3],
                           payload={"category": "book"}, request_id="example-create-1")
    print(client.get("example-1"))
    print(client.search([1, 2, 3], k=10, min_visible=result.token))
    changed = client.update("example-1", [2, 3, 4], revision=1,
                            payload={"category": "book"}, request_id="example-update-1")
    for record in client.iter_records(limit=20):
        print(record.id, record.revision, list(record.vector))
    client.delete("example-1", expected_revision=changed.token.revision,
                  request_id="example-delete-1")
```

返回值是生成的 Protobuf 对象；错误为 `grpc.RpcError`，使用 `exc.code()` 和 `exc.details()` 读取。也可直接使用 `vectorstore.messages` 构造消息。`put` 为 upsert，`list` 返回单页，`iter_records` 自动分页。更新替换整个 vector 和 payload，不做局部合并。再次执行示例时需考虑已有记录及墓碑的版本。

## Go SDK

包路径为 `example.com/vector-storage/sdk/go`，包名 `vectorstore`。这是当前仓库 module 路径，尚未发布公共 Go module；在仓库内可直接使用，其他本地项目可用 go.mod 的 replace 指向此仓库。发布时应统一修改 module 和 proto go_package 并重新生成代码。

```go
import (
    "context"
    "os"
    pb "example.com/vector-storage/api/vectorstore/v1"
    vectorstore "example.com/vector-storage/sdk/go"
)

ctx := context.Background()
client, err := vectorstore.Dial(ctx, "127.0.0.1:18250", os.Getenv("API_TOKEN"), vectorstore.Options{})
if err != nil { panic(err) }
defer client.Close()
written, err := client.Insert(ctx, "go-example", []float64{1, 2, 3}, nil, "go-create-1")
if err != nil { panic(err) }
result, err := client.Search(ctx, &pb.SearchRequest{
    Vector: []float64{1, 2, 3}, K: 10, MinVisible: written.Token,
})
if err != nil { panic(err) }
_ = result
```

Client 内嵌生成的完整 VectorStoreClient，全部 RPC 方法可直接使用，并额外提供 Insert/Update；所有方法自动附加认证和超时。Go 错误通过 `status.Code(err)` 判断。完整可运行的只读示例：

```bash
RPC_TARGET=127.0.0.1:18250 bash scripts/go.sh run ./examples/grpc-go
```

## TLS

本机示例使用明文 gRPC。服务端设置 `GRPC_TLS_CERT` 和 `GRPC_TLS_KEY` 可开启服务器 TLS；证书需包含客户端连接地址。Python 使用 `Client(..., tls=True, root_certificates=ca_bytes)`；Go 使用 `Options{TLSConfig: &tls.Config{RootCAs: pool}}`。未实现客户端证书认证（mTLS）或细粒度授权。单测覆盖 Go SDK 通过受信任证书连接 TLS 服务。

## 生成、测试与边界

```bash
.venv/bin/python -m pip install -r scripts/rpc-tools.txt
GOBIN="$PWD/.tools" bash scripts/go.sh install google.golang.org/protobuf/cmd/protoc-gen-go@v1.30.0
GOBIN="$PWD/.tools" bash scripts/go.sh install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0
bash scripts/generate-rpc.sh
bash scripts/go.sh test -race ./...
PYTHONPATH=sdk/python .venv/bin/python -m unittest discover -s sdk/python/tests -v
.venv/bin/python scripts/verify_rpc.py
```

真实验收使用 2 Master + 2 Worker + 1 Gateway、真实 TiKV/SeaweedFS，Python 写入后 Go 读取和更新，再由 Python 验证；包含认证、幂等/CAS、分页、过滤、可见性、快照/清理/重建和删除，结果在 reports/rpc-http.json。已加入统一验收入口。

目前是同步 unary RPC，无流式批量导入、异步 Python 客户端、自动服务发现、自动负载均衡或 SDK 发布流程。vectorctl 的业务命令仍走 HTTP；本机扩缩容仍走控制 socket，两者均保留兼容。

## 按分片订阅变更

新增 `ReadChanges(ChangeRequest) -> ChangePage`；HTTP 等价接口是 `POST /v1/changes`。这是有界分页拉取接口，适合消费者自行轮询和保存断点，不是长连接推送。

请求字段：`partition`（如 index-0000）、`stream`（data 或 graph）、`after`（不包含该序号，初始为 0）、`limit`（默认 100，最大 256）。一个游标只属于一个 namespace、分片、流；不同分片不能共用游标，也不存在全局顺序。消费一页并持久化处理结果后，再持久化响应 `next`。重试可能重复返回事件，消费者应按分片/流/sequence 幂等处理。

- data：返回包含完整 Record 的顺序事件，包括更新和墓碑；它表达 TiKV 已持久化，不代表已可检索。
- graph：只在 INDEX_REPLICATION=graph 时提供；只返回已发布条目，不返回 committed-but-unpublished。条目含 previous、position、term、kind 和 object manifest。消费者通过已有 SeaweedFS S3 端点读取 object.key，核验 size/SHA256，再处理内容。RPC 不转发大对象，也不下发 S3 凭据。
- delta 对象中的 records 是记录前后版本，nodes 将 label 映射到 record ID/revision，新增 links 提供被修改节点的完整分层邻接表：internal_id、label、layers。先安装本批所有节点映射，再替换邻接表。邻居使用内部节点 ID，旧记录版本可能仍留在图中，须根据 records 的当前 revision/deleted 过滤。
- links 是 HNSW 图边，**不是每个向量的精确 Top-K 最近邻表**。本次版本之前的图日志对象可能没有 links 字段；不能把缺失字段当成空邻接表。
- kind=reset 表示 Leader 重建后的完整 IndexSnapshot，消费者必须加载并替换整个图及映射，再应用后续 delta；不能跨 reset 复用旧内部节点编号。
- 若 data 游标已经低于清理水位，响应 `reset_required=true`、floor、checkpoint（若有），events 为空、next 保持原值。消费者必须从经校验的 checkpoint 恢复记录基线，再从快照 Offset 续读。没有可用快照时应停止并请求基线，不能直接把 after 改成 floor。分页 List 不提供一致性快照，不能作为无损补齐基线。
- 单独读取订阅页不注册保留租约，不会阻止 prune。需要保留承诺时，启用下述消费保留模式并先 acquire、处理后 ACK。Worker 全部离线时，只要 Gateway、TiKV 与所需对象存储可用，已有日志仍可订阅。

Python：

```python
page = client.changes('index-0000', stream='data', after=saved_offset, limit=100)
if page.reset_required:
    raise RuntimeError('需要先从 checkpoint 恢复')
for event in page.data:
    apply_idempotently(event.sequence, event.record)
save_offset(page.next)
```

Go（已有 Client 内嵌生成的 RPC client）：

```go
page, err := client.ReadChanges(ctx, &vectorv1.ChangeRequest{
    Partition: "index-0000", Stream: "graph", After: savedOffset, Limit: 100,
})
```

需使用与现有 RPC 相同的 API_TOKEN 和 Gateway 角色。无效参数返回 InvalidArgument/HTTP 400；未启用图日志返回 Unimplemented/HTTP 501。`reset_required` 是正常协议状态，应显式处理。

## 消费 ACK 与保留租约

在 `CONSUMER_RETENTION=1` 的新 graph namespace 中，使用 `ManageConsumer` RPC 或 `POST /v1/consumers`。旧模式仍可无租约订阅，但此接口返回 Unimplemented/501。

请求：partition、stream、consumer、session、epoch、ack、ttl_seconds、action。action 为 acquire / ack / release / list；list 是只读操作，只需 partition 和 stream。响应 leases 按消费者名称返回 session、epoch、ack、expires_ms，并返回当前 floor/head。租约时间使用 TiKV 时间，消费者应提前续租，不要等本机时间到期才续。

```python
import uuid
session = uuid.uuid4().hex
state = client.consumer('index-0000', stream='graph', action='acquire',
                        consumer='analytics', session=session, ack=saved_sequence)
lease = state.leases['analytics']
# 拉取并校验图对象，在下游持久化本页全部效果之后，才推进 ACK。
state = client.consumer('index-0000', stream='graph', action='ack',
                        consumer='analytics', session=session,
                        epoch=lease.epoch, ack=processed_sequence, ttl_seconds=60)
# 处理耗时较长时，以原 ACK 续租，不提前确认尚未处理的数据。
```

Go Client 内嵌的 `ManageConsumer(ctx, *vectorv1.ConsumerRequest)` 提供同样接口。租约是保留承诺，不是 exactly-once 下游提交：消费者仍需幂等写入，并协调下游事务与自己的断点。ACK 是消费者对完成处理的声明，服务端不验证下游实际完成。

有效租约不能被另一 session 抢占。冲突返回 Aborted/HTTP 409；过期、已释放或旧 epoch 返回 FailedPrecondition/409；从已清理位置 acquire 返回 FailedPrecondition/410；无效参数为 InvalidArgument/400。遇到过期应查询最新 floor，必要时恢复检查点，并使用新 session acquire。

启用保留模式后，`Prune` / `POST /v1/log/prune` 同时执行数据日志清理、图日志清理与登记对象 sweep。原有返回值仍是数据日志 floor；图 floor 在 GetCluster 的 graph_heads 中查看，consumer_retention 表示是否开启。清理失败可能已完成某些分片或阶段，可检查状态后重新执行；操作有界且可继续推进。

## 聚类布局扩展

Layout.routing 和 ClusterStatus.routing 描述固定中心，protobuf 为 ClusterRouting(version, centers)，每个 Center 包含 vector。聚类布局 Binding.data_partition 为 -1（字段类型扩展为 int32，原非负值编码不变），表示索引没有唯一数据绑定。ClusterStatus.data_progress 返回各 data-NNNN 的 head/completed_lsn/floor/owner，GraphHead.sources 的键为来源数据分区十进制编号。data 订阅和租约请求使用 data-NNNN，graph 使用 index-NNNN；两种 LSN 不可混用。数据 reset 快照为 data-records-v1 的 JSON Record 数组，完整协议见 [CLUSTER_ROUTING.md](CLUSTER_ROUTING.md)。Python/Go SDK 生成类型已更新。
