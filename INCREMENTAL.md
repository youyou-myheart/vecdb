# 增量日志、快照位点与并发索引（v0.3）

本轮把每 500 ms 重读全部 Outbox 改为从消费位点读取新事件。轮询间隔仍为 500 ms，但没有新写入时只读少量元数据，不扫描历史日志。同时实现快照绑定位点、受快照保护的分批清理、HNSW 并行查询和持续写入时的重建追赶。

## 数据流

```mermaid
flowchart TB
    W[写入请求及 request_id] --> TX[同一个 TiKV 乐观事务]
    TX --> R[完整记录或墓碑]
    TX --> ACK[幂等凭证]
    TX --> HEAD[log-head 加 1]
    TX --> E[log / 固定宽度序号：写后镜像]
    E --> B[从 cursor + 1 顺序读取，校验连续性]
    B --> APPLY[应用到版本表、属性倒排及 HNSW]
    APPLY --> CURSOR[应用成功后推进内存 cursor]
    CURSOR --> FREEZE[同一同步锁下导出图与 cursor]
    FREEZE --> S3[SeaweedFS 不可变快照：包含 offset]
    S3 --> VERIFY[完整回读与摘要校验]
    VERIFY --> CAS[TiKV manifest CAS：offset 不能倒退]
    CAS --> M[当前持久化快照及 offset]
    M --> GC[分批删除序号不超过 offset 的日志]
    GC --> FLOOR[同事务推进 log-floor]
    M --> RESTORE[重启加载图及 offset]
    RESTORE --> B
    FLOOR --> LAG{cursor 落后于 floor?}
    LAG -->|是| BOOT[在同一 TiKV 快照读取全部记录及 head]
    BOOT --> APPLY
```

### 为什么这个游标不会漏并发提交

每个写事务先读取 `log-head=H`，再一起写入 `H+1`、`log/H+1`、记录和幂等凭证。两个事务如果读到同一个 H，就会竞争同一个 head 键；TiKV 只允许其中一个成功，另一个需要重试。序号不预分配，失败事务不会留下已确认的缺口。因此读取某个事务快照中的 head，也能读取该快照中所有尚未清理且序号不超过 head 的事件。

只有明确的 TiKV write conflict 才自动重试（最多 16 次尝试，带有限等待）；提交结果不确定仍返回错误，调用者使用相同 request_id 重试。每次重试先检查凭证，已提交操作不会产生第二条事件。单个 head 是有意保留的单分片写热点，不是无限扩展方案。

## 消费、恢复与清理

一次 Sync 固定首次读取的 head 作为目标，最多每批 256 条，或累计约 8 MiB 后停止该批。检查事件键与内部序号逐条连续；遇到缺口、坏数据、迭代错误，不跳过该批。每批应用成功才更新 cursor，下一批失败不会撤销已经完成的前缀；重试从最后成功位点继续。持续新增写入不会让一次 Sync 无限追赶。

cursor 只存在内存中时不能用于清理。Checkpoint 在同一个同步锁下导出图、记录和 cursor，随后释放锁上传并校验对象。对象与 manifest 都包含 `log_version=1` 和 `offset`，加载时两者必须一致。发布检查代际和位点单调性，禁止旧索引发布倒退的位点。

`POST /v1/log/prune` 每次最多清理 256 条，只能删除到当前已发布快照的 offset，并在同一个 TiKV 事务中推进 floor。它不调用 checkpoint，也不因内存已消费就提前删除。重复执行安全；事务失败不会只推进 floor 而保留不一致的删除状态。接口需现有 Bearer token。当前由调用方触发清理，没有默认定时清理或自动 checkpoint。

```bash
curl -X POST http://127.0.0.1:18090/v1/checkpoint \
  -H 'Authorization: Bearer local-vector-test-token'
curl -X POST http://127.0.0.1:18090/v1/log/prune \
  -H 'Authorization: Bearer local-vector-test-token'
```

调用 prune 多次，直到返回的 `retained_after` 不再增长，即已追上当前快照位点。新日志不会因这个调用被误删；下次 checkpoint 后才可继续推进。

重启先从快照 offset 继续消费。落后于 floor 的消费者在一个 TiKV 事务快照中读取完整 record 表和 head，补齐查询视图并将 cursor 设为该 head。记录表保留墓碑，因此旧事件或旧幂等请求不会使删除记录复活。这个恢复路径是有限容量的完整扫描（目前上限 100,000 条记录），不是每次轮询都扫描。没有自动清理 record 墓碑、幂等凭证、旧对象或旧版随机 ID Outbox。

## 旧版升级

**升级现有 namespace 前必须停止全部旧版本写入进程，再启动新版本；不支持新旧写者混跑。** 新版本将 schema 从 v1 升到 `v2:dimension=…:metric=l2:log=1`；旧版本之后无法重新打开该 schema，但无法阻止已经运行的旧进程继续写入。

旧快照没有安全消费位点，新服务会从完整记录表和 head 的同一事务快照建立基线，再开始增量读取。旧随机事件 ID 不转换为序号，历史 Outbox 保留。首次成功 checkpoint 之后即可清理新格式日志。升级后不能直接回滚旧二进制，应从备份恢复到独立 namespace。此次自动化验收使用新建随机 namespace，没有替换正在运行的 demo 服务或迁移其数据。

## HNSW 并行查询

hnswlib 包装层使用 C++14 共享读锁，搜索可并发，插入、加载和保存使用排他锁。搜索 ef 和距离计算计数改为请求局部变量，避免不同请求串改 ef，也避免共享计数器争用。visited list 使用上游自带的线程安全池。图的二进制布局未变。

原生补丁集中在 `internal/ann/hnswlib/hnswalg.h`，保持原虚方法接口，新增带局部 ef 的搜索入口。计数现在记录实际距离函数调用（再加 Go 精确重排），与 v0.2 的邻接访问估计不能直接对比。Go race 和独立 C++ ThreadSanitizer 压测共同验证并发路径。

## 持续写入时重建

旧实现只要发现 epoch 变化就放弃新图。现在同一索引只允许一个重建者：锁内获取记录基线，锁外建图，同时把后续写入按 ID 合并到最新版本增量表。重建者不断摘取增量并在锁外应用；尾部不超过 256 条时，短暂阻止索引写入，应用尾部后交换图，释放旧图。

普通写入不再直接导致重建冲突。重复重建仍返回冲突；取消或构建失败保留旧图。写入速度长期高于追赶速度时仍可能超时，需要调节流量和重建资源，不能保证任意负载下完成。重建时保留新旧图、记录基线和增量，峰值内存高于稳定查询状态；最后尾部暂停的时间取决于维度和建图参数，并非固定毫秒上限。

## 运行与验收

- `EXTENDED_ACCEPTANCE=1 bash scripts/acceptance.sh`：一键执行以下全部检查，任一步失败停止。
- `bash scripts/acceptance.sh`：UT、Go race、召回门禁、真实 TiKV/SeaweedFS、日志并发与清理恢复、HTTP 强杀恢复、可见延迟、Python S3。
- `bash scripts/native-check.sh`：原生 ThreadSanitizer，8 个并发查询线程与写入线程；要求支持 TSan 的 clang++。
- `CAPACITY_ACCEPTANCE=1 bash scripts/go.sh test -count=1 -run TestCapacityAcceptance -v ./internal/vector`：1 万／5 万条、128 维，召回与延迟、4 路查询吞吐、持续写入时重建、进程峰值 RSS。
- 设置 `VECTOR_DATASET=/absolute/path/records.json` 可改用真实记录数组（每条包含唯一 id、正 revision、同维 vector，必须为存活记录）。真实数据模式使用样本向量加微小扰动构造查询，生产验收仍需要独立查询集和业务过滤分布。

报告：`reports/acceptance.status`、`reports/ann-acceptance.json`、`reports/capacity.json`、`reports/visibility-latency.json`、`reports/native-tsan.status`、`reports/coverage.txt`。延迟样本量有限，不是生产 SLA；不同规模使用不同 ef，必须连同参数解读召回和性能。

## 本机验收结果（2026-11-12）

最终 `EXTENDED_ACCEPTANCE=1 bash scripts/acceptance.sh` 通过；Go 总语句覆盖率 85.4%（vector 88.2%、原生包装层 82.0%、HTTP/main 71.8%）。Go race、go vet、真实后端集成、HTTP 清理后强杀恢复、原生 ThreadSanitizer 均通过。Go 测试运行于 darwin-amd64/Rosetta，TSan 使用本机 clang 架构；不代表跨平台矩阵已验收。

| 合成数据规模（128 维） | ef_search | Recall@10 | 单路 ANN P95 / P99 | 4 路 QPS：串行基线 → 并行 | 持续写入时重建 | 进程峰值 RSS（含重建） |
|---|---:|---:|---|---|---|---|
| 10,000 | 256 | 99.0% | 0.365 / 0.430 ms | 4,561 → 17,271 | 1.50 s | 约 130 MiB |
| 50,000 | 1024 | 98.2% | 2.360 / 2.500 ms | 611 → 2,256 | 12.90 s | 约 497 MiB |

并发对照在同一实现、同一组查询和同样 4 个客户端下，通过外部互斥锁模拟串行入口；测量的是内存索引，不包括 HTTP/TiKV/S3。重建写入目标频率为 100 次/秒，不承诺实际达到该速率。50 条召回查询与短时吞吐测试仅用于回归和容量初测。

真实 TiKV 验证了 8 个并发写者提交 96 条连续事件、快照覆盖前缀清理以及之后的删除恢复。500 ms 轮询下的 12 次小写入测得写入 P95 9.51 ms，提交至可见 P95 508.81 ms；样本不足以估计生产尾延迟。没有提供真实业务向量和查询集，因此生产数据容量/召回验收仍待完成。

## 补充：有规律的合成数据

新增结构化合成验收，替代“只有独立高斯向量”的单一视角，同时保留原高斯测试作为基线。数据由 16 个不同中心的主题簇构成，头部主题约占一半；簇内由 8 个潜在因子映射到 128 维，加入小幅噪声，所以维度间相关。约 5% 是近重复内容，约 1% 是远离主题簇的离群点。topic 属性与几何分布相关，tenant 属性交叉分布，rare 属性用于稀疏 AND 过滤。

每种规模测试 8 类查询，每类 40 个，在初始、更新删除后、恢复重建后三个阶段分别与精确结果对照。更新阶段移动 5% 的向量并修改 topic/tenant，另外删除 5%。每个非空场景独立要求 Recall@10 ≥95%，不通过总体平均掩盖某类查询失败。结果见 reports/structured.json；运行方式和用例见 ACCEPTANCE.md。数据有明确规律，但仍是合成数据，不等同于真实 embedding 或生产负载。

### 结构化数据暴露的问题与验收参数

首次使用原有按 ID 顺序重建时，5 万条数据的“恢复重建后主题边界”Recall@10 为 89.7%，未通过门槛。ID 按主题分布会形成连续的插入批段，因此重建现在用固定种子 42 打散基线记录插入顺序，避免直接继承 ID 的分组顺序。快照字节格式、记录版本和返回距离语义不变。

插入顺序调整后，1 万条离群点查询在默认 ef=256 下仍只有 93.5%（恢复重建阶段），说明重排插入顺序并不能替代搜索预算标定。本轮采用 **1 万条 ef=512、5 万条 ef=1024** 的验收配置，未降低 95% 召回门槛。服务默认 ef=256 没有修改；使用这类分布时应显式设置请求 `ef_search`，然后按实际数据校准延迟与召回。

| 规模 | ef_search | 初始阶段最差非空场景 Recall@10 | 更新删除后 | 恢复重建后 |
|---|---:|---:|---:|---:|
| 10,000 | 512 | 99.50% | 99.44% | 96.00% |
| 50,000 | 1024 | 98.50% | 98.75% | 96.25% |

共 48 个“规模 × 阶段 × 场景”组合、1,920 次查询，每次与当前精确真值比较。5 万条测试有部分过滤查询走精确兜底，报告单独记录 plans 和 fallback_queries；不能把这些成功结果全部归为 HNSW 召回。这个测试发现了重建顺序敏感性，也保留了对离群点和跨主题边界的持续回归检查。
