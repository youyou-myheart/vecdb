# 单进程七服务端到端验收流水线

本流水线在**一个 Go 测试进程**内启动七个真实服务实例：

- 2 个 Gateway；
- 2 个 Master，使用 TiKV 选主；
- 3 个 Worker，32 分片、双副本、load 调度。

各实例拥有独立的节点身份、生命周期、监听端口及 TiKV 客户端。公共请求和 Gateway→Worker 请求都经过真实 HTTP 连接，不用内存 mock 代替网络。端口由系统动态分配，不占用现有 onebox 固定端口，也不会停止已有集群。

TiKV、PD、SeaweedFS 仍是外部依赖。**单进程是指七个向量服务实例，不包含存储组件。** 流水线不会启动 vector-server/cluster_local 子进程。

聚类模式 `TestClusteredOneProcessPipeline` 使用 4 个 DataPartition、3 个固定簇 IndexPartition；增加两轮全 Worker 停止期间持续写入、跨簇修改/删除/恢复、GC 后再恢复和指标收敛。完整门禁 `bash scripts/acceptance-dispatch.sh` 同时覆盖四种模式，并附加真实进程反复 SIGSTOP/SIGKILL 场景。用例和报告规则见 [QUALITY.md](QUALITY.md)。

## 运行

在仓库根目录执行：

```bash
bash scripts/acceptance-onebox.sh
```

入口会自动编译测试并开启 Go race detector，无需提前构建 vector-server/vectorctl。依赖与地址沿用项目配置，PD_ENDPOINTS 本用例接受一个 PD 地址；默认 PD 12379、SeaweedFS S3 18333。使用随机 namespace 隔离数据。

已纳入 `bash scripts/acceptance.sh`。单独运行底层测试也可以：

```bash
ONEBOX_ACCEPTANCE=1 bash scripts/go.sh test -count=1 -race -timeout=10m \
  -run '^TestOneProcessPipeline$' -v ./cmd/vector-server
```

## 串行流程

| 阶段 | 操作和断言 |
|---|---|
| 启动 | 同进程七实例注册；32 分片双副本 ready；非 Worker 不加载 HNSW |
| 写入/读取 | 交替通过两个 Gateway 写入 96 条有规律向量；跨 Gateway 读取；相同请求幂等；重复创建和不存在读取返回正确错误 |
| 更新/检索 | CAS 更新、旧版本冲突、可见性 token；等待两份副本追到已知数据位点后，核对全量 ID、版本、属性、距离和排序；检查属性过滤 |
| 快照/删除/分页 | 保存快照、清理日志；写入删除尾部；跨分片游标分页无遗漏、重复、墓碑泄漏 |
| Master 切换 | 停止当前主实例，继续查询，等待备用主接管且 epoch 增长 |
| Worker 排空 | 调用 drain，查询过程中等待新副本就绪并移除旧分配，再停止该 Worker |
| 重建 | 重建并保存快照，确认更新和删除仍正确；再写入快照之后的增量更新 |
| Gateway 退出 | 停止第一个 Gateway，通过第二个继续读取并检查最新版本 |
| Worker 重启 | 原节点注册过期后在同一进程创建新 session，确认重新获得 ready 副本，查询结果与增量更新正确 |
| 清理 | 通过各实例自身生命周期关闭 HTTP、协调循环、同步循环和 TiKV 客户端；失败也执行清理 |

可见性 token 只约束指定记录，不能充当全局事务快照。因此全量对比之前会显式等待两份副本到达测试已知的分片消费位点，不依靠固定睡眠时间假设同步完成。

## 结果

- `reports/onebox-pipeline.status`：独立入口的最终通过/失败标记。
- `reports/onebox-pipeline.json`：PID、拓扑、namespace、已完成阶段、每阶段耗时、最后请求、最终状态和清理结果。
- `reports/onebox-pipeline.log`：详细日志；失败时包含断言和源码位置。

正常执行不会留下向量服务进程；测试数据保留在独立 namespace 供排查。正常失败会进入清理；测试进程被外部 SIGKILL 或测试总超时强制终止时，不能保证写出最终报告，可根据入口 status/log 判断。

本用例模拟的是**同进程内单个服务实例的退出/重启**，不模拟单独杀死某个 OS 进程。共享进程故障会让七个实例一起退出；真实 SIGSTOP/SIGKILL、跨进程故障隔离仍由原有 replicas/roles 验收负责。该流水线使用 HTTP 业务接口；gRPC 与 SDK 的专门验收保持独立。
