# Review、故障验收与质量门禁

最新聚类投递实现的 review 覆盖：来源窗口恢复、连续完成位点、稀疏投递去重、入队/构图/事务提交边界、主备接管、快照与 GC、HTTP/gRPC 状态、验收脚本自身的失败处理。

## 本次修复

| 问题 | 影响 | 修复及自动化证据 |
|---|---|---|
| 完成位点事务实际提交，但客户端收到错误，内存窗口未裁剪 | 下次 tick 读到新 completed，但窗口仍停在旧事件，后续日志不再投递 | 每轮按持久化 completed 清理缓存，断续缓存从日志重建；`TestDispatchRecoversUncertainCompletedCommit` 注入“提交成功但返回错误”，修复前稳定失败 |
| 接收端只检查事件递增，没有验证目标范围完整 | 漏掉同一目标的早期事件、改写载荷或伪造尾部事件也能入队，推进 source commit 后可能掩盖遗漏 | 入队时与同一 TiKV 快照中的有界来源日志比对，按固定路由重算目标子集；`TestRoutedRejectsIncompleteOrChangedDelivery` 三种变体修复前全部失败 |
| 队列持有调用方的切片/属性引用，取出时重新编码计算大小 | 入队后内容可能改变，且持锁重复 JSON 编码 | 入队取得记录所有权并缓存编码大小；测试修改原请求、重试与字节计数归零 |
| 流水线把 source completed 当作全部副本可查询 | Follower 允许落后，造成全量结果断言偶发失败 | 全量 oracle 前检查所有来源完成、committed=visible、两份副本的完整 GraphPosition 一致；Python harness 单测覆盖缺副本、落后、未发布和过期 owner |
| 状态中保留旧 owner，不能仅用 owner 非空判断可用 | 故障时监控可能把过期 dispatcher 当成可用 | 增加 HTTP/gRPC `owner_available`，来源于租约解析结果；指标区分历史 epoch 与当前可用性 |
| 原有消费租约测试用 acquire 时的旧到期时间模拟 ACK 后过期 | ACK 已续租，测试受毫秒级耗时影响而偶发失败 | 使用最新 ACK 返回的持久化到期时间；故障在完整回归中被捕获，修复后针对测试重复 30 次通过 |

日志校验只在接收端入队时执行，一次最多 32 条来源日志、2 MiB；构图前与最终提交仍检查 source epoch、Index Leader epoch 和 commit 边界。去重已提交批次无需重新读取已 GC 的日志，不引入每事件路由表或额外 Index 输入日志。成功返回给业务方的条件仍然是原始数据事务持久化。

## 运行入口

```bash
# 不需要 TiKV/S3；需要 Go/C++ 工具链、Python SDK 依赖。
.venv/bin/python scripts/quality.py --lane unit

# 完整 smoke：使用已经启动的本地 PD/TiKV/SeaweedFS。
bash scripts/acceptance-dispatch.sh
```

`unit` 包含全部 Go UT + race + coverage、vet、Python SDK 和验收脚本 UT。`smoke` 在此基础上运行真实存储集成、四种模式的七服务流水线、独立进程聚类故障矩阵和旧模式订阅/GC 回归。旧的 `scripts/acceptance.sh` 仍保留全项目扩展验收入口。

每次产生独立的 `reports/quality/<UTC时间>-<随机ID>/report.json`、阶段日志、coverage 和本次验收 JSON 副本；`reports/quality/latest.json` 指向最近一次结果内容。报告记录 commit、dirty 标记、命令、耗时、退出码和失败原因。门禁会删除阶段声明的旧报告，缺少新报告、非 passed、清理失败、子命令失败、超时都不能算通过。工作区文件锁阻止两个 quality runner 互相覆盖共享二进制和旧脚本报告。

每个阶段运行在独立进程组；完成、失败、超时或 SIGINT/SIGTERM 后清理该组内的子孙进程，包括被 SIGSTOP 暂停的 Worker。外部 SIGKILL 无法被捕获；此时 `running` 报告不代表通过。不要同时运行其他直接调用旧验收脚本的任务，它们不遵守 quality runner 的工作区锁。

## 故障矩阵

| 层级 | 场景 | 必须满足的结果 |
|---|---|---|
| Go UT | 完成位点不确定提交；失败事务；重复批次；源 epoch 失效；乱序/缺失/篡改事件；缓存丢失 | 不跳过日志、不错误推进进度、可重试恢复、图构造不重复 |
| Go UT | 穷举不同来源目标 ACK 组合，包含广播删除 | 只推进连续完成前缀，不能跨过最早未完成事件 |
| 真实 TiKV | 提交图引用和 source commit 前并发撤销来源租约 | 事务被 fencing，无半提交的图日志或完成凭证 |
| 单 PID 七服务 | 2 Gateway、2 Master、3 Worker；CRUD/CAS/分页/过滤、主切换、排空、重建、Gateway/Worker 重启 | 无结果遗漏/重复，版本、距离、属性正确；非 Worker 不加载 HNSW |
| 单 PID 七服务聚类模式 | 4 Data → 3 Index；两轮全 Worker 停止，每轮 144 条结构化写入；跨簇移动→删除→恢复；快照/GC 后再故障 | 原始读写继续，跨批次恢复；幂等凭证仍返回原结果；383 条最终记录逐条核对；积压和副本滞后归零 |
| 多进程 | 所有 Worker SIGSTOP 后业务写入；旧 owner 长暂停→租约失效→新 owner 接管→恢复旧 owner | 写入不等待构图；旧 epoch 无法覆盖新状态；来源位点不倒退 |
| 多进程 | 三轮 Master 与 Worker 同时 SIGKILL，其中一轮两个 Worker 同时故障；4 路并发写入、同 ID CAS 竞争、重试、跨簇更新/删除/恢复；每轮先快照/GC | 原始数据读写成功；恰好一个 CAS 成功、一个冲突；每轮恢复后完整 oracle；接管 epoch 增长；精确结果与 ANN recall 检查 |
| 多进程旧模式 | 消费 lease pin/ACK/release、图对象 GC、落后副本快照恢复、GC 后 Leader 故障 | 不清理被租约保护的日志；reset 可恢复；图副本可继续构建 |
| 观测 | 鉴权、指标格式与数值、位点、队列、错误/去重计数、故障积压及恢复收敛 | 故障不能显示为零积压的假健康；指标能被 Prometheus parser 读取；RPC uint64 不经 float64 |
| 脚本自测 | 非零退出、超时、缺报告、旧成功报告、cleanup=false、依赖启动失败 | 所有失败都向上传播，不能误报 passed |

七服务各实例通过真实 HTTP 通信；TiKV/S3 是外部服务。独立进程场景才使用 OS SIGSTOP/SIGKILL。数据按固定簇中心、渐变偏移、跨簇更新构造，不是纯随机向量。

## CI 与边界

2026-11-16 本地执行 `bash scripts/acceptance-dispatch.sh`，10 个阶段全部通过；报告运行 ID 为 `20261116T103659Z-263fda42`（本地路径 `reports/quality/<运行ID>/report.json`）。四种七服务模式全部通过；聚类单 PID 场景最终核对 383 条记录，多进程反复故障场景最终核对 246 条记录；18 个 Python harness UT、5 个 Python SDK UT 通过，测试启动的全部服务/子进程已停止。RPC 生成物可重复生成且无差异，CI YAML 与 Compose 配置通过本地解析校验。

`.github/workflows/quality.yml` 在 push main、PR 和手工触发时分别运行 unit/smoke，并检查 Go 格式与生成的 RPC 代码一致性；Linux smoke 用 `compose.ci.yaml` 启动独立 PD/TiKV 8.5.8、SeaweedFS 4.53。失败也上传日志和报告、停止容器。分支保护是否将这些检查设为必需，需要在 GitHub 仓库设置中配置；仓库文件本身不能强制禁止绕过合并。

本轮本地验证使用真实 macOS TiKV/SeaweedFS；GitHub Ubuntu 容器任务的执行结果应以远端 Actions 为准。它们不是跨机网络分区、时钟漂移、磁盘损坏、对象存储宕机、TiKV 多节点失去多数派或长时间生产压测。故障恢复期间最近邻查询可以暂时失败；本轮保证恢复后的完整性，不声称同时失去多个副本仍能无中断查询。
