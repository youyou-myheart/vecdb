"""Render a One-box performance matrix and an optional baseline as Chinese Markdown."""

import html
import json
import math
import os
import re
from collections.abc import Iterable, Mapping
from pathlib import Path
from urllib.parse import quote

if __package__:
    from .perf_report_ann import append_ann
else:
    from perf_report_ann import append_ann


NA = "N/A"
QUERY_NAMES = {
    "sdk_routed_vector": "向量选片",
    "sdk_search_by_id": "按 ID 查询",
    "sdk_all_partitions": "全片默认策略",
    "sdk_hot_single": "热向量单片默认策略",
    "sdk_hot_all": "热向量全片默认策略",
    "sdk_exact_all": "全片精确",
    "sdk_min_visible": "min_visible",
    "sdk_follower": "Follower",
}
WRITE_NAMES = {
    "seed": "植入点读样本",
    "put_hot_shard0_c64": "热数据分片写",
    "mixed_read80_write20_c32": "80% 点读 + 20% 写入",
    "idempotent_replay_c16": "幂等重放",
    "ack_to_visible_c1": "可见性测试中的写 ACK",
}
MATCH = (
    "region_control",
    "cpu_quota",
    "dimensions",
    "data_shards",
    "index_shards",
    "index_replicas",
    "workers",
    "seed_records",
)


def obj(value):
    return value if isinstance(value, Mapping) else {}


def get(value, path):
    for part in path.split("."):
        value = obj(value).get(part)
    return value


def number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    return value


def fmt(value):
    if value is None:
        return NA
    if isinstance(value, bool):
        return "是" if value else "否"
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("测量数值必须是有限数或 null")
        return f"{value:,.4f}".rstrip("0").rstrip(".")
    if isinstance(value, int):
        return f"{value:,}"
    return html.escape(str(value), quote=False).replace("\n", " ").replace("|", r"\|")


def load(value, label):
    if isinstance(value, Mapping):
        return dict(value), None
    if not isinstance(value, (str, os.PathLike)):
        raise ValueError(f"{label}须为 JSON 对象或文件路径")
    path = Path(value)
    with path.open(encoding="utf-8") as stream:
        doc = json.load(stream)
    if not isinstance(doc, dict):
        raise ValueError(f"{label}顶层须为 JSON 对象")
    return doc, path


def validate(matrix, label):
    if not any(k in matrix for k in ("status", "queries", "writes", "sdk_writes", "resources", "failover")):
        raise ValueError(f"{label}缺少性能矩阵字段")
    for key in (
        "writes",
        "sdk_writes",
        "resources",
        "catchup",
        "failover",
        "search_write_mixed",
        "quality_after_writes",
        "query_settings",
        "write_settings",
        "region_config",
        "metadata",
    ):
        if matrix.get(key) is not None and not isinstance(matrix[key], dict):
            raise ValueError(f"{label}.{key} 须为 JSON 对象")
    if matrix.get("queries") is not None and not isinstance(matrix["queries"], list):
        raise ValueError(f"{label}.queries 须为数组")
    if matrix.get("partial_errors") is not None and not isinstance(matrix["partial_errors"], (dict, list)):
        raise ValueError(f"{label}.partial_errors 须为 JSON 对象或数组")
    if matrix.get("region_control") is not None and not isinstance(matrix["region_control"], bool):
        raise ValueError(f"{label}.region_control 须为布尔值")


def digest(value):
    if value is None:
        return None
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", value):
        raise ValueError("binary_sha256 必须是 64 位十六进制 SHA-256")
    return value.lower()


def field(row, selector):
    return selector(row) if callable(selector) else get(row, selector)


def values(value):
    return (
        ", ".join(f"{fmt(k)}×{fmt(v)}" for k, v in value.items()) if isinstance(value, dict) and value else NA
    )


def data_rows(matrix, key):
    rows = get(matrix, key)
    if rows is None:
        return {}
    if not isinstance(rows, dict):
        raise ValueError(f"{key} 须为 JSON 对象")
    for name, row in rows.items():
        if not isinstance(row, dict):
            raise ValueError(f"{key}.{name} 须为 JSON 对象")
    return rows


def query_rows(matrix):
    result = {}
    for row in matrix.get("queries") or []:
        if not isinstance(row, dict) or not isinstance(row.get("scenario"), str) or not row["scenario"]:
            raise ValueError("queries 中的记录缺少场景名")
        c = row.get("concurrency")
        if type(c) is not int or c < 1:
            raise ValueError("查询并发数必须是正整数")
        key = (row["scenario"], c)
        if key in result:
            raise ValueError(f"重复的查询场景 {key}")
        result[key] = row
    return result


def same(rows, keys):
    return all(get(rows[0], key) is not None and get(rows[0], key) == get(rows[1], key) for key in keys)


def table(lines, headers, rows):
    if rows:
        lines.extend(["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"])
        lines.extend("| " + " | ".join(row) + " |" for row in rows)
    else:
        lines.append("未提供此项测量。")
    lines.append("")


class MatrixReport:
    def __init__(self, matrices):
        self.matrices = matrices
        self.lines = []
        self.match_keys = MATCH + (("region_config",) if any(m.get("region_config") for m in matrices) else ())
        self.can_compare = len(matrices) == 2 and same(matrices, self.match_keys)

    def section(self, name, caption=None):
        self.lines.extend(["### " + name, ""])
        if caption:
            self.lines.extend([caption, ""])

    def grid(self, data, label, columns, *, delta=None, guards=(), phase_guards=(), first="场景"):
        """One row per scenario. The two lines in a paired cell preserve both observations."""
        maps = [data(m) for m in self.matrices]
        keys = dict.fromkeys(k for mapping in maps for k in mapping)
        rows = []
        paired = len(maps) == 2
        for key in keys:
            observations = [mapping.get(key, {}) for mapping in maps]
            items = [fmt(label(key))]
            for _, selector in columns:
                items.append("<br>".join(fmt(field(r, selector)) for r in observations))
            if delta and paired:
                change = None
                phase_keys = phase_guards(key) if callable(phase_guards) else phase_guards
                if self.can_compare and same(self.matrices, phase_keys) and same(observations, guards):
                    a, b = [number(get(r, delta)) for r in observations]
                    if a is not None and b is not None and b > 0:
                        change = f"{(a / b - 1) * 100:+.1f}%"
                items.append(fmt(change))
            rows.append(items)
        headers = [first, *(name for name, _ in columns)]
        if delta and paired:
            headers.append("吞吐变化")
        table(self.lines, headers, rows)


def write_name(key):
    return WRITE_NAMES.get(key, key.replace("put_c", "分散写 C").replace("get_c", "点读 C"))


def conditions(report, sources):
    report.section("运行条件")
    headers = ["条件", "本轮"] + (["基线"] if len(sources) == 2 else [])

    def mode(r):
        return {True: "Region 受控", False: "TiKV 默认 Region 策略"}.get(
            r.get("region_control"), "Region 策略未记录"
        )

    def topology(r):
        return " · ".join(fmt(r.get(k)) for k in ("workers", "data_shards", "index_shards", "index_replicas"))

    specs = [
        ("标识", "version"),
        ("运行状态", "status"),
        ("TiKV 配置组", mode),
        ("Worker · 数据分片 · 索引分片 · 每片索引副本", topology),
        ("向量维度", "dimensions"),
        ("初始种子条数", "seed_records"),
        ("CPU 配额 / 周期（µs）", "cpu_quota"),
        ("One-box 存储文件系统", "metadata.scratch_fs_type"),
        ("查询 K", "query_settings.k"),
        ("每查询场景样本量", "query_settings.n_per_scenario_concurrency"),
        ("查询时全库记录数", "query_settings.live_records"),
        ("热查询使用的向量 ID", "query_settings.hot_id"),
        ("热查询使用的索引分片", "query_settings.hot_partition"),
        ("查询随机种子", "query_settings.seed"),
        ("写入随机种子", "write_settings.seed"),
        ("采集时源码 Git 提交", "metadata.git_rev"),
        ("采集时源码工作区有改动", "metadata.git_dirty"),
        ("服务端二进制 SHA-256", "binary_sha256"),
        ("开始时间", "metadata.started_at"),
        ("服务端 GOMAXPROCS 环境覆盖", "metadata.overrides.GOMAXPROCS"),
        ("接管任期探测方式", lambda r: get(r, "failover.epoch_probe") or get(r, "metadata.failover_probe")),
    ]
    if any(m.get("region_config") for m in report.matrices):
        specs.extend(
            [
                (
                    "Region 大小阈值（split · max）",
                    lambda r: " · ".join(
                        fmt(get(r, "region_config.coprocessor." + k))
                        for k in ("region-split-size", "region-max-size")
                    ),
                ),
                (
                    "负载分裂阈值（QPS · 字节）",
                    lambda r: " · ".join(
                        fmt(get(r, "region_config.split." + k)) for k in ("qps-threshold", "byte-threshold")
                    ),
                ),
            ]
        )
    rows = [[name, *(fmt(field(m, spec)) for m in report.matrices)] for name, spec in specs]
    rows.append(
        [
            "原始 JSON",
            *("[查看数据](" + quote(str(path), safe="/._~%:-") + ")" if path else NA for path in sources),
        ]
    )
    table(report.lines, headers, rows)
    if len(sources) == 2:
        mismatches = [k for k in report.match_keys if not same(report.matrices, (k,))]
        reason = (
            "条件相同的行给出相对基线的吞吐变化。"
            if not mismatches
            else "以下条件不同或缺失，吞吐变化记作 N/A。涉及 " + "、".join(mismatches) + "。"
        )
        report.lines.extend(["表格内每格上行为本轮，下行为基线。" + reason, ""])
    report.lines.extend(
        [
            "QPS 单位为成功请求/秒，时延单位为毫秒。闭环场景按成功样本计算经验分位数。失败和未发出请求另计。短窗口、调度和后台工作会带来波动，单轮差值没有置信区间。N/A 表示未测、字段缺失或无法比较。",
            "",
        ]
    )


def render_search(report):
    report.section(
        "SDK 查询",
        "按 ID 查询包含读取源向量和检索两步。默认策略可能在分片内部执行精确检索，具体取决于运行阈值。扇出列按“最终分片数×请求数”列出。召回率为相对离线全库精确结果的平均 Recall@K。",
    )
    cols = [
        ("样本", "n"),
        ("成功", "succeeded"),
        ("失败", "errors"),
        ("QPS", "rps"),
        ("P50", "p50_ms"),
        ("P95", "p95_ms"),
        ("P99", "p99_ms"),
        ("扇出", lambda r: values(r.get("final_fanout_histogram"))),
        ("Recall@K", "recall_at_k_mean"),
    ]

    def query_conditions(key):
        fields = (
            "query_settings.k",
            "query_settings.seed",
            "query_settings.query_ids",
            "query_settings.live_records",
        )
        if key[0].startswith("sdk_hot_"):
            return fields + ("query_settings.hot_id", "query_settings.hot_partition")
        return fields

    report.grid(
        query_rows,
        lambda k: f"{QUERY_NAMES.get(k[0], k[0])} C{k[1]}",
        cols,
        delta="rps",
        guards=("n", "warmup"),
        phase_guards=query_conditions,
    )

    def follower(m):
        return {k: v for k, v in query_rows(m).items() if v.get("follower_per_partition_ms")}

    if any(follower(m) for m in report.matrices):
        report.lines.extend(["Follower 每分片耗时 P99", ""])
        times = [
            ("排队", "queue_ms"),
            ("主副本获取位点", "leader_capture_ms"),
            ("等待位点", "wait_apply_ms"),
            ("查询", "query_ms"),
        ]
        report.grid(
            follower, lambda k: f"C{k[1]}", [(n, "follower_per_partition_ms." + k + ".p99") for n, k in times]
        )


def render_writes(report):
    report.section(
        "Gateway 点读与写入",
        "闭环并发。均衡写和热数据分片写按其数据分片分布区分。幂等回放的成功数是返回的回执数。",
    )
    timings = [
        ("尝试", "requests"),
        ("成功", "success"),
        ("失败", "errors"),
        ("QPS", "rps"),
        ("P50", "p50_ms"),
        ("P95", "p95_ms"),
        ("P99", "p99_ms"),
    ]
    report.grid(
        lambda m: data_rows(m, "writes"),
        write_name,
        [("并发", "concurrency"), *timings],
        delta="rps",
        guards=("concurrency", "requests", "client_processes"),
        phase_guards=("write_settings.seed",),
    )

    def subops(m):
        return {
            (name, op): r
            for name, p in data_rows(m, "writes").items()
            for op, r in obj(p.get("operations")).items()
            if len(obj(p.get("operations"))) > 1
        }

    if any(subops(m) for m in report.matrices):
        report.lines.extend(
            ["混合与可见性阶段的分项。ACK 到可见记录从收到持久化回执到首次确认目标版本可检索的耗时。", ""]
        )
        names = {"get": "GET", "put": "PUT ACK", "ack_to_visible": "ACK 到可见"}
        report.grid(subops, lambda k: f"{k[0]} · {names.get(k[1], k[1])}", timings)
    report.section("并行搜索与写入")
    report.grid(
        lambda m: data_rows(m, "search_write_mixed.operations"),
        str,
        [
            ("目标 QPS", "offered_rps"),
            ("计划", "scheduled"),
            ("成功", "success"),
            ("失败", "errors"),
            ("未发", "unsent"),
            ("成功 QPS", "rps"),
            ("P50", "p50_ms"),
            ("P95", "p95_ms"),
            ("P99", "p99_ms"),
            ("计划到完成 P99", "schedule_to_complete.p99_ms"),
            ("源 ID 命中次数", "known_query_id_found"),
        ],
    )


def sorted_rates(m):
    def key(rate):
        try:
            return (0, float(rate))
        except (ValueError, TypeError):
            return (1, str(rate))

    return dict(sorted(data_rows(m, "sdk_writes").items(), key=lambda item: key(item[0])))


def render_paced(report):
    report.section(
        "Go SDK 定速写",
        "窗口内 ACK QPS 只计算截止时刻前已收到的持久化回执。未发请求包括并发已满或错过调度窗口，RPC 失败仅计实际调用。窗口结束后继续收回在途回执。",
    )
    report.grid(
        sorted_rates,
        str,
        [
            ("窗口秒", "seconds"),
            ("客户端 × 并发上限", lambda r: f"{fmt(r.get('clients'))} × {fmt(r.get('inflight_per_client'))}"),
            ("计划", "offered"),
            ("调用", "invoked"),
            ("窗口 ACK", "durable_acks_in_window"),
            ("窗后 ACK", "after_window_acks"),
            ("窗口 ACK QPS", "ack_qps_in_window"),
            ("未发", "dropped_unsent"),
            ("RPC 失败", "rpc_failure_count"),
        ],
        delta="ack_qps_in_window",
        guards=("target_qps", "seconds", "clients", "inflight_per_client", "offered"),
        first="目标 QPS",
    )
    report.grid(
        sorted_rates,
        str,
        [
            ("并发满未发", "dropped_inflight_cap"),
            ("调度未发", "dropped_scheduler_window"),
            ("全部 ACK", "durable_acks"),
            ("ACK P50", "p50_ack_ms"),
            ("ACK P95", "p95_ack_ms"),
            ("ACK P99", "p99_ack_ms"),
            ("窗后 ACK 尾延迟", "after_window_ack_tail_ms"),
            ("副本追平", "all_replicas_caught"),
            ("末次 ACK 到确认追平上界", "confirmed_after_last_ack_ms_upper"),
        ],
        first="目标 QPS",
    )
    report.section(
        "索引可见性与追平",
        "表中追平时间以秒计，为轮询观测时间。写回执、数据分片已分发和全体索引副本可见分别观测。",
    )
    report.grid(
        lambda m: data_rows(m, "catchup"),
        str,
        [
            ("最低预期头", "expected_head_min"),
            ("确认头", "confirmed_head"),
            ("首轮数据落后条数", "initial_data_lag"),
            ("数据追平秒", "data_caught_s"),
            ("副本追平秒", "replicas_caught_s"),
        ],
    )


def render_quality(report):
    report.section(
        "写后已追平时的检索质量",
        "同批查询与当时全片精确 Top-K 比较。自身命中数是查询 ID 落入 Top-K 的次数，与所有 Top-K 邻居的召回率分别统计。",
    )
    modes = {
        "exact_all": "全片精确",
        "ann_default_all": "全片默认策略",
        "ann_nearest_one": "最近单片默认策略",
        "ann_ef1024_all": "全片 ef=1024（仍服从精排阈值）",
    }
    report.grid(
        lambda m: data_rows(m, "quality_after_writes.modes"),
        lambda k: modes.get(k, k),
        [
            ("样本", "requests"),
            ("成功", "success"),
            ("失败", "errors"),
            ("部分结果", "partial"),
            ("少于 K", "short_top10"),
            ("自身命中", "self_in_top10"),
            ("可对照样本", "recall_queries"),
            ("Recall@10", "recall_at_10"),
            ("距离阈值召回", "distance_threshold_recall_at_10"),
            ("P99", "p99_ms"),
        ],
    )


def render_resources(report):
    report.section(
        "资源使用",
        "CPU 为整个阶段平均占用核数。RSS 为阶段结束时的 MiB。TiKV RPC 为同一阶段的节点级增量，包含后台活动。kv_commit 也计入图日志和元数据事务。未记录的计数器显示 N/A。",
    )
    rpc_prefix = "tikv_grpc_msg_duration_seconds_count|type="

    def calls(r, kinds):
        return " · ".join(fmt(obj(get(r, "counters.tikv")).get(rpc_prefix + k)) for k in kinds)

    report.grid(
        lambda m: data_rows(m, "resources"),
        str,
        [
            ("阶段秒", "wall_s"),
            ("Worker CPU", "resources.worker.avg_cpu_cores"),
            ("TiKV CPU", "resources.tikv.avg_cpu_cores"),
            ("PD CPU", "resources.pd.avg_cpu_cores"),
            ("Worker RSS", "resources.worker.rss_end_mib"),
            ("TiKV RSS", "resources.tikv.rss_end_mib"),
            ("Raw Get · BatchGet · Scan", lambda r: calls(r, ("raw_get", "raw_batch_get", "raw_scan"))),
            ("Raw BatchPut · kv_commit", lambda r: calls(r, ("raw_batch_put", "kv_commit"))),
        ],
    )


def render_failover(report):
    report.section(
        "故障切换与数据校验",
        "各探测的时间从注入故障算起。首轮失败探测与最终通过状态分开记录。任期探测方式见运行条件，方式不同或未记录时不能直接比较发现任期的时延。",
    )

    def flags(m):
        f = m.get("failover")
        return {"接管": f} if f else {}

    report.grid(
        flags,
        str,
        [
            ("分片", "partition"),
            ("通过", "passed"),
            ("旧任期", "old_epoch"),
            ("新任期", "new_epoch.value.epoch"),
            ("探测总秒数", "elapsed_s"),
            ("限时秒数", "deadline_s"),
            ("新增持久化条数", "durable_new_writes"),
        ],
    )

    def probes(m):
        return {
            k: v
            for k, v in obj(m.get("failover")).items()
            if isinstance(v, dict) and ("observed_ms" in v or "attempts" in v)
        }

    if any(probes(m) for m in report.matrices):
        report.grid(
            probes, str, [("尝试", "attempts"), ("失败尝试", "failed_attempts"), ("观测 ms", "observed_ms")]
        )

    def crud(m):
        rows = {}
        for key in ("phases", "checks"):
            for op, v in data_rows(m, "failover.crud." + key).items():
                rows[key + " · " + op] = v
        return rows

    if any(m.get("failover", {}).get("crud") for m in report.matrices if m.get("failover")):
        report.lines.extend(["CRUD 创建、更新、删除及验证", ""])
        report.grid(
            crud,
            str,
            [
                ("计划", "planned"),
                ("成功", "success"),
                ("失败", "errors"),
                ("跳过", "skipped"),
                ("QPS", "rps"),
                ("P99", "p99_ms"),
            ],
        )


def issues(matrix):
    messages = []
    failure_counts = []

    def add(name, details):
        lines = str(details).strip().splitlines()
        if lines:
            messages.append(f"{fmt(name)} · {fmt(lines[-1])[:340]}")

    def collect(name, row, count, *examples):
        value = row.get(count)
        if value is None:
            return
        if type(value) is not int or value < 0:
            raise ValueError(f"{name}.{count} 须为非负整数")
        failure_counts.append(value)
        if value:
            detail = next((row[k] for k in examples if row.get(k)), None)
            add(name, f"{count} = {value}" + (f"，样例 {detail}" if detail else ""))

    if matrix.get("exception"):
        add("运行异常", matrix["exception"])
    partial = matrix.get("partial_errors")
    if isinstance(partial, list):
        for item in partial:
            add("阶段错误", item)
    else:
        for key, item in obj(partial).items():
            add(key, item)
    for (scenario, concurrency), row in query_rows(matrix).items():
        name = f"{QUERY_NAMES.get(scenario, scenario)} C{concurrency}"
        collect(name, row, "errors", "error_examples")
        collect(name, row, "warmup_errors", "warmup_error_examples")
    for path in (
        "writes",
        "search_write_mixed.operations",
        "quality_after_writes.modes",
        "failover.crud.phases",
        "failover.crud.checks",
    ):
        for name, row in data_rows(matrix, path).items():
            collect(f"{path} · {name}", row, "errors", "failures", "first_errors")
            if row.get("startup_errors"):
                add(name + " 客户端启动", row["startup_errors"])
    for name, row in sorted_rates(matrix).items():
        collect(f"SDK {name}", row, "rpc_failure_count", "examples")
        if row.get("all_replicas_caught") is False:
            add(f"SDK {name} 副本追平", row.get("last_validation_error") or "未通过")
    fail = obj(matrix.get("failover"))
    if fail.get("passed") is False or fail.get("setup_error") or fail.get("error"):
        add("故障切换", fail.get("setup_error") or fail.get("error") or "未通过")
    if get(matrix, "failover.crud.passed") is False:
        add("CRUD", "未通过")
    for name, row in data_rows(matrix, "resources").items():
        if row.get("warnings"):
            add("资源采集 " + name, row["warnings"])
    if messages:
        return messages
    if failure_counts:
        return ["已记录失败数的测量场景均为 0。未发请求与故障注入期间的探测重试单独列示。"]
    return ["未记录测量阶段的失败计数。"]


def render_report(
    current, *, baseline=None, supplements=(), current_source=None, baseline_source=None, current_sha=None
):
    """Return Markdown. Matrix and supplement inputs can be paths or decoded JSON objects."""
    matrices = []
    sources = []
    for value, source, label in ((current, current_source, "本轮"), (baseline, baseline_source, "基线")):
        if value is None and label == "基线":
            continue
        matrix, path = load(value, label)
        validate(matrix, label)
        sha = digest(matrix.get("binary_sha256"))
        if label == "本轮" and current_sha is not None:
            override = digest(current_sha)
            if sha and sha != override:
                raise ValueError("current_sha 与本轮报告中的 binary_sha256 不符")
            sha = override
        if sha:
            matrix["binary_sha256"] = sha
        matrices.append(matrix)
        sources.append(source if source is not None else path)
    if isinstance(supplements, (str, bytes, os.PathLike, Mapping)) or not isinstance(supplements, Iterable):
        raise ValueError("supplements 须为 ANN 报告序列")
    extras = []
    for i, s in enumerate(supplements, 1):
        ann, path = load(s, "ANN 补充")
        source = path.name if path else f"传入的第 {i} 份专项记录"
        extras.append((source, ann))
    report = MatrixReport(matrices)
    conditions(report, sources)
    render_search(report)
    render_writes(report)
    render_paced(report)
    render_quality(report)
    render_resources(report)
    render_failover(report)
    report.section("异常记录")
    for i, m in enumerate(matrices):
        group = ("本轮", "基线")[i]
        report.lines.extend("- " + group + " · " + item for item in issues(m))
    report.lines.append("")
    if extras:
        append_ann(report.lines, extras, matrices[0].get("binary_sha256"))
    return "\n".join(report.lines).rstrip() + "\n"
