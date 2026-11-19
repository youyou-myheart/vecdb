"""Render native ANN policy runs that used the matrix run's server binary."""

import math
import re


def _number(value, decimals=0):
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
        return "—"
    return f"{value:,.{decimals}f}"


def _text(value):
    text = str(value).replace("\n", " ").replace("\r", " ")
    return text.replace("|", r"\|").replace("<", "&lt;").replace(">", "&gt;")


def _sha(value):
    if isinstance(value, str) and re.fullmatch(r"[0-9a-fA-F]{64}", value.strip()):
        return value.strip().lower()
    return None


def _table(lines, headers, rows):
    lines.extend(["", "| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"])
    for row in rows:
        lines.append("| " + " | ".join(_text(cell) for cell in row) + " |")
    lines.append("")


def _metric_samples(report):
    """Keep absent metrics absent, including when a run ended before its last scrape."""
    samples = {}
    metrics = report.get("final_metrics")
    if not isinstance(metrics, dict):
        return samples
    for rows in metrics.values():
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, str):
                continue
            match = re.fullmatch(r"(vecdb_index_[a-z_]+)\{[^}]*\}\s+(\S+)", row)
            if not match:
                continue
            try:
                value = float(match[2])
            except ValueError:
                continue
            if math.isfinite(value) and value >= 0:
                samples.setdefault(match[1], []).append(value)
    return samples


def _quality(lines, report):
    rows = []
    stages = [
        ("初次", "旧向量", "quality"),
        ("初次", "扰动向量", "nearby_quality"),
        ("初次", "新增向量", "new_record_quality"),
        ("维护后", "旧向量", "after_reset_quality"),
        ("维护后", "扰动向量", "after_reset_nearby_quality"),
        ("维护后", "新增向量", "after_reset_new_record_quality"),
    ]
    for stage, group, key in stages:
        data = report.get(key)
        if not isinstance(data, dict):
            continue
        count = data.get("queries")
        recall = data.get("recall_at_10")
        percentage = "—"
        if isinstance(recall, (int, float)) and not isinstance(recall, bool) and 0 <= recall <= 1:
            percentage = f"{recall:.2%}"
        denominator = 10 * count if type(count) is int and count >= 0 else None
        overlap = f"{_number(data.get('overlap'))} / {_number(denominator)}"
        rows.append(
            (
                stage,
                group,
                _number(count),
                _number(data.get("self_hits")),
                _number(data.get("exact_self")),
                overlap,
                percentage,
            )
        )
    if rows:
        _table(
            lines,
            [
                "观测阶段",
                "查询样本",
                "查询数",
                "返回自身",
                "精确搜索返回自身",
                "与全片精确 Top-10 重合",
                "Recall@10",
            ],
            rows,
        )
    else:
        lines.extend(["", "召回样本未记录。", ""])


def _latency(lines, report):
    tests = report.get("latency")
    if not isinstance(tests, list) or not tests:
        return
    rows = []
    errors = []
    for run, item in enumerate(tests, 1):
        if not isinstance(item, dict):
            errors.append(f"第 {run} 轮记录格式无效。")
            continue
        histogram = item.get("search_rpc_partitions")
        counts = "—"
        if isinstance(histogram, dict) and histogram:
            counts = "，".join(f"{label} × {_number(count)}" for label, count in sorted(histogram.items()))
        rows.append(
            (
                run,
                _number(item.get("concurrency")),
                _number(item.get("requests")),
                _number(item.get("success")),
                _number(item.get("errors")),
                _number(item.get("rps"), 1),
                _number(item.get("p50_ms"), 3),
                _number(item.get("p95_ms"), 3),
                _number(item.get("p99_ms"), 3),
                counts,
            )
        )
        examples = item.get("error_examples")
        if isinstance(examples, list) and examples:
            errors.append(f"第 {run} 轮错误样例为 {_text(examples[0])[:200]}。")
    if rows:
        lines.extend(
            [
                "",
                "SDK 向量查询逐轮计时，吞吐与延迟按成功请求统计。末列记录每次 Search RPC 的分片数及其出现次数。",
            ]
        )
        _table(
            lines,
            [
                "轮次",
                "并发",
                "请求",
                "成功",
                "失败",
                "成功 req/s",
                "P50 ms",
                "P95 ms",
                "P99 ms",
                "RPC 分片数序列 × 请求数",
            ],
            rows,
        )
    lines.extend(errors)


def _conditions(lines, report, metrics):
    overrides = report.get("environment_overrides")
    if overrides == {}:
        policy = "默认（未指定环境覆盖）"
    elif isinstance(overrides, dict):
        # Report only measurement settings; arbitrary environment entries may contain credentials.
        controls = []
        for name in sorted(overrides):
            if name in (
                "HNSW_EXACT_THRESHOLD",
                "HNSW_RECALL_INTERVAL",
                "HNSW_RECALL_TARGET",
                "HNSW_RECALL_COOLDOWN",
                "HNSW_RECALL_CONSECUTIVE",
            ):
                controls.append(f"{name}={_text(overrides[name])}")
        policy = f"覆盖 {len(overrides)} 项"
        if controls:
            policy += "（" + "，".join(controls) + "）"
    else:
        policy = "未记录"
    counts = report.get("sdk_snapshot_counts")
    distribution = "未记录"
    if isinstance(counts, dict) and counts:
        distribution = "，".join(f"{_text(shard)}={_number(count)}" for shard, count in sorted(counts.items()))
    thresholds = set(metrics.get("vecdb_index_exact_threshold", []))
    threshold = "、".join(_number(n) for n in sorted(thresholds)) if thresholds else "未记录"
    coverage = ""
    if (
        len(thresholds) == 1
        and isinstance(counts, dict)
        and counts
        and all(type(n) is int and n >= 0 for n in counts.values())
    ):
        limit = next(iter(thresholds))
        small = sum(n <= limit for n in counts.values())
        coverage = f"，其中 {small}/{len(counts)} 个分片在精确搜索阈值内"
    seed_counts = report.get("seed_counts")
    total = report.get("total_records")
    if (
        total is None
        and isinstance(seed_counts, list)
        and seed_counts
        and all(type(n) is int and n >= 0 for n in seed_counts)
    ):
        total = sum(seed_counts)
    lines.append(
        f"共 {_number(total)} 条记录。策略为{policy}，副本报告的精确搜索阈值为 {threshold}。"
        f"SDK 路由快照计数为 {distribution}{coverage}。"
    )


def _maintenance(lines, report, metrics):
    totals = []
    names = [("probes_total", "巡检"), ("rebuilds_total", "重建"), ("failures_total", "失败")]
    for key, label in names:
        values = metrics.get("vecdb_index_recall_" + key)
        totals.append(f"{label} {_number(sum(values) if values else None)}")
    lines.extend(["", "末次副本指标合计" + "、".join(totals) + "。"])
    if "reset_events" in report or "idle_verification_s" in report or "all_shards_validated_s" in report:
        events = report.get("reset_events")
        resets = None
        if (
            isinstance(events, dict)
            and events
            and all(isinstance(entries, list) for entries in events.values())
        ):
            resets = sum(len(entries) for entries in events.values())
        healthy = report.get("healthy_unrebuilt_shards")
        idle = report.get("idle_verification_s")
        idle_from = report.get("all_shards_validated_s", report.get("first_all_reset_s"))
        idle_resets = None
        if resets is not None and type(idle) in (int, float) and type(idle_from) in (int, float):
            observations = [event for entries in events.values() for event in entries]
            if all(
                isinstance(event, dict) and type(event.get("sec")) in (int, float) for event in observations
            ):
                idle_resets = sum(event["sec"] > idle_from for event in observations)
        healthy_count = len(healthy) if isinstance(healthy, list) else None
        validated = report.get("all_shards_validated_s")
        lines.append(
            f"观测到 {_number(resets)} 条 reset 日志，{_number(healthy_count)} 个分片达标且未重建。"
            f"全部分片完成验证用时 {_number(validated, 2)} 秒，之后的空闲观测持续 {_number(idle, 2)} 秒，"
            f"期间新增 {_number(idle_resets)} 条 reset。"
        )


def append_ann(lines, supplements, current_sha):
    """Append verified supplementary ANN runs from (source name, JSON object) pairs."""
    if not supplements:
        return
    lines.extend(["", "### ANN 策略专项", ""])
    expected = _sha(current_sha)
    if expected:
        lines.append(
            f"主矩阵服务端二进制 SHA-256 为 `{expected}`。专项使用独立数据集，"
            "以全片精确 Top-10 衡量 SDK 路由查询的召回。入选分片在阈值内执行精确搜索，超过阈值时可使用 HNSW。"
        )
    for source, report in supplements:
        if not isinstance(report, dict):
            raise ValueError(f"ANN 报告须为 JSON 对象（{source}）")
        label = _text(report.get("label") or source)
        lines.extend(["", f"#### {label}", "", f"来源为 {_text(source)}。"])
        actual = _sha(report.get("binary_sha256"))
        if expected is None or actual is None:
            missing = "主矩阵" if expected is None else "专项报告"
            lines.append(f"{missing}缺少有效的服务端二进制 SHA-256，无法核实构建，已跳过该专项数据。")
            continue
        if actual != expected:
            lines.append(f"专项 SHA-256 为 `{actual}`，与主矩阵构建不同，已跳过该专项数据。")
            continue
        status = report.get("status")
        status_name = "未记录"
        if isinstance(status, str) and status:
            status_name = {"passed": "通过", "failed": "失败", "running": "进行中"}.get(status, status)
        lines.append(f"专项状态为{_text(status_name)}。")
        if status != "passed":
            lines.append("以下阶段已有结果，完整专项尚未验证。")
        if report.get("exception") or report.get("error"):
            lines.append("专项错误为 " + _text(report.get("exception") or report["error"])[:400] + "。")
        metrics = _metric_samples(report)
        _conditions(lines, report, metrics)
        _quality(lines, report)
        _maintenance(lines, report, metrics)
        _latency(lines, report)
