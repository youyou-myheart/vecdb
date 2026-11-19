"""Run the native VecDB performance matrix or render an existing set of measurements."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
PROFILES = {"controlled": True, "default": False}
PROFILE_NAMES = {True: "TiKV 受控 Region 配置", False: "TiKV 默认 Region 配置"}


def read_matrix(path, declared_profile=None):
    with path.open(encoding="utf-8") as stream:
        data = json.load(stream)
    if isinstance(data, dict) and declared_profile is not None:
        if "region_control" not in data:
            data["region_control"] = declared_profile
        elif data["region_control"] is not declared_profile:
            raise ValueError(f"{path} 的 Region 配置与指定值不符")
    if not isinstance(data, dict) or type(data.get("region_control")) is not bool:
        raise ValueError(f"{path} 缺少矩阵字段 region_control，无法确定测试配置")
    return data


def input_sets(paths, declared_defaults=()):
    by_profile = {}
    for path in [*paths, *declared_defaults]:
        row = read_matrix(path, False if path in declared_defaults else None)
        key = row["region_control"]
        if key in by_profile:
            raise ValueError(f"{path} 与 {by_profile[key][0]} 指定了同一组 Region 配置")
        by_profile[key] = (path, row)
    return by_profile


def load_baselines(paths, declared_defaults, sha):
    baseline = input_sets(paths, declared_defaults)
    if sha:
        if not baseline or not re.fullmatch(r"[a-f0-9]{64}", sha):
            raise ValueError("--baseline-sha256 需要基线 JSON 和有效的 SHA-256")
        for path, data in baseline.values():
            if data.get("binary_sha256") not in (None, sha):
                raise ValueError(f"{path} 内嵌的二进制 SHA-256 与指定值不同")
            data["binary_sha256"] = sha
    return baseline


def mismatches(current, previous):
    # Search policy and server revision intentionally vary between builds.
    keys = ("cpu_quota", "dimensions", "data_shards", "index_shards", "index_replicas", "workers", "seed_records")
    different = [key for key in keys if current.get(key) is None or current.get(key) != previous.get(key)]
    for group, fields in (("query_settings", ("seed", "n_per_scenario_concurrency", "k", "query_ids")),
                          ("write_settings", ("counts", "seed", "data_partitions")),
                          ("metadata", ("data_backend", "index_replication", "lease_ttl", "pd_max_replicas", "scratch_fs_type"))):
        for key in fields:
            left, right = current.get(group, {}), previous.get(group, {})
            if key in left and key in right and left[key] != right[key]:
                different.append(f"{group}.{key}")
    return different


def overview(data):
    queries = data.get("queries", [])
    routed = next((q for q in queries if q.get("scenario") == "sdk_routed_vector" and q.get("concurrency") == 16), {})
    writes = data.get("writes", {})
    fixed = data.get("sdk_writes", {}).get("4000", {})
    rows = (("SDK 自动选片查询，16 并发", routed), ("Gateway 点查，16 并发", writes.get("get_c16", {})),
            ("Gateway 分散写，16 并发", writes.get("put_c16", {})))
    values = {name: (row.get("rps"), row.get("p99_ms")) for name, row in rows}
    values["Go SDK 定速写，目标 4,000/s"] = (fixed.get("ack_qps_in_window"), fixed.get("p99_ack_ms"))
    return values


def quality_counts(hits, total):
    if hits is None or not total:
        return "—"
    return f"{hits:,}/{total:,} ({hits / total:.1%})"


def outcome_table(current):
    lines = ["## 召回、持续写入与故障恢复", "",
             "| 本次工况 | 追平后单片查回自身 | 并行读写查回自身 | 1,000/s 窗口 ACK/s | 该档 P99 ms | 终止 Owner 后首笔持久化新写 s |",
             "|---|---:|---:|---:|---:|---:|"]
    for profile, (_, row) in current.items():
        settled = row.get("quality_after_writes", {}).get("modes", {}).get("ann_nearest_one", {})
        mixed = row.get("search_write_mixed", {}).get("operations", {}).get("search", {})
        steady = row.get("sdk_writes", {}).get("1000", {})
        recovery = row.get("failover", {}).get("post_kill_put", {})
        elapsed = recovery.get("observed_ms") if "value" in recovery else None
        lines.append(" | ".join((
            f"| {PROFILE_NAMES[profile]}", quality_counts(settled.get("self_in_top10"), settled.get("success")),
            quality_counts(mixed.get("known_query_id_found"), mixed.get("success")),
            number(steady.get("ack_qps_in_window"), 1), number(steady.get("p99_ack_ms"), 2),
            number(elapsed / 1000 if elapsed is not None else None, 2) + " |",
        )))
    lines.extend(["", "自身命中率按成功查询统计。并行读写使用持续增长的索引，追平后查询使用固定语料。故障恢复计时从终止持有分片的 Worker 开始。"])
    return lines


def number(value, places):
    return "—" if value is None else f"{value:,.{places}f}"


def relative(path, output):
    return Path(os.path.relpath(path.resolve(), output.parent.resolve())).as_posix()


def render(current_paths, baseline_paths, supplements, output, baseline_sha=None, default_baseline=()):
    if __package__:
        from .perf_report_render import render_report
    else:
        from perf_report_render import render_report

    current = input_sets(current_paths)
    baseline = load_baselines(baseline_paths, default_baseline, baseline_sha)
    all_inputs = [*current_paths, *baseline_paths, *default_baseline, *supplements]
    if any(output.resolve() == path.resolve() for path in all_inputs):
        raise ValueError("报告输出路径与输入文件相同")

    lines = ["# VecDB One-box 整体性能报告", ""]
    for profile, (_, row) in current.items():
        windows = row.get("sdk_writes", {})
        confirmed = sum(w.get("all_replicas_caught") is True for w in windows.values())
        state = row.get("status", "未知")
        lines.append(f"- **{PROFILE_NAMES[profile]}** · 状态 `{state}`，已测 {len(windows)} 个定速写窗口，"
                     f"其中 {confirmed} 个确认全部副本追平。")
        for rate, window in windows.items():
            if window.get("all_replicas_caught") is False:
                detail = str(window.get("last_validation_error") or "")
                message = f"  {rate}/s 档共收到 {number(window.get('durable_acks'), 0)} 个持久化回执，副本追平未能确认。"
                if "failed to decode region range key" in detail and "invalid marker byte" in detail:
                    message += "校验时状态接口报告 Region 范围键解码失败（`invalid marker byte`）。"
                lines.append(message)
    lines.extend(["", "查询和点查吞吐按成功请求计。定速写吞吐只计投放窗口结束前返回的持久化 ACK。下表列出客户端 P99。",
                  "", "| TiKV 配置 | 场景 | 基线吞吐 /s | 本次吞吐 /s | 变化 | 基线 P99 ms | 本次 P99 ms |",
                  "|---|---|---:|---:|---:|---:|---:|"])
    sections = []
    annotated = set()
    for profile, (path, row) in current.items():
        old_path, old = baseline.get(profile, (None, None))
        issues = mismatches(row, old) if old is not None else []
        if issues:
            sections.append(f"**{PROFILE_NAMES[profile]}** 的基线与本次配置存在差异（{', '.join(issues)}），此节只展示本次测量。基线文件 [{old_path.name}](<{relative(old_path, output)}>)。")
            old_path, old = None, None
        previous = overview(old or {})
        for name, (rate, p99) in overview(row).items():
            base_rate, base_p99 = previous[name]
            comparable = rate is not None and base_rate
            if name.startswith("Go SDK") and comparable:
                now = row["sdk_writes"]["4000"]
                prior = old["sdk_writes"]["4000"]
                comparable = all(prior.get(key) is not None and prior[key] == now.get(key)
                                 for key in ("seconds", "offered", "clients", "target_qps", "inflight_per_client"))
            delta = f"{(rate / base_rate - 1) * 100:+.1f}%" if comparable else "—"
            lines.append(f"| {PROFILE_NAMES[profile]} | {name} | {number(base_rate, 1)} | {number(rate, 1)} | {delta} | {number(base_p99, 2)} | {number(p99, 2)} |")
        sha = row.get("binary_sha256")
        extras = supplements if sha not in annotated else ()
        body = render_report(row, baseline=old, supplements=extras,
                             current_source=relative(path, output),
                             baseline_source=relative(old_path, output) if old_path else None)
        annotated.add(sha)
        sections.append(f"## {PROFILE_NAMES[profile]}\n\n{body.strip()}")
    lines.extend(["", *outcome_table(current)])
    for profile in baseline.keys() - current.keys():
        sections.append(f"提供了{PROFILE_NAMES[profile]}的基线，但本次没有对应测量。")
    if default_baseline:
        sections.append("历史默认 Region 基线的配置由调用者显式指定。")
    if baseline_sha:
        sections.append(f"历史基线的服务端二进制摘要由命令参数指定为 `{baseline_sha}`。")
    if supplements:
        links = " · ".join(f"[{p.name}](<{relative(p, output)}>)" for p in supplements)
        sections.append(f"ANN 专项原始记录 · {links}")
    args = ["python3", "scripts/overall_performance.py", "report"]
    for flag, paths in (("--current", current_paths), ("--baseline", baseline_paths),
                        ("--default-baseline", default_baseline), ("--ann", supplements)):
        for path in paths:
            args.extend((flag, os.path.relpath(path.resolve(), ROOT)))
    if baseline_sha:
        args.extend(("--baseline-sha256", baseline_sha))
    args.extend(("--output", os.path.relpath(output.resolve(), ROOT)))
    sections.append("## 重新生成报告\n\n在仓库根目录执行以下命令。\n\n```bash\n" + shlex.join(args) + "\n```")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=output.parent, delete=False) as stream:
        temp = Path(stream.name)
        try:
            stream.write("\n".join(lines) + "\n\n" + "\n\n".join(sections) + "\n")
        except BaseException:
            temp.unlink(missing_ok=True)
            raise
    temp.replace(output)
    return output


def build(package, output):
    go = shutil.which("go") or str(ROOT / ".tools/go/bin/go")
    if not Path(go).is_file():
        raise ValueError("构建需要 Go；也可用 --server 与 --driver 指定已构建的程序")
    subprocess.run([go, "build", "-buildvcs=false", "-o", str(output), package], cwd=ROOT, check=True)
    return output


def run(args):
    # Reject unusable comparison files before starting a multi-minute workload.
    load_baselines(args.baseline, args.default_baseline, args.baseline_sha256)
    for path in args.ann:
        with path.open(encoding="utf-8") as stream:
            if not isinstance(json.load(stream), dict):
                raise ValueError(f"{path} 不是 ANN 专项 JSON 对象")
    if __package__:
        from .perf_collect.runner import collect
    else:
        from perf_collect.runner import collect

    args.output_dir.mkdir(parents=True, exist_ok=True)
    if args.scratch:
        args.scratch.mkdir(parents=True, exist_ok=True)
    profiles = list(PROFILES) if args.profile == "both" else [args.profile]
    targets = {p: args.output_dir / f"current-{p}.json" for p in profiles}
    report = args.output_dir / "report.md"
    if any(path.exists() for path in [*targets.values(), report]):
        raise ValueError(f"{args.output_dir} 已包含同名结果，请选择新目录；已有 JSON 可用 report 重新渲染")
    with tempfile.TemporaryDirectory(prefix="vecdb-perf-bin-", dir=args.scratch) as tmp:
        tmp = Path(tmp)
        server = args.server or build("./cmd/vector-server", tmp / "vector-server")
        driver = args.driver or build("./scripts/writebench", tmp / "writebench")
        status = 0
        for profile, path in targets.items():
            try:
                data = collect(output=path, server=server, driver=driver, scratch=args.scratch,
                               base_port=args.base_port, label=f"current-{profile}",
                               region_control=PROFILES[profile], pd=args.pd, tikv=args.tikv, weed=args.weed)
                if data["status"] != "complete":
                    status = 1
            except KeyboardInterrupt:
                status = 130
                break
            except Exception as err:
                print(f"{profile}: {err}", file=sys.stderr, flush=True)
                status = 1
    collected = [path for path in targets.values() if path.is_file()]
    if collected:
        render(collected, args.baseline, args.ann, report, args.baseline_sha256, args.default_baseline)
        print(report)
    return status


def comparison_options(parser):
    parser.add_argument("--baseline", type=Path, action="append", default=[], help="同工况基线 JSON，可重复")
    parser.add_argument("--default-baseline", type=Path, action="append", default=[],
                        help="显式指定采用默认 Region 配置的历史基线，兼容旧 JSON 缺少 region_control")
    parser.add_argument("--baseline-sha256", help="为缺少内嵌摘要的历史基线补充已核实的服务端 SHA-256")
    parser.add_argument("--ann", type=Path, action="append", default=[], help="额外的 ANN 实测 JSON，可重复，按二进制 SHA-256 匹配")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(required=True, dest="command")
    measure = subs.add_parser("run", help="启动隔离 One-box，实测并生成 Markdown")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    measure.add_argument("--output-dir", type=Path, default=ROOT / "reports/overall" / stamp)
    measure.add_argument("--profile", choices=("both", *PROFILES), default="both")
    measure.add_argument("--base-port", type=int, default=28740)
    measure.add_argument("--scratch", type=Path, help="存储数据及临时二进制使用的目录，默认取系统临时目录")
    for tool in ("server", "driver", "pd", "tikv", "weed"):
        measure.add_argument("--" + tool, type=Path, help=f"{tool} 可执行文件路径")
    comparison_options(measure)
    report = subs.add_parser("report", help="只读取已有 JSON 重新生成报告")
    report.add_argument("--current", type=Path, action="append", required=True, help="本次矩阵 JSON，每种配置一份")
    report.add_argument("--output", type=Path, help="默认写入首份矩阵所在目录的 report.md")
    comparison_options(report)
    args = parser.parse_args()
    try:
        if args.command == "report":
            output = args.output or args.current[0].parent / "report.md"
            print(render(args.current, args.baseline, args.ann, output, args.baseline_sha256, args.default_baseline))
            return 0
        def stop(signum, frame):
            raise KeyboardInterrupt
        signal.signal(signal.SIGTERM, stop)
        return run(args)
    except (OSError, ValueError, ImportError, subprocess.CalledProcessError) as err:
        parser.exit(1, f"{err}\n")


if __name__ == "__main__":
    sys.exit(main())
