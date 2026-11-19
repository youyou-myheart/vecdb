"""Boundary snapshots for the single-host vecdb performance drivers (stdlib only)."""

import concurrent.futures
import math
import os
import re
import time
import urllib.request

PAGE = os.sysconf("SC_PAGE_SIZE")
TICKS = os.sysconf("SC_CLK_TCK")
MIB = 1024 * 1024
LINE = re.compile(rb'^([^\s{]+)(?:\{(.*)\})?\s+(\S+)')
LABEL = re.compile(rb'(\w+)="((?:[^"\\]|\\.)*)"')
WORKER = {
    "vecdb_pipeline_stage_seconds_count": ("stage", "outcome"),
    "vecdb_pipeline_stage_seconds_sum": ("stage", "outcome"),
    "vecdb_pipeline_batch_items_count": ("stage", "outcome"),
    "vecdb_pipeline_batch_items_sum": ("stage", "outcome"),
    "vecdb_index_batch_records_count": ("outcome",),
    "vecdb_index_batch_records_sum": ("outcome",),
    "vecdb_index_consumer_cycles_total": ("reason",),
    "vecdb_graph_commit_conflict_retries_total": (),
    "vecdb_route_plan_fallback_total": (),
    "vecdb_dispatch_wake_total": ("reason",),
}
TIKV = {
    "tikv_grpc_msg_duration_seconds_count": ("type",),
    "tikv_grpc_msg_fail_total": ("type",),
    "tikv_storage_command_total": ("type", "command"),
    "tikv_scheduler_command_duration_seconds_count": ("type",),
}
PD = {
    "grpc_server_handled_total": ("grpc_service", "grpc_method", "grpc_code"),
    "pd_server_tso_events": ("type",),
    "pd_server_tso_handle_duration_seconds_count": (),
    "pd_server_request_duration_seconds_count": ("type", "name"),
    "pd_server_request_total": ("type", "name"),
}
SCHEMAS = {"worker": WORKER, "pd": PD, "tikv": TIKV}


def _proc(proc):
    # Comm may contain spaces or parentheses. Rest starts at field 3 (state).
    with open(f"/proc/{proc.pid}/stat", "rb") as f:
        fields = f.read().rsplit(b")", 1)[1].split()
    return dict(pid=proc.pid, start_ticks=int(fields[19]),
                cpu_s=(int(fields[11]) + int(fields[12])) / TICKS,
                rss_mib=int(fields[21]) * PAGE / MIB)


def _reduce(lines, schema):
    prefixes = tuple(k.encode() for k in schema)
    totals = {}
    for line in lines:
        if not line.startswith(prefixes):
            continue
        sample = LINE.match(line)
        if not sample:
            continue
        name = sample[1].decode("ascii")
        if name not in schema:
            continue
        labels = dict(LABEL.findall(sample[2] or b""))
        key = name
        for field in schema[name]:
            value = labels.get(field.encode())
            if value is not None:
                key += "|" + field + "=" + value.decode("utf-8", "replace")
        value = float(sample[3])
        if math.isfinite(value):
            totals[key] = totals.get(key, 0) + value
    return totals


def _metrics(role, url, headers):
    started = time.monotonic()
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=4) as resp:
            data = resp.read(12 * MIB + 1)
        if len(data) > 12 * MIB:
            raise ValueError("metrics response exceeds 12 MiB")
        return dict(counts=_reduce(data.splitlines(), SCHEMAS[role]),
                    bytes=len(data), seconds=round(time.monotonic() - started, 4))
    except (OSError, ValueError) as err:
        return {"error": str(err)}


def capture(c, storage):
    started = time.monotonic()
    workers = set(c.workers)
    procmap = {name: (("worker" if name in workers else name.rsplit("-", 3)[-3]), proc)
               for name, proc in c.children.items()}
    procmap.update({name: (name, proc) for name, proc in storage.current.items()})
    processes = {}
    for name, (role, proc) in procmap.items():
        try:
            processes[name] = dict(role=role, **_proc(proc))
        except (OSError, ValueError, IndexError) as err:
            processes[name] = {"role": role, "error": str(err)}

    auth = {"Authorization": "Bearer " + c.env["API_TOKEN"]}
    endpoints = {w: ("worker", c.urls[w] + "/metrics?scope=local", auth) for w in workers}
    endpoints.update({name: (name, f"http://127.0.0.1:{storage.base + offset}/metrics", {})
                      for name, offset in (("pd", 0), ("tikv", 3)) if name in storage.current})
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        futures = {name: pool.submit(_metrics, *args) for name, args in endpoints.items()}
        metrics = {name: dict(role=endpoints[name][0], **job.result()) for name, job in futures.items()}
    return dict(monotonic_s=started, scrape_s=round(time.monotonic() - started, 4),
                processes=processes, metrics=metrics)


def diff(start, end):
    wall = end["monotonic_s"] - start["monotonic_s"]
    usage, deltas, warnings = {}, {}, []
    for name in sorted(set(start["processes"]) | set(end["processes"])):
        a = start["processes"].get(name, {})
        b = end["processes"].get(name, {})
        if "cpu_s" not in a or "cpu_s" not in b or (a["pid"], a["start_ticks"]) != (b["pid"], b["start_ticks"]):
            warnings.append(f"{name}: process unavailable or restarted")
            continue
        group = usage.setdefault(b["role"], {"cpu_s": 0, "rss_start_mib": 0, "rss_end_mib": 0})
        group["cpu_s"] += b["cpu_s"] - a["cpu_s"]
        group["rss_start_mib"] += a["rss_mib"]
        group["rss_end_mib"] += b["rss_mib"]
        group["avg_cpu_cores"] = group["cpu_s"] / wall if wall else 0
    for name in sorted(set(start["metrics"]) | set(end["metrics"])):
        a = start["metrics"].get(name, {})
        b = end["metrics"].get(name, {})
        if "counts" not in a or "counts" not in b:
            warnings.append(f"{name}: missing metrics ({a.get('error', '')}, {b.get('error', '')})")
            continue
        pa, pb = start["processes"].get(name, {}), end["processes"].get(name, {})
        if "start_ticks" not in pa or "start_ticks" not in pb or (pa["pid"], pa["start_ticks"]) != (pb["pid"], pb["start_ticks"]):
            warnings.append(f"{name}: counter baseline unavailable")
            continue
        role = b["role"]
        group = deltas.setdefault(role, {})
        if not a["counts"] and not b["counts"]:
            warnings.append(f"{name}: no selected metric family exported")
        for key, value in b["counts"].items():
            change = value - a["counts"].get(key, 0)
            if change < 0:
                warnings.append(f"{name}: counter reset for {key}")
            elif change:
                group[key] = group.get(key, 0) + change
    return dict(wall_s=round(wall, 4), scrape_start_s=start["scrape_s"], scrape_end_s=end["scrape_s"],
                resources={role: {k: round(v, 4) for k, v in values.items()} for role, values in usage.items()},
                counters=deltas, warnings=warnings)
