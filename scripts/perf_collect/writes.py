"""Deterministic HTTP data-path phases for LocalCluster. Imports do not start clients."""
import concurrent.futures
import hashlib
import http.client
import json
import math
import os
import random
import subprocess
import sys
import threading
import time
from urllib.parse import urlsplit

DEFAULTS = {
    "seed": 192, "c1": 1200, "c16": 1600, "c64": 2048,
    "hot": 2048, "mixed": 2000, "replay": 640, "visible": 64,
}


def packed(value):
    return json.dumps(value, separators=(",", ":"), allow_nan=False)


def _shard(name, n):
    return int.from_bytes(hashlib.sha256(name.encode()).digest()[:8], "big") % n


def _rows(prefix, count, centers, shards, random_seed, hot=False):
    rng = random.Random(f"{random_seed}:{prefix}")
    counter = 0
    rows = []
    for i in range(count):
        target = 0 if hot else i % shards
        while True:
            name = f"wp-{prefix}-{counter:08d}"
            counter += 1
            if _shard(name, shards) == target:
                break
        center = centers[rng.randrange(len(centers))]
        vector = [round(float(v) + rng.uniform(-0.025, 0.025), 7) for v in center]
        rows.append([name, vector, target])
    return rows


def _ops(kind, rows):
    return [[kind, *row] for row in rows]


def _prepare(op):
    kind, name, vec, shard = op
    path = "/v1/records/" + name
    payload = {"request_id": name, "expected_revision": 0, "vector": vec,
               "payload": {"wp_id": name}}
    body = None if kind == "get" else packed(payload).encode()
    visible = None
    if kind == "put_visible":
        visible = packed({"vector": vec, "k": 1, "exact": True, "filter": {"wp_id": name},
                          "min_visible": {"id": name, "revision": 1}}).encode()
    return kind, name, vec, shard, path, body, visible


class RequestFailed(Exception):
    pass


def _http(conn, method, path, body, headers):
    conn.request(method, path, body, headers)
    response = conn.getresponse()
    raw = response.read()
    if response.status != 200:
        raise RequestFailed(f"HTTP {response.status}: {raw.decode(errors='replace')}")
    return json.loads(raw)


def _run_lane(cfg, ops, barrier, event):
    address = urlsplit(cfg["url"])
    conn = http.client.HTTPConnection(address.hostname, address.port, timeout=cfg["timeout"])
    headers = {"Authorization": "Bearer " + cfg["token"], "Content-Type": "application/json"}
    samples = []
    failures = []
    startup_errors = []
    try:
        try:
            _http(conn, "GET", "/healthz", None, headers)
        except Exception as exc:
            startup_errors.append(f"{type(exc).__name__}: {exc}")
            conn.close()
        barrier.wait()
        event.wait()
        start = time.monotonic_ns()
        for kind, name, vec, shard, path, body, visible in ops:
            start_request = time.monotonic_ns()
            metric = "put" if kind == "put_visible" else kind
            try:
                if kind == "get":
                    record = _http(conn, "GET", path, None, headers)
                    if (record.get("id") != name or record.get("revision") != 1
                            or record.get("deleted") or record.get("vector") != vec
                            or (record.get("payload") or {}).get("wp_id") != name):
                        raise RequestFailed(f"unexpected record {record!r}")
                else:
                    receipt = _http(conn, "PUT", path, body, headers)
                    if receipt.get("durable") is not True or receipt.get("token") != {"id":name,"revision":1}:
                        raise RequestFailed(f"unexpected durable receipt {receipt!r}")
            except Exception as exc:
                end_request = time.monotonic_ns()
                samples.append([metric, (end_request-start_request)/1e6, shard, False])
                failures.append({"op":metric,"id":name,"error":f"{type(exc).__name__}: {exc}"})
                conn.close()
                continue
            end_request = time.monotonic_ns()
            samples.append([metric, (end_request-start_request)/1e6, shard, True])
            if visible is not None:
                try:
                    result = _http(conn, "POST", "/v1/search", visible, headers)
                    hits = result.get("hits") or []
                    if len(hits) != 1 or hits[0].get("id") != name or hits[0].get("revision") != 1:
                        raise RequestFailed(f"query did not see acknowledged revision: {result!r}")
                    samples.append(["ack_to_visible", (time.monotonic_ns()-end_request)/1e6, shard, True])
                except Exception as exc:
                    samples.append(["ack_to_visible", (time.monotonic_ns()-end_request)/1e6, shard, False])
                    failures.append({"op":"ack_to_visible","id":name,"error":f"{type(exc).__name__}: {exc}"})
                    conn.close()
        end = time.monotonic_ns()
        return dict(start=start, end=end, samples=samples, failures=failures, startup_errors=startup_errors)
    finally:
        conn.close()


def _child():
    cfg = json.loads(sys.stdin.readline())
    lanes = [[_prepare(op) for op in lane] for lane in cfg.pop("lanes")]
    barrier = threading.Barrier(len(lanes)+1)
    event = threading.Event()
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(lanes)) as pool:
        jobs = [pool.submit(_run_lane, cfg, lane, barrier, event) for lane in lanes]
        barrier.wait()
        print("READY", flush=True)
        if sys.stdin.readline().strip() != "GO":
            os._exit(2)
        cpu_start = time.process_time()
        event.set()
        results = [job.result() for job in jobs]
        results[0]["process_cpu_s"] = time.process_time() - cpu_start
    print(packed(results), flush=True)


def _percentiles(vals):
    if not vals:
        return {"p50_ms":None,"p95_ms":None,"p99_ms":None,"max_ms":None}
    values = sorted(vals)
    return {**{f"p{p}_ms":round(values[math.ceil(p*len(values)/100)-1],3) for p in (50,95,99)},
            "max_ms":round(values[-1],3)}


def _summary(samples, seconds):
    success = [s[1] for s in samples if s[3]]
    return {"requests":len(samples),"success":len(success),"errors":len(samples)-len(success),
            "rps":round(len(success)/seconds,1),"attempt_rps":round(len(samples)/seconds,1),
            **_percentiles(success)}


def _phase(c, name, ops, concurrency, timeout):
    if not ops:
        raise ValueError(f"empty workload: {name}")
    concurrency = min(concurrency, len(ops))
    nproc = min(4, max(1, math.ceil(concurrency/8)))
    lanes = [ops[i::concurrency] for i in range(concurrency)]
    cfg = {"url":c.gateway, "token":c.env["API_TOKEN"], "timeout":timeout}
    procs = []
    results = []
    try:
        for i in range(nproc):
            proc = subprocess.Popen([sys.executable, os.path.abspath(__file__), "--worker"],
                                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
            procs.append(proc)
            print(packed({**cfg,"lanes":lanes[i::nproc]}), file=proc.stdin, flush=True)
        for proc in procs:
            if proc.stdout.readline().strip() != "READY":
                raise RuntimeError(f"load generator failed to initialize for {name}")
        for proc in procs:
            print("GO", file=proc.stdin, flush=True)
            proc.stdin.close()
        for proc in procs:
            output = proc.stdout.read()
            if proc.wait() != 0:
                raise RuntimeError(f"load generator failed in {name} with exit {proc.returncode}")
            results.extend(json.loads(output))
    finally:
        for proc in procs:
            if proc.poll() is None:
                proc.terminate()
            proc.wait()
            if proc.stdin and not proc.stdin.closed:
                proc.stdin.close()
            proc.stdout.close()
    elapsed = (max(r["end"] for r in results)-min(r["start"] for r in results))/1e9
    samples = [s for r in results for s in r["samples"]]
    failures = [f for r in results for f in r["failures"]]
    by_shard = {str(i):0 for i in range(c.partitions)}
    for kind, _, shard, ok in samples:
        if kind == "put" and ok:
            by_shard[str(shard)] += 1
    main = [s for s in samples if s[0] != "ack_to_visible"]
    client_cpus = [round(r["process_cpu_s"]/elapsed, 3) for r in results if "process_cpu_s" in r]
    return {"name":name,"concurrency":concurrency,"client_processes":nproc,"elapsed_s":round(elapsed,6),
            "client_cpu_cores":round(sum(client_cpus),3),"client_cpu_cores_by_process":client_cpus,
            **_summary(main,elapsed),"operations":{k:_summary([s for s in samples if s[0]==k],elapsed)
                                                  for k in sorted({s[0] for s in samples})},
            "durable_new_writes":sum(by_shard.values()),"durable_new_by_shard":by_shard,
            "failures":failures,"startup_errors":[e for r in results for e in r["startup_errors"]]}


def run_writes(c, centers, *, counts=None, seed=20261119, after_phase=None):
    """Run sequential phases on an existing cluster. Use a fresh namespace for each call."""
    settings = dict(DEFAULTS)
    if counts:
        if set(counts)-set(settings):
            raise ValueError(f"unknown counts: {set(counts)-set(settings)}")
        settings.update(counts)
    results = {}
    def emit(name, ops, cc):
        report = _phase(c, name, ops, cc, 12)
        results[name] = report
        if after_phase:
            after_phase(name, report)
        return report
    def rows(name, n, hot=False):
        return _rows(name, n, centers, c.partitions, seed, hot)
    seeds = rows("seed", settings["seed"])
    inserted = emit("seed", _ops("put",seeds), 16)
    failed_seed_ids = {e["id"] for e in inserted["failures"]}
    seeds = [row for row in seeds if row[0] not in failed_seed_ids]
    if not seeds:
        raise RuntimeError("No successful seed writes; point reads cannot be measured")
    def read_rows(n, label):
        rng = random.Random(f"{seed}-{label}-read")
        return [rng.choice(seeds) for _ in range(n)]
    for cc in [1,16,64]:
        n = settings[f"c{cc}"]
        if n:
            emit(f"put_c{cc}", _ops("put",rows(f"u-{cc}",n)), cc)
            emit(f"get_c{cc}", _ops("get",read_rows(n,cc)), cc)
    if settings["hot"]:
        emit("put_hot_shard0_c64", _ops("put",rows("hot",settings["hot"],hot=True)), 64)
    if settings["mixed"]:
        n = settings["mixed"]
        writes = iter(_ops("put",rows("mixed",(n+4)//5)))
        reads = iter(_ops("get",read_rows(n-(n+4)//5,"mixed")))
        ops = [next(writes) if i % 5 == 0 else next(reads) for i in range(n)]
        emit("mixed_read80_write20_c32",ops,32)
    if settings["replay"]:
        emit("idempotent_replay_c16", _ops("replay",read_rows(settings["replay"],"replay")), 16)
    if settings["visible"]:
        emit("ack_to_visible_c1", _ops("put_visible",rows("visible",settings["visible"])), 1)
    return {"settings":{"counts":settings,"seed":seed,"data_partitions":c.partitions,
                        "balanced_writes":"equal number per data shard, index cluster chosen independently",
                        "hot_reads":f"random over {len(seeds)} confirmed immutable seed records",
                        "protocol":"per-thread persistent HTTP/1.1, up to four independent Python processes, closed loop",
                        "percentiles":"nearest rank over successful requests, per-operation errors retained",
                        "visibility":"ACK receipt received through first successful exact filtered min_visible query, one attempt per write"},
            "phases":results,"durable_new_writes":sum(p["durable_new_writes"] for p in results.values()),
            "failures":sum(len(p["failures"]) for p in results.values())}


if __name__ == "__main__":
    if sys.argv[1:] != ["--worker"]:
        raise SystemExit("Import run_writes from the parent one-box runner")
    _child()
