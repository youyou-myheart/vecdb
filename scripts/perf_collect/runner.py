"""Collect the native RawKV/graph one-box matrix into a checkpointed JSON file."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import re
import socket
import subprocess
import sys
import tempfile
import time
import traceback
from urllib.request import urlopen
import uuid

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / 'scripts'), str(ROOT / 'sdk/python')]
from cluster_local import LocalCluster
from onebox_e2e import Storage
from verify_dispatch import converged
from . import resources
from .crud import run_crud
from .mixed import run_failover, run_mixed
from .queries import run_queries
from .sdk_write import run_sdk_writes
from .search_quality import run_quality
from .writes import run_writes

DATA_SHARDS, INDEX_SHARDS, DIMENSION, RECORDS = 4, 4, 64, 8192
SDK_RATES = (500, 1500, 4000, 8000)
SDK_SUSTAINED = (1000, 60)
FAILOVER_METHOD = 'ResolveWriteRoute (target data shard)'
REGION_CONTROL = """
[coprocessor]
region-split-size="2GiB"
region-max-size="3GiB"
region-split-keys=20000000
region-max-keys=30000000
[split]
detect-times=10000
qps-threshold=10000000
byte-threshold=1073741824
region-cpu-overload-threshold-ratio=0.0
"""


def dataset():
    rng = random.Random(20261119)
    centers = [[3.0 if i == j else 0.0 for i in range(DIMENSION)] for j in range(INDEX_SHARDS)]
    records = {}
    for i in range(RECORDS):
        a, b = centers[i % INDEX_SHARDS], centers[(i+1) % INDEX_SHARDS]
        mix = .45 if (i // INDEX_SHARDS) % 8 == 0 else 0.0
        records[f'bench-seed-{i:05d}'] = [round((1-mix)*x + mix*y + rng.gauss(0,.65), 5)
                                         for x, y in zip(a, b)]
    return centers, records


def drain(c, expected=0, timeout=240):
    start = time.monotonic()
    initial = data = None
    while True:
        state = c.status()
        pos = state['data_progress']
        head = sum(d['head'] for d in pos.values())
        lag = sum(d['head'] - d['completed_lsn'] for d in pos.values())
        elapsed = round(time.monotonic() - start, 3)
        if initial is None:
            initial = lag
        if lag == 0 and head >= expected and data is None:
            data = elapsed
        if converged(state, c.partitions, INDEX_SHARDS) and head >= expected:
            return dict(confirmed_head=head, expected_head_min=expected, initial_data_lag=initial,
                        data_caught_s=data, replicas_caught_s=elapsed,
                        per_shard_heads={k: d['head'] for k, d in pos.items()})
        if elapsed >= timeout:
            raise TimeoutError(f'index catchup: head={head}, minimum={expected}, lag={lag}, graph={state["graph_positions"]}')
        time.sleep(.5)


def _sha(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1024*1024), b''):
            h.update(block)
    return h.hexdigest()


def _git(*args):
    try:
        return subprocess.check_output(['git', *args], cwd=ROOT, text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def _utc():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def _filesystem_type(path):
    """Resolve the innermost Linux mount containing the actual scratch directory."""
    try:
        rows = Path('/proc/self/mountinfo').read_text().splitlines()
    except OSError:
        return 'unknown'
    filesystem = 'unknown'
    deepest = -1
    for line in rows:
        mount, separator, details = line.partition(' - ')
        fields = mount.split()
        if not separator or len(fields) < 5 or not details.split():
            continue
        # mountinfo escapes space, tab, newline, and backslash as octal.
        location = Path(re.sub(r'\\([0-7]{3})', lambda m: chr(int(m[1], 8)), fields[4]))
        if path.is_relative_to(location) and len(location.parts) >= deepest:
            deepest = len(location.parts)
            filesystem = details.split()[0]
    return filesystem


def _write_json(path, result):
    # Readers always see a complete previous or next checkpoint.
    with tempfile.NamedTemporaryFile('w', dir=path.parent, prefix=path.name+'.', delete=False) as tmp:
        temp_path = Path(tmp.name)
        try:
            json.dump(result, tmp, indent=2, allow_nan=False)
            tmp.write('\n')
            tmp.flush()
            os.replace(temp_path, path)
        finally:
            temp_path.unlink(missing_ok=True)


def _region_diagnostics(result, base, logs):
    try:
        with urlopen(f'http://127.0.0.1:{base}/pd/api/v1/regions', timeout=2) as response:
            regions = json.load(response)['regions']
        fields = ('id', 'start_key', 'end_key', 'approximate_size', 'approximate_keys')
        result['regions'] = [{k: r.get(k) for k in fields} for r in regions]
        with urlopen(f'http://127.0.0.1:{base+3}/config', timeout=2) as response:
            active = json.load(response)
        result['region_config'] = {k: active[k] for k in ('split', 'coprocessor')}
    except (OSError, ValueError, KeyError) as err:
        result['region_diagnostic_error'] = str(err)
    result['region_log'] = {}
    for path in logs.rglob('*.log'):
        if path.stat().st_size > 80_000_000:
            continue
        with path.open(errors='replace') as src:
            lines = [line.strip()[:800] for line in src if len(line) < 18000 and
                     any(t in line for t in ('load base split region','invalid marker byte','execute admin command'))]
        if lines:
            result['region_log'][str(path.relative_to(logs))] = dict(count=len(lines), first=lines[:3], last=lines[-2:])


def _workloads(c, storage, centers, records, result, save, core_only, driver, sdk_path):
    label = result['version']
    def log(*args):
        print(time.strftime('%H:%M:%S'), label, *args, flush=True)

    storage.start()
    c.scale(result['workers'])
    c.launch('gateway', 2)
    c.launch('master', 0)
    c.launch('master', 1)
    ready = c.wait_ready()
    drain(c)
    result['placement'] = {'data': {k: d['owner']['node'] for k, d in ready['data_progress'].items()},
                           'index': {k: [o['node'] for o in row['owners']] for k, row in ready['replica_owners'].items()}}
    log('preload', len(records), result['placement'])
    def seed(item):
        name, vec = item
        ack = c.request('/v1/records/'+name, 'PUT', dict(vector=vec, expected_revision=0, request_id=name))
        if ack.get('durable') is not True or ack.get('token') != dict(id=name, revision=1):
            raise ValueError(f'unexpected preload receipt: {ack!r}')
    start = time.monotonic()
    with ThreadPoolExecutor(max_workers=24) as pool:
        for _ in pool.map(seed, records.items()):
            pass
    result['seed_s'] = round(time.monotonic()-start, 3)
    caught = result['catchup']['preload'] = drain(c, len(records))
    expected = caught['confirmed_head']
    save()
    previous = resources.capture(c, storage)

    def reset_measurement(*_):
        nonlocal previous
        previous = resources.capture(c, storage)

    def capture(name):
        nonlocal previous
        current = resources.capture(c, storage)
        result['resources'][name] = resources.diff(previous, current)
        previous = current

    def queried(row):
        name = f'{row["scenario"]}_c{row["concurrency"]}'
        result['queries'].append(row)
        if row['errors']:
            result['partial_errors'][name] = row['errors']
        log(name, row['rps'], row['p99_ms'], row['errors'])
        save()
    q = run_queries(c, records, progress=queried)
    result['query_settings'] = {k:v for k,v in q.items() if k != 'queries'}
    capture('query_matrix')

    def wrote(name, row):
        nonlocal expected
        result['writes'][name] = row
        if row['errors']:
            result['partial_errors']['writes_'+name] = row['errors']
        visible_errors = row['operations'].get('ack_to_visible', {}).get('errors', 0)
        if visible_errors:
            result['partial_errors']['visible_'+name] = visible_errors
        capture('writes_'+name)
        log(name, row.get('rps'), row.get('errors'))
        save()
        delta = row.get('durable_new_writes', 0)
        if delta:
            reached = result['catchup']['writes_'+name] = drain(c, expected+delta)
            expected = reached['confirmed_head']
            reset_measurement()
            save()
    result['write_settings'] = run_writes(c, centers, after_phase=wrote)['settings']
    quality = run_quality(c, centers, records, 'after_closed_loop_writes')
    result['quality_after_writes'] = quality
    for mode, row in quality['modes'].items():
        if row['errors'] or row['partial'] or row['short_top10']:
            result['partial_errors']['quality_'+mode] = {k: row[k] for k in ('errors','partial','short_top10')}
    log('quality', {k: v['self_in_top10_rate'] for k, v in quality['modes'].items()})
    save()
    if core_only:
        return

    reset_measurement()
    mixed = result['search_write_mixed'] = run_mixed(c, centers, records)
    capture('search_write_mixed')
    for name, row in mixed['operations'].items():
        if row['errors']:
            result['partial_errors']['mixed_'+name] = row['errors']
    save()
    result['catchup']['search_write_mixed'] = drain(c, expected+mixed['durable_new_writes'])
    save()
    def sdk_row(row):
        key = str(row['rate'])
        result['sdk_writes'][key] = row
        if row['rpc_failure_count'] or not row['all_replicas_caught']:
            result['partial_errors']['sdk_'+key] = dict(rpc_failures=row['rpc_failure_count'], caught=row['all_replicas_caught'])
        log('Go SDK', key, row['ack_qps_in_window'], row['p99_ack_ms'], row['rpc_failure_count'], row['all_replicas_caught'])
        save()
    run_sdk_writes(c, centers, rates=SDK_RATES, seconds=5, sustained=SDK_SUSTAINED, after_phase=sdk_row,
                   key_prefix='matrix-gow', driver=driver, artifact_dir=sdk_path,
                   on_start=reset_measurement, on_end=lambda rate: capture('sdk_'+str(rate)))
    rows = result['sdk_writes']
    all_rates = {str(rate) for rate in (*SDK_RATES, SDK_SUSTAINED[0])}
    skipped = sorted(all_rates - rows.keys(), key=int)
    if skipped:
        result['partial_errors']['sdk_skipped_rates'] = skipped
    recovery = result['failover'] = {'epoch_probe': FAILOVER_METHOD}
    if not skipped and all(row['all_replicas_caught'] for row in rows.values()):
        before = sum(d['head'] for d in c.status()['data_progress'].values())
        reset_measurement()
        crud = recovery['crud'] = run_crud(c, centers)
        capture('crud')
        if not crud['passed']:
            result['partial_errors']['crud'] = 'some operations or checks failed'
        save()
        try:
            reached = result['catchup']['crud'] = drain(c, before+crud['durable_new_writes'])
            recovery.update(crud_converged=True, crud_head_advance=reached['confirmed_head']-before)
        except TimeoutError as exc:
            recovery['crud_converged'] = False
            result['partial_errors']['crud_catchup'] = str(exc)
    else:
        result['partial_errors']['crud_skipped'] = 'SDK stage did not converge'
    save()
    recovery.update(run_failover(c, centers))
    if not recovery['passed']:
        result['partial_errors']['failover'] = recovery.get('setup_error', 'one or more owner recovery probes failed')
    log('owner recovery', recovery['passed'])


def collect(*, output, server, driver, scratch=None, base_port=28740, label='current', region_control=False,
            pd=None, tikv=None, weed=None, core_only=False):
    """Run one version in isolated native storage. output is a JSON file; scratch is a parent directory."""
    output = Path(output).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    tools = ROOT / '.tools'
    bins = {'server': server, 'driver': driver or tools/'writebench', 'weed': weed or tools/'weed'}
    for name, value in (('pd',pd),('tikv',tikv)):
        bins[name] = value or tools/'tiup-home'/'components'/name/'v8.5.8'/(name+'-server')
    bins = {k: Path(v).expanduser().resolve() for k,v in bins.items()}
    result = dict(version=label, collector_version=1, dimensions=DIMENSION, data_shards=DATA_SHARDS, index_shards=INDEX_SHARDS,
                  index_replicas=2, workers=3, seed_records=RECORDS, region_control=region_control, core_only=core_only,
                  queries=[], writes={}, sdk_writes={}, catchup={}, resources={}, partial_errors={}, status='running')
    meta = result['metadata'] = dict(started_at=_utc(), platform=platform.platform(), python=platform.python_version(),
                    git_rev=_git('rev-parse','HEAD'), failover_probe=FAILOVER_METHOD, base_port=base_port,
                    data_backend='raw', index_replication='graph', lease_ttl='6s', pd_max_replicas=1,
                    storage='native PD, TiKV and SeaweedFS on loopback',
                    binary_paths={k: str(v) for k,v in bins.items()},
                    overrides={k:v for k,v in os.environ.items() if k.startswith('HNSW_') or k == 'GOMAXPROCS'})
    git_status = _git('status','--porcelain','--untracked-files=normal')
    meta['git_dirty'] = bool(git_status) if git_status is not None else None
    result['sdk_settings'] = dict(rates=SDK_RATES, seconds=5, sustained=SDK_SUSTAINED, clients=4, inflight_per_client=32,
                    window='wall-clock fixed arrival window, nearest-rank SDK invocation to durable ACK, successful calls only',
                    artifacts='per-rate aggregate and timelines in sdk_writes; client raw samples are temporary')
    try:
        result['cpu_quota'] = Path('/sys/fs/cgroup/cpu.max').read_text().strip()
    except OSError:
        result['cpu_quota'] = None
    start = time.monotonic()
    def save():
        _write_json(output, result)
    save()
    try:
        if not 1024 <= base_port <= 65535-10013:
            raise ValueError('base port must be 1024..55522 to leave room for SeaweedFS gRPC')
        for name,path in bins.items():
            if name == 'driver' and core_only and not path.exists():
                continue
            if not path.is_file() or not os.access(path, os.X_OK):
                raise FileNotFoundError(f'{name}: expected executable at {path}')
        result['binary_sha256'] = _sha(bins['server'])
        result['driver_sha256'] = _sha(bins['driver']) if bins['driver'].is_file() else None
        save()
        # Storage readiness probes existing sockets. Fail before launching if another
        # deployment already owns an address, including SeaweedFS derived gRPC ports.
        for offset in (*range(4), *range(10, 14), *range(100, 106), 200, *range(10010, 10014)):
            with socket.socket() as check:
                check.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                check.bind(('127.0.0.1', base_port+offset))
        if scratch is not None:
            scratch = Path(scratch).expanduser().resolve()
            scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch, prefix='vecdb-perf-') as state:
            tmp = Path(state).resolve()
            meta['scratch_fs_type'] = _filesystem_type(tmp)
            logs = tmp/'logs'
            logs.mkdir()
            centers, records = dataset()
            route = tmp/'routing.json'
            route.write_text(json.dumps(dict(version=1, centers=centers)))
            config = tmp/'tikv.toml'
            settings = (ROOT/'tikv-local.toml').read_text().replace('reserve-space = "512MiB"', 'reserve-space = "96MiB"')
            config.write_text(settings+'\n[raft-engine]\ntarget-file-size="16MiB"\n'+(REGION_CONTROL if region_control else ''))
            meta['tikv_config_sha256'] = _sha(config)
            env = dict(os.environ, API_TOKEN=uuid.uuid4().hex, CLUSTER_TOKEN=uuid.uuid4().hex, DATA_BACKEND='raw',
                       CONSUMER_RETENTION='0', INDEX_REPLICATION='graph', CLUSTER_ROUTING_FILE=str(route), LEASE_TTL='6s',
                       PD_ENDPOINTS=f'127.0.0.1:{base_port}', S3_ENDPOINT=f'127.0.0.1:{base_port+13}', S3_BUCKET='onebox-e2e',
                       S3_ACCESS_KEY='poc-access', S3_SECRET_KEY='poc-secret-local-only')
            storage = Storage(tmp/'storage', logs, base_port, pd_binary=bins['pd'], tikv_binary=bins['tikv'],
                              weed_binary=bins['weed'], tikv_config=config)
            c = LocalCluster('perf-matrix-v1', partitions=DATA_SHARDS, dimension=DIMENSION, base_port=base_port+100,
                             grpc_port=base_port+200, replicas=2, environment=env, directory=logs/'nodes', server_binary=bins['server'])
            # Identical node ids keep placement deterministic between otherwise independent fresh runs.
            c.prefix = 'perf-matrix-v1'
            try:
                _workloads(c, storage, centers, records, result, save, core_only, bins['driver'], tmp/'sdk-writes')
            except BaseException:
                result['log_tails'] = {}
                for path in logs.rglob('*.log'):
                    with path.open('rb') as f:
                        f.seek(max(0, path.stat().st_size-10000))
                        result['log_tails'][str(path.relative_to(logs))] = f.read().decode(errors='replace')
                raise
            finally:
                try:
                    if 'pd' in storage.current and storage.current['pd'].poll() is None:
                        _region_diagnostics(result, base_port, logs)
                finally:
                    try:
                        c.close()
                    finally:
                        storage.close()
        result['status'] = 'completed_with_failures' if result['partial_errors'] else ('complete_core' if core_only else 'complete')
    except BaseException:
        result['status'] = 'failed'
        result['exception'] = traceback.format_exc()
        raise
    finally:
        meta.update(finished_at=_utc(), elapsed_s=round(time.monotonic()-start, 3))
        save()
    return result
