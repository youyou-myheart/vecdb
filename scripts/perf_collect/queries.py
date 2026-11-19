"""Timed gRPC query scenarios on a stable corpus. The caller supplies the live One-box."""
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
from heapq import nsmallest
from math import ceil, isclose, isfinite
import random
import threading
import time

from vectorstore import Client, messages


def _distance(a, b):
    return sum((x - y) * (x - y) for x, y in zip(a, b))


def _percentile(values, p):
    if not values:
        return None
    return round(values[max(0, ceil(p * len(values)) - 1)], 4)


def _distribution(values):
    values = sorted(values)
    return {f'p{int(p * 100)}': _percentile(values, p) for p in (0.5, 0.95, 0.99)}


def _truth(vector, records, k):
    best = nsmallest(k, ((_distance(vector, record[0]), rid) for rid, record in records.items()))
    cutoff = best[-1][0]
    required = frozenset(rid for score, rid in best if score < cutoff)
    return required, cutoff, k - len(required)


def _measure(client, method, query_ids, records, truth, k, n, c, warmup, local, index_count, hot_partition):
    def rpc(i):
        rid = query_ids[i % len(query_ids)]
        vector, revision = records[rid]
        if method == 'sdk_search_by_id':
            return client.search_by_id(rid, k=k)
        if method == 'sdk_routed_vector':
            return client.search(vector, k=k)
        if method in ('sdk_all_partitions', 'sdk_hot_all'):
            return client.search(vector, k=k, all_partitions=True)
        if method == 'sdk_hot_single':
            return client.search(vector, k=k, index_partitions=[hot_partition])
        if method == 'sdk_exact_all':
            return client.search(vector, k=k, exact=True)
        if method == 'sdk_min_visible':
            return client.search(vector, k=k, min_visible=messages.Token(id=rid, revision=revision))
        if method == 'sdk_follower':
            return client.follower_search(vector, k=k)
        raise ValueError(method)

    # All measured calls reuse the same SDK channel. Warmup, route loading,
    # recall calculation, result checks, percentiles and JSON encoding are outside timing.
    warmup_errors = []
    for i in range(warmup):
        local.fanouts = []
        try:
            rpc(i)
        except Exception as exc:
            warmup_errors.append(type(exc).__name__ + ": " + str(exc)[:350])

    def one_worker(slot):
        captured = []
        barrier.wait()
        for i in range(slot, n, c):
            local.fanouts = []
            start = time.perf_counter_ns()
            try:
                response = rpc(i)
                end = time.perf_counter_ns()
                failure = ''
            except Exception as exc:
                end = time.perf_counter_ns()
                response = None
                failure = type(exc).__name__ + ': ' + str(exc)[:350]
            captured.append((i, start, end, response, failure, local.fanouts))
        return captured

    barrier = threading.Barrier(c + 1)
    with ThreadPoolExecutor(max_workers=c) as pool:
        futures = [pool.submit(one_worker, slot) for slot in range(c)]
        barrier.wait()
        captured = [row for f in futures for row in f.result()]

    elapsed = (max(row[2] for row in captured) - min(row[1] for row in captured)) / 1e9
    durations, recalls, fanout, extra_searches = [], [], Counter(), 0
    barriers = {name: [] for name in ('queue_ms', 'leader_capture_ms', 'wait_apply_ms', 'query_ms')}
    rpc_failures, invalid, error_examples = 0, 0, []
    for i, start, end, resp, failure, fanouts in captured:
        if failure:
            rpc_failures += 1
            if len(error_examples) < 5:
                error_examples.append(failure)
            continue
        try:
            rid = query_ids[i % len(query_ids)]
            required, cutoff, tie_slots = truth[rid]
            if resp.partial or len(resp.hits) != k:
                raise ValueError(f'partial={resp.partial}, expected {k} hits, got {len(resp.hits)}')
            seen = set()
            ties = 0
            previous = -1.0
            for hit in resp.hits:
                if hit.id in seen or hit.id not in records:
                    raise ValueError('duplicate or unknown hit ' + hit.id)
                seen.add(hit.id)
                vector, revision = records[hit.id]
                distance = _distance(records[rid][0], vector)
                if hit.revision != revision or not isfinite(hit.distance) or not isclose(distance, hit.distance, rel_tol=1e-6, abs_tol=1e-8):
                    raise ValueError(f'invalid distance or revision on {hit.id}')
                if hit.distance < previous:
                    raise ValueError('hits not ordered by distance')
                previous = hit.distance
                ties += distance == cutoff
            recall = (len(required & seen) + min(ties, tie_slots)) / k
            if method == 'sdk_exact_all' and recall != 1.0:
                raise ValueError(f'exact recall={recall}')
            if method == 'sdk_follower':
                indices = {r.index_partition for r in resp.follower_reads}
                if len(indices) != index_count or len(resp.follower_reads) != index_count:
                    raise ValueError('missing per-index follower proof')
                for r in resp.follower_reads:
                    if not r.replica or not r.owner_epoch or r.applied.sequence < r.target.sequence:
                        raise ValueError('follower failed to apply captured leader position')
                    if r.applied.sequence == r.target.sequence and r.applied != r.target:
                        raise ValueError('conflicting follower position')
                    for field in barriers:
                        barriers[field].append(getattr(r, field))
                fanouts = [len(indices)]
            if not fanouts:
                raise ValueError('search made no observable RPC')
            fanout[fanouts[-1]] += 1
            extra_searches += len(fanouts) - 1
            recalls.append(recall)
            durations.append((end - start) / 1e6)
        except (ValueError, KeyError) as exc:
            invalid += 1
            if len(error_examples) < 5:
                error_examples.append(str(exc))
    result = dict(scenario=method, concurrency=c, n=n, warmup=warmup, elapsed_s=round(elapsed, 5),
                  succeeded=len(durations), rpc_failures=rpc_failures, validation_failures=invalid,
                  warmup_errors=len(warmup_errors), warmup_error_examples=warmup_errors[:2],
                  errors=rpc_failures + invalid, error_examples=error_examples,
                  rps=round(len(durations) / elapsed, 2), attempted_rps=round(n / elapsed, 2),
                  final_fanout_histogram=dict(sorted(fanout.items())), repeated_search_rpcs=extra_searches,
                  recall_at_k_mean=round(sum(recalls) / len(recalls), 6) if recalls else None,
                  recall_at_k_min=round(min(recalls), 6) if recalls else None)
    result.update({f'{p}_ms': val for p, val in _distribution(durations).items()})
    if method == 'sdk_follower':
        result['follower_per_partition_ms'] = {field: _distribution(values) for field, values in barriers.items()}
    return result


def run_queries(c, records, *, n=1000, concurrencies=(1, 16), k=10, query_count=32, grpc_target=None,
                warmup=16, scenarios=None, timeout=5.0, progress=None):
    normalized = {}
    for rid, item in records.items():
        if isinstance(item, dict):
            if item.get('deleted'):
                continue
            value, revision = item['vector'], item.get('revision', 1)
        else:
            value, revision = item, 1
        normalized[str(rid)] = (tuple(value), int(revision))
    if not (0 < k <= len(normalized)) or n < 1 or query_count < 1 or any(cc < 1 or cc > n for cc in concurrencies):
        raise ValueError('nonempty corpus, positive samples and 1 <= concurrency <= n required')
    rng = random.Random(17836)
    query_ids = rng.sample(sorted(normalized), min(query_count, len(normalized)))
    target = grpc_target or (f'127.0.0.1:{c.grpc_port}' if c.grpc_port else None)
    if not target:
        raise ValueError('LocalCluster must expose a gRPC gateway for SDK queries')
    methods = list(scenarios) if scenarios is not None else [
        'sdk_routed_vector', 'sdk_search_by_id', 'sdk_all_partitions',
        'sdk_hot_single', 'sdk_hot_all', 'sdk_exact_all', 'sdk_min_visible', 'sdk_follower']
    report = dict(queries=[], seed=17836, n_per_scenario_concurrency=n, k=k,
                  live_records=len(normalized), query_ids=query_ids, endpoint=target,
                  quality='mean/min tie-aware Recall@k versus complete offline squared-L2 oracle',
                  workload='Closed-loop Python SDK gRPC; converged read-only corpus; latency from before SDK call to decoded response. Validation and oracle outside measured window.')
    with Client(target, c.env['API_TOKEN'], timeout=timeout) as client:
        if c.replicas < 2 and 'sdk_follower' in methods:
            methods.remove('sdk_follower')
            report['follower_skip_reason'] = 'requires two index replicas'
        layout = client.layout()
        index_count = len(layout.bindings)
        if index_count == 0 or not layout.routing.centers:
            raise ValueError('clustered routing and index bindings required')
        centers = [tuple(x.vector) for x in layout.routing.centers]
        field_dim = len(centers[0])
        if any(len(x[0]) != field_dim for x in normalized.values()):
            raise ValueError('oracle and index dimensions differ')
        membership = Counter()
        nearest = {}
        for rid, (vec, _) in normalized.items():
            i = min(range(len(centers)), key=lambda j: _distance(vec, centers[j]))
            part = f'index-{i:04d}'
            membership[part] += 1
            nearest[rid] = part
        hot_partition = min(membership, key=lambda part: (-membership[part], part))
        if membership[hot_partition] < k:
            raise ValueError('hot partition has fewer than k vectors')
        hot_id = next((rid for rid in query_ids if nearest[rid] == hot_partition), None)
        if hot_id is None:
            hot_id = next(rid for rid in sorted(normalized) if nearest[rid] == hot_partition)
        report.update(hot_id=hot_id, hot_partition=hot_partition, shard_counts=dict(sorted(membership.items())),
                      dimension=field_dim, index_count=index_count)
        deadline = time.monotonic() + 16
        while True:
            ranked = client._search_routes(normalized[hot_id][0], timeout)
            if ranked and all(row[2] is not None for row in ranked):
                break
            if time.monotonic() >= deadline:
                break
            time.sleep(.15)
        report['sdk_route_snapshot'] = [{'index': part, 'count': count} for _, part, count in ranked]
        oracle = {rid: _truth(normalized[rid][0], normalized, k) for rid in set(query_ids) | {hot_id}}
        local = threading.local()
        original = client.stub.Search

        def observe(request, **options):
            # Empty protobuf partition list means broadcast.
            local.fanouts.append(len(request.index_partitions) or index_count)
            return original(request, **options)

        client.stub.Search = observe
        for cc in concurrencies:
            for method in methods:
                ids = [hot_id] if method.startswith('sdk_hot_') else query_ids
                result = _measure(client, method, ids, normalized, oracle, k, n, cc, warmup, local,
                                  index_count, hot_partition)
                report['queries'].append(result)
                if progress:
                    progress(result)
    return report
