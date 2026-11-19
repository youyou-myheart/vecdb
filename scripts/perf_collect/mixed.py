"""One-box benchmark supplements. Each is intended to run once in a fresh namespace."""
import hashlib
import http.client
import json
import math
import random
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit


def _shard(name, partitions):
    return int.from_bytes(hashlib.sha256(name.encode()).digest()[:8], 'big') % partitions


def _pack(value):
    return json.dumps(value, separators=(',', ':'), allow_nan=False).encode()


class _Client:
    def __init__(self, c, timeout):
        address = urlsplit(c.gateway)
        self.conn = http.client.HTTPConnection(address.hostname, address.port, timeout=timeout)
        self.headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + c.env['API_TOKEN']}

    def call(self, method, path, body=None, timeout=None):
        if timeout is not None:
            self.conn.timeout = timeout
            if self.conn.sock:
                self.conn.sock.settimeout(timeout)
        try:
            self.conn.request(method, path, body=body, headers=self.headers)
            reply = self.conn.getresponse()
            raw = reply.read()
            if reply.status // 100 != 2:
                raise RuntimeError(f'HTTP {reply.status} {raw[:600].decode(errors="replace")}')
            return json.loads(raw)
        except Exception:
            self.conn.close()
            raise


def _vector(item):
    if isinstance(item, dict):
        return item['vector']
    if len(item) == 2 and isinstance(item[0], (list, tuple)):
        return item[0]
    return item


def _write_body(name, vector):
    return _pack({'request_id': name, 'expected_revision': 0, 'vector': vector, 'payload': {'sample': name}})


def _receipt(name, receipt):
    if receipt.get('durable') is not True or receipt.get('token') != {'id': name, 'revision': 1}:
        raise ValueError(f'unexpected receipt for {name}: {receipt}')


def _percentiles(samples):
    samples = sorted(samples)
    return {f'p{p}_ms': round(samples[math.ceil(len(samples)*p/100)-1], 3) if samples else None
            for p in (50, 95, 99)}


def run_mixed(c, centers, records):
    duration, rate, lanes = 8, 200, {'search': 8, 'put': 4}
    count = rate * duration
    rng = random.Random(59003)
    anchors = sorted(key for key, item in records.items() if not (isinstance(item, dict) and item.get('deleted')))
    if not anchors:
        raise ValueError('ordinary ANN mixed phase requires existing visible records')
    k = min(10, len(anchors))
    anchors = rng.sample(anchors, min(64, len(anchors)))
    queries = [(name, _pack({'vector': _vector(records[name]), 'k': k})) for name in anchors]
    puts = []
    for i in range(count):
        name = f'supp-mix-{i:08d}'
        vector = [round(float(v) + rng.uniform(-0.025, 0.025), 7) for v in rng.choice(centers)]
        puts.append((name, _write_body(name, vector)))

    def lane(op, lane_id, start):
        samples, failures, shard_counts, self_hits = [], [], Counter(), 0
        client = _Client(c, 2)
        arr = queries if op == 'search' else puts
        try:
            for i in range(lane_id, count, lanes[op]):
                scheduled = start + i/rate
                time.sleep(max(0, scheduled-time.monotonic()))
                begin = time.monotonic()
                if begin >= start+duration:
                    break
                name, body = arr[i % len(arr)]
                try:
                    if op == 'put':
                        _receipt(name, client.call('PUT', '/v1/records/'+name, body))
                        shard_counts[_shard(name, c.partitions)] += 1
                    else:
                        hits = client.call('POST', '/v1/search', body).get('hits') or []
                        ids = {hit['id'] for hit in hits}
                        distances = [hit['distance'] for hit in hits]
                        if (len(ids) != k or len(hits) != k or distances != sorted(distances)
                                or not all(hit['revision'] >= 1 and math.isfinite(hit['distance']) for hit in hits)):
                            raise ValueError(f'malformed top-{k}: {hits!r}'[:600])
                        self_hits += int(name in ids)
                    end = time.monotonic()
                    samples.append(((end-begin)*1000, (end-scheduled)*1000))
                except Exception as e:
                    failures.append({'slot': i, 'id': name, 'error': f'{type(e).__name__}: {e}'})
        finally:
            client.conn.close()
        return samples, failures, shard_counts, self_hits, time.monotonic()

    start = time.monotonic()+0.15
    results = {'k': k, 'duration_s': duration, 'partition_count': c.partitions, 'operations': {}}
    with ThreadPoolExecutor(max_workers=sum(lanes.values())) as pool:
        pending = {op: [pool.submit(lane, op, n, start) for n in range(nlanes)] for op, nlanes in lanes.items()}
        for op, futures in pending.items():
            samples, failures, by_shard, self_hits, end = [], [], Counter(), 0, start+duration
            for future in futures:
                s, f, b, h, e = future.result()
                samples.extend(s)
                failures.extend(f)
                by_shard.update(b)
                self_hits += h
                end = max(end, e)
            ok = len(samples)
            elapsed = end-start
            metric = {'offered_rps': rate, 'scheduled': count, 'concurrency': lanes[op], 'success': ok,
                      'requests': ok+len(failures), 'errors': len(failures), 'unsent': count-ok-len(failures),
                      'rps': round(ok/elapsed, 2), 'elapsed_s': round(elapsed, 4),
                      **_percentiles([s[0] for s in samples]), 'schedule_to_complete': _percentiles([s[1] for s in samples]),
                      'first_errors': sorted(failures, key=lambda item: item['slot'])[:10]}
            if op == 'put':
                results.update(durable_new_writes=ok, durable_new_by_shard=dict(sorted(by_shard.items())))
            else:
                metric.update(known_query_id_found=self_hits, known_query_id_found_fraction=round(self_hits/ok, 5) if ok else None)
            results['operations'][op] = metric
    return results


def run_failover(c, centers):
    partition = 'data-0000'
    names = [next(name for n in range(100000) if _shard(name := f'supp-fo-{stage}-{n}', c.partitions) == 0)
             for stage in ('before', 'after')]
    before_id, after_id = names
    vec = [float(v) for v in centers[0]]
    bodies = {name: _write_body(name, vec) for name in names}
    paths = {name: '/v1/records/'+name for name in names}
    result = {'partition': partition, 'passed': False, 'durable_new_writes': 0, 'deadline_s': 35}
    setup = _Client(c, 4)
    try:
        progress = setup.call('GET', '/v1/cluster')['data_progress'][partition]
        old = progress['owner']
        node = old['node']
        if (not progress['owner_available'] or node not in c.workers or node not in c.children
                or c.children[node].poll() is not None
                or not any(n != node and c.children[n].poll() is None for n in c.workers)):
            raise ValueError('current owner and a surviving test-owned worker are required')
        result.update(old_node=node, old_epoch=old['epoch'])
        old_receipt = setup.call('PUT', paths[before_id], bodies[before_id])
        _receipt(before_id, old_receipt)
        result['durable_new_writes'] = 1
    except Exception as e:
        return dict(result, setup_error=f'{type(e).__name__}: {e}')
    finally:
        setup.conn.close()
    started = time.monotonic()
    deadline = started+35
    from vectorstore import Client, messages
    route_client = Client(f'127.0.0.1:{c.grpc_port}', c.env['API_TOKEN'], timeout=2)
    result['epoch_probe'] = 'ResolveWriteRoute (target data shard)'
    c.children[node].kill()
    c.workers.remove(node)

    def probe(label, action, stop=deadline):
        client, errors = _Client(c, 1.5), []
        count = 0
        try:
            while time.monotonic() < stop:
                remaining = stop-time.monotonic()
                if remaining <= 0:
                    break
                count += 1
                try:
                    value = action(client, min(1.5, remaining))
                    if value is not None:
                        return {'attempts': count, 'failed_attempts': len(errors), 'errors': errors[:15],
                                'observed_ms': round((time.monotonic()-started)*1000, 2), 'value': value}
                except Exception as e:
                    errors.append({'at_ms': round((time.monotonic()-started)*1000, 2), 'error': f'{type(e).__name__}: {e}'})
                time.sleep(min(0.15, max(0, stop-time.monotonic())))
        finally:
            client.conn.close()
        return {'attempts': count, 'failed_attempts': len(errors), 'errors': errors[:15], 'timeout_at': label}

    def owner(client, timeout):
        new = route_client.stub.ResolveWriteRoute(
            messages.GetRequest(id=before_id), timeout=timeout, metadata=route_client.metadata)
        if (new.epoch > old['epoch'] and new.partition == 0
                and new.endpoint != old.get('write_endpoint') and new.session != old.get('session')):
            return dict(endpoint=new.endpoint, session=new.session, epoch=new.epoch)

    def put_after(client, timeout):
        ack = client.call('PUT', paths[after_id], bodies[after_id], timeout)
        _receipt(after_id, ack)
        return ack

    def replay(client, timeout):
        ack = client.call('PUT', paths[before_id], bodies[before_id], timeout)
        if ack != old_receipt:
            raise ValueError(f'idempotent replay returned {ack!r}, expected {old_receipt!r}')
        return True

    def point(name):
        def check(client, timeout):
            record = client.call('GET', paths[name], timeout=timeout)
            if (record.get('id') != name or record.get('revision') != 1 or record.get('deleted')
                    or record.get('payload') != {'sample': name} or record.get('vector') != vec):
                raise ValueError(f'unexpected record {record!r}'[:600])
            return True
        return check

    with ThreadPoolExecutor(max_workers=3) as pool:
        epoch = pool.submit(probe, 'new_epoch', owner)
        ack = pool.submit(probe, 'post_kill_put', put_after)
        result['post_kill_put'] = ack.result()
        if 'value' in result['post_kill_put']:
            result['durable_new_writes'] += 1
        if 'value' in result['post_kill_put']:
            checks = {'old_receipt_replay': replay, 'old_point_get': point(before_id), 'new_point_get': point(after_id)}
            pending = {k: pool.submit(probe, k, check) for k, check in checks.items()}
            for k, task in pending.items():
                result[k] = task.result()
            result['passed'] = all('value' in result[k] for k in checks)
        result['new_epoch'] = epoch.result()
        result['passed'] = result['passed'] and 'value' in result['new_epoch']
    route_client.close()
    result['killed_process_exit_code'] = c.children[node].poll()
    result['elapsed_s'] = round(time.monotonic()-started, 4)
    return result
