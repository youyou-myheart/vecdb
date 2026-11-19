"""Controlled closed-loop CRUD benchmark against a fresh onebox namespace."""
import http.client
import json
import math
import random
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit


def _json(body):
    return json.dumps(body, separators=(',', ':'), allow_nan=False).encode()


def _p(vals, q):
    return round(sorted(vals)[math.ceil(q*len(vals))-1], 3) if vals else None


def run_crud(c, centers):
    count, concurrency = 512, 16
    rng = random.Random(20261119)
    rows = {}
    for i in range(count):
        rid = f'crud-perf-{i:04d}'
        center = centers[i % len(centers)]
        before, after = ([round(float(x)+rng.uniform(-0.025, 0.025), 7) for x in center] for _ in range(2))
        payload = {'crud_id': rid, 'stage': 'updated'}
        rows[rid] = {
            'create': _json({'request_id': 'crud-c-'+rid, 'expected_revision': 0, 'vector': before,
                             'payload': {'crud_id': rid, 'stage': 'created'}}),
            'update': _json({'request_id': 'crud-u-'+rid, 'expected_revision': 1, 'vector': after, 'payload': payload}),
            'delete': _json({'request_id': 'crud-d-'+rid, 'expected_revision': 2}),
            'after': after, 'after_payload': payload,
            'search': _json({'vector': after, 'exact': True, 'k': 1, 'filter': {'crud_id': rid},
                             'min_visible': {'id': rid, 'revision': 3}})}
    url = urlsplit(c.gateway)
    clients = [http.client.HTTPConnection(url.hostname, url.port, timeout=8) for _ in range(concurrency)]
    headers = {'Authorization': 'Bearer '+c.env['API_TOKEN'], 'Content-Type': 'application/json'}
    result = {'records': count, 'concurrency': concurrency, 'update_moves_cluster': False, 'phases': {}, 'checks': {}}
    metrics = {}
    acks = {}

    def call(conn, method, path, body):
        conn.request(method, path, body=body, headers=headers)
        reply = conn.getresponse()
        raw = reply.read()
        if reply.status != 200:
            raise ValueError(f'HTTP {reply.status}: {raw[:500].decode(errors="replace")}')
        return json.loads(raw)

    def check(conn, op, rid):
        row, path = rows[rid], '/v1/records/'+rid
        if op in ('create', 'update', 'delete', 'replay_delete'):
            rev = {'create': 1, 'update': 2, 'delete': 3, 'replay_delete': 3}[op]
            method = 'DELETE' if rev == 3 else 'PUT'
            body = row['delete'] if rev == 3 else row[op]
            ack = call(conn, method, path, body)
            if ack.get('durable') is not True or ack.get('token') != {'id': rid, 'revision': rev}:
                raise ValueError(f'unexpected {op} receipt: {ack!r}')
            if op == 'replay_delete' and ack != acks[rid]:
                raise ValueError(f'delete replay changed receipt: {ack!r} vs {acks[rid]!r}')
            return ack if op == 'delete' else None
        if op == 'search_deleted':
            reply = call(conn, 'POST', '/v1/search', row['search'])
            if 'hits' not in reply or reply.get('partial') is not False or reply['hits'] not in ([], None):
                raise ValueError(f'deleted id visible or partial search: {reply!r}'[:500])
            return
        rec = call(conn, 'GET', path, None)
        if op == 'get_updated':
            correct = (rec.get('id') == rid and rec.get('revision') == 2 and not rec.get('deleted')
                       and rec.get('vector') == row['after'] and rec.get('payload') == row['after_payload'])
        else:
            correct = rec.get('id') == rid and rec.get('revision') == 3 and rec.get('deleted') is True
        if not correct:
            raise ValueError(f'unexpected {op} record: {rec!r}'[:500])

    def stage(pool, op, keys, planned):
        def lane(conn, lane_keys):
            samples, errors = [], []
            for rid in lane_keys:
                t = time.perf_counter()
                try:
                    receipt = check(conn, op, rid)
                    samples.append((rid, (time.perf_counter()-t)*1000, receipt))
                except Exception as e:
                    conn.close()
                    errors.append({'id': rid, 'error': f'{type(e).__name__}: {e}'})
            return samples, errors
        t = time.perf_counter()
        tasks = [pool.submit(lane, conn, keys[n::concurrency]) for n, conn in enumerate(clients)]
        samples, errors = [], []
        for task in tasks:
            sample, error = task.result()
            samples.extend(sample)
            errors.extend(error)
        elapsed = time.perf_counter()-t
        values = [lat for _, lat, _ in samples]
        metrics[op] = {'planned': planned, 'requests': len(keys), 'skipped': planned-len(keys),
                       'success': len(samples), 'errors': len(errors), 'first_errors': errors[:8],
                       'elapsed_s': round(elapsed, 4), 'rps': round(len(samples)/elapsed, 2),
                       'p50_ms': _p(values, .50), 'p95_ms': _p(values, .95), 'p99_ms': _p(values, .99)}
        if op == 'delete':
            acks.update({rid: ack for rid, _, ack in samples})
        return sorted(rid for rid, _, _ in samples)

    try:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            created = stage(pool, 'create', sorted(rows), count)
            updated = stage(pool, 'update', created, count)
            sample = random.Random(7352).sample(updated, min(16, len(updated)))
            stage(pool, 'get_updated', sample, 16)
            deleted = stage(pool, 'delete', updated, count)
            sample = random.Random(9288).sample(deleted, min(16, len(deleted)))
            for op in ('get_tombstone', 'replay_delete', 'search_deleted'):
                stage(pool, op, sample, 16)
    finally:
        for conn in clients:
            conn.close()
    for op, stat in metrics.items():
        group = 'phases' if op in ('create', 'update', 'delete') else 'checks'
        result[group][op] = stat
    result['durable_new_writes'] = sum(v['success'] for v in result['phases'].values())
    result['passed'] = all(v['success'] == v['planned'] for v in metrics.values())
    return result
