"""Sequential read-only ANN quality probes. Call after the index catches up."""
import hashlib
import http.client
import json
import math
import random
import time
from collections import Counter
from urllib.parse import urlsplit


def _vector(value):
    if isinstance(value, dict):
        return value['vector']
    if len(value) == 2 and isinstance(value[0], (tuple, list)):
        return value[0]
    return value


def _pct(values, p):
    return round(sorted(values)[math.ceil(len(values)*p)-1], 3) if values else None


def run_quality(c, centers, records, label='after_catchup'):
    ids = sorted(key for key, item in records.items() if not (isinstance(item, dict) and item.get('deleted')))
    anchors = random.Random(59003).sample(ids, min(64, len(ids)))
    if len(ids) < 10 or not centers:
        raise ValueError('quality corpus must contain at least ten records and one index center')
    partitions = [f'index-{i:04d}' for i in range(len(centers))]
    vectors = {name: _vector(records[name]) for name in anchors}
    nearest = {name: min(range(len(centers)), key=lambda i: sum((x-y)**2 for x, y in zip(vec, centers[i])))
               for name, vec in vectors.items()}
    if any(len(vectors[n]) != len(center) for n in anchors for center in centers):
        raise ValueError('corpus and center dimensions differ')
    u = urlsplit(c.gateway)
    conn = http.client.HTTPConnection(u.hostname, u.port, timeout=5)
    headers = {'Authorization': 'Bearer '+c.env['API_TOKEN'], 'Content-Type': 'application/json'}
    exact = {}
    result = {'label': label, 'queries_per_mode': len(anchors), 'k': 10, 'sequential_concurrency': 1,
              'anchor_sample_sha256': hashlib.sha256('\n'.join(anchors).encode()).hexdigest(),
              'anchor_nearest_index_count': dict(Counter(partitions[nearest[n]] for n in anchors)), 'modes': {}}
    try:
        for mode in ('exact_all', 'ann_default_all', 'ann_nearest_one', 'ann_ef1024_all'):
            times, errors, examples, recalls, by_distance = [], [], [], [], []
            stat = dict.fromkeys(('partial', 'partial_field_absent', 'short_top10', 'self_in_top10', 'self_rank1',
                                  'any_zero_distance', 'zero_distance_at_rank10', 'self_zero_distance',
                                  'partial_or_short_exact_oracle'), 0)
            for name in anchors:
                body = {'k': 10, 'vector': vectors[name]}
                if mode == 'exact_all':
                    body.update(exact=True, index_partitions=partitions)
                if mode == 'ann_nearest_one':
                    body['index_partitions'] = [partitions[nearest[name]]]
                if mode == 'ann_ef1024_all':
                    body.update(ef_search=1024, index_partitions=partitions)
                encoded = json.dumps(body, separators=(',', ':')).encode()
                try:
                    t = time.perf_counter()
                    conn.request('POST', '/v1/search', body=encoded, headers=headers)
                    response = conn.getresponse()
                    raw = response.read()
                    elapsed_ms = (time.perf_counter()-t)*1000
                    if response.status != 200:
                        raise ValueError(f'HTTP {response.status}: {raw[:400]!r}')
                    output = json.loads(raw)
                    hits = output['hits'] or []
                    rank = [h['id'] for h in hits]
                    distances = [h['distance'] for h in hits]
                    if (len(rank) > 10 or len(set(rank)) != len(rank) or distances != sorted(distances)
                            or not all(math.isfinite(d) and d >= 0 for d in distances)):
                        raise ValueError(f'malformed top10 {hits!r}'[:400])
                except Exception as exc:
                    errors.append({'id': name, 'error': f'{type(exc).__name__}: {exc}'})
                    conn.close()
                    continue
                times.append(elapsed_ms)
                partial = bool(output.get('partial'))
                stat['partial'] += int(partial)
                stat['partial_field_absent'] += int('partial' not in output)
                stat['short_top10'] += int(len(rank) < 10)
                stat['self_in_top10'] += int(name in rank)
                stat['self_rank1'] += int(rank[:1] == [name])
                stat['any_zero_distance'] += int(0 in distances)
                stat['zero_distance_at_rank10'] += int(len(rank) == 10 and distances[-1] == 0)
                stat['self_zero_distance'] += int(name in rank and distances[rank.index(name)] == 0)
                if mode == 'exact_all' and len(rank) == 10 and not partial:
                    exact[name] = (rank, distances)
                if name in exact:
                    ref, ref_distances = exact[name]
                    recalls.append(len(set(rank).intersection(ref))/10)
                    by_distance.append(sum(d <= ref_distances[-1] for d in distances)/10)
                else:
                    stat['partial_or_short_exact_oracle'] += 1
                if len(examples) < 3 and (mode == 'exact_all' or name not in rank or (name in exact and recalls[-1] < 1)):
                    examples.append({'id': name, 'nearest_index': partitions[nearest[name]],
                                     'top3': list(zip(rank[:3], distances[:3])), 'self_rank': rank.index(name)+1 if name in rank else None,
                                     'exact_top3': list(zip(exact[name][0][:3], exact[name][1][:3])) if name in exact else None})
            success = len(times)
            result['modes'][mode] = {'requests': len(anchors), 'success': success, 'errors': len(errors),
                                     'first_errors': errors[:3], **stat, 'self_in_top10_rate': stat['self_in_top10']/success if success else None,
                                     'self_rank1_rate': stat['self_rank1']/success if success else None, 'recall_queries': len(recalls),
                                     'recall_at_10': sum(recalls)/len(recalls) if recalls else None,
                                     'distance_threshold_recall_at_10': sum(by_distance)/len(by_distance) if by_distance else None,
                                     'p50_ms': _pct(times, .5), 'p99_ms': _pct(times, .99), 'examples': examples}
    finally:
        conn.close()
    return result
