"""Bounded onebox latency evidence; no record IDs or cross-process clock math."""
from concurrent.futures import ThreadPoolExecutor
import json
import math
import re
import time
import urllib.request

SAMPLE = re.compile(r'^(\w+)(?:\{(.*)\})? ([^ ]+)(?: .*)?$')
LABEL = re.compile(r'(\w+)=("(?:\\.|[^"\\])*")')


def parse_metrics(text):
    histograms, queues, applied = {}, {}, {}
    for line in text.splitlines():
        if not line or line.startswith('#'): continue
        match = SAMPLE.fullmatch(line)
        if not match: raise ValueError('invalid metrics sample')
        name, encoded, value = match.groups()
        labels = {k: json.loads(v) for k,v in LABEL.findall(encoded or '')}
        value = float(value)
        if name.startswith('vecdb_pipeline_stage_seconds_') or name.startswith('vecdb_index_batch_records_'):
            stage = labels.get('stage', 'batch_records')
            key = stage+'/'+labels['outcome']
            h = histograms.setdefault(key, dict(count=0, sum=0, buckets={}))
            if name.endswith('_count'): h['count'] += value
            elif name.endswith('_sum'): h['sum'] += value
            elif name.endswith('_bucket'):
                bound = labels['le']; h['buckets'][bound] = h['buckets'].get(bound,0)+value
        elif name.startswith('vecdb_index_queue_'):
            queues.setdefault(labels['replica'], {})[name.removeprefix('vecdb_index_queue_')] = value
        elif name == 'vecdb_graph_local_applied_sequence':
            applied[labels['replica']] = int(value)
    return dict(histograms=histograms, queues=queues, applied=applied)


def collect(endpoints, token):
    def scrape(item):
        node, url = item
        start = time.monotonic()
        req = urllib.request.Request(url+'/metrics?scope=local', headers={'Authorization':'Bearer '+token})
        try:
            with urllib.request.urlopen(req, timeout=2) as response:
                result = parse_metrics(response.read().decode())
            result['scrape_seconds'] = time.monotonic()-start
            return node, result
        except (OSError, ValueError) as exc:
            return node, dict(error=type(exc).__name__)
    with ThreadPoolExecutor(max_workers=len(endpoints) or 1) as pool:
        return dict(pool.map(scrape, endpoints.items()))


def interval(before, after):
    """Subtract counters per process before aggregation; reject resets/missing nodes."""
    totals = {}
    if before.keys() != after.keys(): raise ValueError('process set changed during metrics interval')
    for node, end in after.items():
        start = before[node]
        if 'error' in end or 'error' in start: raise ValueError('metrics scrape failed: '+node)
        for key in start['histograms'].keys() | end['histograms'].keys():
            h = end['histograms'].get(key, dict(count=0,sum=0,buckets={}))
            old = start['histograms'].get(key, dict(count=0,sum=0,buckets={}))
            delta = dict(count=h['count']-old['count'],sum=h['sum']-old['sum'],buckets={})
            if delta['count'] < 0 or delta['sum'] < -1e-9: raise ValueError('histogram reset: '+node)
            for bound in h['buckets'].keys() | old['buckets'].keys():
                change = h['buckets'].get(bound,0)-old['buckets'].get(bound,0)
                if change < 0: raise ValueError('bucket reset: '+node)
                delta['buckets'][bound] = change
            total = totals.setdefault(key, dict(count=0,sum=0,buckets={}))
            total['count'] += delta['count']; total['sum'] += delta['sum']
            for bound, change in delta['buckets'].items(): total['buckets'][bound] = total['buckets'].get(bound,0)+change
    for h in totals.values():
        h['mean'] = h['sum']/h['count'] if h['count'] else None
        for fraction in (.5,.95,.99):
            bound = next((float(b) for b,n in sorted(h['buckets'].items(),key=lambda x:float(x[0])) if h['count'] and n>=fraction*h['count']), math.inf)
            h[f'p{int(fraction*100)}_upper_bound'] = bound if math.isfinite(bound) else None
    return totals


def drain_summary(rows):
    """Retrospective fixed frontier: last status is fully converged with no writers.

    Observation times are upper bounds, not exact event times. Global replica
    positions are heartbeat reports; local gauges expose actual in-memory apply.
    """
    if not rows: raise ValueError('empty drain trace')
    final = rows[-1]['state']
    heads = final['graph_heads']
    targets = {replica['partition']:head['committed']['sequence']
               for index, head in heads.items() for replica in final['replica_owners'][index]['owners']}
    first = {}
    for row in rows:
        state = row['state']
        local = {replica:sequence for node in row['local'].values() for replica,sequence in node.get('applied',{}).items()}
        checks = dict(
            source_commits=all(state['graph_heads'][index].get('sources',{}).get(source,0)>=lsn
                for index,head in heads.items() for source,lsn in head.get('sources',{}).items()),
            graph_visible=all(state['graph_heads'][index]['visible']['sequence']>=head['committed']['sequence'] for index,head in heads.items()),
            replicas_local=all(local.get(replica,-1)>=sequence for replica,sequence in targets.items()),
            replicas_reported=all(state.get('graph_positions',{}).get(replica,{}).get('sequence',-1)>=sequence for replica,sequence in targets.items()),
            source_completed=all(state['data_progress'][source]['completed_lsn']>=d['head'] for source,d in final['data_progress'].items()))
        for name, done in checks.items():
            if done: first.setdefault(name, row['elapsed_seconds'])
    return dict(first_observed_seconds=first, all_converged_seconds=rows[-1]['elapsed_seconds'], samples=len(rows),
                max_observation_gap_seconds=max((b['elapsed_seconds']-a['elapsed_seconds'] for a,b in zip(rows,rows[1:])),default=0),
                initial_pending_events=sum(d['head']-d['completed_lsn'] for d in rows[0]['state']['data_progress'].values()),
                drain_stage_metrics=interval(rows[0]['local'],rows[-1]['local']))
