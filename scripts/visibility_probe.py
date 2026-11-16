"""Measure published/applied frontiers AND ordinary search visibility from one t0."""
from concurrent.futures import ThreadPoolExecutor
import json
import math
import time
import urllib.request


def target_index(record, centers):
    if record.get('deleted'):
        return [f'index-{n:04d}' for n in range(len(centers))]
    vector = record['vector']
    n = min(range(len(centers)), key=lambda n: sum((a-b)**2 for a,b in zip(vector,centers[n])))
    return [f'index-{n:04d}']


def add_targets(targets, source, events, centers):
    for event in events:
        for index in target_index(event['record'], centers):
            targets.setdefault(index,{})[str(source)] = max(targets.get(index,{}).get(str(source),0),event['sequence'])


def matches(hits, expected):
    """Full final ID/revision/payload/distance oracle, including absent tombstones."""
    hits = hits or []
    if len(hits) != len(expected) or len({h['id'] for h in hits}) != len(hits): return False
    for hit in hits:
        record = expected.get(hit['id'])
        if record is None or hit['revision'] != record['revision'] or (hit.get('payload') or {}) != record['payload']: return False
        if not math.isclose(hit['distance'],sum(v*v for v in record['vector']),rel_tol=1e-9,abs_tol=1e-6): return False
    return True


def replica_queries(state):
    return {owner['partition']+':'+str(owner['epoch']):(index,owner)
            for index,group in state.get('replica_owners',{}).items() for owner in group['owners']}


def positions_ready(state, local, sources, positions, baseline, indexes, replicas):
    """Pin each graph frontier once its required source LSNs have committed.

    completed_lsn and heartbeat graph_positions deliberately do not gate this.
    """
    applied = {replica:sequence for node in local.values() for replica,sequence in node.get('applied',{}).items()}
    epochs = {replica:epoch for node in local.values() for replica,epoch in node.get('applied_epochs',{}).items()}
    if not state.get('ready') or not state.get('fully_replicated'): return False
    heads = state.get('graph_heads',{})
    if len(heads) != indexes: return False
    done = True
    for index, head in heads.items():
        required = sources.get(index,{})
        if index not in positions:
            if not all(head.get('sources',{}).get(source,0)>=lsn for source,lsn in required.items()):
                done = False; continue
            positions[index] = head['committed'] if required else baseline['graph_heads'][index]['visible']
        target = positions[index]['sequence']
        owners = state.get('replica_owners',{}).get(index,{}).get('owners',[])
        # Migration temporarily keeps an extra serving replica. All serving
        # owners must apply; waiting for the retirement heartbeat is unrelated
        # to visibility. Missing configured replicas still fails closed.
        if len(owners) < replicas or head['visible']['sequence'] < target:
            done = False
        if any(not owner.get('ready') or applied.get(owner['partition'],-1)<target or epochs.get(owner['partition'])!=owner['epoch'] for owner in owners):
            done = False
    return done and len(positions)==indexes


def probe(fixture, baseline, expected, t0, *, name, target_seconds=1, observe_seconds=30, poll_seconds=.02):
    centers = baseline['routing']['centers']; index_count = len(centers)
    active = {id:r for id,r in expected.items() if not r['deleted']}
    by_index = {f'index-{n:04d}':{} for n in range(index_count)}
    for id, record in active.items(): by_index[target_index(record,centers)[0]][id] = record
    assert len(active)<=1000, 'full-query probe currently supports at most 1000 live records'
    query = dict(vector=[0.0]*len(centers[0]),k=1000,exact=True)
    # No min_visible, sync, checkpoint or rebuild call is made by this probe.
    api_token, peer_token = fixture.c.env['API_TOKEN'], fixture.c.env['CLUSTER_TOKEN']
    namespace = 'vector-v0/'+fixture.c.namespace.encode().hex()+'/'
    positions, successes, rows, transport_errors = {}, {}, [], []
    state = baseline

    def request(url, token, body=None):
        started = time.monotonic()
        req = urllib.request.Request(url, data=None if body is None else json.dumps(body).encode(),
            headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
        try:
            with urllib.request.urlopen(req,timeout=3) as response: result = json.load(response)
            return dict(value=result,elapsed=time.monotonic()-t0,duration=time.monotonic()-started)
        except (OSError,ValueError) as exc:
            return dict(error=type(exc).__name__,elapsed=time.monotonic()-t0,duration=time.monotonic()-started)

    def discover():
        def source(item):
            name, progress = item; after = progress['head']; end = None; events = []
            while end is None or after<end:
                result = request(fixture.urls[0]+'/v1/changes',api_token,dict(partition=name,stream='data',after=after,limit=256))
                if 'error' in result: raise RuntimeError('source target discovery: '+result['error'])
                page = result['value']
                assert not page['reset_required'], 'target log compacted during measurement'
                if end is None: end = page['head']
                batch = [event for event in (page.get('data') or []) if event['sequence']<=end]
                if after<end: assert batch and batch[0]['sequence']==after+1 and page['next']>after, 'target log gap'
                events.extend(batch); after = page['next']
            return name,end,events
        targets, heads, count = {}, {}, 0
        with ThreadPoolExecutor(max_workers=len(baseline['data_progress'])) as pool:
            for name,head,events in pool.map(source,baseline['data_progress'].items()):
                heads[name]=head; count+=len(events)
                add_targets(targets,int(name.split('-')[-1]),events,centers)
        return dict(sources=targets,heads=heads,events=count,elapsed=time.monotonic()-t0)

    started = time.monotonic(); first_positions = None; final_queries = None; target = None
    path = fixture.logs/(name+'-visibility.jsonl')
    with ThreadPoolExecutor(max_workers=24) as pool, path.open('w') as output:
        discovery = pool.submit(discover)
        while time.monotonic()-t0<observe_seconds:
            checked = replica_queries(state)
            status_future = pool.submit(request,fixture.urls[0]+'/v1/cluster',api_token)
            metrics_start = time.monotonic()
            metrics_future = pool.submit(lambda: (fixture.local_metrics(),time.monotonic()-t0))
            queries = {}
            for key,(index,owner) in checked.items():
                body = dict(namespace=namespace,partition=owner['partition'],epoch=owner['epoch'],operation='search',query=query)
                queries[key]=(index,pool.submit(request,owner['url']+'/internal/partition',peer_token,body))
            for n,url in enumerate(fixture.urls):
                queries['gateway-'+str(n)]=(None,pool.submit(request,url+'/v1/search',api_token,query))
            status = status_future.result(); local, metrics_elapsed = metrics_future.result()
            if 'error' not in status: state = status['value']
            if target is None and discovery.done(): target = discovery.result()
            good_positions = target is not None and 'error' not in status and positions_ready(state,local,target['sources'],positions,baseline,index_count,2)
            observation = max(status['elapsed'],metrics_elapsed)
            current = replica_queries(state)
            # Re-evaluate membership every observation; a former owner's success
            # cannot prove that a replacement replica has applied or can query.
            if good_positions:
                if first_positions is None: first_positions = observation
            else: first_positions = None
            outcomes = {}
            for key,(index,future) in queries.items():
                result = future.result()
                if 'error' in result:
                    transport_errors.append(dict(key=key,error=result['error'],elapsed=result['elapsed']))
                    outcomes[key]=dict(error=result['error'],seconds=result['duration'])
                    continue
                wanted = active if index is None else by_index[index]
                ok = matches(result['value'].get('hits'),wanted)
                outcomes[key]=dict(correct=ok,seconds=result['duration'],graph_position=result['value'].get('graph_position'))
                if ok: successes.setdefault(key,result['elapsed'])
                else: successes.pop(key,None)
            required = set(current) | {'gateway-'+str(n) for n in range(len(fixture.urls))}
            all_queries = len(current)>=index_count*2 and required<=successes.keys()
            final_queries = max(successes[key] for key in required) if all_queries else None
            row = dict(elapsed=time.monotonic()-t0,position_observation_seconds=observation,
                positions_ready=good_positions,queries_ready=all_queries,query_outcomes=outcomes,
                graph_targets=positions,discovery_ready=target is not None,
                graph_heads=state.get('graph_heads',{}),replica_owners=state.get('replica_owners',{}),
                local_applied={node:dict(applied=v.get('applied',{}),epochs=v.get('applied_epochs',{})) for node,v in local.items()},
                metadata_seconds=status['duration'],metrics_seconds=metrics_elapsed-(metrics_start-t0),
                pending_events=sum(d['head']-d['completed_lsn'] for d in state.get('data_progress',{}).values()))
            rows.append(row);output.write(json.dumps(row)+'\n');output.flush()
            if good_positions and all_queries: break
            time.sleep(poll_seconds)
        if target is None: target = discovery.result()
    complete = first_positions is not None and final_queries is not None
    total = max(first_positions,final_queries) if complete else None
    result = dict(correct=complete,within_target=complete and total<=target_seconds,target_seconds=target_seconds,
        positions_seconds=first_positions,queries_seconds=final_queries,both_seconds=total,
        source_targets=target,graph_targets=positions,live_records=len(active),records=len(expected),
        initial_pending_events=rows[0]['pending_events'] if rows else None,
        pending_observation_seconds=rows[0]['position_observation_seconds'] if rows else None,
        samples=len(rows),poll_seconds=poll_seconds,
        max_observation_gap_seconds=max((b['elapsed']-a['elapsed'] for a,b in zip(rows,rows[1:])),default=0),
        probe_setup_seconds=started-t0,transport_errors=transport_errors,
        counting='from last acknowledged writer quiescence; no implicit query sync; heartbeat progress excluded')
    (fixture.logs/(name+'-visibility.json')).write_text(json.dumps(result,indent=2)+'\n')
    return result
