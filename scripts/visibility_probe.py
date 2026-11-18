"""Measure published/applied frontiers AND ordinary search visibility from one t0."""
from concurrent.futures import ThreadPoolExecutor
import json
import math
import time
from probe_http import request as http_request, classify_error, RequestJournal
from query_buckets import MAX_SEARCH_K


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


def matches(hits, expected, distances=None):
    """Full final ID/revision/payload/distance oracle, including absent tombstones."""
    hits = hits or []
    if len(hits) != len(expected) or len({h['id'] for h in hits}) != len(hits): return False
    for hit in hits:
        record = expected.get(hit['id'])
        if record is None or hit['revision'] != record['revision'] or (hit.get('payload') or {}) != record['payload']: return False
        distance=distances[hit['id']] if distances is not None else sum(v*v for v in record['vector'])
        if not math.isclose(hit['distance'],distance,rel_tol=1e-9,abs_tol=1e-6): return False
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


def membership(state):
    return {key:(index,owner.get('node'),owner.get('session'),owner.get('url'),owner['epoch'])
            for key,(index,owner) in replica_queries(state).items()}


def query_plan(fixture, expected, centers):
    layout=getattr(fixture,'visibility_buckets',None)
    active={id:r for id,r in expected.items() if not r['deleted']}
    if layout is None:
        assert len(active)<=MAX_SEARCH_K, 'more than 1000 live records requires an explicit bounded bucket layout'
        groups=[('all',None,active)]
    else: groups=layout.partition(expected)
    plans=[]
    for label,filters,records in groups:
        by_index={f'index-{n:04d}':{} for n in range(len(centers))}
        for id,r in records.items(): by_index[target_index(r,centers)[0]][id]=r
        query=dict(vector=[0.0]*len(centers[0]),k=MAX_SEARCH_K,exact=True)
        if filters is not None: query['filter']=filters
        plans.append(dict(label=label,query=query,expected=records,by_index=by_index))
    return plans


def probe(fixture, baseline, expected, t0, *, name, target_seconds=1, observe_seconds=30, poll_seconds=.02):
    entered=time.monotonic()
    centers=baseline['routing']['centers']; index_count=len(centers)
    plans=query_plan(fixture,expected,centers)
    plan_seconds=time.monotonic()-entered
    distances={id:sum(v*v for v in r['vector']) for id,r in expected.items() if not r['deleted']}
    api_token,peer_token=fixture.c.env['API_TOKEN'],fixture.c.env['CLUSTER_TOKEN']
    namespace='vector-v0/'+fixture.c.namespace.encode().hex()+'/'
    positions,rows,transport_errors,control_errors={ },[],[],[]
    target=None; first_positions=final_queries=total=first_frontier=None
    http_path=fixture.logs/(name+'-requests.jsonl')
    collect_progress=getattr(fixture,'local_progress',fixture.local_metrics)

    def request(url,token,body=None,submitted=None):
        result=http_request(url,token,t0,body,submitted)
        result.update(journal.record(result))
        return result

    def observe_metrics():
        start=time.monotonic()
        value=collect_progress()
        return dict(value=value,started_seconds=start-t0,elapsed=time.monotonic()-t0)

    def scrape_profile(local):
        return {node:{k:v[k] for k in ('scrape_seconds','http_seconds','parse_seconds','parse_mode','response_bytes','error','http_status') if k in v}
                for node,v in local.items()}

    def metric_errors(local, phase, elapsed):
        for node,value in local.items():
            if 'error' in value:
                control_errors.append(dict(observation='local_metrics',phase=phase,node=node,elapsed=elapsed,details=value))

    def discover():
        def source(item):
            source_name,progress=item; after=progress['head']; end=None; events=[]
            while end is None or after<end:
                result=request(fixture.urls[0]+'/v1/changes',api_token,dict(partition=source_name,stream='data',after=after,limit=256))
                if 'error' in result: raise RuntimeError('source target discovery: '+json.dumps(result))
                page=result['value']; assert not page['reset_required'], 'target log compacted during measurement'
                if end is None: end=page['head']
                batch=[e for e in (page.get('data') or []) if e['sequence']<=end]
                if after<end:
                    assert batch and all(e['sequence']==after+n+1 for n,e in enumerate(batch)), 'target log gap'
                    assert page['next']==batch[-1]['sequence'], 'target log next mismatch'
                events.extend(batch); after=page['next']
            return source_name,end,events
        sources,heads,count={}, {},0; mutations=dict(insert=0,update=0,delete=0)
        routing_seconds=0
        with ThreadPoolExecutor(max_workers=len(baseline['data_progress'])) as pool:
            for source_name,head,events in pool.map(source,baseline['data_progress'].items()):
                heads[source_name]=head;count+=len(events)
                route_start=time.monotonic()
                add_targets(sources,int(source_name.split('-')[-1]),events,centers)
                routing_seconds+=time.monotonic()-route_start
                for event in events:
                    r=event['record'];kind='delete' if r.get('deleted') else ('insert' if r['revision']==1 else 'update')
                    mutations[kind]+=1
        return dict(sources=sources,heads=heads,events=count,mutations=mutations,elapsed=time.monotonic()-t0,
            routing_seconds=routing_seconds)

    started=time.monotonic(); query_attempts=0; verification_sum=0
    path=fixture.logs/(name+'-visibility.jsonl')
    # The pool exits before the journal closes, including on exceptions. A
    # successful sweep explicitly flushes all request evidence inside its t0.
    with RequestJournal(http_path) as journal,ThreadPoolExecutor(max_workers=24) as pool,path.open('w') as output:
        discovery=pool.submit(discover)
        while time.monotonic()-t0<observe_seconds:
            iteration=time.monotonic()
            early_metrics=pool.submit(observe_metrics)
            before=request(fixture.urls[0]+'/v1/cluster',api_token)
            if 'error' in before:
                early=early_metrics.result()
                metric_errors(early['value'],'before_queries',early['elapsed'])
                control_errors.append(before);time.sleep(poll_seconds);continue
            state=before['value']
            assert state['routing']==baseline['routing'], 'routing changed during fixed-route oracle'
            checked=replica_queries(state); queries={}
            query_started=time.monotonic()
            for plan in plans:
                bucket=plan['label']
                for key,(index,owner) in checked.items():
                    body=dict(namespace=namespace,partition=owner['partition'],epoch=owner['epoch'],operation='search',query=plan['query'])
                    queries[(key,bucket)]=(plan['by_index'][index],pool.submit(request,owner['url']+'/internal/partition',peer_token,body,time.monotonic()))
                for n,url in enumerate(fixture.urls):
                    queries[('gateway-'+str(n),bucket)]=(plan['expected'],pool.submit(request,url+'/v1/search',api_token,plan['query'],time.monotonic()))
            query_attempts+=len(queries); outcomes={}; errors=[]; good_queries=True; verified=[]
            # A complete sweep must pass. A previous successful bucket cannot
            # mask a later error, stale revision, or failed empty-bucket query.
            for (key,bucket),(wanted,future) in queries.items():
                result=future.result(); label=key+'/'+bucket
                if 'error' in result:
                    good_queries=False;errors.append(dict(result,key=key,bucket=bucket))
                    outcomes[label]={k:v for k,v in result.items() if k!='value'}
                    continue
                verifying=time.monotonic()
                ok=matches(result['value'].get('hits'),wanted,distances)
                verification_sum+=time.monotonic()-verifying
                verified.append(time.monotonic()-t0)
                good_queries=good_queries and ok
                outcomes[label]=dict(correct=ok,seconds=result['duration'],queue_seconds=result['queue_seconds'],request_id=result['request_id'],
                    response_seconds=result['elapsed'],verified_seconds=verified[-1],graph_position=result['value'].get('graph_position'),
                    request_profile={k:result[k] for k in ('prepare_seconds','open_seconds','read_seconds','json_seconds',
                        'record_seconds','record_lock_wait_seconds','record_encode_seconds','record_write_seconds') if k in result})
            query_done=time.monotonic()
            # The source frontier is part of this same measurement. Wait for
            # its proof once, rather than discarding a complete query sweep
            # solely because target discovery finished a few milliseconds later.
            discovery_wait=time.monotonic()
            if target is None: target=discovery.result()
            discovery_wait=time.monotonic()-discovery_wait
            # Close the membership race after every query completes; this read
            # and epoch/membership checks count inside the same t0 window.
            after_future=pool.submit(request,fixture.urls[0]+'/v1/cluster',api_token)
            metrics_start=time.monotonic()
            local=collect_progress(); metrics_elapsed=time.monotonic()-t0
            after=after_future.result()
            if 'error' in after: control_errors.append(after)
            current=after.get('value',state)
            assert current['routing']==baseline['routing'], 'routing changed during fixed-route oracle'
            stable='error' not in after and membership(state)==membership(current)
            good_positions=target is not None and 'error' not in after and positions_ready(current,local,target['sources'],positions,baseline,index_count,2)
            position_verified=time.monotonic()-t0
            early=early_metrics.result()
            metric_errors(early['value'],'before_queries',early['elapsed'])
            metric_errors(local,'after_queries',metrics_elapsed)
            # Diagnostic only: compare the earlier observation with the SAME
            # final targets and member set. It never substitutes for queries
            # or the final post-query epoch/membership check.
            early_ready=(good_positions and stable and
                positions_ready(state,early['value'],target['sources'],dict(positions),baseline,index_count,2))
            early_elapsed=max(before['elapsed'],early['elapsed'])
            if first_frontier is None:
                if early_ready:first_frontier=early_elapsed
                elif good_positions:first_frontier=position_verified
            for error in errors:
                retired=not error['key'].startswith('gateway') and membership(current).get(error['key'])!=membership(state).get(error['key'])
                error['classification']=classify_error(error,retired=retired,gateway=error['key'].startswith('gateway'))
                transport_errors.append(error)
            all_queries=good_queries and stable and len(checked)>=index_count*2
            candidate_queries=max(verified) if all_queries and verified else None
            row=dict(elapsed=time.monotonic()-t0,position_observation_seconds=max(after['elapsed'],metrics_elapsed),
                positions_ready=good_positions,queries_ready=all_queries,membership_stable=stable,
                query_outcomes=outcomes,graph_targets=positions,discovery_ready=target is not None,
                queried_replica_owners=state.get('replica_owners',{}),replica_owners=current.get('replica_owners',{}),graph_heads=current.get('graph_heads',{}),
                local_applied={node:dict(applied=v.get('applied',{}),epochs=v.get('applied_epochs',{})) for node,v in local.items()},
                early_position_observation=dict(ready=early_ready,elapsed=early_elapsed,metadata_response_seconds=before['elapsed'],
                    metadata_started_seconds=before['started_seconds'],graph_heads=state.get('graph_heads',{}),
                    cluster_ready=state.get('ready'),fully_replicated=state.get('fully_replicated'),
                    metrics_started_seconds=early['started_seconds'],metrics_elapsed=early['elapsed'],
                    local_applied={node:dict(applied=v.get('applied',{}),epochs=v.get('applied_epochs',{})) for node,v in early['value'].items()}),
                scrape_profile=dict(before=scrape_profile(early['value']),after=scrape_profile(local)),
                metadata_before_seconds=before['duration'],metadata_after_seconds=after['duration'],
                metrics_seconds=metrics_elapsed-(metrics_start-t0),query_sweep_seconds=query_done-query_started,
                target_discovery_wait_seconds=discovery_wait,
                iteration_seconds=time.monotonic()-iteration,
                pending_events=sum(d['head']-d['completed_lsn'] for d in current.get('data_progress',{}).values()))
            if good_positions and all_queries:
                first_positions=position_verified;final_queries=candidate_queries
                journal.flush()
                total=time.monotonic()-t0
                row['completion_seconds']=total
            rows.append(row);output.write(json.dumps(row)+'\n');output.flush()
            if total is not None: break
            time.sleep(poll_seconds)
        if target is None: target=discovery.result()
        journal.flush()
        journal_profile=journal.summary()
    complete=total is not None
    result=dict(correct=complete,within_target=complete and total<=target_seconds,target_seconds=target_seconds,
        positions_seconds=first_positions,queries_seconds=final_queries,both_seconds=total,
        source_targets=target,graph_targets=positions,live_records=sum(not r['deleted'] for r in expected.values()),records=len(expected),
        buckets=[dict(label=p['label'],filter=p['query'].get('filter'),expected_live_records=len(p['expected']),k=p['query']['k']) for p in plans],
        query_attempts=query_attempts,verification_cpu_wall_seconds=verification_sum,
        first_frontier_observed_seconds=first_frontier,
        initial_pending_events=rows[0]['pending_events'] if rows else None,pending_observation_seconds=rows[0]['position_observation_seconds'] if rows else None,
        samples=len(rows),poll_seconds=poll_seconds,max_observation_gap_seconds=max((b['elapsed']-a['elapsed'] for a,b in zip(rows,rows[1:])),default=0),
        probe_setup_seconds=started-t0,transport_errors=transport_errors,control_errors=control_errors,
        profiling=dict(clock='time.monotonic wall time',pre_probe_seconds=entered-t0,query_plan_seconds=plan_seconds,
            request_journal=journal_profile),
        counting='last durable ACK; independent oracle and post-query membership/epoch validation included; no implicit query sync')
    (fixture.logs/(name+'-visibility.json')).write_text(json.dumps(result,indent=2)+'\n')
    return result
