"""One bounded healthy-load capacity trial; never automatically advances a tier."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
import random
import signal
import threading
import time
from zoneinfo import ZoneInfo

from onebox_e2e import Onebox, ROOT
from onebox_freshness import hardware, publishers_ready, summarize
from pipeline_probe import interval
from quality import save
from resource_guard import ResourceGuard, clean_owned_state, host_sample, limit_reason
from visibility_probe import probe, target_index
from query_buckets import QueryBuckets
from probe_http import request as diagnostic_request, classify_error


class ClusteredData:
    """Dense separated centers, four subclusters, deterministic ID noise/moves."""
    def __init__(self, dimension, seed):
        self.dimension, self.seed = dimension, seed
        rng = random.Random(seed)
        scale = math.sqrt(dimension)
        self.centers = [[round(rng.uniform(-4,4)/scale,12) for _ in range(dimension)] for _ in range(8)]

    def vector(self, number, revision):
        cluster = (number+revision//3)%8
        rng = random.Random(self.seed+number*1009)
        scale = math.sqrt(self.dimension)
        subcluster = (number//8)%4
        return [self.centers[cluster][d] + (.20*math.sin((subcluster+1)*(d+1))
                +rng.uniform(-.035,.035)+.004*math.sin(revision*(d+1)))/scale
                for d in range(self.dimension)]


def recall_result(query, active, hits, k=10):
    ranked = sorted((sum((a-b)**2 for a,b in zip(query,r['vector'])),id) for id,r in active.items())[:k]
    ids = [h['id'] for h in hits or []]
    assert len(ids)==k and len(set(ids))==k, 'ANN size/duplicate error'
    for hit in hits:
        r = active[hit['id']]
        assert hit['revision']==r['revision'] and (hit.get('payload') or {})==r['payload']
        distance = sum((a-b)**2 for a,b in zip(query,r['vector']))
        assert math.isclose(hit['distance'],distance,rel_tol=1e-5,abs_tol=1e-6), 'ANN distance error'
    recall = len(set(ids)&{id for _,id in ranked})/k
    assert recall>=.9, ('ANN recall below .9',recall)
    return recall


def hot_source_ids(records, count, partitions=16):
    """Distinct existing IDs sharing one data hash partition; no oracle lock serialization."""
    groups={n:[] for n in range(partitions)}
    for number in range(records-128):
        source=int.from_bytes(hashlib.sha256(f'v-{number:04d}'.encode()).digest()[:8],'big')%partitions
        groups[source].append(number)
    source=max(groups,key=lambda n:len(groups[n]))
    if len(groups[source])<count: raise ValueError('not enough distinct IDs for requested hot-source burst')
    return source,groups[source][:count]


class CapacityTrial(Onebox):
    guard = None

    def persist(self):
        self.report['measurements'] = self.measurements.summary()
        save(self.logs/'report.json',self.report)  # Do not overwrite other onebox runs' latest pointer.

    def request(self,*args,**kwargs):
        if self.guard: self.guard.check()
        # Healthy acceptance fails on the first request error; same 3s timeout.
        kwargs['retry'] = False
        return super().request(*args,**kwargs)

    def check_background(self):
        if self.guard: self.guard.check()
        super().check_background()

    def start_workload(self):
        assert not self.workers
        self.work_stop.clear()
        mutable = self.args.records-128
        def writer(shard):
            n=0; next_at=time.monotonic(); period=4/self.args.rate
            try:
                ids=list(range(shard,mutable,4))
                while not self.work_stop.is_set():
                    self.oracle.mutate(ids[n%len(ids)]); n+=1
                    next_at=max(next_at+period,time.monotonic())
                    self.work_stop.wait(max(0,next_at-time.monotonic()))
            except BaseException as exc:
                with self.failure_lock: self.failures.append(('writer',shard,repr(exc)))
        def reader(shard):
            n=0
            try:
                while not self.work_stop.is_set():
                    anchor=mutable+(n+shard*7)%64; n+=1
                    record=self.request('/v1/records/'+f'v-{anchor:04d}')
                    self.oracle.check_record(record,self.oracle.snapshot()[record['id']])
                    result=self.request('/v1/search','POST',dict(vector=record['vector'],k=10))
                    hits=result.get('hits') or []
                    assert len({h['id'] for h in hits})==len(hits)
                    assert all(math.isfinite(h['distance']) and h['distance']>=0 for h in hits)
                    self.work_stop.wait(.4)
            except BaseException as exc:
                with self.failure_lock: self.failures.append(('reader',shard,repr(exc)))
        self.workers=[threading.Thread(target=writer,args=(n,)) for n in range(4)]+[threading.Thread(target=reader,args=(n,)) for n in range(2)]
        for worker in self.workers: worker.start()

    def verify_records_and_recall(self):
        """No Sync/min_visible/checkpoint. Called after timed visibility completes."""
        expected=self.oracle.snapshot(); active={id:r for id,r in expected.items() if not r['deleted']}
        seen=set(); cursor=''
        while True:
            page=self.request('/v1/records?include_deleted=true&limit=83'+('&cursor='+cursor if cursor else ''))
            for record in page['records']:
                assert record['id'] not in seen
                self.oracle.check_record(record,expected[record['id']]); seen.add(record['id'])
            cursor=page.get('next_cursor','')
            if not cursor: break
        assert seen==set(expected), 'record listing missing IDs'
        recalls=[]
        for index in range(8):
            anchor=next(r for r in active.values() if target_index(r,self.centers)==[f'index-{index:04d}'])
            query=[v+.007*math.cos(d+1)/math.sqrt(self.dimension) for d,v in enumerate(anchor['vector'])]
            for url in self.urls:
                result=self.request('/v1/search','POST',dict(vector=query,k=10),url=url)
                recalls.append(recall_result(query,active,result['hits']))
        return dict(queries=len(recalls),min_recall=min(recalls),mean_recall=sum(recalls)/len(recalls))


def run(trial,args):
    trial.visibility_buckets=args.bucket_layout
    # A dirty worktree commit ID alone cannot identify the harness that ran.
    # Preserve only repository scripts, never the generated token environment.
    source_dir=trial.logs/'harness-source'; source_dir.mkdir()
    hashes={}
    for name in ('capacity_explore.py','onebox_e2e.py','onebox_freshness.py','cluster_local.py',
                 'resource_guard.py','pipeline_probe.py','visibility_probe.py','query_buckets.py','probe_http.py','quality.py','verify_dispatch.py'):
        raw=(ROOT/'scripts'/name).read_bytes()
        (source_dir/name).write_bytes(raw)
        hashes['scripts/'+name]=hashlib.sha256(raw).hexdigest()
    trial.report.update(kind='bounded-capacity-exploration',hardware=hardware(),
        harness_sha256=hashes,
        binary_sha256=hashlib.sha256((ROOT/'.tools/vector-server').read_bytes()).hexdigest(),
        workload=dict(records=args.records,dimension=args.dimension,target_mutations_per_second=args.rate,
            writers=4,readers=2,reader_pause_seconds=.4,rounds=args.rounds,load_seconds=args.load_seconds,
            seed=args.seed,distribution='8 dense clusters x 4 subclusters, stable ID noise; cluster moves every 3 revisions',
            mutation_policy='round tail: 4 inserts, 4 updates, 4 fresh live-record deletes; workload delete every 11th revision and restore next revision',
            initial_seed_rate=args.seed_rate,bucket_width=args.bucket_layout.width,bucket_count=args.bucket_layout.count,burst_writes=args.burst_writes,
            clock='last durable mutation ACK; all current serving replicas and both gateways; no query sync'),
        budget=dict(new_data_and_logs_gib=2,soft_stop_gib=1.5,min_free_disk_gib=15,max_owned_rss_gib=3,max_trial_seconds=240))
    with trial.case('ready_cluster'):
        trial.storage.start(); trial.check_background()
        for slot in (0,1,2,35,3,4,5): trial.check_background(); trial.launch(slot)
        trial.wait('replicas',lambda:trial.state().get('fully_replicated',False))
        trial.wait('sources',lambda:all(d.get('owner_available') for d in trial.state()['data_progress'].values()))
        trial.wait('publishers',lambda:publishers_ready(trial.state()))
    results=[]

    def measure(name,baseline,before,load_seconds=None,load_only_seconds=None,burst=None):
        trial.check_background()
        expected=trial.oracle.snapshot(); acks=dict(trial.oracle.acknowledged)
        t0=max(a['monotonic'] for a in acks.values())
        save(trial.logs/(name+'-targets.json'),dict(records=expected,acknowledgements={id:dict(a,seconds_from_t0=a['monotonic']-t0) for id,a in acks.items()}))
        with trial.case(name) as row:
            row.update(probe(trial,baseline,expected,t0,name=name,target_seconds=1))
            after=trial.local_metrics()
            save(trial.logs/(name+'-metrics-before.json'),before)
            save(trial.logs/(name+'-metrics-after.json'),after)
            row['pipeline_stages']=interval(before,after)
            if load_seconds:
                row['load_and_settle_seconds']=load_seconds
                row['actual_mutations_per_second']=row['source_targets']['events']/load_seconds
                row['target_mutations_per_second']=args.rate
                row['tail_mutations']=12
                row['hot_source_burst']=burst
                row['load_only_seconds']=load_only_seconds
                row['load_only_mutations_per_second']=(row['source_targets']['events']-12-(burst or {}).get('requests',0))/load_only_seconds
            results.append(row)
            trial.report['visibility_summary']=dict(initial=summarize(results[:1]),steady=summarize(results[1:]))
            assert row['correct'], 'visibility never converged'
            row['recall']=trial.verify_records_and_recall()
            assert not row['control_errors'], 'control/metadata probe errors require investigation'
            assert all(e['classification']=='retired_member_fenced' for e in row['transport_errors']), 'unexplained probe errors require investigation'
            # Preserve complete >1s measurements and then stop. Never escalate
            # a tier after a missed target or use eventual success to pass it.
            assert row['within_target'], ('one-second boundary reached',row['both_seconds'])
        print(name,json.dumps({k:row.get(k) for k in ('both_seconds','positions_seconds','queries_seconds','initial_pending_events','actual_mutations_per_second','recall')}),flush=True)

    baseline=trial.state(); before=trial.local_metrics(); trial.measurements.phase='initial_writes'
    def seed(shard):
        next_at=time.monotonic()
        for n in range(shard,args.records,4):
            trial.check_background();trial.oracle.mutate(n,initial=True)
            next_at=max(next_at+4/args.seed_rate,time.monotonic())
            if n+4<args.records: time.sleep(max(0,next_at-time.monotonic()))
    seed_started=time.monotonic()
    with ThreadPoolExecutor(max_workers=4) as pool: list(pool.map(seed,range(4)))
    trial.report['initial_seed_seconds']=time.monotonic()-seed_started
    measure('initial_visibility',baseline,before)
    for n in range(args.rounds):
        baseline=trial.state(); before=trial.local_metrics(); trial.measurements.phase=f'load_{n+1}'
        started=time.monotonic(); trial.start_workload(); trial.hold(args.load_seconds); trial.stop_workload()
        load_only_seconds=time.monotonic()-started
        burst=None
        if args.burst_writes:
            source,ids=hot_source_ids(args.records,args.burst_writes)
            trial.measurements.phase=f'hot_source_burst_{n+1}'
            burst_started=time.monotonic()
            with ThreadPoolExecutor(max_workers=args.burst_writes) as pool:
                list(pool.map(lambda id:trial.oracle.mutate(id,deleted=False),ids))
            burst=dict(source=source,requests=len(ids),seconds=time.monotonic()-burst_started,
                includes='durable writes, immediate raw reads, and existing idempotency replays; alternating both gateways')
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures=[pool.submit(trial.oracle.mutate,args.records+4*n+j,initial=True) for j in range(4)]
            futures += [pool.submit(trial.oracle.mutate,j,deleted=False) for j in range(4)]
            futures += [pool.submit(trial.oracle.mutate,args.records-1-4*n-j,deleted=True) for j in range(4)]
            for future in futures: future.result()
        duration=time.monotonic()-started
        measure(f'steady_visibility_{n+1}',baseline,before,duration,load_only_seconds,burst)
    if args.diagnose_retirement: diagnose_retirement(trial)
    trial.finish()


def retirement_ready(state, local, index, old):
    """Plan removal precedes Worker lease release/local retirement."""
    worker=local.get(old['node'])
    return (state.get('fully_replicated',False) and worker is not None
            and all(o['partition']!=old['partition'] for o in state['replica_owners'][index]['owners'])
            and old['partition'] not in worker.get('applied_epochs',{}))


def diagnose_retirement(trial):
    """Separate operator diagnostic, outside all healthy freshness samples."""
    with trial.case('retired_member_diagnostic') as row:
        before=trial.state()
        index,group=next(iter(before['replica_owners'].items())); old=group['owners'][0]
        row['old_owner']=old
        trial.request('/v1/drain','POST',{},url=old['url'])
        def retired():
            state=trial.state()
            return state.get('fully_replicated') and all(o['partition']!=old['partition'] for o in state['replica_owners'][index]['owners'])
        row['plan_removal_seconds']=trial.wait('old owner removed from plan',retired,seconds=45)
        # A plan no longer routing to a replica does not revoke its live lease
        # instantly. Require evidence that the Worker actually removed it;
        # never treat a subsequent successful/failed query as that evidence.
        row['local_retirement_seconds']=trial.wait('old owner removed locally',
            lambda:retirement_ready(trial.state(),trial.local_metrics(),index,old),seconds=10)
        row['retired_local_metrics']=trial.local_metrics()[old['node']]
        row['replacement_owners']=trial.state()['replica_owners'][index]['owners']
        query=dict(vector=[0.0]*trial.dimension,k=1000,exact=True)
        body=dict(namespace='vector-v0/'+trial.c.namespace.encode().hex()+'/',partition=old['partition'],epoch=old['epoch'],operation='search',query=query)
        response=diagnostic_request(old['url']+'/internal/partition',trial.c.env['CLUSTER_TOKEN'],time.monotonic(),body)
        row['response']=response
        assert classify_error(response,retired=True)=='retired_member_fenced', {k:v for k,v in response.items() if k!='value'}
        path=trial.logs/'services'/(old['node']+'.log')
        lines=path.read_text().splitlines()
        matched=[line for line in lines if 'request_id='+response['request_id'] in line]
        assert len(matched)==1 and 'status=503 code=fenced' in matched[0], 'missing correlated server log'
        row.update(service_log=str(path),service_log_excerpt=matched[0],classification='expected retired-owner rejection; separate from timed healthy load')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--records',type=int,default=640)
    parser.add_argument('--dimension',type=int,default=16)
    parser.add_argument('--rate',type=float,default=20)
    parser.add_argument('--rounds',type=int,default=5)
    parser.add_argument('--load-seconds',type=float,default=8)
    parser.add_argument('--seed',type=int,default=20261117)
    parser.add_argument('--seed-rate',type=float,default=20)
    parser.add_argument('--burst-writes',type=int,default=0,help='concurrent writes to distinct IDs of one DataPartition after each steady phase (0..64)')
    parser.add_argument('--diagnose-retirement',action='store_true')
    parser.add_argument('--base-port',type=int,default=27400)
    parser.add_argument('--series',required=True)
    args=parser.parse_args()
    if not (640<=args.records<=2000 and 16<=args.dimension<=256 and 1<=args.rounds<=10 and 0<args.rate<=160 and 0<args.seed_rate<=40 and 0<args.load_seconds<=20):
        parser.error('bounded exploration: records 640..2000, dimension 16..256, rounds 1..10, rate <=160, seed rate <=40, load <=20s')
    if not args.series.replace('-','').replace('_','').isalnum(): parser.error('series must be a simple directory name')
    if not 0<=args.burst_writes<=64: parser.error('burst writes must be 0..64')
    if args.burst_writes: hot_source_ids(args.records,args.burst_writes)
    if not 1024<=args.base_port<=55000: parser.error('base port 1024..55000 required')
    args.report_root=ROOT/'reports/capacity-explore'/args.series
    args.state_root=ROOT/'.state/capacity-explore'/args.series
    args.report_root.mkdir(parents=True,exist_ok=True)
    preflight=host_sample(ROOT)
    save(args.report_root/('preflight-'+datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%H%M%S')+'.json'),preflight)
    reason=limit_reason(preflight)
    if reason: raise RuntimeError('preflight refused: '+reason)
    data=ClusteredData(args.dimension,args.seed)
    args.centers=data.centers; args.vector_factory=data.vector; args.oracle_capacity=args.records+4*args.rounds
    args.bucket_layout=QueryBuckets(args.oracle_capacity)
    args.payload_factory=args.bucket_layout.payload
    args.steady_seconds=args.load_seconds; args.latency_only=True; args.keep_running=False
    trial=None
    def interrupted(signum,frame): raise KeyboardInterrupt
    signal.signal(signal.SIGTERM,interrupted)
    try:
        trial=CapacityTrial(args)
        (trial.state_dir/'.capacity-owned').write_text(trial.run_id)
        trial.guard=ResourceGuard(trial,args.report_root)
        trial.guard.sample(); trial.guard.thread.start()
        print('TRIAL '+str(trial.logs),flush=True)
        run(trial,args)
    except BaseException as exc:
        if trial: trial.report.update(status='failed',error=repr(exc)); trial.persist()
        raise
    finally:
        signal.signal(signal.SIGTERM,signal.SIG_IGN); signal.signal(signal.SIGINT,signal.SIG_IGN)
        if trial:
            trial.measurements.phase='cleanup'
            try:
                try: trial.close()
                finally:
                    if trial.guard:
                        trial.guard.close()
                        trial.report['resources']=trial.guard.summary()
                    trial.report['data_before_cleanup']=str(trial.state_dir)
                    clean_owned_state(trial.state_dir,args.state_root,trial.run_id,trial.report.get('all_children_stopped',False))
                    trial.report['owned_data_removed']=True
            except BaseException as exc:
                trial.report.update(status='failed',cleanup_error=repr(exc))
                raise
            finally:
                trial.persist()
                print(json.dumps(dict(status=trial.report['status'],report=str(trial.logs/'report.json'),resources=trial.report.get('resources')),indent=2),flush=True)


if __name__=='__main__': main()
