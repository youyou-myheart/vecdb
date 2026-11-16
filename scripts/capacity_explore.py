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
                    anchor=mutable+(n+shard*7)%100; n+=1
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
    trial.report.update(kind='bounded-capacity-exploration',hardware=hardware(),
        binary_sha256=hashlib.sha256((ROOT/'.tools/vector-server').read_bytes()).hexdigest(),
        workload=dict(records=args.records,dimension=args.dimension,target_mutations_per_second=args.rate,
            writers=4,readers=2,reader_pause_seconds=.4,rounds=args.rounds,load_seconds=args.load_seconds,
            seed=args.seed,distribution='8 dense clusters x 4 subclusters, stable ID noise; cluster moves every 3 revisions',
            mutation_policy='round tail insert/update/delete; workload delete every 11th revision and restore next revision',
            clock='last durable mutation ACK; all current serving replicas and both gateways; no query sync'),
        budget=dict(new_data_and_logs_gib=2,soft_stop_gib=1.5,min_free_disk_gib=15,max_owned_rss_gib=3,max_trial_seconds=240))
    with trial.case('ready_cluster'):
        trial.storage.start(); trial.check_background()
        for slot in (0,1,2,35,3,4,5): trial.check_background(); trial.launch(slot)
        trial.wait('replicas',lambda:trial.state().get('fully_replicated',False))
        trial.wait('sources',lambda:all(d.get('owner_available') for d in trial.state()['data_progress'].values()))
        trial.wait('publishers',lambda:publishers_ready(trial.state()))
    results=[]

    def measure(name,baseline,before,load_seconds=None,load_only_seconds=None):
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
                row['tail_mutations']=3
                row['load_only_seconds']=load_only_seconds
                row['load_only_mutations_per_second']=(row['source_targets']['events']-3)/load_only_seconds
            results.append(row)
            trial.report['visibility_summary']=dict(initial=summarize(results[:1]),steady=summarize(results[1:]))
            assert row['correct'], 'visibility never converged'
            row['recall']=trial.verify_records_and_recall()
            # Preserve complete >1s measurements and then stop. Never escalate
            # a tier after a missed target or use eventual success to pass it.
            assert row['within_target'], ('one-second boundary reached',row['both_seconds'])
        print(name,json.dumps({k:row.get(k) for k in ('both_seconds','positions_seconds','queries_seconds','initial_pending_events','actual_mutations_per_second','recall')}),flush=True)

    baseline=trial.state(); before=trial.local_metrics(); trial.measurements.phase='initial_writes'
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda n:trial.oracle.mutate(n,initial=True),range(args.records)))
    measure('initial_visibility',baseline,before)
    for n in range(args.rounds):
        baseline=trial.state(); before=trial.local_metrics(); trial.measurements.phase=f'load_{n+1}'
        started=time.monotonic(); trial.start_workload(); trial.hold(args.load_seconds); trial.stop_workload()
        load_only_seconds=time.monotonic()-started
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures=[pool.submit(trial.oracle.mutate,args.records+n,initial=True),
                pool.submit(trial.oracle.mutate,args.records-2,deleted=False),
                pool.submit(trial.oracle.mutate,args.records-1,deleted=True)]
            for future in futures: future.result()
        duration=time.monotonic()-started
        measure(f'steady_visibility_{n+1}',baseline,before,duration,load_only_seconds)
    trial.finish()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--records',type=int,default=640)
    parser.add_argument('--dimension',type=int,default=16)
    parser.add_argument('--rate',type=float,default=20)
    parser.add_argument('--rounds',type=int,default=5)
    parser.add_argument('--load-seconds',type=float,default=8)
    parser.add_argument('--seed',type=int,default=20261117)
    parser.add_argument('--base-port',type=int,default=27400)
    parser.add_argument('--series',required=True)
    args=parser.parse_args()
    if not (640<=args.records<=980 and 16<=args.dimension<=256 and 1<=args.rounds<=10 and args.records+args.rounds<=1000 and 0<args.rate<=160 and 0<args.load_seconds<=20):
        parser.error('bounded exploration: records 640..980, dimension 16..256, rounds 1..10, rate <=160, load <=20s')
    if not args.series.replace('-','').replace('_','').isalnum(): parser.error('series must be a simple directory name')
    if not 1024<=args.base_port<=55000: parser.error('base port 1024..55000 required')
    args.report_root=ROOT/'reports/capacity-explore'/args.series
    args.state_root=ROOT/'.state/capacity-explore'/args.series
    args.report_root.mkdir(parents=True,exist_ok=True)
    preflight=host_sample(ROOT)
    save(args.report_root/('preflight-'+datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%H%M%S')+'.json'),preflight)
    reason=limit_reason(preflight)
    if reason: raise RuntimeError('preflight refused: '+reason)
    data=ClusteredData(args.dimension,args.seed)
    args.centers=data.centers; args.vector_factory=data.vector; args.oracle_capacity=args.records+args.rounds
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
