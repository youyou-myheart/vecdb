"""Repeat the <=1s published/applied AND real-query acceptance on native onebox."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import hashlib
import json
import os
import platform
import signal
import subprocess
import threading
import time

from onebox_e2e import Onebox, ROOT, percentile
from quality import save
from pipeline_probe import interval
from visibility_probe import probe


def hardware():
    memory = model = None
    try:
        memory = int(subprocess.check_output(['sysctl','-n','hw.memsize'],stderr=subprocess.DEVNULL)) if platform.system()=='Darwin' else os.sysconf('SC_PAGE_SIZE')*os.sysconf('SC_PHYS_PAGES')
    except (OSError,ValueError,subprocess.CalledProcessError): pass
    if platform.system()=='Darwin':
        try: model=subprocess.check_output(['sysctl','-n','machdep.cpu.brand_string'],stderr=subprocess.DEVNULL,text=True).strip()
        except (OSError,subprocess.CalledProcessError): pass
    return dict(os=platform.platform(),architecture=platform.machine(),cpu_model=model,logical_cpus=os.cpu_count(),memory_bytes=memory)


def summarize(rows):
    values = [r['both_seconds'] for r in rows if r.get('both_seconds') is not None]
    return dict(samples=len(rows),completed=len(values),passed=sum(r['within_target'] for r in rows),
        p50_seconds=percentile(values,.5),p95_seconds=percentile(values,.95),p99_seconds=percentile(values,.99),
        max_seconds=max(values) if values else None,
        note='Nearest-rank empirical percentiles; small sample tail estimates are not a production SLA.')


def publishers_ready(state):
    """Read/query replicas may be ready before Master selects a graph writer."""
    heads,plans=state.get('graph_heads',{}),state.get('replica_plans',{})
    if not heads or len(heads)!=len(state.get('routing',{}).get('centers',[])): return False
    for index,head in heads.items():
        leader=head.get('leader',{});publisher=plans.get(index,{}).get('publisher')
        if not publisher or leader.get('partition')!=publisher: return False
        owners=state.get('replica_owners',{}).get(index,{}).get('owners',[])
        if not any(o.get('ready') and all(o.get(k)==leader.get(k) for k in ('partition','epoch','node','session')) for o in owners): return False
    return True


def run(fixture,args):
    fixture.report.update(kind='one-second-visibility',hardware=hardware(),
        binary_sha256=hashlib.sha256((ROOT/'.tools/vector-server').read_bytes()).hexdigest(),
        target_seconds=args.target_seconds,freshness_rounds=args.freshness_rounds,
        workload=dict(initial_records=640,final_records=640+args.freshness_rounds,dimension=16,
            writer_threads=4,reader_threads=2,writer_pause_seconds=.2,reader_pause_seconds=.4,
            per_round_load_seconds=args.load_seconds,tail_batch=['insert','update','delete'],
            initial_condition='storage data path, source owners, replicas and graph publishers ready; no business sync/warmup writes',
            start_policy='last durable mutation ACK after all writers have settled; join/GET and probe setup overhead remain inside the clock'))
    with fixture.case('boot_all_native_processes') as row:
        fixture.storage.start()
        for slot in (0,1,2,35,3,4,5): fixture.launch(slot)
        fixture.wait('all replicas ready',lambda:fixture.state().get('fully_replicated',False))
        fixture.wait('all source dispatchers ready',lambda:all(d.get('owner_available') for d in fixture.state()['data_progress'].values()))
        row['graph_publishers_ready_seconds']=fixture.wait('all graph publishers ready',lambda:publishers_ready(fixture.state()))
    fixture.watcher=threading.Thread(target=fixture.sample);fixture.watcher.start()
    results=[]

    def measure(name,baseline,stop_signal,metrics_before):
        fixture.measurements.phase=name
        expected=fixture.oracle.snapshot()
        acks=dict(fixture.oracle.acknowledged)
        t0=max(a['monotonic'] for a in acks.values())
        row=dict(name=name,status='running',started_at=datetime.now().astimezone().isoformat())
        fixture.report['cases'].append(row);fixture.persist()
        save(fixture.logs/(name+'-targets.json'),dict(records=expected,
            acknowledgements={id:dict(a,seconds_from_t0=a['monotonic']-t0) for id,a in acks.items()}))
        row.update(probe(fixture,baseline,expected,t0,name=name,target_seconds=args.target_seconds))
        metrics_after=fixture.local_metrics()
        save(fixture.logs/(name+'-metrics-before.json'),metrics_before)
        save(fixture.logs/(name+'-metrics-after.json'),metrics_after)
        row.update(stop_signal_to_last_ack_seconds=t0-stop_signal,
            pipeline_stages=interval(metrics_before,metrics_after),
            status='passed' if row['within_target'] else 'failed')
        results.append(row);fixture.persist()
        print(name,json.dumps({k:row[k] for k in ('status','positions_seconds','queries_seconds','both_seconds','initial_pending_events')}),flush=True)
        if not row['correct']: raise AssertionError('visibility did not converge: '+name)
        # Full record/HTTP/gRPC/ANN/receipt oracle remains outside the timed probe.
        fixture.verify()

    baseline=fixture.state();metrics_before=fixture.local_metrics();fixture.measurements.phase='initial_bulk_writes'
    stop_signal=time.monotonic()
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda n:fixture.oracle.mutate(n,initial=True),range(640)))
    measure('initial_bulk_visibility',baseline,stop_signal,metrics_before)
    for n in range(args.freshness_rounds):
        baseline=fixture.state();metrics_before=fixture.local_metrics();fixture.measurements.phase=f'steady_round_{n+1}_writes'
        started=time.monotonic();fixture.start_workload();fixture.hold(args.load_seconds)
        stop_signal=time.monotonic();fixture.stop_workload()
        # Every round ends with acknowledged insert/update/delete witnesses.
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures=[pool.submit(fixture.oracle.mutate,640+n,initial=True),
                     pool.submit(fixture.oracle.mutate,500+n%12,deleted=False),
                     pool.submit(fixture.oracle.mutate,600+n,deleted=True)]
            for future in futures: future.result()
        load_and_settle=time.monotonic()-started
        measure(f'steady_stop_visibility_{n+1:02d}',baseline,stop_signal,metrics_before)
        results[-1]['load_and_settle_seconds']=load_and_settle
        phase=fixture.measurements.summary()[f'steady_round_{n+1}_writes']
        results[-1]['write_attempt_failures']=phase.get('attempt_failure',{}).get('count',0)
        results[-1]['mutations_per_second']=results[-1]['source_targets']['events']/load_and_settle
        fixture.persist()
    fixture.report['freshness_summary']=dict(initial_bulk=summarize(results[:1]),steady=summarize(results[1:]))
    fixture.persist()
    assert all(row['within_target'] for row in results), fixture.report['freshness_summary']
    assert not any(row.get('write_attempt_failures') for row in results), 'steady business request failures'
    fixture.finish()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--freshness-rounds',type=int,default=20)
    parser.add_argument('--load-seconds',type=float,default=5)
    parser.add_argument('--target-seconds',type=float,default=1)
    parser.add_argument('--base-port',type=int,default=25400)
    args=parser.parse_args()
    if not 1<=args.freshness_rounds<=32 or args.load_seconds<=0 or args.target_seconds<=0:
        parser.error('rounds 1..32 and positive load/target durations required')
    args.steady_seconds=args.load_seconds;args.recovery_seconds=1;args.rounds=6;args.latency_only=True;args.keep_running=False
    fixture=None
    def interrupted(signum,frame): raise KeyboardInterrupt
    signal.signal(signal.SIGTERM,interrupted)
    try:
        fixture=Onebox(args);run(fixture,args)
    except BaseException as exc:
        if fixture: fixture.report.update(status='failed',error=repr(exc));fixture.persist()
        raise
    finally:
        signal.signal(signal.SIGTERM,signal.SIG_IGN);signal.signal(signal.SIGINT,signal.SIG_IGN)
        if fixture: fixture.close()


if __name__=='__main__': main()
