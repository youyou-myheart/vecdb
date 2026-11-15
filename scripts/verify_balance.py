"""32 fixed shards, two replicas; placement convergence and query continuity."""
import json
import os
import threading
import subprocess
import time
import uuid
from cluster_local import LocalCluster, ROOT

def main():
    env=dict(os.environ,API_TOKEN='balance-public-token',CLUSTER_TOKEN='balance-private-token',LEASE_TTL='12s',BALANCE_COOLDOWN='1s')
    c=LocalCluster('balance-test/'+uuid.uuid4().hex,partitions=32,dimension=3,base_port=18700,replicas=2,scheduler='load',environment=env,directory=ROOT/'reports/balance-processes')
    report={'status':'running','checks':[]};errors=[];stop=threading.Event();thread=None;queries=[];visible=[None]
    def counts(state):
        result={n:0 for n in c.workers}
        for plan in state['replica_plans'].values():
            assert len({a['node'] for a in plan['desired']})==2
            for a in plan['desired']:result[a['node']]=result.get(a['node'],0)+1
        return result
    def query():
        while not stop.is_set():
            try:
                began=time.monotonic()
                hits=c.request('/v1/search','POST',{'vector':[0,0,0],'k':500,'exact':True,'min_visible':visible[0]})['hits']
                assert [h['id'] for h in hits]==[str(i) for i in range(160)]
                queries.append(time.monotonic()-began)
            except Exception as e:errors.append(repr(e))
            stop.wait(.1)
    try:
        c.start(3);initial=c.wait_ready(timeout=120)
        initial_counts=counts(initial);assert max(initial_counts.values())-min(initial_counts.values())<=1,initial_counts
        for i in range(160):visible[0]=c.request('/v1/records/'+str(i),'PUT',{'request_id':'create-'+str(i),'vector':[i,0,0]})['token']
        c.request('/v1/checkpoint','POST',{});time.sleep(5)
        thread=threading.Thread(target=query);thread.start()
        began=time.monotonic();c.scale(4)
        max_migrations=0;deadline=time.monotonic()+360;last_signature=None;last_change=time.monotonic()
        while True:
            state=c.status();current=counts(state)
            migrations=sum({a['partition'] for a in p['desired']}!={a['partition'] for a in p['serving']} for p in state['replica_plans'].values())
            max_migrations=max(max_migrations,migrations)
            signature=tuple((i,tuple(sorted(a['partition'] for a in p['desired']))) for i,p in sorted(state['replica_plans'].items()))
            if signature!=last_signature:
                last_signature=signature;last_change=time.monotonic()
            assert migrations<=1,(migrations,current)
            if state['fully_replicated'] and migrations==0 and max(current.values()) <= min(current.values()) * 1.2 + 1 and time.monotonic()-last_change>=12:
                break
            if time.monotonic()>deadline:raise TimeoutError(current)
            time.sleep(.3)
        report.update(initial_counts=initial_counts,expanded_counts=current,convergence_seconds=time.monotonic()-began,max_concurrent_balance_moves=max_migrations)
        report['checks'].append('32_shards_64_replicas_three_to_four_workers_balanced')
        # Confirm telemetry is real and query traffic reached every Worker.
        time.sleep(5);state=c.status()
        loads={n['id']:n.get('load',{}) for n in state['nodes'] if 'worker' in n['roles']}
        assert all(any(s['estimated_bytes']>0 for s in loads[n].values()) for n in c.workers)
        assert all(any(s['busy']>0 for s in loads[n].values()) for n in c.workers)
        report['checks'].append('live_vector_size_and_query_time_telemetry')
        before={i:sorted(a['partition'] for a in p['desired']) for i,p in state['replica_plans'].items()}
        time.sleep(5);after=c.status()
        assert before=={i:sorted(a['partition'] for a in p['desired']) for i,p in after['replica_plans'].items()}
        report['checks'].append('balanced_cluster_does_not_churn')
        def ctl(*args):
            result=subprocess.run([str(ROOT/'.tools/vectorctl'),'--endpoint',c.gateway,'cluster',*args],env=env,cwd=ROOT,text=True,capture_output=True,timeout=30)
            assert result.returncode==0,(args,result.stderr)
            return json.loads(result.stdout)
        page=ctl('tasks','--limit','5')
        assert len(page['tasks'])==5 and page.get('next'),page
        task=ctl('task',page['tasks'][0]['id'])
        assert task['id']==page['tasks'][0]['id'] and task['stage'] in ('Completed','Superseded'),task
        explain=ctl('explain','index-0000')
        assert len(explain['partitions'])==1 and explain['partitions'][0]['version']>=1,explain
        preview=ctl('balance','--dry-run')
        assert len(preview['partitions'])==32 and not any(p['changed'] for p in preview['partitions']),preview
        tasks=c.request('/v1/cluster/tasks?limit=200')['tasks']
        assert all(t['stage'] in ('Completed','Superseded') for t in tasks),tasks
        report['checks'].append('task_pagination_explain_cli_dry_run_and_no_active_task_leaks')

        stop.set();thread.join(timeout=40);assert not errors,errors
        report.update(status='passed',successful_queries=len(queries),max_query_seconds=max(queries),errors=errors)
    except BaseException as e:
        report.update(status='failed',error=repr(e),errors=errors)
        if hasattr(e,'read'):
            report['response']=e.read().decode()
        raise
    finally:
        stop.set()
        if thread:thread.join(timeout=40)
        c.close();report['all_children_stopped']=all(p.poll() is not None for p in c.children.values())
        (ROOT/'reports/balance-http.json').write_text(json.dumps(report,indent=2))
    print(json.dumps(report,indent=2))
if __name__=='__main__':main()
