"""Continuous exact queries while replica Workers fail, rejoin and drain."""
import json
import os
import signal
import threading
import time
import uuid
from pathlib import Path
from cluster_local import LocalCluster, ROOT

def main():
    env=dict(os.environ,API_TOKEN='replica-public-token',CLUSTER_TOKEN='replica-private-token',LEASE_TTL='6s')
    c=LocalCluster('replicas-test/'+uuid.uuid4().hex,partitions=4,dimension=3,base_port=18600,replicas=2,environment=env,directory=ROOT/'reports/replica-processes')
    report={'status':'running','checks':[]};errors=[];stop=threading.Event();thread=None;paused=None;queries=[]; writer=None; latest=[None]; written=[]
    def check():
        q={'vector':[0,0,0],'k':1000,'exact':True}
        token=latest[0]
        if token:q['min_visible']=token
        got=c.request('/v1/search','POST',q)['hits']
        if token:assert got[0]['revision']>=token['revision']
        assert [r['id'] for r in got]==[str(i) for i in sorted(range(640),key=lambda i:(i*i+(i%5)**2,str(i)))]
    def query_loop():
        while not stop.is_set():
            try:
                start=time.monotonic();check();queries.append(time.monotonic()-start)
            except Exception as exc: errors.append(repr(exc))
            stop.wait(.05)
    def writer_loop():
        sequence=0
        while not stop.is_set():
            try:
                sequence+=1
                token=c.request('/v1/records/0','PUT',{'request_id':'concurrent-'+str(sequence),'vector':[0,0,0]})['token']
                latest[0]=token;written.append(token['revision'])
            except Exception as exc:errors.append('write: '+repr(exc))
            stop.wait(.3)
    try:
        c.start(2);c.wait_ready()
        for i in range(640):
            token=c.request('/v1/records/'+str(i),'PUT',{'request_id':'create-'+str(i),'vector':[i,i%5,0]})['token']
        c.request('/v1/search','POST',{'vector':[0,0,0],'k':1,'min_visible':token})
        # Wait for both independently consuming replicas, then checkpoint.
        time.sleep(3);check();c.request('/v1/checkpoint','POST',{});c.request('/v1/log/prune','POST',{})
        thread=threading.Thread(target=query_loop);thread.start()
        writer=threading.Thread(target=writer_loop);writer.start()
        paused=c.workers[0];os.kill(c.children[paused].pid,signal.SIGSTOP)
        time.sleep(9)
        assert c.status()['ready'];report['checks'].append('SIGSTOP_single_worker_queries_continue')
        os.kill(c.children[paused].pid,signal.SIGCONT);paused=None;c.wait_ready()
        c.scale(3);c.wait_ready();report['checks'].append('scale_out_build_before_retire')
        c.scale(2);c.wait_ready();report['checks'].append('drain_before_scale_in_no_missing_partition')
        killed=c.workers[0];c.children[killed].kill();c.children[killed].wait(timeout=10)
        time.sleep(9);assert c.status()['ready'];report['checks'].append('SIGKILL_single_worker_queries_continue')
        c.workers.remove(killed)
        # A different available slot avoids the surviving Worker's listener.
        new=c.launch('worker',7);c.workers.append(new);c.wait_ready()
        check();report['checks'].append('replacement_restores_snapshot_and_log')
        stop.set();writer.join(timeout=40);thread.join(timeout=40);assert not thread.is_alive();assert not errors,errors
        # Verify current versions are visible even if Gateway changes replicas.
        token=c.request('/v1/records/0','PUT',{'request_id':'updated','vector':[0,0,0],'payload':{'version':'new'}})['token']
        got=c.request('/v1/search','POST',{'vector':[0,0,0],'k':1,'min_visible':token})['hits'][0]
        assert got['revision']==token['revision'] and got['payload']['version']=='new'
        report['checks'].append('visibility_token_checked_on_selected_replica')
        report.update(status='passed',successful_queries=len(queries),concurrent_writes=len(written),max_query_seconds=max(queries),errors=errors)
    except BaseException as exc:
        report.update(status='failed',error=repr(exc),errors=errors);raise
    finally:
        stop.set()
        if paused and c.children[paused].poll() is None:os.kill(c.children[paused].pid,signal.SIGCONT)
        if writer:writer.join(timeout=40)
        if thread:thread.join(timeout=40)
        c.close()
        report['all_children_stopped']=all(p.poll() is not None for p in c.children.values())
        (ROOT/'reports/replicas-http.json').write_text(json.dumps(report,indent=2))
    print(json.dumps(report,indent=2))
if __name__=='__main__':main()
