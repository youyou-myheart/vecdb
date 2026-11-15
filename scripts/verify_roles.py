"""Two Masters + two Workers + one Gateway using the same executable."""
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import uuid

import verify_cluster as transport

ROOT=Path(__file__).resolve().parent.parent
NODES={'master-a':('master',18100),'master-b':('master',18101),'worker-a':('worker',18102),'worker-b':('worker',18103),'gateway':('gateway',18104)}
transport.URLS={node:f'http://127.0.0.1:{port}' for node,(_,port) in NODES.items()}
call=transport.call

def main():
    report_path=ROOT/'reports/roles-http.json';report_path.parent.mkdir(exist_ok=True)
    report={'status':'running','scope':'five OS processes on one host; real TiKV/PD/SeaweedFS; no cross-host partition or storage-quorum failure test'}
    report_path.write_text(json.dumps(report));namespace='roles-test/'+uuid.uuid4().hex
    prefix='vector-v0/'+namespace.encode().hex()+'/'
    processes={};paused=set();logs=[];checks=[];timings=[]
    base=dict(os.environ,API_TOKEN=transport.TOKEN,CLUSTER_TOKEN=transport.INTERNAL,CLUSTER_MODE='1',PARTITIONS='4',NAMESPACE=namespace,DIMENSION='16',LEASE_TTL='6s')
    for _,port in NODES.values():
        with socket.socket() as s:s.bind(('127.0.0.1',port))
    def launch(node):
        role,port=NODES[node];log=(ROOT/'reports'/f'roles-{node}.log').open('a');logs.append(log)
        proc=subprocess.Popen([str(ROOT/'.tools/vector-server')],cwd=ROOT,env=dict(base,NODE_ID=node,ROLES=role,ADVERTISE_URL=transport.URLS[node],LISTEN_ADDR=f'127.0.0.1:{port}'),stdout=log,stderr=log)
        processes[node]=proc
        for _ in range(150):
            if proc.poll() is not None:raise RuntimeError('startup failed '+node)
            try:
                assert call(node,'GET','/healthz')['namespace']==namespace
                return proc
            except OSError:time.sleep(.1)
        raise TimeoutError('startup '+node)
    def status():return call('gateway','GET','/v1/cluster')
    def wait_for(predicate,description,timeout=25):
        start=time.monotonic();last=None
        while time.monotonic()-start<timeout:
            last=status()
            if predicate(last):return last,time.monotonic()-start
            time.sleep(.1)
        raise AssertionError((description,last))
    def preferred(index):return max(('worker-a','worker-b'),key=lambda n:hashlib.sha256((index+'\0'+n).encode()).hexdigest())
    def balanced(s):return s['ready'] and s['master_available'] and all(o['node']==preferred(i) for i,o in s['owners'].items())
    rows={}
    def verify():
        got=call('gateway','POST','/v1/search',{'vector':[0.]*16,'k':1000,'exact':True})['hits']
        want=sorted(rows,key=lambda id:(sum(x*x for x in rows[id]['vector']),id))
        assert [h['id'] for h in got]==want
        for h in got:assert h['revision']==rows[h['id']]['revision'] and h['payload']==rows[h['id']]['payload']
    try:
        for node in NODES:launch(node)
        initial,_=wait_for(balanced,'initial placement')
        leader=initial['leader']['node'];standby='master-b' if leader=='master-a' else 'master-a';epoch=initial['leader']['epoch']
        for node,(role,_) in NODES.items():
            s=call(node,'GET','/v1/cluster');assert s['node']['roles']==[role]
            if role!='worker':assert s['local_partitions']==[]
        call('worker-a','POST','/v1/search',{'vector':[0.]*16,'k':1},expected=403)
        call('master-a','PUT','/v1/records/forbidden',{'request_id':'forbidden','vector':[0.]*16},expected=403)
        checks.append('same_binary_role_isolation_master_worker_gateway')
        for j in range(800):
            id=str(j);v=[math.sin((j%8+1)*(k+1)*.13)+.02*math.cos((j+1)*(k+1)) for k in range(16)]
            rows[id]={'vector':v,'payload':{'group':str(j%4)},'revision':1}
            token=call('gateway','PUT','/v1/records/'+id,dict(request_id='create-'+id,vector=v,payload=rows[id]['payload']))['token']
        call('gateway','POST','/v1/search',{'vector':[0.]*16,'k':10,'min_visible':token});verify()
        manifest=call('gateway','POST','/v1/checkpoint',{});assert len(manifest['partitions'])==4
        call('gateway','POST','/v1/log/prune',{});checks.append('gateway_remote_query_checkpoint_prune')
        before_owners={i:o['epoch'] for i,o in initial['owners'].items()}
        os.kill(processes[leader].pid,signal.SIGSTOP);paused.add(leader)
        # Queries continue while waiting for standby election; no request goes through Master.
        began=time.monotonic();queries=0
        while True:
            verify();queries+=1;s=status()
            if s['master_available'] and s['leader']['node']==standby and s['leader']['epoch']>epoch:break
            if time.monotonic()-began>20:raise AssertionError('master takeover timeout')
            time.sleep(.2)
        timings.append({'event':'master_SIGSTOP_takeover','seconds':time.monotonic()-began})
        assert {i:o['epoch'] for i,o in s['owners'].items()}==before_owners
        checks.append('standby_master_elected_without_query_or_worker_lease_interruption')
        os.kill(processes[leader].pid,signal.SIGCONT);paused.remove(leader);time.sleep(2.2)
        s=status();assert s['leader']['node']==standby
        assert all(a['leader_epoch']==s['leader']['epoch'] for a in s['assignments'].values())
        checks.append('resumed_old_master_remains_standby')
        # Pause every Master beyond TTL. Existing assignments and Worker leases still serve.
        for node in ('master-a','master-b'):
            os.kill(processes[node].pid,signal.SIGSTOP);paused.add(node)
        began=time.monotonic()
        while time.monotonic()-began<7.2:verify();time.sleep(.3)
        s=status();assert not s['master_available'] and s['ready'];checks.append('all_masters_unavailable_workers_continue_renewing_and_serving')
        for node in ('master-a','master-b'):
            os.kill(processes[node].pid,signal.SIGCONT);paused.remove(node)
        s,_=wait_for(balanced,'master election after pause')
        a_owned={i:o for i,o in s['owners'].items() if o['node']=='worker-a'};assert a_owned
        ids=[id for id in rows if f"index-{int.from_bytes(hashlib.sha256(id.encode()).digest()[:8],'big')%4:04d}" in a_owned]
        updated,deleted=ids[:2];rows[updated]={'vector':[0.]*16,'payload':{'group':'moved'},'revision':2}
        call('gateway','PUT','/v1/records/'+updated,dict(request_id='tail-update',vector=rows[updated]['vector'],payload=rows[updated]['payload']))
        call('gateway','DELETE','/v1/records/'+deleted,{'request_id':'tail-delete'});del rows[deleted]
        os.kill(processes['worker-a'].pid,signal.SIGSTOP);paused.add('worker-a')
        s,elapsed=wait_for(lambda s:s['ready'] and all(o['node']=='worker-b' for o in s['owners'].values()),'worker takeover')
        timings.append({'event':'worker_SIGSTOP_takeover','seconds':elapsed});verify()
        for i,old in a_owned.items():assert s['owners'][i]['epoch']>old['epoch']
        checks.append('master_reassigns_expired_worker_and_recovers_snapshot_plus_tail')
        os.kill(processes['worker-a'].pid,signal.SIGCONT);paused.remove('worker-a')
        for i,old in a_owned.items():
            for operation in ('search','checkpoint','prune'):
                reply=call('worker-a','POST','/internal/partition',{'namespace':prefix,'partition':i,'epoch':old['epoch'],'operation':operation,'query':{'vector':[0.]*16,'k':1}},expected=503,internal=True)
                assert reply['code']=='fenced'
        checks.append('old_worker_epoch_rejected_after_resume')
        s,_=wait_for(balanced,'worker rejoin');verify()
        processes['worker-a'].kill();assert processes['worker-a'].wait(timeout=10)==-signal.SIGKILL
        _,elapsed=wait_for(lambda s:s['ready'] and all(o['node']=='worker-b' for o in s['owners'].values()),'worker SIGKILL takeover');timings.append({'event':'worker_SIGKILL_takeover','seconds':elapsed});verify()
        launch('worker-a');final,_=wait_for(balanced,'worker restart');verify();checks.append('worker_SIGKILL_restart_and_reassignment')
        # A combined-role node is also supported and participates in both planes.
        report.update(status='passed',namespace=namespace,checks=checks,live_records=len(rows),queries_during_master_failover=queries,failover_timings=timings,initial_leader=initial['leader'],final_leader=final['leader'],final_owners=final['owners'])
    except BaseException as e:
        report.update(status='failed',namespace=namespace,error=repr(e),checks=checks);raise
    finally:
        for node,p in processes.items():
            if p.poll() is None:
                if node in paused:os.kill(p.pid,signal.SIGCONT)
                p.terminate()
        for p in processes.values():
            if p.poll() is None:
                try:p.wait(timeout=10)
                except subprocess.TimeoutExpired:p.kill();p.wait()
        for log in logs:log.close()
        report_path.write_text(json.dumps(report,indent=2))
    print(json.dumps(report,indent=2))

if __name__=='__main__':main()
