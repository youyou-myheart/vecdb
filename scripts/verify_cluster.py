"""Two combined-role processes: elected Master, RPC fan-out and worker recovery."""
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid

ROOT=Path(__file__).resolve().parent.parent
TOKEN='cluster-public-test-token'
INTERNAL='cluster-internal-test-secret'
URLS={'node-a':'http://127.0.0.1:18094','node-b':'http://127.0.0.1:18095'}

def call(node,method,path,data=None,expected=200,internal=False):
    req=urllib.request.Request(URLS[node]+path,method=method,headers={'Authorization':'Bearer '+(INTERNAL if internal else TOKEN),'Content-Type':'application/json'},data=None if data is None else json.dumps(data).encode())
    try:
        with urllib.request.urlopen(req,timeout=35) as r: status,raw=r.status,r.read()
    except urllib.error.HTTPError as e: status,raw=e.code,e.read()
    result=json.loads(raw) if raw else {}
    assert status==expected,(node,path,status,result)
    return result

def winner(index):
    return max(URLS,key=lambda node:hashlib.sha256((index+'\0'+node).encode()).hexdigest())

def main():
    root=ROOT/'reports';root.mkdir(exist_ok=True);path=root/'cluster-http.json'
    report={'status':'running','scope':'two OS processes, one TiKV/PD/SeaweedFS deployment; not a quorum or cross-host network partition test'}
    path.write_text(json.dumps(report,indent=2));namespace='cluster-test/'+uuid.uuid4().hex
    prefix='vector-v0/'+namespace.encode().hex()+'/'
    processes={};logs=[];paused=set();checks=[];timings=[]
    for port in (18094,18095):
        with socket.socket() as probe:probe.bind(('127.0.0.1',port))
    base=dict(os.environ,API_TOKEN=TOKEN,CLUSTER_TOKEN=INTERNAL,CLUSTER_MODE='1',PARTITIONS='4',NAMESPACE=namespace,DIMENSION='16',LEASE_TTL='6s')
    def launch(node):
        log=(root/(node+'-cluster.log')).open('a');logs.append(log)
        p=subprocess.Popen([str(ROOT/'.tools/vector-server')],cwd=ROOT,env=dict(base,NODE_ID=node,ADVERTISE_URL=URLS[node],LISTEN_ADDR=URLS[node].split('//')[1]),stdout=log,stderr=log)
        processes[node]=p
        for _ in range(100):
            if p.poll() is not None:raise RuntimeError('startup failed: '+node)
            try:
                assert call(node,'GET','/healthz')['namespace']==namespace
                return p
            except (OSError,urllib.error.URLError):time.sleep(.1)
        raise TimeoutError('startup '+node)
    def wait_owners(node,single=None):
        start=time.monotonic();last=None
        for _ in range(200):
            last=call(node,'GET','/v1/cluster')
            owners=last['owners']
            if last['ready'] and len(owners)==4 and all(o['node']==(single or winner(i)) for i,o in owners.items()):
                return last,time.monotonic()-start
            time.sleep(.1)
        raise AssertionError(('owners not ready',last))
    def data_partition(id):return int.from_bytes(hashlib.sha256(id.encode()).digest()[:8],'big')%4
    rows={}
    def verify(node):
        q={'vector':[0.]*16,'k':1000,'exact':True}
        got=call(node,'POST','/v1/search',q)['hits']
        want=sorted(rows,key=lambda id:(sum(x*x for x in rows[id]['vector']),id))
        assert [h['id'] for h in got]==want
        for h in got:
            r=rows[h['id']]
            assert h['revision']==r['revision'] and h['payload']==r['payload']
            assert math.isclose(h['distance'],sum(x*x for x in r['vector']),rel_tol=1e-12,abs_tol=1e-12)
    try:
        launch('node-a');launch('node-b');initial,_=wait_owners('node-b')
        assert {o['node'] for o in initial['owners'].values()}==set(URLS)
        checks.append('two_process_rendezvous_placement_ready_after_restore')
        for j in range(800):
            id=str(j);v=[math.sin((j%8+1)*(k+1)*.13)+.02*math.cos((j+1)*(k+1)) for k in range(16)]
            rows[id]={'vector':v,'payload':{'group':str(j%4)},'revision':1}
            token=call('node-a' if j%2 else 'node-b','PUT','/v1/records/'+id,dict(request_id='create-'+id,vector=v,payload=rows[id]['payload']))['token']
        call('node-b','POST','/v1/search',{'vector':[0.]*16,'k':10,'min_visible':token})
        verify('node-a');verify('node-b');checks.append('both_gateways_write_and_remote_global_exact_topk')
        snapshot=call('node-b','POST','/v1/checkpoint',{});assert len(snapshot['partitions'])==4
        call('node-b','POST','/v1/log/prune',{});checks.append('remote_checkpoint_and_fenced_prune')
        owned_by_a={i:o for i,o in initial['owners'].items() if o['node']=='node-a'}
        candidates=[id for id in rows if f'index-{data_partition(id):04d}' in owned_by_a]
        updated,deleted=candidates[:2]
        rows[updated]={'vector':[0.]*16,'payload':{'group':'moved'},'revision':2}
        call('node-b','PUT','/v1/records/'+updated,dict(request_id='tail-update',vector=rows[updated]['vector'],payload=rows[updated]['payload']))
        call('node-b','DELETE','/v1/records/'+deleted,{'request_id':'tail-delete'});del rows[deleted]
        os.kill(processes['node-a'].pid,signal.SIGSTOP);paused.add('node-a')
        takeover,elapsed=wait_owners('node-b',single='node-b');timings.append({'event':'SIGSTOP_takeover','seconds':elapsed})
        for i,old in owned_by_a.items():assert takeover['owners'][i]['epoch']>old['epoch']
        verify('node-b');checks.append('paused_owner_lease_expiry_takeover_and_tail_replay')
        os.kill(processes['node-a'].pid,signal.SIGCONT);paused.remove('node-a')
        for i,old in owned_by_a.items():
            for operation in ('search','checkpoint','prune'):
                result=call('node-a','POST','/internal/partition',{'namespace':prefix,'partition':i,'epoch':old['epoch'],'operation':operation,'query':{'vector':[0.]*16,'k':1}},expected=503,internal=True)
                assert result['code']=='fenced'
        checks.append('resumed_old_epoch_cannot_query_publish_or_prune')
        balanced,_=wait_owners('node-b');verify('node-a');verify('node-b')
        prior_session=balanced['node']['session']
        processes['node-a'].kill();assert processes['node-a'].wait(timeout=10)==-signal.SIGKILL
        _,elapsed=wait_owners('node-b',single='node-b');timings.append({'event':'SIGKILL_takeover','seconds':elapsed})
        verify('node-b');checks.append('combined_role_master_and_worker_SIGKILL_takeover')
        launch('node-a');final,_=wait_owners('node-b');verify('node-a');verify('node-b')
        assert all(o['ready'] for o in final['owners'].values());checks.append('restart_rebalances_and_preserves_records')
        # Existing public credentials cannot call the peer endpoint.
        call('node-b','POST','/internal/partition',{},expected=401)
        # A single-process binary path must not bypass cluster ownership.
        bad=subprocess.Popen([str(ROOT/'.tools/vector-server')],cwd=ROOT,env=dict(base,CLUSTER_MODE='0',LISTEN_ADDR='127.0.0.1:18096'),stdout=logs[-1],stderr=logs[-1])
        try:assert bad.wait(timeout=10)!=0
        finally:
            if bad.poll() is None:bad.kill();bad.wait()
        checks.append('internal_auth_and_single_process_mode_fence')
        report.update(status='passed',namespace=namespace,checks=checks,failover_timings=timings,initial_owners=initial['owners'],final_owners=final['owners'],live_records=len(rows))
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
        for f in logs:f.close()
        path.write_text(json.dumps(report,indent=2))
    print(json.dumps(report,indent=2))

if __name__=='__main__':main()
