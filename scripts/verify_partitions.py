"""Four data partitions / four separately named index partitions on real backends."""
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
URL='http://127.0.0.1:18093'
TOKEN='partition-test-token'

def call(method,path,data=None,expected=200):
    request=urllib.request.Request(URL+path,method=method,headers={'Authorization':'Bearer '+TOKEN,'Content-Type':'application/json'},data=None if data is None else json.dumps(data).encode())
    try:
        with urllib.request.urlopen(request,timeout=35) as response:
            status,body=response.status,json.load(response)
    except urllib.error.HTTPError as e:
        status,body=e.code,json.load(e)
    assert status==expected,(status,body)
    return body

def main():
    path=ROOT/'reports/partitions-http.json'
    report={'status':'running'};path.write_text(json.dumps(report))
    namespace='partition-test/'+uuid.uuid4().hex
    env=dict(os.environ,API_TOKEN=TOKEN,NAMESPACE=namespace,DIMENSION='16',PARTITIONS='4',LISTEN_ADDR='127.0.0.1:18093')
    with socket.socket() as probe: probe.bind(('127.0.0.1',18093))
    proc=None
    with (ROOT/'reports/partitions-server.log').open('w') as log:
        def start(configuration=env):
            p=subprocess.Popen([str(ROOT/'.tools/vector-server')],cwd=ROOT,env=configuration,stdout=log,stderr=log)
            for _ in range(120):
                if p.poll() is not None: raise RuntimeError('startup failed; see partitions-server.log')
                try:
                    assert call('GET','/healthz')['namespace']==namespace
                    return p
                except (OSError,urllib.error.URLError): time.sleep(.1)
            p.kill();p.wait();raise TimeoutError('readiness')
        try:
            proc=start();layout=call('GET','/v1/layout')
            assert layout['data_partitions']==4 and layout['epoch']==1
            assert layout['bindings']==[{'data_partition':j,'index_partition':f'index-{j:04d}'} for j in range(4)]
            rows={};counts=[0]*4
            for j in range(800):
                id=str(j);vector=[math.sin((j%8+1)*(k+1)*.13)+.03*math.cos((j+1)*(k+1)) for k in range(16)]
                rows[id]={'vector':vector,'payload':{'group':str(j%4)},'revision':1}
                token=call('PUT','/v1/records/'+id,dict(request_id='create-'+id,vector=vector,payload=rows[id]['payload']))['token']
                counts[int.from_bytes(hashlib.sha256(id.encode()).digest()[:8],'big')%4]+=1
            query={'vector':[0.]*16,'k':20,'exact':True,'min_visible':token}
            def verify():
                for filter in (None,{'group':'1'},{'group':'absent'}):
                    q=dict(query)
                    if filter: q['filter']=filter
                    got=call('POST','/v1/search',q)['hits']
                    eligible=[(sum(x*x for x in r['vector']),id) for id,r in rows.items() if not filter or r['payload']['group']==filter['group']]
                    want=[id for _,id in sorted(eligible)[:20]]
                    assert [h['id'] for h in got]==want
                    for h in got: assert h['revision']==rows[h['id']]['revision'] and h['payload']==rows[h['id']]['payload']
            verify()
            checkpoint=call('POST','/v1/checkpoint',{})
            assert checkpoint['format']=='partition-set-v1' and len(checkpoint['partitions'])==4
            for j in range(4):
                m=checkpoint['partitions'][f'index-{j:04d}']
                assert m['offset']==counts[j] and f'/index/index-{j:04d}/snapshots/' in m['key']
            floors=call('POST','/v1/log/prune',{})['partitions']
            assert floors=={f'index-{j:04d}':counts[j] for j in range(4)}
            rows['0']={'vector':[0.]*16,'payload':{'group':'moved'},'revision':2}
            call('PUT','/v1/records/0',dict(request_id='tail-update',vector=rows['0']['vector'],payload=rows['0']['payload']))
            query['min_visible']=call('DELETE','/v1/records/1',{'request_id':'tail-delete'})['token'];del rows['1']
            other=next(str(j) for j in range(2,800) if int.from_bytes(hashlib.sha256(str(j).encode()).digest()[:8],'big')%4!=int.from_bytes(hashlib.sha256(b'0').digest()[:8],'big')%4)
            call('PUT','/v1/records/'+other,{'request_id':'tail-update','vector':[0.]*16},expected=409)
            proc.kill();assert proc.wait(timeout=10)==-signal.SIGKILL
            proc=start();assert call('GET','/v1/layout')==layout;verify()
            assert call('GET','/v1/records/1')['deleted']
            # All four local indexes have >128 live points: this also exercises ANN fan-out.
            ann=call('POST','/v1/search',{'vector':[0.]*16,'k':20,'ef_search':512})['hits']
            assert ann[0]['id']=='0' and len({h['id'] for h in ann})==20
            exact=call('POST','/v1/search',{'vector':[0.]*16,'k':20,'exact':True})['hits']
            recall=len({h['id'] for h in ann}&{h['id'] for h in exact})/20
            assert recall>=.95
            rebuilt=call('POST','/v1/rebuild',{});assert len(rebuilt['partitions'])==4
            proc.terminate();proc.wait(timeout=10);proc=None
            # A different partition count must fail before exposing a misleading view.
            bad=subprocess.Popen([str(ROOT/'.tools/vector-server')],cwd=ROOT,env=dict(env,PARTITIONS='8'),stdout=log,stderr=log)
            try: assert bad.wait(timeout=10)!=0
            finally:
                if bad.poll() is None: bad.kill();bad.wait()
            report.update(status='passed',namespace=namespace,layout=layout,records_per_partition=counts,recall_at_20=recall,
                checks=['persisted_two_layer_identity_mapping','independent_index_snapshot_paths_and_offsets','global_exact_topk_and_filters',
                        'namespace_wide_idempotency_across_hash_partitions','prune_then_SIGKILL_replay_update_delete',
                        'ANN_fanout_and_global_merge','partition_rebuild','changed_partition_count_rejected'],
                scope='four logical partitions in one process; real TiKV/SeaweedFS; no distributed ownership or cross-partition transaction snapshot')
        except BaseException as e:
            report.update(status='failed',error=repr(e));raise
        finally:
            if proc is not None and proc.poll() is None:
                proc.terminate()
                try: proc.wait(timeout=10)
                except subprocess.TimeoutExpired: proc.kill();proc.wait()
            path.write_text(json.dumps(report,indent=2))
    print(json.dumps(report,indent=2))

if __name__=='__main__':main()
