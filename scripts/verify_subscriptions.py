"""Pull subscriptions through HTTP, Python RPC and CLI against real backends."""
import hashlib
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
import boto3
from cluster_local import LocalCluster, ROOT
sys.path.insert(0,str(ROOT/'sdk/python'))
from vectorstore import Client


def main():
    env=dict(os.environ,API_TOKEN='subscription-public-token',CLUSTER_TOKEN='subscription-private-token',
             INDEX_REPLICATION='graph',LEASE_TTL='9s')
    c=LocalCluster('subscription/'+uuid.uuid4().hex,partitions=1,dimension=2,replicas=2,
                   base_port=18830,grpc_port=18870,environment=env,directory=ROOT/'reports/subscription-processes')
    report=dict(status='running',checks=[])
    s3=boto3.client('s3',endpoint_url='http://'+env.get('S3_ENDPOINT','127.0.0.1:18333'),
        aws_access_key_id=env.get('S3_ACCESS_KEY','poc-access'),aws_secret_access_key=env.get('S3_SECRET_KEY','poc-secret-local-only'),region_name='us-east-1')
    def wait(predicate):
        end=time.monotonic()+60
        while time.monotonic()<end:
            if predicate():return
            time.sleep(.2)
        raise TimeoutError('replica progress')
    try:
        c.start(2);c.wait_ready()
        for n in range(96):c.request('/v1/records/'+str(n),'PUT',dict(request_id='new-'+str(n),vector=[n%6,n/100]))
        c.request('/v1/records/0','PUT',dict(request_id='update-0',vector=[8,9]))
        token=c.request('/v1/records/1','DELETE',dict(request_id='delete-1'))['token']
        c.request('/v1/search','POST',dict(vector=[0,0],k=1,min_visible=token))
        with Client('127.0.0.1:18870',env['API_TOKEN']) as sdk:
            after=0;records={}
            while True:
                page=sdk.changes('index-0000',after=after,limit=7)
                assert not page.reset_required
                for event in page.data:
                    assert event.sequence==after+1
                    records[event.record.id]=event.record;after=event.sequence
                assert after==page.next
                if after==page.head:break
            assert after==98 and len(records)==96 and records['1'].deleted and records['0'].revision==2
            empty=sdk.changes('index-0000',after=after)
            assert not empty.data and empty.next==98
            report['checks'].append('rpc_data_pages_contiguous_update_delete_and_empty_tail')
            cursor=0;graph_records={};seen_links=0
            while True:
                page=c.request('/v1/changes','POST',dict(partition='index-0000',stream='graph',after=cursor,limit=1))
                for event in page['graph']:
                    assert event['position']['sequence']==cursor+1
                    m=event['object']
                    raw=s3.get_object(Bucket=env.get('S3_BUCKET','vector-index-poc'),Key=m['key'])['Body'].read()
                    assert len(raw)==m['size'] and hashlib.sha256(raw).hexdigest()==m['sha256']
                    delta=json.loads(raw)
                    for change in delta['records']:graph_records[change['after']['id']]=change['after']
                    for link in delta.get('links',[]):
                        assert link['label']>0 and len(link['layers'])>0
                        seen_links+=1
                    cursor=event['position']['sequence']
                if cursor==page['head']:break
            assert seen_links>=96 and graph_records['1']['deleted'] and graph_records['0']['revision']==2
            report['checks'].append('http_published_graph_tail_verified_objects_and_readable_adjacency')
            run=subprocess.run([str(ROOT/'.tools/vectorctl'),'--endpoint',c.gateway,'changes','--partition','index-0000','--stream','graph','--after',str(cursor)],env=env,text=True,capture_output=True,timeout=30)
            assert run.returncode==0,run.stderr
            assert json.loads(run.stdout)['next']==cursor
            report['checks'].append('vectorctl_resume_cursor')
            # Rebuild is a reset event, never silently presented as an edge delta.
            c.request('/v1/rebuild','POST',{})
            resets=sdk.changes('index-0000',stream='graph',after=cursor)
            assert any(e.kind=='reset' for e in resets.graph)
            cursor=resets.next
            wait(lambda: all(o['offset']>=98 for o in c.status()['replica_owners']['index-0000']['owners']))
            c.request('/v1/checkpoint','POST',{});c.request('/v1/log/prune','POST',{})
            old=sdk.changes('index-0000',stream='data',after=0)
            assert old.reset_required and old.floor>0 and old.next==0 and not old.data
            assert old.checkpoint.offset>=old.floor
            report['checks'].append('reset_event_and_explicit_compacted_data_cursor_with_checkpoint')
            # Subscriptions are storage-backed and remain available without Workers.
            c.scale(0)
            tail=sdk.changes('index-0000',stream='graph',after=cursor)
            assert tail.next==cursor and not tail.graph
            report['checks'].append('subscriptions_do_not_depend_on_worker_availability')
        report['status']='passed'
    except BaseException as exc:
        report.update(status='failed',error=repr(exc));raise
    finally:
        c.close();report['all_children_stopped']=all(p.poll() is not None for p in c.children.values())
        (ROOT/'reports/subscriptions.json').write_text(json.dumps(report,indent=2))
    print(json.dumps(report,indent=2))

if __name__=='__main__':main()
