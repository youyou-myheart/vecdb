"""Leased reference retention and checkpoint recovery on a real local cluster."""
import json
import os
import signal
import sys
import time
import uuid
import boto3
from botocore.exceptions import ClientError
from cluster_local import LocalCluster, ROOT
sys.path.insert(0,str(ROOT/'sdk/python'))
from vectorstore import Client
import grpc


def main():
    env=dict(os.environ,API_TOKEN='retention-public-token',CLUSTER_TOKEN='retention-private-token',
             INDEX_REPLICATION='graph',CONSUMER_RETENTION='1',LEASE_TTL='9s')
    c=LocalCluster('retention/'+uuid.uuid4().hex,partitions=1,dimension=2,replicas=2,
                   base_port=18940,grpc_port=18980,environment=env,directory=ROOT/'reports/retention-processes')
    report=dict(status='running',checks=[]);paused=None
    s3=boto3.client('s3',endpoint_url='http://'+env.get('S3_ENDPOINT','127.0.0.1:18333'),
        aws_access_key_id=env.get('S3_ACCESS_KEY','poc-access'),aws_secret_access_key=env.get('S3_SECRET_KEY','poc-secret-local-only'),region_name='us-east-1')
    bucket=env.get('S3_BUCKET','vector-index-poc')
    def wait(name,fn):
        end=time.monotonic()+60
        while time.monotonic()<end:
            value=fn()
            if value:return value
            time.sleep(.15)
        raise TimeoutError(name)
    def head():return c.status()['graph_heads']['index-0000']
    def ready_at(offset):
        state=c.status();owners=state['replica_owners']['index-0000']['owners']
        return len(owners)==2 and all(o.get('offset',0)>=offset for o in owners)
    def absent(key):
        try:s3.head_object(Bucket=bucket,Key=key)
        except ClientError as e:
            if e.response['ResponseMetadata']['HTTPStatusCode']==404:return True
            raise
        return False
    try:
        c.start(2);c.wait_ready()
        assert c.status()['consumer_retention']
        with Client('127.0.0.1:18980',env['API_TOKEN']) as client:
            # Protect offset zero before a checkpoint can make these events eligible for automatic GC.
            leases={};session=uuid.uuid4().hex
            for stream in ('data','graph'):
                result=client.consumer('index-0000',stream=stream,action='acquire',consumer='sink',session=session,ack=0,ttl_seconds=60)
                leases[stream]=result.leases['sink'].epoch
            for i in range(40):c.request('/v1/records/'+str(i),'PUT',dict(request_id='new-'+str(i),vector=[i%4,i/100]))
            wait('initial indexing',lambda:ready_at(40))
            c.request('/v1/checkpoint','POST',{})
            first=client.changes('index-0000',stream='graph',limit=1).graph[0]
            visible=head()['visible']['sequence']
            c.request('/v1/log/prune','POST',{})
            assert client.consumer('index-0000',stream='data').floor==0
            assert head()['floor']['sequence']==0
            assert first.payload.size > 0 and first.payload_data and not first.object.key
            delta = json.loads(first.payload_data)
            assert delta['format'] == 'hnsw-data-reference-v1'
            assert delta['delta']['records'] is None
            assert client.changes('index-0000',stream='graph',limit=1).graph[0].payload_data == first.payload_data
            report['checks'].append('active_graph_lease_pins_reference_payload_and_source_logs')
            for stream,ack in [('data',40),('graph',visible)]:
                client.consumer('index-0000',stream=stream,action='ack',consumer='sink',session=session,epoch=leases[stream],ack=ack)
            c.request('/v1/log/prune','POST',{})
            assert client.consumer('index-0000',stream='data').floor==40
            assert head()['floor']['sequence']==visible
            assert client.changes('index-0000',stream='graph',limit=1).reset_required
            assert not absent(head()['checkpoint']['key'])
            page=client.changes('index-0000',stream='graph',after=0)
            assert page.reset_required and page.next==0 and page.checkpoint_position.sequence==visible
            report['checks'].append('ack_advances_reference_GC_and_preserves_checkpoint')
            for stream in leases:
                client.consumer('index-0000',stream=stream,action='release',consumer='sink',session=session,epoch=leases[stream])
            try:client.consumer('index-0000',action='ack',consumer='sink',session=session,epoch=leases['data'],ack=40)
            except grpc.RpcError as e:assert e.code()==grpc.StatusCode.FAILED_PRECONDITION
            else:raise AssertionError('released session renewed')
            replacement=client.consumer('index-0000',action='acquire',consumer='sink',session=uuid.uuid4().hex,ack=40)
            assert replacement.leases['sink'].epoch>leases['data']
            report['checks'].append('released_session_fenced_and_new_session_gets_new_epoch')
            state=c.status();plan=state['replica_plans']['index-0000'];owners=state['replica_owners']['index-0000']['owners']
            leader=next(o for o in owners if o['partition']==plan['publisher'])
            follower=next(o for o in owners if o['partition']!=plan['publisher'])
            paused=follower['node'];os.kill(c.children[paused].pid,signal.SIGSTOP)
            for i in range(40,45):c.request('/v1/records/'+str(i),'PUT',dict(request_id='new-'+str(i),vector=[i%4,i/100]))
            wait('leader publishes while follower paused',lambda:head()['visible']['data_offset']>=45)
            c.request('/v1/checkpoint','POST',{});c.request('/v1/log/prune','POST',{})
            assert head()['floor']['sequence']>visible
            os.kill(c.children[paused].pid,signal.SIGCONT);paused=None
            wait('lagging follower snapshot recovery',lambda:ready_at(45))
            report['checks'].append('lagging_follower_recovers_after_graph_reference_log_GC')
            oldterm=head()['term'];killed=leader['node'];c.children[killed].kill();c.children[killed].wait(timeout=10);c.workers.remove(killed)
            c.request('/v1/records/tail','PUT',dict(request_id='tail',vector=[0,0]))
            wait('post-GC takeover',lambda:head()['term']>oldterm and head()['visible']['data_offset']>=46)
            report['checks'].append('recovered_follower_promotes_and_resumes_writes_after_GC')
        report['status']='passed'
    except BaseException as exc:
        report.update(status='failed',error=repr(exc));raise
    finally:
        if paused and c.children[paused].poll() is None:os.kill(c.children[paused].pid,signal.SIGCONT)
        c.close();report['all_children_stopped']=all(p.poll() is not None for p in c.children.values())
        (ROOT/'reports/retention-processes.json').write_text(json.dumps(report,indent=2))
    print(json.dumps(report,indent=2))

if __name__=='__main__':main()
