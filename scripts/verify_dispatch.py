"""Clustered data dispatch against real TiKV/SeaweedFS and separate processes."""
import json
import os
import signal
import sys
import time
import uuid
from cluster_local import LocalCluster, ROOT
sys.path.insert(0,str(ROOT/'sdk/python'))
from vectorstore import Client, messages


def main():
    directory=ROOT/'reports/dispatch-processes';directory.mkdir(parents=True,exist_ok=True)
    route=directory/'routing.json';route.write_text(json.dumps(dict(version=1,centers=[[-10,0],[0,10],[10,0]])))
    env=dict(os.environ,API_TOKEN='dispatch-public-token',CLUSTER_TOKEN='dispatch-private-token',
             INDEX_REPLICATION='graph',CONSUMER_RETENTION='1',LEASE_TTL='6s',CLUSTER_ROUTING_FILE=str(route))
    c=LocalCluster('dispatch/'+uuid.uuid4().hex,partitions=4,dimension=2,replicas=2,
                   base_port=19040,grpc_port=19080,environment=env,directory=directory)
    report=dict(status='running',checks=[]);paused=[]
    def wait(name,fn):
        deadline=time.monotonic()+90
        while time.monotonic()<deadline:
            value=fn()
            if value:return value
            time.sleep(.2)
        raise TimeoutError(name+': '+json.dumps(c.status()))
    def caught():
        s=c.status()
        return len(s.get('data_progress',{}))==4 and all(d['completed_lsn']==d['head'] for d in s['data_progress'].values())
    def put(id,v,request=None):
        return c.request('/v1/records/'+id,'PUT',dict(request_id=request or uuid.uuid4().hex,vector=v))
    try:
        c.start(3);c.wait_ready()
        with Client('127.0.0.1:19080',env['API_TOKEN'],timeout=60) as client:
            layout=client.layout();assert layout.data_partitions==4 and len(layout.bindings)==3 and len(layout.routing.centers)==3
            assert len(client.cluster().data_progress)==4
            for node in c.workers:
                os.kill(c.children[node].pid,signal.SIGSTOP);paused.append(node)
            started=time.monotonic();put('offline',[-10,0]);assert time.monotonic()-started<3
            assert client.get('offline').revision==1
            assert not caught()
            for node in paused:os.kill(c.children[node].pid,signal.SIGCONT)
            paused=[]
            wait('dispatcher restart from durable data',caught)
            report['checks'].append('business_writes_return_without_index_or_dispatcher')
            for i in range(90):
                center=[[-10,0],[0,10],[10,0]][i%3]
                put(str(i),[center[0]+(i//3)*.001,center[1]])
            wait('many data sources into three clusters',caught)
            result=client.search([10,0],k=5,exact=True,min_visible=messages.Token(id='2',revision=1),timeout=60)
            assert result.hits[0].id=='2'
            assert all(h.id.isdigit() and int(h.id)%3==2 for h in result.hits)
            report['checks'].append('four_data_partitions_three_clustered_indexes')
            put('2',[-10,0]);put('2',[0,10]);put('2',[10,0])
            client.delete('3',request_id='delete-3')
            wait('cross-cluster updates and delete',caught)
            result=client.search([10,0],k=100,exact=True,timeout=60)
            assert len(result.hits)==90 and len({h.id for h in result.hits})==90
            assert all(h.id!='3' and (h.id!='2' or h.revision==4) for h in result.hits)
            report['checks'].append('repeated_updates_delete_and_authoritative_candidate_validation')
            lease=client.consumer('data-0000',consumer='sink',action='acquire',session=uuid.uuid4().hex,ttl_seconds=300)
            manifest=client.checkpoint(timeout=60);assert manifest.partitions['data-0000'].format=='data-records-v1'
            client.prune(timeout=60)
            assert client.consumer('data-0000').floor==0
            assert all(x.floor<=x.completed_lsn<=x.head for x in client.cluster().data_progress.values())
            page=client.changes('data-0001');assert page.reset_required and page.checkpoint.format=='data-records-v1'
            for i in range(3):assert client.changes(f'index-{i:04d}',stream='graph').reset_required
            report['checks'].append('separate_data_graph_subscriptions_leases_checkpoints_and_GC')
            state=c.status();victim=state['data_progress']['data-0000']['owner']['node']
            c.children[victim].kill();c.children[victim].wait(timeout=10);c.workers.remove(victim)
            put('after-failure',[10,0])
            wait('source and index ownership failover after GC',caught)
            result=client.search([10,0],k=100,exact=True,timeout=60)
            assert any(h.id=='after-failure' for h in result.hits)
            report['checks'].append('dispatcher_and_graph_leader_takeover_after_GC')
        report['status']='passed'
    except BaseException as exc:
        report.update(status='failed',error=repr(exc));raise
    finally:
        for node in paused:
            if c.children[node].poll() is None:os.kill(c.children[node].pid,signal.SIGCONT)
        c.close();report['all_children_stopped']=all(p.poll() is not None for p in c.children.values())
        (ROOT/'reports/dispatch-processes.json').write_text(json.dumps(report,indent=2))
    print(json.dumps(report,indent=2))

if __name__=='__main__':main()
