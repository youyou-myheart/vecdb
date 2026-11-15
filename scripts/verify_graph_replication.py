"""Real process failures; distinct host labels on ONE physical test machine."""
import hashlib
import json
import os
import signal
import time
import urllib.request
import uuid
from cluster_local import LocalCluster, ROOT


def main():
    env = dict(os.environ, API_TOKEN='graph-public-test-token',
               CLUSTER_TOKEN='graph-private-test-token', LEASE_TTL='9s',
               INDEX_REPLICATION='graph', REPLICA_FAILURE_DOMAIN='host')
    c = LocalCluster('graph-replication/' + uuid.uuid4().hex, partitions=4,
                     dimension=8, replicas=2, base_port=18740, environment=env,
                     directory=ROOT / 'reports/graph-processes')
    report = dict(status='running', physical_hosts=1, simulated_failure_domains=True, checks=[])
    paused = None
    def wait(name, predicate, timeout=60):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = predicate()
            if result:
                return result
            time.sleep(.1)
        raise TimeoutError(name)
    def graph_head():
        return c.status().get('graph_heads', {}).get('index-0000', {})
    def caught_up():
        s = c.status()
        h = s.get('graph_heads', {}).get('index-0000', {}).get('visible')
        owners = s.get('replica_owners', {}).get('index-0000', {}).get('owners', [])
        return len(owners) == 2 and h and all(s.get('graph_positions', {}).get(o['partition']) == h for o in owners)
    def query_owner(owner):
        req = urllib.request.Request(owner['url'] + '/internal/partition', method='POST',
            headers={'Authorization':'Bearer ' + env['CLUSTER_TOKEN'], 'Content-Type':'application/json'},
            data=json.dumps(dict(namespace='vector-v0/' + c.namespace.encode().hex() + '/',
                partition=owner['partition'], epoch=owner['epoch'], operation='search',
                query=dict(vector=[0.0]*8, k=20, ef_search=256))).encode())
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.load(r)
    ids = []
    n = 0
    while len(ids) < 320:
        key = 'clustered-' + str(n)
        if int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], 'big') % 4 == 0:
            ids.append(key)
        n += 1
    def put(i):
        # Eight clusters with a small deterministic within-cluster drift.
        v = [(i % 8)*10 + ((i // 8)*(k+3) % 31)/100 for k in range(8)]
        return c.request('/v1/records/' + ids[i], 'PUT', dict(request_id='write-'+str(i), vector=v))
    try:
        c.launch('master',0);c.launch('master',1);c.launch('gateway',2)
        for slot, host in [(3,'host-a'),(4,'host-b')]:
            c.env['HOST_ID'] = host
            c.workers.append(c.launch('worker',slot))
        c.wait_ready()
        for i in range(300): put(i)
        wait('initial graph replicas', caught_up)
        s = c.status();plan = s['replica_plans']['index-0000']
        owners = s['replica_owners']['index-0000']['owners']
        leader = next(o for o in owners if o['partition']==plan['publisher'])
        follower = next(o for o in owners if o['partition']!=plan['publisher'])
        a,b = query_owner(leader),query_owner(follower)
        assert a['graph_position']==b['graph_position'] and a['hits']==b['hits']
        report['checks'].append('same_version_same_ANN_results_on_structured_data')
        paused = follower['node'];os.kill(c.children[paused].pid,signal.SIGSTOP)
        before = graph_head()['visible']['data_offset']
        for i in range(300,310): put(i)
        wait('leader publishes without follower', lambda: graph_head()['visible']['data_offset'] >= before+10, timeout=5)
        report['checks'].append('paused_follower_does_not_block_leader_publication')
        os.kill(c.children[paused].pid,signal.SIGCONT);paused=None
        wait('follower replay', caught_up)
        old_term = graph_head()['term']
        killed = leader['node'];c.children[killed].kill();c.children[killed].wait(timeout=10);c.workers.remove(killed)
        # Raw storage access does not depend on graph leadership.
        assert c.request('/v1/records/'+ids[0])['revision']==1
        put(310)
        assert c.request('/v1/records/'+ids[310])['revision']==1
        stale = query_owner(follower)
        assert stale['hits']
        report['checks'].append('raw_reads_writes_and_surviving_snapshot_queries_during_takeover')
        wait('promoted graph leader',lambda: graph_head()['term']>old_term and graph_head()['visible']['data_offset']>=311)
        report['checks'].append('SIGKILL_leader_epoch_takeover_and_data_log_resume')
        c.env['HOST_ID']='host-c';c.workers.append(c.launch('worker',5));c.wait_ready()
        wait('replacement replay', caught_up)
        c.request('/v1/checkpoint','POST',{})
        c.request('/v1/rebuild','POST',{})
        wait('leader_only_rebuild_replicated',caught_up)
        c.request('/v1/log/prune','POST',{})
        s=c.status();owners=s['replica_owners']['index-0000']['owners']
        a,b=map(query_owner,owners)
        assert a['graph_position']==b['graph_position'] and a['hits']==b['hits']
        report['checks'].append('replacement_checkpoint_rebuild_prune_and_replica_equivalence')
        report['status']='passed';report['final_graph_head']=graph_head()
    except BaseException as exc:
        report.update(status='failed',error=repr(exc));raise
    finally:
        if paused and c.children[paused].poll() is None:
            os.kill(c.children[paused].pid,signal.SIGCONT)
        c.close()
        report['all_children_stopped']=all(p.poll() is not None for p in c.children.values())
        (ROOT/'reports/graph-processes.json').write_text(json.dumps(report,indent=2))
    print(json.dumps(report,indent=2))

if __name__=='__main__':main()
