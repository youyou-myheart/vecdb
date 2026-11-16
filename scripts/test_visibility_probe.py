import copy
import io
import json
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from visibility_probe import add_targets, matches, positions_ready, probe
from onebox_freshness import summarize, publishers_ready


class VisibilityProbeTest(unittest.TestCase):
    def test_initial_write_readiness_requires_claimed_current_publisher(self):
        owner=dict(partition='a',node='w',session='s',epoch=2,ready=True)
        s=dict(routing=dict(centers=[[0,0]]),graph_heads={'i':dict(leader=owner)},
               replica_plans={'i':dict(publisher='a')},replica_owners={'i':dict(owners=[owner])})
        self.assertTrue(publishers_ready(s))
        for variant in ('missing','epoch','publisher'):
            state=copy.deepcopy(s)
            if variant=='missing':state['graph_heads']['i']['leader']={}
            if variant=='epoch':state['graph_heads']['i']['leader']=dict(owner,epoch=1)
            if variant=='publisher':state['replica_plans']['i']['publisher']='b'
            self.assertFalse(publishers_ready(state),variant)

    def state(self,sequence=4):
        owners=[dict(partition='a',epoch=1,ready=True,url='http://a'),dict(partition='b',epoch=2,ready=True,url='http://b')]
        pos=dict(sequence=sequence,data_offset=sequence,hash=str(sequence))
        return dict(ready=True,fully_replicated=True,routing=dict(centers=[[0,0]],version=1),
            graph_heads={'index-0000':dict(sources={'0':7},committed=pos,visible=pos)},
            replica_owners={'index-0000':dict(owners=owners)},graph_positions={},
            data_progress={'data-0000':dict(head=7,completed_lsn=0)})

    def local(self):
        return dict(worker=dict(applied={'a':4,'b':4},applied_epochs={'a':1,'b':2}))

    def test_targets_follow_sparse_routes_and_broadcast_deletes(self):
        targets={}
        add_targets(targets,2,[dict(sequence=3,record=dict(vector=[1,0])),
                              dict(sequence=5,record=dict(vector=[19,0])),
                              dict(sequence=8,record=dict(deleted=True))],[[0,0],[20,0]])
        self.assertEqual(targets,{'index-0000':{'2':8},'index-0001':{'2':8}})

    def test_positions_require_publish_actual_apply_and_current_epoch(self):
        state=self.state(); baseline=self.state(1); targets={'index-0000':{'0':7}}
        for change in ('source','publish','apply','epoch','missing_replica','missing_metrics'):
            s=copy.deepcopy(state);local=self.local()
            if change=='source': s['graph_heads']['index-0000']['sources']['0']=6
            if change=='publish': s['graph_heads']['index-0000']['visible']=dict(sequence=3)
            if change=='apply': local['worker']['applied']['b']=3
            if change=='epoch': local['worker']['applied_epochs']['b']=1
            if change=='missing_replica': s['replica_owners']['index-0000']['owners'].pop()
            if change=='missing_metrics': local={}
            self.assertFalse(positions_ready(s,local,targets,{},baseline,1,2),change)
        # Heartbeat graph_positions are absent and completed_lsn is still zero.
        self.assertTrue(positions_ready(state,self.local(),targets,{},baseline,1,2))

    def test_migration_requires_every_extra_serving_replica_to_apply(self):
        state=self.state();local=self.local()
        state['replica_owners']['index-0000']['owners'].append(dict(partition='c',epoch=3,ready=True,url='http://c'))
        targets={'index-0000':{'0':7}}
        self.assertFalse(positions_ready(state,local,targets,{},self.state(1),1,2))
        local['worker']['applied']['c']=3;local['worker']['applied_epochs']['c']=3
        self.assertFalse(positions_ready(state,local,targets,{},self.state(1),1,2))
        local['worker']['applied']['c']=4
        self.assertTrue(positions_ready(state,local,targets,{},self.state(1),1,2))
        local['worker']['applied_epochs']['c']=2
        self.assertFalse(positions_ready(state,local,targets,{},self.state(1),1,2))

    def test_query_oracle_rejects_stale_revision_ghost_and_wrong_distance(self):
        expected={'v':dict(id='v',revision=2,vector=[1,2],payload={'p':'x'})}
        hit=dict(id='v',revision=2,distance=5,payload={'p':'x'})
        self.assertTrue(matches([hit],expected))
        for hits in ([],[hit,hit],[dict(hit,revision=1)],[dict(hit,distance=4)],[dict(hit,id='deleted')]):
            self.assertFalse(matches(hits,expected))

    def test_complete_probe_never_synchronizes_queries(self):
        baseline=self.state(0);baseline['data_progress']['data-0000']['head']=0
        expected={'v':dict(id='v',revision=2,vector=[1,2],payload={},deleted=False)}
        state=self.state();calls=[]
        def request(req,timeout):
            path=req.selector;body=json.loads(req.data) if req.data else None;calls.append(path)
            if path=='/v1/changes': result=dict(head=7,next=7,reset_required=False,data=[dict(sequence=n,record=expected['v']) for n in range(1,8)])
            elif path=='/v1/cluster': result=state
            elif path in ('/v1/search','/internal/partition'):
                q=body if path=='/v1/search' else body['query']
                self.assertNotIn('min_visible',q)
                if path=='/internal/partition': self.assertEqual(body['operation'],'search')
                result=dict(hits=[dict(id='v',revision=2,distance=5,payload={})],graph_position=dict(sequence=4))
            else: self.fail('unexpected side effect: '+path)
            return io.BytesIO(json.dumps(result).encode())
        with tempfile.TemporaryDirectory() as directory, patch('urllib.request.urlopen',request):
            fixture=SimpleNamespace(logs=Path(directory),urls=['http://g1','http://g2'],
                c=SimpleNamespace(namespace='test',env=dict(API_TOKEN='a',CLUSTER_TOKEN='p')),local_metrics=self.local)
            result=probe(fixture,baseline,expected,time.monotonic(),name='test',target_seconds=10)
        self.assertTrue(result['correct']);self.assertTrue(result['within_target'])
        self.assertGreaterEqual(calls.count('/internal/partition'),2)
        self.assertGreaterEqual(calls.count('/v1/search'),2)

    def test_complete_probe_checks_extra_serving_replica_queries(self):
        baseline=self.state(0);baseline['data_progress']['data-0000']['head']=7
        state=self.state(0);state['replica_owners']['index-0000']['owners'].append(dict(partition='c',epoch=3,ready=True,url='http://c'))
        local=self.local();local['worker']['applied']['c']=0;local['worker']['applied_epochs']['c']=3
        calls=[]
        def request(req,timeout):
            body=json.loads(req.data) if req.data else None
            if req.selector=='/v1/changes': result=dict(head=7,next=7,reset_required=False,data=[])
            elif req.selector=='/v1/cluster': result=state
            else:
                if req.selector=='/internal/partition': calls.append(body['partition'])
                result=dict(hits=[])
            return io.BytesIO(json.dumps(result).encode())
        with tempfile.TemporaryDirectory() as directory, patch('urllib.request.urlopen',request):
            fixture=SimpleNamespace(logs=Path(directory),urls=['http://g1','http://g2'],
                c=SimpleNamespace(namespace='test',env=dict(API_TOKEN='a',CLUSTER_TOKEN='p')),local_metrics=lambda:local)
            result=probe(fixture,baseline,{},time.monotonic(),name='test',target_seconds=10)
        self.assertTrue(result['within_target']);self.assertIn('c',calls)

    def test_summary_keeps_missed_and_incomplete_rounds(self):
        s=summarize([dict(both_seconds=.2,within_target=True),dict(both_seconds=2,within_target=False),dict(both_seconds=None,within_target=False)])
        self.assertEqual((s['samples'],s['completed'],s['passed'],s['max_seconds']),(3,2,1,2))


if __name__=='__main__': unittest.main()
