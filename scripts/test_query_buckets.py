import copy
import io
import json
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import urllib.error

from probe_http import request, classify_error, RequestJournal
from query_buckets import BUCKET_FIELD, QueryBuckets
from visibility_probe import probe, target_index


class BucketProbeTest(unittest.TestCase):
    def records(self,count,layout):
        return {f'v-{n:04d}':dict(id=f'v-{n:04d}',revision=1,vector=[float(n%2*100),n*.001],payload=layout.payload(n,1),deleted=False) for n in range(count)}

    def state(self):
        pos=dict(sequence=1,data_offset=1,hash='h')
        return dict(ready=True,fully_replicated=True,routing=dict(version=1,centers=[[0,0],[100,0]]),
            graph_heads={f'index-{i:04d}':dict(committed=pos,visible=pos,sources={}) for i in range(2)},
            replica_owners={f'index-{i:04d}':dict(owners=[dict(partition=f'r{i}{j}',epoch=1,node=f'w{j}',session='s',url=f'http://w{j}',ready=True) for j in range(2)]) for i in range(2)},
            data_progress={'data-0000':dict(head=1,completed_lsn=1)})

    def run_probe(self,expected,layout,*,alter=None,replace=False,seconds=2,discovery_delay=0,target_seconds=1,metric_error=False):
        state=self.state();calls=[];states=0;metric_calls=0
        def network(req,timeout):
            nonlocal states,state
            self.assertEqual(timeout,3)
            body=json.loads(req.data) if req.data else None
            if req.selector=='/v1/changes':
                if discovery_delay:time.sleep(discovery_delay)
                result=dict(head=1,next=1,reset_required=False,data=[])
            elif req.selector=='/v1/cluster':
                states+=1
                if replace and states==2:
                    state=copy.deepcopy(state)
                    state['replica_owners']['index-0000']['owners'].append(dict(partition='extra',epoch=2,node='w2',session='new',url='http://w2',ready=True))
                result=state
            else:
                q=body['query'] if req.selector=='/internal/partition' else body
                self.assertEqual(q['k'],1000);self.assertTrue(q['exact']);self.assertNotIn('min_visible',q)
                bucket=q['filter'][BUCKET_FIELD]
                replica=body['partition'] if req.selector=='/internal/partition' else req.host
                calls.append((replica,bucket))
                index=next((i for i,g in state['replica_owners'].items() if any(o['partition']==replica for o in g['owners'])),None)
                hits=[dict(id=id,revision=r['revision'],payload=r['payload'],distance=sum(v*v for v in r['vector'])) for id,r in expected.items()
                      if not r['deleted'] and r['payload'][BUCKET_FIELD]==bucket and (index is None or target_index(r,state['routing']['centers'])==[index])]
                if alter: hits=alter(replica,bucket,hits,states,req)
                result=dict(hits=hits,graph_position=dict(sequence=1))
            return io.BytesIO(json.dumps(result).encode())
        def metrics():
            nonlocal metric_calls
            metric_calls+=1
            if metric_error and metric_calls==1:return {'worker':dict(error='HTTPError',http_status=503,response_body='unavailable')}
            owners=[o for g in state['replica_owners'].values() for o in g['owners']]
            return dict(worker=dict(applied={o['partition']:1 for o in owners},applied_epochs={o['partition']:o['epoch'] for o in owners}))
        with tempfile.TemporaryDirectory() as directory,patch('urllib.request.urlopen',network):
            fixture=SimpleNamespace(logs=Path(directory),urls=['http://g0','http://g1'],visibility_buckets=layout,
                c=SimpleNamespace(namespace='test',env=dict(API_TOKEN='a',CLUSTER_TOKEN='b')),local_metrics=metrics)
            result=probe(fixture,copy.deepcopy(state),expected,time.monotonic(),name='test',target_seconds=target_seconds,observe_seconds=seconds,poll_seconds=0)
            trace=[json.loads(l) for l in (Path(directory)/'test-visibility.jsonl').read_text().splitlines()]
            requests=[json.loads(l) for l in (Path(directory)/'test-requests.jsonl').read_text().splitlines()]
            self.assertEqual(len(requests),result['profiling']['request_journal']['records'])
        return result,calls,trace

    def test_domain_is_stable_exhaustive_disjoint_and_bounded(self):
        layout=QueryBuckets(2040);expected=self.records(2000,layout)
        groups=layout.partition(expected)
        self.assertEqual(len(groups),6)
        ids=[id for _,_,records in groups for id in records]
        self.assertEqual(len(ids),2000);self.assertEqual(len(set(ids)),2000)
        self.assertTrue(all(len(records)<=500 for _,_,records in groups))
        self.assertFalse(groups[-1][2]);self.assertFalse(groups[-2][2])
        self.assertEqual(layout.payload(501,2)[BUCKET_FIELD],layout.payload(501,99)[BUCKET_FIELD])
        for bad in ('v-02040','v-2040','v-1','other'):
            with self.assertRaises(ValueError):layout.number(bad)
        with self.assertRaises(ValueError):QueryBuckets(2000,1001)
        expected['v-0001']['payload'][BUCKET_FIELD]='9999'
        with self.assertRaises(AssertionError):layout.partition(expected)

    def test_over_1000_checks_all_buckets_all_replicas_and_gateways(self):
        layout=QueryBuckets(2040);expected=self.records(2000,layout)
        # An entire deleted bucket must still be queried as empty everywhere.
        for n in range(500):expected[f'v-{n:04d}'].update(deleted=True,vector=[],payload={})
        result,calls,_=self.run_probe(expected,layout)
        self.assertTrue(result['correct']);self.assertEqual(result['live_records'],1500)
        self.assertEqual(set(calls),{(endpoint,f'{bucket:04d}') for endpoint in ('r00','r01','r10','r11','g0','g1') for bucket in range(6)})

    def test_empty_bucket_ghost_and_stale_or_corrupt_hits_fail(self):
        layout=QueryBuckets(80,20);expected=self.records(80,layout)
        for fault in ('ghost','missing','revision','payload','distance'):
            def alter(replica,bucket,hits,states,req):
                if replica!='r00':return hits
                if fault=='ghost' and bucket=='0004':return [dict(id='deleted',revision=1,payload={},distance=0)]
                if bucket=='0000' and hits:
                    hits=copy.deepcopy(hits)
                    if fault=='missing':hits.pop()
                    if fault=='revision':hits[0]['revision']+=1
                    if fault=='payload':hits[0]['payload']={}
                    if fault=='distance':hits[0]['distance']+=1
                return hits
            result,_,_=self.run_probe(expected,layout,alter=alter,seconds=.08)
            self.assertFalse(result['correct'],fault)
            self.assertIsNotNone(result['first_frontier_observed_seconds'],fault)

    def test_journal_flush_is_inside_the_same_completion_clock(self):
        layout=QueryBuckets(80,20)
        flush=RequestJournal.flush
        def slow_flush(journal):
            time.sleep(.03)
            flush(journal)
        with patch.object(RequestJournal,'flush',slow_flush):
            result,_,_=self.run_probe(self.records(80,layout),layout)
        self.assertTrue(result['correct'])
        self.assertGreaterEqual(result['both_seconds']-result['queries_seconds'],.03)
        self.assertLess(result['first_frontier_observed_seconds'],result['both_seconds'])

    def test_late_discovery_is_counted_without_discarding_a_valid_sweep(self):
        layout=QueryBuckets(80,20)
        result,_,_=self.run_probe(self.records(80,layout),layout,discovery_delay=.05,target_seconds=.01)
        self.assertTrue(result['correct']);self.assertFalse(result['within_target'])
        self.assertGreaterEqual(result['both_seconds'],.05)
        self.assertEqual(result['query_attempts'],30)

    def test_early_metrics_error_remains_visible_after_final_recovery(self):
        layout=QueryBuckets(80,20)
        result,_,_=self.run_probe(self.records(80,layout),layout,metric_error=True)
        self.assertTrue(result['correct'])
        self.assertEqual(len(result['control_errors']),1)
        error=result['control_errors'][0]
        self.assertEqual(error['phase'],'before_queries')
        self.assertEqual(error['details']['http_status'],503)

    def test_membership_change_requires_every_bucket_on_new_owner(self):
        layout=QueryBuckets(80,20)
        result,calls,trace=self.run_probe(self.records(80,layout),layout,replace=True)
        self.assertTrue(result['correct']);self.assertFalse(trace[0]['membership_stable'])
        self.assertEqual({b for r,b in calls if r=='extra'},{f'{n:04d}' for n in range(5)})

    def test_previous_bucket_success_cannot_mask_current_http_error(self):
        layout=QueryBuckets(80,20)
        def alter(replica,bucket,hits,states,req):
            if replica=='r00':
                if states==1 and bucket=='0001':return []
                if states>1 and bucket=='0000':
                    raise urllib.error.HTTPError(req.full_url,503,'busy',{},io.BytesIO(b'{"code":"unavailable","error":"busy"}'))
            return hits
        # Allow a complete second sweep even on a loaded test host. This is
        # the mock observation window, not the HTTP timeout or 1s SLA gate.
        result,_,_=self.run_probe(self.records(80,layout),layout,alter=alter,seconds=1)
        self.assertFalse(result['correct'])
        self.assertTrue(result['transport_errors'])
        self.assertTrue(all(e['classification']=='unexplained_or_current_member_error' for e in result['transport_errors']))

    def test_http_failure_preserves_status_body_trace_and_timing(self):
        def network(req,timeout):
            self.assertEqual(timeout,3)
            trace=req.get_header('X-vecdb-probe-id')
            raise urllib.error.HTTPError(req.full_url,503,'fenced',{'X-Vecdb-Probe-ID':trace},io.BytesIO(b'{"code":"fenced","error":"partition ownership lost"}'))
        with patch('urllib.request.urlopen',network):result=request('http://w/internal/partition','never-log-this-token',time.monotonic(),{})
        self.assertEqual(result['http_status'],503)
        self.assertEqual(result['server_request_id'],result['request_id'])
        self.assertEqual(result['response_json']['code'],'fenced')
        self.assertNotIn('never-log-this-token',json.dumps(result))
        self.assertEqual(classify_error(result,retired=True),'retired_member_fenced')
        self.assertEqual(classify_error(result,retired=False),'unexplained_or_current_member_error')
        self.assertGreaterEqual(result['elapsed'],result['started_seconds'])


if __name__=='__main__':unittest.main()
