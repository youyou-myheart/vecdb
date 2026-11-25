import copy
import io
import unittest
import urllib.error
from unittest.mock import patch
from pipeline_probe import parse_metrics, interval, drain_summary, collect


class PipelineProbeTest(unittest.TestCase):
    def test_graph_entry_bytes_preserve_stored_and_original_sizes(self):
        text = '\n'.join([
            'vecdb_graph_entry_bytes_total{index_partition="i",representation="delta"} 1500',
            'vecdb_graph_entry_bytes_total{index_partition="i",representation="stored"} 500'])
        self.assertEqual(parse_metrics(text)['graph_entry_bytes'], {'i': {'delta': 1500, 'stored': 500}})

    def test_local_leases_are_observed_without_changing_progress_probe(self):
        text = '\n'.join([
            'vecdb_index_local_lease_valid{index_partition="i",replica="r"} 1',
            'vecdb_index_local_lease_remaining_seconds{index_partition="i",replica="r"} 0.75',
            'vecdb_index_local_head_ready{index_partition="i",replica="r"} 0'])
        self.assertEqual(parse_metrics(text)['index_leases'], {'r': dict(lease_valid=1, lease_remaining_seconds=.75, head_ready=0)})
        self.assertEqual(parse_metrics(text, progress_only=True)['index_leases'], {})

    def test_epoch_control_counters_are_retained_without_changing_frontier(self):
        text = '\n'.join([
            'vecdb_index_source_epoch_advances_total{index_partition="i",source="2"} 3',
            'vecdb_index_source_epoch_rejections_total{index_partition="i",phase="commit"} 4',
            'vecdb_index_source_epoch_rejections_total{index_partition="i",phase="admission"} 5'])
        self.assertEqual(parse_metrics(text)['source_epochs'], dict(advances={'i':{'2':3}}, rejections={'i':{'commit':4,'admission':5}}))
        self.assertEqual(parse_metrics(text, progress_only=True)['source_epochs'], dict(advances={}, rejections={}))

    def test_batch_size_histograms_are_separate_from_duration_histograms(self):
        snapshot=parse_metrics('''
vecdb_pipeline_batch_items_count{stage="data_commit",partition="data-0000",outcome="success"} 3
vecdb_pipeline_batch_items_sum{stage="data_commit",partition="data-0000",outcome="success"} 12
vecdb_pipeline_batch_items_bucket{stage="data_commit",partition="data-0000",outcome="success",le="8"} 3
vecdb_pipeline_stage_seconds_count{stage="data_commit",partition="data-0000",outcome="success"} 2
''')
        self.assertEqual(snapshot['histograms']['batch_items_data_commit/success']['sum'],12)
        self.assertEqual(snapshot['histograms']['data_commit/success']['count'],2)

    def test_metrics_http_error_keeps_evidence_without_credentials(self):
        def failed(req,timeout):
            raise urllib.error.HTTPError(req.full_url,503,'unavailable',{},io.BytesIO(b'backend unavailable'))
        with patch('urllib.request.urlopen',failed):
            row=collect({'worker':'http://worker'},'private-token',progress_only=True)['worker']
        self.assertEqual(row['http_status'],503)
        self.assertEqual(row['response_body'],'backend unavailable')
        self.assertNotIn('private-token',str(row))

    def test_scrape_profiles_http_and_parsing_without_losing_progress(self):
        text=b'vecdb_graph_local_applied_sequence{index_partition="i",replica="r"} 7\n'
        with patch('urllib.request.urlopen',side_effect=lambda *a,**kw:io.BytesIO(text)):
            row=collect({'worker':'http://worker'},'secret')['worker']
        self.assertEqual(row['applied'],{'r':7})
        self.assertEqual(row['response_bytes'],len(text))
        self.assertGreaterEqual(row['scrape_seconds'],row['http_seconds']+row['parse_seconds'])
        self.assertEqual(row['parse_mode'],'full')

    def test_progress_mode_preserves_uint64_epochs_and_every_replica(self):
        text='\n'.join(f'{name}{{index_partition="i",replica="r{n}"}} {2**64-1-n}'
            for name in ('vecdb_graph_local_applied_sequence','vecdb_graph_local_owner_epoch') for n in range(3))
        text+='\nvecdb_pipeline_stage_seconds_count{stage="graph_apply",outcome="success"} 8\n'
        full=parse_metrics(text);progress=parse_metrics(text,progress_only=True)
        for field in ('applied','applied_epochs'):
            self.assertEqual(progress[field],full[field])
            self.assertEqual(progress[field],{f'r{n}':2**64-1-n for n in range(3)})
        self.assertFalse(progress['histograms']);self.assertTrue(full['histograms'])
        for value in ('nan','-1',str(2**64)):
            with self.assertRaises(ValueError):
                parse_metrics(f'vecdb_graph_local_owner_epoch{{replica="r"}} {value}',progress_only=True)

    def metrics(self, count, total):
        return {'node':parse_metrics(f'''
vecdb_pipeline_stage_seconds_bucket{{outcome="success",partition="replica-a",stage="index_queue_wait",le="1"}} 0
vecdb_pipeline_stage_seconds_bucket{{outcome="success",partition="replica-a",stage="index_queue_wait",le="8"}} {count}
vecdb_pipeline_stage_seconds_bucket{{outcome="success",partition="replica-a",stage="index_queue_wait",le="+Inf"}} {count}
vecdb_pipeline_stage_seconds_count{{outcome="success",partition="replica-a",stage="index_queue_wait"}} {count}
vecdb_pipeline_stage_seconds_sum{{outcome="success",partition="replica-a",stage="index_queue_wait"}} {total}
vecdb_index_queue_oldest_seconds{{replica="replica-a"}} 3
vecdb_graph_local_applied_sequence{{index_partition="index-0000",replica="replica-a"}} 5
''')}

    def test_delta_not_lifetime_average_and_quantile_is_upper_bound(self):
        result=interval(self.metrics(10,20),self.metrics(12,32))['index_queue_wait/success']
        self.assertEqual((result['count'],result['mean'],result['p95_upper_bound']),(2,6,8))
        self.assertEqual(self.metrics(1,2)['node']['applied'],{'replica-a':5})

    def test_missing_scrape_and_process_reset_fail_closed(self):
        for before, after in [(self.metrics(12,32),self.metrics(10,20)),
                              (self.metrics(1,2),{}),(self.metrics(1,2),{'node':{'error':'TimeoutError'}})]:
            with self.assertRaises(ValueError): interval(before,after)

    def test_consumer_trigger_and_waiting_metrics(self):
        snapshot = parse_metrics('''
vecdb_index_consumer_cycles_total{index_partition="index-0000",reason="notification"} 42
vecdb_index_consumer_cycles_total{index_partition="index-0000",reason="continue"} 7
vecdb_index_consumer_waiting{index_partition="index-0000",replica="replica-a"} 1
vecdb_graph_commit_conflict_retries_total{index_partition="index-0000"} 9
vecdb_route_plan_fallback_total{index_partition="index-0000"} 3
vecdb_dispatch_wake_total{data_partition="data-0000",reason="graph_commit"} 4
vecdb_graph_local_owner_epoch{index_partition="index-0000",replica="replica-a"} 7
''')
        self.assertEqual(snapshot['consumers']['index-0000'],dict(waiting=1,commit_conflict_retries=9,cycles=dict(notification=42,**{'continue':7})))
        self.assertEqual(snapshot['route_fallbacks'],{'index-0000':3})
        self.assertEqual(snapshot['source_signals'],{'data-0000':{'graph_commit':4}})
        self.assertEqual(snapshot['applied_epochs'],{'replica-a':7})

    def test_drain_separates_commits_apply_and_heartbeat(self):
        state=dict(graph_heads={'index-0000':dict(sources={'0':5},committed={'sequence':3},visible={'sequence':3})},
                   replica_owners={'index-0000':{'owners':[{'partition':'a'},{'partition':'b'}]}},
                   graph_positions={'a':{'sequence':3},'b':{'sequence':3}},
                   data_progress={'data-0000':dict(head=5,completed_lsn=5,progress_fresh=True)})
        base=dict(elapsed_seconds=4,state=state,local={'node':dict(histograms={},applied={'a':3,'b':3})})
        rows=[copy.deepcopy(base) for _ in range(4)]
        for n,row in enumerate(rows): row['elapsed_seconds']=n+1
        rows[0]['state']['graph_heads']['index-0000']['sources']['0']=4
        rows[0]['state']['graph_heads']['index-0000']['visible']['sequence']=2
        for row in rows[:2]: row['state']['data_progress']['data-0000']['completed_lsn']=4
        for row in rows[:2]: row['local']['node']['applied'].pop('b')
        for row in rows[:3]: row['state']['graph_positions']['b']['sequence']=2
        summary=drain_summary(rows)
        self.assertEqual(summary['first_observed_seconds'],dict(source_commits=2,graph_visible=2,replicas_local=3,replicas_reported=4,source_completed=3))
        self.assertEqual(summary['initial_pending_events'],1)
        self.assertEqual(summary['max_observation_gap_seconds'],1)
        rows[2]['state']['data_progress']['data-0000']['progress_fresh']=False
        self.assertEqual(drain_summary(rows)['first_observed_seconds']['source_completed'],4)
        rows[-1]['state']['data_progress']['data-0000']['progress_fresh']=False
        with self.assertRaisesRegex(ValueError, 'not current'):
            drain_summary(rows)
