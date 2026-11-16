import copy
import unittest
from pipeline_probe import parse_metrics, interval, drain_summary


class PipelineProbeTest(unittest.TestCase):
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

    def test_drain_separates_commits_apply_and_heartbeat(self):
        state=dict(graph_heads={'index-0000':dict(sources={'0':5},committed={'sequence':3},visible={'sequence':3})},
                   replica_owners={'index-0000':{'owners':[{'partition':'a'},{'partition':'b'}]}},
                   graph_positions={'a':{'sequence':3},'b':{'sequence':3}},
                   data_progress={'data-0000':dict(head=5,completed_lsn=5)})
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
