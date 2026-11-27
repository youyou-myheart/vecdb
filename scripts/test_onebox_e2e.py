import unittest
from onebox_e2e import Onebox, Oracle, Measurements, vector_for, percentile, operation_name


class OneboxOracleTest(unittest.TestCase):
    def test_stale_progress_does_not_replace_last_live_observation(self):
        box = Onebox.__new__(Onebox)
        box.centers = [[0, 0]]
        box.last_completed, box.last_head, box.last_terms, box.report = {}, {}, {}, {}

        def sample(head, completed, fresh):
            state = dict(routing=dict(centers=box.centers, version=1),
                         data_progress={'data-0000': dict(floor=0, head=head,
                             completed_lsn=completed, progress_fresh=fresh)})
            box.request = lambda *a, **k: state
            return box.state()

        sample(10, 8, True)
        self.assertFalse(sample(0, 0, False)['data_progress']['data-0000']['progress_fresh'])
        sample(20, 20, False)
        self.assertEqual((box.last_head['data-0000'], box.last_completed['data-0000']), (10, 8))
        for head, completed in [(9, 8), (10, 7)]:
            with self.assertRaises(AssertionError):
                sample(head, completed, True)
        sample(11, 9, True)

    def test_operation_labels_exclude_record_ids_and_cursors(self):
        self.assertEqual(operation_name('GET','/v1/records?limit=10&cursor=secret'), 'GET record-list')
        self.assertEqual(operation_name('GET','/v1/records/private-id'), 'GET records')
        self.assertEqual(operation_name('POST','/v1/search'), 'POST search')

    def test_oracle_checks_payload_vector_and_tombstone(self):
        record = dict(id='x', revision=1, vector=[1, 2], payload={'a':'b'}, deleted=False)
        Oracle.check_record(record, record)
        for key, value in [('revision',2), ('vector',[2,1]), ('payload',{'a':'c'}), ('deleted',True)]:
            with self.assertRaises(AssertionError):
                Oracle.check_record(dict(record, **{key:value}), record)
        Oracle.check_record(dict(id='x',revision=2,deleted=True,vector=None,payload=None),
                            dict(id='x',revision=2,deleted=True,vector=[],payload={}))

    def test_success_and_error_measurements_remain_separate(self):
        stats = Measurements()
        stats.record('attempt_failure',.1,error='HTTP_503',phase='fault')
        stats.record('PUT records',.5,phase='fault')
        report = stats.summary()['fault']
        self.assertEqual(report['PUT records']['count'],1)
        self.assertEqual(report['PUT records']['p99_ms'],500)
        self.assertEqual(report['attempt_failure']['errors'],{'HTTP_503':1})
        self.assertIsNone(percentile([], .99))

    def test_fixture_crosses_fixed_clusters_deterministically(self):
        self.assertEqual(vector_for(7,1),vector_for(7,1))
        self.assertEqual(len(vector_for(7,1)),16)
        self.assertNotEqual(int(vector_for(7,1)[0]/20),int(vector_for(7,4)[0]/20))

    def test_bad_receipt_does_not_advance_oracle(self):
        oracle = Oracle(lambda *a,**k: dict(durable=True,token=dict(id='v-0001',revision=99)))
        with self.assertRaises(AssertionError): oracle.mutate(1,initial=True)
        self.assertEqual(oracle.snapshot(),{})
