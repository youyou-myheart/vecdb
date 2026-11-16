import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch
import verify_dispatch


class DispatchHarnessTest(unittest.TestCase):
    def test_completion_is_not_replica_visibility(self):
        position = {'sequence':2, 'hash':'committed'}
        state = dict(ready=True, fully_replicated=True,
            data_progress={'d':dict(head=9, completed_lsn=9, floor=2, owner_available=True)},
            graph_heads={'i':dict(visible=position, committed=position)},
            replica_owners={'i':dict(owners=[dict(partition='a'), dict(partition='b')])},
            graph_positions={'a':position, 'b':position})
        self.assertTrue(verify_dispatch.converged(state, 1, 1))
        for field in ('data', 'replica', 'unpublished', 'expired', 'missing'):
            bad = copy.deepcopy(state)
            if field == 'data': bad['data_progress']['d']['completed_lsn']=8
            if field == 'replica': bad['graph_positions']['b']={'sequence':1}
            if field == 'unpublished': bad['graph_heads']['i']['visible']={'sequence':1}
            if field == 'expired': bad['data_progress']['d']['owner_available']=False
            if field == 'missing': bad['graph_positions'].pop('b')
            self.assertFalse(verify_dispatch.converged(bad, 1, 1), field)

    def test_failure_cleans_up_and_propagates(self):
        with tempfile.TemporaryDirectory() as tmp:
            cluster = MagicMock()
            cluster.start.side_effect = AssertionError('injected failure')
            cluster.children = {}
            cluster.namespace = 'test'
            with patch.object(verify_dispatch, 'ROOT', Path(tmp)), patch.object(verify_dispatch, 'LocalCluster', return_value=cluster):
                with self.assertRaisesRegex(AssertionError, 'injected failure'):
                    verify_dispatch.main()
            cluster.close.assert_called_once()
            report = json.loads((Path(tmp)/'reports/dispatch-processes.json').read_text())
            self.assertEqual(report['status'], 'failed')
            self.assertTrue(report['all_children_stopped'])
