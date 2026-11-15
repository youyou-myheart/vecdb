"""A failed acceptance assertion must fail CI after writing its report."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
import verify_balance


class BalanceHarnessTest(unittest.TestCase):
    def test_assertion_propagates_after_cleanup_and_report(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'reports').mkdir()
            cluster = MagicMock()
            cluster.start.side_effect = AssertionError('injected acceptance failure')
            cluster.children = {}
            with patch.object(verify_balance, 'ROOT', root), patch.object(verify_balance, 'LocalCluster', return_value=cluster):
                with self.assertRaisesRegex(AssertionError, 'injected acceptance failure'):
                    verify_balance.main()
            cluster.close.assert_called_once()
            report = json.loads((root / 'reports/balance-http.json').read_text())
            self.assertEqual(report['status'], 'failed')
            self.assertTrue(report['all_children_stopped'])
