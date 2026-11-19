import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

from perf_collect import runner


class CollectionStateTests(unittest.TestCase):
    def test_setup_failure_keeps_finished_result(self):
        with TemporaryDirectory() as tmp:
            output = Path(tmp)/'new'/'result.json'
            with self.assertRaises(FileNotFoundError):
                runner.collect(output=output, server=Path(tmp)/'missing', driver=sys.executable)
            result = json.loads(output.read_text())
        self.assertEqual(result['status'], 'failed')
        self.assertIn('expected executable', result['exception'])
        self.assertIn('finished_at', result['metadata'])

    def test_stage_failure_preserves_checkpoint_and_reaps_services(self):
        storage, cluster = Mock(current={}), Mock()
        def fail(_cluster, _storage, _centers, _records, result, save, *_):
            result['writes']['put_c1'] = {'success': 9}
            save()
            raise TimeoutError('injected failure after PUT')
        with TemporaryDirectory() as tmp:
            output = Path(tmp)/'result.json'
            with (patch.object(runner, 'Storage', return_value=storage),
                  patch.object(runner, 'LocalCluster', return_value=cluster),
                  patch.object(runner, '_workloads', side_effect=fail),
                  patch.object(runner, 'dataset', return_value=([], {})),
                  patch.object(runner.socket, 'socket')):
                with self.assertRaises(TimeoutError):
                    runner.collect(output=output, server=sys.executable, driver=sys.executable,
                                   pd=sys.executable, tikv=sys.executable, weed=sys.executable, scratch=tmp)
            result = json.loads(output.read_text())
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['writes']['put_c1']['success'], 9)
        self.assertIn('injected failure', result['exception'])
        self.assertEqual(result['binary_sha256'], result['driver_sha256'])
        storage.close.assert_called_once()
        cluster.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
