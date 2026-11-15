import subprocess
import io
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from cluster_local import LocalCluster, ControlHandler


class LauncherTests(unittest.TestCase):
    def cluster(self):
        return LocalCluster('unit-test', environment={
            'API_TOKEN': 'unit-public-token', 'CLUSTER_TOKEN': 'unit-private-token'})

    def test_invalid_configuration_has_no_side_effects(self):
        with patch('cluster_local.subprocess.Popen') as spawn:
            for count in (-1, 33):
                with self.assertRaises(ValueError):
                    self.cluster().start(count)
            with self.assertRaises(ValueError):
                LocalCluster('unit', environment={})
            with self.assertRaises(ValueError):
                LocalCluster('unit', partitions=65)
            spawn.assert_not_called()

    def test_scale_down_preserves_survivors_and_stops_removed_children(self):
        cluster = self.cluster()
        cluster.workers = ['one', 'two', 'three']
        cluster.stop = Mock()
        cluster.scale(1)
        self.assertEqual(cluster.workers, ['one'])
        self.assertEqual([c.args[0] for c in cluster.stop.call_args_list], ['three', 'two'])
        cluster.scale(1)
        self.assertEqual(cluster.stop.call_count, 2)

    def test_start_failure_cleans_up_existing_children(self):
        cluster = self.cluster()
        cluster.launch = Mock(side_effect=['master', RuntimeError('port busy')])
        cluster.close = Mock()
        with self.assertRaisesRegex(RuntimeError, 'port busy'):
            cluster.start()
        cluster.close.assert_called_once()

    def test_stop_escalates_only_owned_process_after_timeout(self):
        cluster = self.cluster()
        proc = Mock()
        proc.poll.return_value = None
        proc.wait.side_effect = [subprocess.TimeoutExpired('child', 10), 0]
        cluster.children['owned'] = proc
        cluster.stop('owned')
        proc.terminate.assert_called_once()
        proc.kill.assert_called_once()
        self.assertEqual(proc.wait.call_count, 2)

    def test_readiness_requires_new_assignment_and_session(self):
        cluster = self.cluster()
        cluster.partitions = 1
        cluster.workers = ['new']
        state = dict(ready=True, master_available=True,
            owners={'index': {'node': 'new', 'session': 'old-session'}},
            assignments={'index': {'node': 'new', 'session': 'new-session'}},
            nodes=[{'id': 'new', 'roles': ['worker']}])
        correct = dict(state, owners={'index': {'node': 'new', 'session': 'new-session'}})
        cluster.status = Mock(side_effect=[state, correct])
        with patch('cluster_local.time.sleep'):
            self.assertEqual(cluster.wait_ready(), correct)
        self.assertEqual(cluster.status.call_count, 2)

    def test_registered_worker_does_not_mean_master_finished_scheduling(self):
        cluster = self.cluster()
        cluster.partitions = 1
        cluster.workers = ['one', 'two']
        import hashlib
        target = max(cluster.workers, key=lambda n: hashlib.sha256(('index' + '\0' + n).encode()).digest())
        previous = next(n for n in cluster.workers if n != target)
        def state(node):
            return dict(ready=True, master_available=True,
                owners={'index': {'node': node, 'session': 'session'}},
                assignments={'index': {'node': node, 'session': 'session'}},
                nodes=[{'id': n, 'roles': ['worker']} for n in cluster.workers])
        cluster.status = Mock(side_effect=[state(previous), state(target)])
        with patch('cluster_local.time.sleep'):
            self.assertEqual(cluster.wait_ready(), state(target))
        self.assertEqual(cluster.status.call_count, 2)


class ControlTests(unittest.TestCase):
    def command(self, request, cluster=None):
        handler = ControlHandler.__new__(ControlHandler)
        handler.connection = Mock()
        handler.rfile = io.BytesIO(request)
        handler.wfile = io.BytesIO()
        handler.server = SimpleNamespace(cluster=cluster or Mock(), stopping=False)
        handler.handle()
        return json.loads(handler.wfile.getvalue()), handler.server

    def test_invalid_commands_do_not_mutate(self):
        for raw in (b'bad\n', b'{}\n', b'[]\n', b'x'*4097,
                    b'{"action":"scale","workers":true}\n',
                    b'{"action":"stop","unknown":1}\n'):
            cluster = Mock()
            reply, server = self.command(raw, cluster)
            self.assertFalse(reply['ok'])
            self.assertFalse(server.stopping)
            cluster.scale.assert_not_called()
            cluster.close.assert_not_called()

    def test_scale_waits_and_reports_failure(self):
        cluster = Mock()
        cluster.wait_ready.side_effect = TimeoutError('not ready')
        reply, _ = self.command(b'{"action":"scale","workers":3}\n', cluster)
        cluster.scale.assert_called_once_with(3)
        cluster.wait_ready.assert_called_once()
        self.assertEqual(reply, {'ok': False, 'error': 'not ready'})

    def test_stop_is_acknowledged_before_supervisor_cleanup(self):
        reply, server = self.command(b'{"action":"stop"}\n')
        self.assertTrue(reply['ok'])
        self.assertTrue(server.stopping)
        server.cluster.close.assert_not_called()


if __name__ == '__main__':
    unittest.main()
