from pathlib import Path
import unittest

from quality import stages_for
from verify_performance import Profile, checks


class PerformanceGateTest(unittest.TestCase):
    def setUp(self):
        self.profile = Profile()
        n = self.profile.qps * self.profile.seconds
        self.writes = dict(planned=n, invoked=n, not_sent=0, full_window=True, durable_acks=n,
                           rpc_failures={}, ack_qps_in_window=190., p99_ack_ms_upper=200)
        self.follower = dict(status='confirmed', pinned_durable_events=n, probe_errors=0, first_confirmed_ms=1000)

    def test_exact_limits_pass(self):
        self.assertTrue(all(checks(self.profile, self.writes, self.follower).values()))

    def test_write_gates_fail_independently(self):
        for change, gate in [
            (dict(full_window=False), 'full_window'), (dict(planned=5000), 'full_window'),
            (dict(not_sent=1), 'every_write_sent'), (dict(invoked=5999), 'every_write_sent'),
            (dict(durable_acks=5999), 'every_write_durable'), (dict(rpc_failures={'Unavailable': 1}), 'every_write_durable'),
            (dict(ack_qps_in_window=189.99), 'window_throughput'), (dict(ack_qps_in_window=None), 'window_throughput'),
            (dict(p99_ack_ms_upper=201), 'write_p99'), (dict(p99_ack_ms_upper=None), 'write_p99'),
        ]:
            with self.subTest(change=change):
                self.assertFalse(checks(self.profile, dict(self.writes, **change), self.follower)[gate])

    def test_final_coverage_and_response_deadline(self):
        for change, gate in [
            (dict(status='not_confirmed'), 'follower_read'), (dict(probe_errors=1), 'follower_read'),
            (dict(pinned_durable_events=5999), 'durable_frontier'), (dict(pinned_durable_events=6001), 'durable_frontier'),
            (dict(first_confirmed_ms=None), 'follower_latency'),
            (dict(first_confirmed_ms=1001, applied_upper_ms=950), 'follower_latency'),
        ]:
            with self.subTest(change=change):
                self.assertFalse(checks(self.profile, self.writes, dict(self.follower, **change))[gate])

    def test_smoke_builds_current_driver_and_requires_new_report(self):
        unit = stages_for('unit', 'python', Path('.'))
        self.assertNotIn('performance-smoke', [s.name for s in unit])
        smoke = stages_for('smoke', 'python', Path('.'))
        names = [s.name for s in smoke]
        run = names.index('performance-smoke')
        for build in ('build-server', 'build-writebench'):
            self.assertLess(names.index(build), run)
        self.assertEqual(smoke[run].artifacts, ('reports/performance-smoke.json',))
