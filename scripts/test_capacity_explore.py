from pathlib import Path
from types import SimpleNamespace
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

from capacity_explore import ClusteredData, recall_result
from onebox_e2e import Oracle
from resource_guard import GIB, ResourceGuard, clean_owned_state, host_sample, limit_reason, tree_bytes
from visibility_probe import target_index


class CapacityExplorationTest(unittest.TestCase):
    def test_dense_data_reproducible_routed_and_high_dimensional(self):
        for dimension in (16,64,256):
            data=ClusteredData(dimension,20261117)
            self.assertEqual(data.centers,ClusteredData(dimension,20261117).centers)
            for n in range(32):
                v=data.vector(n,1)
                self.assertEqual(len(v),dimension)
                self.assertEqual(v,data.vector(n,1))
                self.assertEqual(target_index(dict(vector=v),data.centers),[f'index-{n%8:04d}'])
                self.assertEqual(target_index(dict(vector=data.vector(n,4)),data.centers),[f'index-{(n+1)%8:04d}'])
                self.assertTrue(all(a!=b for a,b in zip(v,data.vector(n+8,1))))

    def test_oracle_accepts_explicit_capacity_and_vector_factory(self):
        expected={}
        def request(path,method='GET',body=None,**kwargs):
            if method=='GET': return expected
            expected.update(id='v-0990',revision=1,vector=body['vector'],payload=body['payload'],deleted=False)
            return dict(durable=True,token=dict(id='v-0990',revision=1))
        o=Oracle(request,capacity=1000,vector_factory=lambda n,r:[n,r])
        self.assertEqual(o.mutate(990,initial=True)['vector'],[990,1])

    def test_recall_checks_distance_revision_and_neighbors(self):
        active={str(n):dict(vector=[float(n)],revision=1,payload={}) for n in range(20)}
        hits=[dict(id=str(n),revision=1,distance=float(n*n),payload={}) for n in range(10)]
        self.assertEqual(recall_result([0],active,hits),1)
        for bad in ([dict(hits[0],revision=2)]+hits[1:], [dict(hits[0],distance=100)]+hits[1:], hits[2:]+[dict(id=str(n),revision=1,distance=float(n*n)) for n in (18,19)]):
            with self.assertRaises(AssertionError): recall_result([0],active,bad)

    def test_resource_limits_fail_closed(self):
        safe=dict(disk_free_bytes=20*GIB,memory_pressure=1,owned_bytes=GIB,owned_rss_bytes=GIB)
        self.assertIsNone(limit_reason(safe))
        for field,value in (('disk_free_bytes',15*GIB),('memory_pressure',2),('owned_bytes',1.5*GIB),('owned_rss_bytes',3*GIB),('elapsed_seconds',240)):
            self.assertIsNotNone(limit_reason(dict(safe,**{field:value})))

    def test_cleanup_requires_marker_exact_parent_and_stopped_children(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); owned=root/'trial';owned.mkdir()
            (owned/'.capacity-owned').write_text('trial')
            other=root/'untouched';other.mkdir();(other/'existing').write_text('keep')
            self.assertGreater(tree_bytes(owned)['logical'],0)
            for parent,name,stopped in ((root,'trial',False),(other,'trial',True),(root,'wrong',True)):
                with self.assertRaises(RuntimeError): clean_owned_state(owned,parent,name,stopped)
            link=root/'link';link.symlink_to(owned,target_is_directory=True)
            with self.assertRaises(RuntimeError): clean_owned_state(link,root,'link',True)
            clean_owned_state(owned,root,'trial',True)
            self.assertEqual((other/'existing').read_text(),'keep')

    def test_trip_only_terminates_owned_live_handles(self):
        live,dead=Mock(),Mock();live.poll.return_value=None;dead.poll.return_value=0
        fixture=SimpleNamespace(c=SimpleNamespace(children={'a':live}),storage=SimpleNamespace(children=[dead]),work_stop=threading.Event())
        guard=ResourceGuard(fixture,Path('.'))
        guard.trip('pressure');guard.trip('subsequent error')
        self.assertEqual(guard.reason,'pressure')
        self.assertTrue(fixture.work_stop.is_set())
        self.assertEqual(live.terminate.call_count,2);dead.terminate.assert_not_called()

    def test_sampling_failure_stops_trial_instead_of_losing_guard(self):
        live=Mock();live.poll.return_value=None
        fixture=SimpleNamespace(c=SimpleNamespace(children={'a':live}),storage=SimpleNamespace(children=[]),work_stop=threading.Event())
        guard=ResourceGuard(fixture,Path('.'))
        guard.sample=Mock(side_effect=OSError('metrics unavailable'))
        guard.stop=Mock();guard.stop.is_set.side_effect=[False,True]
        guard.run()
        self.assertIn('resource observation failed',guard.reason)
        live.terminate.assert_called_once()
        with self.assertRaises(RuntimeError): guard.check()

    def test_sustained_system_cpu_stops_after_three_samples(self):
        fixture=SimpleNamespace(c=SimpleNamespace(children={}),storage=SimpleNamespace(children=[]),work_stop=threading.Event())
        guard=ResourceGuard(fixture,Path('.'))
        guard.sample=Mock(return_value=dict(disk_free_bytes=20*GIB,memory_pressure=1,system_cpu_percent=1200,logical_cpus=14))
        guard.stop=Mock();guard.stop.is_set.side_effect=[False,False,False,True]
        guard.run()
        self.assertEqual(guard.sample.call_count,3)
        self.assertIn('system CPU',guard.reason)
        self.assertTrue(fixture.work_stop.is_set())

    def test_host_sample_accounts_only_selected_pids(self):
        def command(args,**kwargs):
            if args[0]=='sysctl': return {'kern.memorystatus_vm_pressure_level':'1','hw.memsize':str(48*GIB),'hw.logicalcpu':'14','vm.swapusage':'used=0'}[args[-1]]
            if args[0]=='vm_stat': return 'page size of 16384 bytes\nPages free: 2.\nPages occupied by compressor: 3.\n'
            return '12 20.0 1024\n13 80.0 2048\n'
        with patch('subprocess.check_output',side_effect=command),patch('shutil.disk_usage',return_value=SimpleNamespace(free=20*GIB)):
            row=host_sample(Path('.'),{12})
        self.assertEqual(row['owned_rss_bytes'],1024*1024)
        self.assertEqual(row['owned_cpu_percent'],20)
        self.assertEqual(row['system_cpu_percent'],100)
        self.assertEqual(row['compressed_bytes'],3*16384)


if __name__=='__main__': unittest.main()
