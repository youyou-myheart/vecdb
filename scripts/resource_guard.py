"""Read-only macOS resource sampling and budgets for an owned native test run."""
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import threading
import time

GIB = 1024**3


def tree_bytes(path):
    allocated = logical = 0
    if not path.exists(): return dict(allocated=0, logical=0)
    for directory, _, files in os.walk(path, followlinks=False):
        for name in files:
            try: stat = (Path(directory)/name).lstat()
            except FileNotFoundError: continue
            allocated += stat.st_blocks*512
            logical += stat.st_size
    return dict(allocated=allocated, logical=logical)


def host_sample(root, pids=()):
    def command(args):
        return subprocess.check_output(args, text=True, timeout=3, stderr=subprocess.DEVNULL)
    pressure = int(command(['sysctl', '-n', 'kern.memorystatus_vm_pressure_level']))
    memory = int(command(['sysctl', '-n', 'hw.memsize']))
    cpus = int(command(['sysctl', '-n', 'hw.logicalcpu']))
    swap = command(['sysctl', '-n', 'vm.swapusage']).strip()
    vm = command(['vm_stat'])
    page = int(re.search(r'page size of (\d+) bytes', vm)[1])
    fields = {k: int(v)*page for k,v in re.findall(r'^([^:\n]+):\s+(\d+)\.', vm, re.M)}
    processes = []
    for line in command(['ps','-axo','pid=,%cpu=,rss=']).splitlines():
        pid,cpu,rss = line.split()
        processes.append(dict(pid=int(pid),cpu_percent=float(cpu),rss_bytes=int(rss)*1024))
    owned = [p for p in processes if p['pid'] in pids]
    return dict(monotonic=time.monotonic(),disk_free_bytes=shutil.disk_usage(root).free,
        memory_bytes=memory,memory_pressure=pressure,swap=swap,logical_cpus=cpus,
        free_bytes=fields.get('Pages free'),inactive_bytes=fields.get('Pages inactive'),
        compressed_bytes=fields.get('Pages occupied by compressor'),
        system_cpu_percent=sum(p['cpu_percent'] for p in processes),
        owned_cpu_percent=sum(p['cpu_percent'] for p in owned),
        owned_rss_bytes=sum(p['rss_bytes'] for p in owned),owned_processes=owned)


def limit_reason(row, *, limit_owned_disk=True):
    # Stop below the hard 2 GiB allowance to leave room for shutdown/compaction.
    if row['disk_free_bytes'] <= 15*GIB: return 'disk free at or below 15 GiB'
    if row['memory_pressure'] != 1: return 'macOS memory pressure is not normal'
    if limit_owned_disk and row.get('owned_bytes',0) >= 1.5*GIB: return 'owned data/logs reached 1.5 GiB soft limit (2 GiB hard budget)'
    if row.get('elapsed_seconds',0) >= 240: return 'trial reached 240 second runtime budget'
    return None


class ResourceGuard:
    limit_owned_disk = True
    limit_busy_cpu = True

    def __init__(self, fixture, report_root):
        self.fixture, self.report_root = fixture, report_root
        self.stop = threading.Event()
        self.reason = None
        self.rows = []
        self.started = time.monotonic()
        self.thread = threading.Thread(target=self.run, name='resource-guard')

    def processes(self):
        return list(self.fixture.c.children.values()) + list(self.fixture.storage.children)

    def sample(self):
        row = host_sample(self.fixture.state_dir.parent, {p.pid for p in self.processes() if p.poll() is None})
        data = tree_bytes(self.fixture.state_dir)
        logs = tree_bytes(self.report_root)
        row.update(elapsed_seconds=time.monotonic()-self.started,phase=self.fixture.measurements.phase,
            owned_bytes=data['allocated']+logs['allocated'],data_bytes=data,report_bytes=logs)
        self.rows.append(row)
        with (self.fixture.logs/'resources.jsonl').open('a') as stream: stream.write(json.dumps(row)+'\n')
        return row

    def trip(self, reason):
        if self.reason is None: self.reason = reason
        self.fixture.work_stop.set()
        # Terminate only handles registered by this fixture. Do not inspect or
        # kill global port occupants. Main's finally block joins and cleans up.
        for p in self.processes():
            if p.poll() is None:
                try: p.terminate()
                except ProcessLookupError: pass

    def run(self):
        busy = slow = 0
        while not self.stop.is_set():
            start = time.monotonic()
            try:
                if self.reason:
                    self.trip(self.reason)
                    self.stop.wait(2)
                    continue
                row = self.sample()
                reason = limit_reason(dict(row, elapsed_seconds=0), limit_owned_disk=self.limit_owned_disk)
                if row.get("elapsed_seconds", 0) >= getattr(getattr(self.fixture, "args", None), "max_seconds", 240):
                    reason = reason or "trial reached configured runtime budget"
                busy = busy+1 if row['system_cpu_percent'] > row['logical_cpus']*80 else 0
                slow = slow+1 if time.monotonic()-start > 3 else 0
                if self.limit_busy_cpu and busy >= 3:
                    reason = reason or 'system CPU above 80% for three samples'
                reason = reason or ('resource sampler delayed for three samples' if slow>=3 else None)
                if reason: self.trip(reason)
            except Exception as exc:
                self.trip('resource observation failed: '+repr(exc))
            self.stop.wait(2)

    def check(self):
        if self.reason: raise RuntimeError(self.reason)

    def close(self):
        self.stop.set()
        if self.thread.ident: self.thread.join(timeout=15)
        if self.thread.is_alive(): raise RuntimeError('resource guard did not stop')

    def summary(self):
        return dict(stop_reason=self.reason,samples=len(self.rows),
            peak_owned_bytes=max((r['owned_bytes'] for r in self.rows),default=0),
            peak_owned_rss_bytes=max((r['owned_rss_bytes'] for r in self.rows),default=0),
            peak_owned_cpu_percent=max((r['owned_cpu_percent'] for r in self.rows),default=0),
            peak_system_cpu_percent=max((r['system_cpu_percent'] for r in self.rows),default=0),
            min_disk_free_bytes=min((r['disk_free_bytes'] for r in self.rows),default=None))


def clean_owned_state(path, expected_parent, run_id, all_stopped):
    """Delete only a newly marked trial directory, after every child exited."""
    marker = path/'.capacity-owned'
    if not all_stopped or path.is_symlink() or path.name != run_id or path.parent.resolve() != expected_parent.resolve():
        raise RuntimeError('refusing cleanup outside stopped owned trial')
    if marker.read_text() != run_id: raise RuntimeError('trial ownership marker mismatch')
    shutil.rmtree(path)
