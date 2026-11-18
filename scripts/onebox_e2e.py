"""Start an isolated native onebox and exercise sustained load and real failures."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from zoneinfo import ZoneInfo

from cluster_local import LocalCluster, ControlServer, ROOT
from quality import save
from verify_dispatch import converged
from pipeline_probe import collect, interval, drain_summary
sys.path.insert(0, str(ROOT/'sdk/python'))
from vectorstore import Client

DIMENSION, DATA_PARTITIONS, INDEX_PARTITIONS, RECORDS = 16, 16, 8, 768
CENTERS = [[float(k*20)]+[0.0]*(DIMENSION-1) for k in range(INDEX_PARTITIONS)]


def vector_for(number, revision):
    center = (number+revision//3) % INDEX_PARTITIONS
    return [CENTERS[center][0]+number*.00001] + [math.sin(number*(d+1)+revision)*.03 for d in range(1, DIMENSION)]


def percentile(values, fraction):
    if not values:
        return None
    return sorted(values)[min(len(values)-1, math.ceil(len(values)*fraction)-1)]


def operation_name(method, path):
    clean = path.split('?', 1)[0]
    resource = 'record-list' if clean == '/v1/records' else clean.split('/')[2]
    return method+' '+resource


class Measurements:
    def __init__(self):
        self.lock = threading.Lock()
        self.phase = 'startup'
        self.data = {}

    def record(self, operation, seconds, error=None, phase=None):
        with self.lock:
            row = self.data.setdefault(phase or self.phase, {}).setdefault(operation, dict(latencies=[], errors={}))
            row['latencies'].append(seconds)
            if error:
                row['errors'][error] = row['errors'].get(error, 0)+1

    def summary(self):
        with self.lock:
            return {phase: {name: dict(count=len(row['latencies']), errors=dict(row['errors']),
                p50_ms=percentile(row['latencies'], .5)*1000, p95_ms=percentile(row['latencies'], .95)*1000,
                p99_ms=percentile(row['latencies'], .99)*1000, max_ms=max(row['latencies'])*1000)
                for name, row in operations.items()} for phase, operations in self.data.items()}


class Storage:
    """Only owns processes and data directories created by this run."""
    def __init__(self, directory, logs, base):
        self.directory, self.logs, self.base = directory, logs, base
        self.children, self.current = [], {}
        self.directory.mkdir(parents=True)
        pd = directory/'pd'; pd.mkdir()
        tikv = directory/'tikv'; tikv.mkdir()
        weed = directory/'seaweed'; weed.mkdir()
        pd_config = directory/'pd.toml'
        pd_config.write_text('[replication]\nmax-replicas = 1\n')
        self.commands = {
            'pd': [str(ROOT/'.tools/tiup-home/components/pd/v8.5.8/pd-server'), '--name=onebox-pd', '--data-dir='+str(pd),
                '--config='+str(pd_config), f'--client-urls=http://127.0.0.1:{base}', f'--advertise-client-urls=http://127.0.0.1:{base}',
                f'--peer-urls=http://127.0.0.1:{base+1}', f'--advertise-peer-urls=http://127.0.0.1:{base+1}'],
            'tikv': [str(ROOT/'.tools/tiup-home/components/tikv/v8.5.8/tikv-server'), '--config='+str(ROOT/'tikv-local.toml'),
                f'--addr=127.0.0.1:{base+2}', f'--advertise-addr=127.0.0.1:{base+2}', f'--status-addr=127.0.0.1:{base+3}',
                f'--pd=127.0.0.1:{base}', '--data-dir='+str(tikv)],
            'seaweed': [str(ROOT/'.tools/weed'), 'server', '-dir='+str(weed), '-ip=127.0.0.1', '-ip.bind=127.0.0.1',
                f'-master.port={base+10}', f'-volume.port={base+11}', f'-filer.port={base+12}', f'-s3.port={base+13}',
                '-volume.max=8', '-master.volumeSizeLimitMB=128', '-master.telemetry=false', '-s3', '-s3.config='+str(ROOT/'s3.json')],
        }

    def launch(self, name):
        with (self.logs/(name+'.log')).open('ab') as log:
            p = subprocess.Popen(self.commands[name], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        self.children.append(p); self.current[name] = p
        port = self.base+dict(pd=0, tikv=2, seaweed=13)[name]
        end = time.monotonic()+90
        while time.monotonic()<end:
            if p.poll() is not None:
                raise RuntimeError(f'{name} exited {p.returncode}; see {self.logs}')
            try:
                with socket.create_connection(('127.0.0.1', port), timeout=.5):
                    return
            except OSError:
                time.sleep(.2)
        raise TimeoutError(name+' startup')

    def start(self):
        for name in ('pd', 'tikv', 'seaweed'):
            self.launch(name)
        # A listening S3 socket can precede filer/volume readiness.
        import boto3
        from botocore.config import Config
        client = boto3.client('s3', endpoint_url=f'http://127.0.0.1:{self.base+13}', aws_access_key_id='poc-access',
            aws_secret_access_key='poc-secret-local-only', region_name='us-east-1',
            config=Config(connect_timeout=2, read_timeout=3, retries={'max_attempts': 0}))
        end = time.monotonic()+60
        while True:
            try:
                client.list_buckets(); break
            except Exception:
                if time.monotonic()>end: raise
                time.sleep(.2)

    def kill_all(self):
        for p in self.current.values():
            if p.poll() is None: p.kill()
        for p in self.current.values(): p.wait(timeout=15)

    def close(self):
        for p in self.children:
            if p.poll() is None:
                p.send_signal(signal.SIGCONT); p.terminate()
        for p in self.children:
            try: p.wait(timeout=10)
            except subprocess.TimeoutExpired: p.kill(); p.wait(timeout=10)


class Oracle:
    def __init__(self, transport, capacity=RECORDS, vector_factory=vector_for, payload_factory=None):
        self.transport = transport
        self.vector_factory = vector_factory
        self.payload_factory = payload_factory or (lambda n,r: dict(group=str(n%8),revision=str(r)))
        self.records = {}
        self.lock = threading.Lock()
        self.id_locks = [threading.Lock() for _ in range(capacity)]
        self.receipts = {}
        self.acknowledged = {}
        self.last_ack = 0.0

    def mutate(self, n, *, initial=False, deleted=None):
        id = f'v-{n:04d}'
        with self.id_locks[n]:
            with self.lock:
                previous = self.records.get(id, {}).get('revision', 0)
            revision = previous+1
            deleted = not initial and revision % 11 == 0 if deleted is None else deleted
            request = dict(request_id=f'{id}-r{revision}', expected_revision=previous)
            vector = [] if deleted else self.vector_factory(n, revision)
            payload = {} if deleted else self.payload_factory(n, revision)
            if not deleted: request.update(vector=vector, payload=payload)
            method = 'DELETE' if deleted else 'PUT'
            response = self.transport('/v1/records/'+id, method, request, budget=120)
            acknowledged = time.monotonic()
            assert response['durable'] and response['token']==dict(id=id, revision=revision), response
            record = dict(id=id, revision=revision, vector=vector, payload=payload, deleted=deleted)
            with self.lock:
                self.records[id] = record
                self.receipts[id] = (method, request, response['token'])
                self.last_ack = max(self.last_ack, acknowledged)
                self.acknowledged[id] = dict(revision=revision,request_id=request['request_id'],monotonic=acknowledged)
            actual = self.transport('/v1/records/'+id)
            self.check_record(actual, record)
            if revision % 7 == 0:
                replay = self.transport('/v1/records/'+id, method, request)
                assert replay['token'] == response['token'], 'idempotency allocated another revision'
            return record

    @staticmethod
    def check_record(actual, expected):
        for key in ('id', 'revision', 'deleted'):
            assert actual.get(key) == expected[key], (key, actual, expected)
        assert (actual.get('vector') or []) == expected['vector'], (actual, expected)
        assert (actual.get('payload') or {}) == expected['payload'], (actual, expected)

    def snapshot(self):
        with self.lock: return dict(self.records)


class Onebox:
    def __init__(self, args):
        self.args = args
        self.dimension = getattr(args, 'dimension', DIMENSION)
        self.centers = getattr(args, 'centers', CENTERS)
        self.run_id = datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%dT%H%M%S')+'-'+uuid.uuid4().hex[:8]
        self.logs = getattr(args, 'report_root', ROOT/'reports/onebox-e2e')/self.run_id; self.logs.mkdir(parents=True)
        self.state_dir = getattr(args, 'state_root', ROOT/'.state/onebox-e2e')/self.run_id
        self.control = ROOT/'.state/onebox-e2e.sock'
        if self.control.exists(): raise RuntimeError('onebox control socket already exists; inspect existing cluster first')
        self.measurements = Measurements()
        self.report = dict(status='running', run_id=self.run_id, started_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
            commit=subprocess.check_output(['git','rev-parse','HEAD'], cwd=ROOT, text=True).strip(),
            dirty=bool(subprocess.check_output(['git','status','--porcelain'],cwd=ROOT)),
            topology=dict(pd=1,tikv=1,seaweed=1,master=2,gateway=2,worker=3,data_partitions=16,index_partitions=8,replicas=2,dimension=self.dimension),
            requested_steady_seconds=args.steady_seconds, continuous_fault_rounds=args.rounds,
            latency_only=args.latency_only, cases=[])
        base = args.base_port
        required = [base+i for i in (0,1,2,3,10,11,12,13,100,101,102,103,104,105,135,180,181,10010,10011,10012)]
        for port in required:
            with socket.socket() as probe:
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                    probe.bind(('127.0.0.1', port))
                except OSError as exc:
                    raise RuntimeError(f'onebox port {port} is already occupied') from exc
        route = self.logs/'routing.json'; route.write_text(json.dumps(dict(version=1, centers=self.centers)))
        env = dict(os.environ, API_TOKEN='onebox-api-'+uuid.uuid4().hex, CLUSTER_TOKEN='onebox-peer-'+uuid.uuid4().hex,
            PD_ENDPOINTS=f'127.0.0.1:{base}', S3_ENDPOINT=f'127.0.0.1:{base+13}', S3_BUCKET='onebox-e2e',
            S3_ACCESS_KEY='poc-access', S3_SECRET_KEY='poc-secret-local-only', INDEX_REPLICATION='graph', CONSUMER_RETENTION='1',
            LEASE_TTL='6s', CLUSTER_ROUTING_FILE=str(route))
        self.c = LocalCluster('onebox-e2e/'+self.run_id, partitions=16, dimension=self.dimension, base_port=base+100,
            grpc_port=base+180, replicas=2, scheduler='load', environment=env, directory=self.logs/'services')
        self.storage = Storage(self.state_dir, self.logs, base)
        self.slots = {}
        self.urls = [f'http://127.0.0.1:{base+102}', f'http://127.0.0.1:{base+135}']
        self.request_counter = 0
        self.request_lock = threading.Lock()
        self.oracle = Oracle(self.request, getattr(args, 'oracle_capacity', RECORDS), getattr(args, 'vector_factory', vector_for), getattr(args, 'payload_factory', None))
        self.work_stop = threading.Event(); self.workers = []
        self.failures = []; self.failure_lock = threading.Lock()
        self.watch_stop = threading.Event(); self.watcher = None
        self.last_completed = {}; self.last_head = {}; self.last_terms = {}
        self.server = None
        self.paused = set()
        self.persist()

    def persist(self):
        self.report['measurements'] = self.measurements.summary()
        save(self.logs/'report.json', self.report)
        save(ROOT/'reports/onebox-e2e-latest.json', self.report)

    def request(self, path, method='GET', data=None, *, budget=40, timeout=3, url=None, retry=True):
        started = time.monotonic(); deadline = started+budget
        phase = self.measurements.phase
        while True:
            with self.request_lock:
                endpoint = url or self.urls[self.request_counter % len(self.urls)]
                self.request_counter += 1
            req = urllib.request.Request(endpoint+path, method=method,
                headers={'Authorization':'Bearer '+self.c.env['API_TOKEN'], 'Content-Type':'application/json'},
                data=None if data is None else json.dumps(data).encode())
            attempt = time.monotonic()
            try:
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    value = json.load(response)
                self.measurements.record(operation_name(method,path), time.monotonic()-started, phase=phase)
                return value
            except urllib.error.HTTPError as exc:
                if exc.code not in (429, 500, 502, 503, 504):
                    raise AssertionError((method, path, exc.code, exc.read().decode())) from exc
                error = 'HTTP_'+str(exc.code)
            except (OSError, TimeoutError) as exc:
                error = type(exc).__name__
            self.measurements.record('attempt_failure', time.monotonic()-attempt, error=error, phase=phase)
            if not retry or time.monotonic() >= deadline:
                raise TimeoutError(f'{method} {path}: {error}')
            time.sleep(.1)

    def launch(self, slot):
        role = 'master' if slot in (0,1) else ('gateway' if slot in (2,35) else 'worker')
        node = self.c.launch(role, slot, grpc_port=self.args.base_port+(181 if slot==35 else 180) if role=='gateway' else None)
        self.slots[slot] = node
        if role == 'worker': self.c.workers.append(node)
        self.report['processes'] = self.inventory()
        return node

    def inventory(self):
        values = {name:dict(pid=p.pid,exit_code=p.poll(),kind='storage') for name,p in self.storage.current.items()}
        values.update({str(slot):dict(node=node,pid=self.c.children[node].pid,exit_code=self.c.children[node].poll(),url=self.c.urls[node]) for slot,node in self.slots.items()})
        return values

    def kill(self, slots):
        for slot in slots:
            node = self.slots[slot]; p = self.c.children[node]
            if p.poll() is None: p.kill()
        for slot in slots:
            node = self.slots[slot]; self.c.children[node].wait(timeout=15)
            if node in self.c.workers: self.c.workers.remove(node)
            self.paused.discard(self.c.children[node].pid)

    def slot_for(self, node):
        return next(slot for slot, value in self.slots.items() if value==node)

    def state(self):
        s = self.request('/v1/cluster', budget=10)
        assert s['routing']['centers']==self.centers and s['routing']['version']==1
        for id,d in s.get('data_progress',{}).items():
            assert d['floor']<=d['completed_lsn']<=d['head'], d
            assert d['completed_lsn']>=self.last_completed.get(id,0), ('completed regressed', id, d)
            assert d['head']>=self.last_head.get(id,0), ('head regressed', id, d)
            self.last_completed[id]=d['completed_lsn']; self.last_head[id]=d['head']
        for id,h in s.get('graph_heads',{}).items():
            assert h['term']>=self.last_terms.get(id,0), ('graph term regressed',id,h)
            self.last_terms[id]=h['term']
        self.report['last_cluster_status']=s
        return s

    def wait(self, name, check, seconds=180):
        started=time.monotonic()
        while time.monotonic()-started<seconds:
            self.check_background()
            if check(): return round(time.monotonic()-started,3)
            time.sleep(.25)
        raise TimeoutError(name)

    def caught(self):
        return converged(self.state(), DATA_PARTITIONS, INDEX_PARTITIONS)

    def check_background(self):
        with self.failure_lock:
            if self.failures: raise AssertionError(self.failures)

    def local_metrics(self, *, progress_only=False):
        endpoints = {node:self.c.urls[node] for slot,node in list(self.slots.items()) if slot in (2,35,3,4,5)}
        return collect(endpoints, self.c.env['API_TOKEN'], progress_only=progress_only)

    def local_progress(self):
        return self.local_metrics(progress_only=True)

    def drain(self):
        started = time.monotonic(); rows = []
        path = self.logs/(self.measurements.phase+'-drain.jsonl')
        with path.open('w') as output:
            while time.monotonic()-started < 180:
                self.check_background()
                with ThreadPoolExecutor(max_workers=2) as pool:
                    status = pool.submit(self.state)
                    local = self.local_metrics()
                    state = status.result()
                row = dict(elapsed_seconds=round(time.monotonic()-started,6),state=state,local=local)
                rows.append(row); output.write(json.dumps(row)+'\n'); output.flush()
                if converged(state,DATA_PARTITIONS,INDEX_PARTITIONS):
                    summary = drain_summary(rows)
                    # Missing local series must not silently claim full observability.
                    if len(summary['first_observed_seconds']) == 5:
                        save(self.logs/(self.measurements.phase+'-latency.json'),summary)
                        return summary
                time.sleep(.1)
        raise TimeoutError('all source and replica positions converge')

    def sample(self):
        while not self.watch_stop.is_set():
            row=dict(time=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),phase=self.measurements.phase,processes=self.inventory(),local=self.local_metrics())
            pids=[str(p.pid) for p in list(self.c.children.values())+list(self.storage.current.values()) if p.poll() is None]
            if pids:
                result=subprocess.run(['ps','-o','pid=,rss=,%cpu=','-p',','.join(pids)],capture_output=True,text=True)
                row['process_rss_kib_cpu']=result.stdout.strip()
            try:
                req=urllib.request.Request(self.urls[0]+'/metrics',headers={'Authorization':'Bearer '+self.c.env['API_TOKEN']})
                with urllib.request.urlopen(req,timeout=2) as response: row['metrics']=response.read().decode()
            except OSError as exc: row['scrape_error']=type(exc).__name__
            with (self.logs/'observations.jsonl').open('a') as file: file.write(json.dumps(row)+'\n')
            self.watch_stop.wait(5)

    def start_workload(self):
        assert not self.workers
        self.work_stop.clear()
        def write_loop(shard):
            n=0
            try:
                while not self.work_stop.is_set():
                    self.oracle.mutate(shard*128+n%128); n+=1
                    self.work_stop.wait(.2)
            except BaseException as exc:
                with self.failure_lock: self.failures.append(('writer',shard,repr(exc)))
        def read_loop(shard):
            n=0
            try:
                while not self.work_stop.is_set():
                    anchor=512+(n+shard*7)%128
                    record=self.request('/v1/records/'+f'v-{anchor:04d}')
                    self.oracle.check_record(record,self.oracle.snapshot()[record['id']])
                    result=self.request('/v1/search','POST',dict(vector=vector_for(anchor,1),k=10),budget=120)
                    hits=result['hits'] or []
                    assert len({h['id'] for h in hits})==len(hits), 'duplicate search ID'
                    assert all(math.isfinite(h['distance']) and h['distance']>=0 for h in hits)
                    n+=1; self.work_stop.wait(.4)
            except BaseException as exc:
                with self.failure_lock: self.failures.append(('reader',shard,repr(exc)))
        self.workers=[threading.Thread(target=write_loop,args=(i,)) for i in range(4)]+[threading.Thread(target=read_loop,args=(i,)) for i in range(2)]
        for worker in self.workers: worker.start()

    def stop_workload(self):
        self.work_stop.set()
        for worker in self.workers:
            worker.join(timeout=130)
            if worker.is_alive(): raise TimeoutError('workload thread did not finish')
        self.workers=[]
        self.check_background()

    def hold(self, seconds):
        end=time.monotonic()+seconds
        while time.monotonic()<end:
            self.check_background()
            time.sleep(min(.25,max(0,end-time.monotonic())))

    def verify(self):
        assert not self.workers, 'full oracle requires quiescent writers'
        assert all(p['exit_code'] is None for p in self.inventory().values()), ('unexpected process exit',self.inventory())
        drain = self.drain()
        elapsed = drain['all_converged_seconds']
        expected=self.oracle.snapshot(); found={}; cursor=''
        save(self.logs/'oracle.json',expected)
        self.report['oracle_sha256']=hashlib.sha256(json.dumps(expected,sort_keys=True).encode()).hexdigest()
        while True:
            page=self.request('/v1/records?include_deleted=true&limit=83'+('&cursor='+cursor if cursor else ''))
            for record in page['records']:
                assert record['id'] not in found
                self.oracle.check_record(record,expected[record['id']]); found[record['id']]=record
            cursor=page.get('next_cursor','')
            if not cursor: break
        assert set(found)==set(expected), (len(found),len(expected))
        query=[0.0]*DIMENSION
        active={id:r for id,r in expected.items() if not r['deleted']}
        for url in self.urls:
            result=self.request('/v1/search','POST',dict(vector=query,k=1000,exact=True),url=url,timeout=30)
            hits=result['hits'] or []
            assert len(hits)==len(active) and len({h['id'] for h in hits})==len(active), (len(hits),len(active))
            for hit in hits:
                r=active[hit['id']]
                assert hit['revision']==r['revision'] and (hit.get('payload') or {})==r['payload'], (hit,r)
                assert math.isclose(hit['distance'],sum(v*v for v in r['vector']),rel_tol=1e-9,abs_tol=1e-6)
            assert [(h['distance'],h['id']) for h in hits]==sorted((h['distance'],h['id']) for h in hits)
        for port in (self.args.base_port+180,self.args.base_port+181):
            with Client(f'127.0.0.1:{port}',self.c.env['API_TOKEN'],timeout=40) as client:
                assert client.layout().data_partitions==16 and len(client.layout().routing.centers)==8
                for n in (512,543,639):
                    query=expected[f'v-{n:04d}']['vector']
                    exact=client.search(query,k=10,exact=True).hits
                    ann=client.search(query,k=10).hits
                    assert len({h.id for h in exact}&{h.id for h in ann})>=9, 'ANN recall below .9'
                    assert ann[0].id==f'v-{n:04d}' and ann[0].distance==0
                filtered=client.search([0.0]*DIMENSION,k=1000,exact=True,filter={'group':'2'}).hits
                assert {h.id for h in filtered}=={id for id,r in active.items() if r['payload']['group']=='2'}
                assert all(p.owner_available for p in client.cluster().data_progress.values())
        # Replayed writes after failover/cold start must return the old receipt.
        for id in sorted(expected)[::47]:
            method,body,token=self.oracle.receipts[id]
            assert self.request('/v1/records/'+id,method,body)['token']==token
        return dict(catchup_seconds=elapsed,latency=drain,records=len(expected),live_records=len(active),
            source_heads={id:d['head'] for id,d in self.report['last_cluster_status']['data_progress'].items()})

    @contextmanager
    def case(self,name):
        self.measurements.phase=name
        row=dict(name=name,status='running',started_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat())
        self.report['cases'].append(row); self.persist(); print('START '+name,flush=True)
        started=time.monotonic()
        try:
            yield row
            self.check_background(); row['status']='passed'
        except BaseException as exc:
            row.update(status='failed',error=repr(exc)); raise
        finally:
            row['seconds']=round(time.monotonic()-started,3); self.persist()
            print(row['status'].upper()+' '+name+f" ({row['seconds']}s)",flush=True)

    def checkpoint(self):
        self.request('/v1/checkpoint','POST',{},timeout=30,budget=120)
        self.request('/v1/log/prune','POST',{},timeout=30,budget=120)

    def run(self):
        with self.case('boot_all_native_processes') as row:
            self.storage.start()
            for slot in (0,1,2,35,3,4,5): self.launch(slot)
            self.wait('seven vector services ready',lambda: self.state().get('fully_replicated', False))
            assert len({p['pid'] for p in self.inventory().values()})==10
            row['processes']=self.inventory()
        self.watcher=threading.Thread(target=self.sample); self.watcher.start()
        with self.case('seed_structured_data_and_protocols') as row:
            with ThreadPoolExecutor(max_workers=8) as pool: list(pool.map(lambda n:self.oracle.mutate(n,initial=True),range(RECORDS)))
            row.update(self.verify()); self.checkpoint()
        with self.case('steady_concurrent_read_write') as row:
            baseline = self.local_metrics()
            save(self.logs/'steady-metrics-before.json',baseline)
            started=time.monotonic(); self.start_workload()
            for offset in range(0,self.args.steady_seconds,30):
                self.hold(min(30,self.args.steady_seconds-offset)); self.persist()
                print(f"steady {min(offset+30,self.args.steady_seconds)}/{self.args.steady_seconds}s",flush=True)
            self.stop_workload(); row['load_seconds']=round(time.monotonic()-started,3); row.update(self.verify())
            after = self.local_metrics()
            save(self.logs/'steady-metrics-after.json',after)
            row['pipeline_stages'] = interval(baseline,after)
            assert row['pipeline_stages'].get('index_queue_wait/success',{}).get('count',0)>0
            assert row['pipeline_stages'].get('index_consumer_work/success',{}).get('count',0)>0, 'event consumer did not execute'
            assert not any(h['count'] for key,h in row['pipeline_stages'].items() if key.startswith('worker_sync/')), 'periodic Worker sync still drives clustered indexes'
            summary=self.measurements.summary()['steady_concurrent_read_write']
            assert 'attempt_failure' not in summary, summary.get('attempt_failure')
        if not self.args.latency_only:
            self.run_faults()
        self.finish()

    def run_faults(self):
        with self.case('gateway_and_master_single_failures') as row:
            self.start_workload(); self.kill([2]); self.hold(3); self.launch(2)
            before=self.state()['leader']; slot=self.slot_for(before['node']); self.kill([slot])
            row['master_takeover_seconds']=self.wait('standby Master elected',lambda: self.state()['leader']['epoch']>before['epoch'])
            self.launch(slot); self.stop_workload(); row.update(self.verify())
        with self.case('stale_worker_resumes_after_lease_takeover') as row:
            before=self.state(); source=before['data_progress']['data-0000']['owner']; slot=self.slot_for(source['node'])
            p=self.c.children[self.slots[slot]]; p.send_signal(signal.SIGSTOP); self.paused.add(p.pid)
            self.start_workload()
            row['takeover_seconds']=self.wait('source epoch increases',lambda:self.state()['data_progress']['data-0000']['owner']['epoch']>source['epoch'])
            p.send_signal(signal.SIGCONT); self.paused.discard(p.pid)
            self.stop_workload(); row.update(self.verify())
        with self.case('S3_pause_with_durable_writes') as row:
            p=self.storage.current['seaweed']; p.send_signal(signal.SIGSTOP); self.paused.add(p.pid)
            self.start_workload(); self.hold(8)
            s=self.state(); row['peak_pending']=sum(d['head']-d['completed_lsn'] for d in s['data_progress'].values())
            assert row['peak_pending']>0
            p.send_signal(signal.SIGCONT); self.paused.discard(p.pid)
            self.stop_workload(); row.update(self.verify())
        with self.case('TiKV_pause_and_uncertain_client_retries') as row:
            self.start_workload(); p=self.storage.current['tikv']; p.send_signal(signal.SIGSTOP); self.paused.add(p.pid)
            self.hold(8); p.send_signal(signal.SIGCONT); self.paused.discard(p.pid)
            self.stop_workload(); row.update(self.verify())
            assert self.measurements.summary()['TiKV_pause_and_uncertain_client_retries'].get('attempt_failure')
        with self.case('two_workers_master_gateway_overlap') as row:
            self.checkpoint(); before=self.state(); master=self.slot_for(before['leader']['node'])
            self.start_workload(); self.kill([3,4,master,2]); self.hold(5)
            for slot in (master,3,4,2): self.launch(slot)
            self.stop_workload(); row.update(self.verify())
        with self.case('all_workers_lost_raw_data_still_available') as row:
            self.checkpoint(); self.kill([3,4,5])
            with ThreadPoolExecutor(max_workers=8) as pool: list(pool.map(lambda n:self.oracle.mutate(640+n%128),range(512)))
            s=self.state(); row['peak_pending']=sum(d['head']-d['completed_lsn'] for d in s['data_progress'].values())
            assert row['peak_pending']>=512
            for slot in (3,4,5): self.launch(slot)
            row.update(self.verify())
        with self.case('continuous_failures_without_catchup_barriers') as row:
            self.start_workload(); row['rounds']=[]
            for n in range(self.args.rounds):
                started=time.monotonic(); slots=[3+n%3]
                if n%2==1: slots.append(0 if n%4==1 else 1)
                self.kill(slots); self.hold(1)
                for slot in slots: self.launch(slot)
                # Immediately inject the next failure; no ready/caught-up wait.
                row['rounds'].append(dict(round=n+1,slots=slots,seconds=round(time.monotonic()-started,3)))
            self.stop_workload(); row.update(self.verify())
        with self.case('entire_stack_SIGKILL_and_persistent_cold_restart') as row:
            self.checkpoint()
            with ThreadPoolExecutor(max_workers=8) as pool: list(pool.map(lambda n:self.oracle.mutate(640+n%128),range(256)))
            self.kill([0,1,2,35,3,4,5]); self.storage.kill_all()
            assert all(p.poll() is not None for p in self.c.children.values())
            recovery_started=time.monotonic()
            self.storage.start()
            for slot in (0,1,2,35,3,4,5): self.launch(slot)
            row.update(self.verify())
            row['cold_recovery_verified_seconds']=round(time.monotonic()-recovery_started,3)
        with self.case('post_recovery_stable_load_and_final_oracle') as row:
            self.start_workload(); self.hold(self.args.recovery_seconds); self.stop_workload()
            row.update(self.verify()); self.checkpoint()
    def finish(self):
        self.report.update(status='passed',finished_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),processes=self.inventory(),
            endpoints=dict(http=self.urls,grpc=[f'127.0.0.1:{self.args.base_port+180}',f'127.0.0.1:{self.args.base_port+181}']),
            namespace=self.c.namespace,control_socket=str(self.control))
        self.persist()
        if self.args.keep_running:
            self.watch_stop.set()
            self.watcher.join(timeout=10)
            self.server=ControlServer(self.control,self.c)
            envfile=ROOT/'.state/onebox-e2e.env'
            envfile.write_text(f"export API_TOKEN='{self.c.env['API_TOKEN']}'\nexport VECTOR_ENDPOINT='{self.urls[0]}'\nexport VECTOR_CONTROL_SOCKET='{self.control}'\n")
            envfile.chmod(0o600)
            self.report['onebox_running']=True; self.persist()
            print('ONEBOX_ACCEPTANCE_PASSED '+str(self.logs/'report.json'),flush=True)
            print('ONEBOX_READY source '+str(envfile),flush=True)
            while not self.server.stopping: self.server.handle_request()

    def close(self):
        self.work_stop.set()
        for pid in list(self.paused):
            try: os.kill(pid,signal.SIGCONT)
            except ProcessLookupError: pass
        self.watch_stop.set()
        for worker in self.workers: worker.join(timeout=130)
        if self.watcher: self.watcher.join(timeout=10)
        self.c.close(); self.storage.close()
        if self.server:
            self.server.server_close(); self.control.unlink(missing_ok=True)
        self.report.update(onebox_running=False,all_children_stopped=all(p.poll() is not None for p in self.c.children.values()) and all(p.poll() is not None for p in self.storage.children))
        self.persist()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--steady-seconds',type=int,default=600)
    parser.add_argument('--recovery-seconds',type=int,default=60)
    parser.add_argument('--rounds',type=int,default=6)
    parser.add_argument('--base-port',type=int,default=25400)
    parser.add_argument('--keep-running',action='store_true')
    parser.add_argument('--latency-only',action='store_true',help='run boot, seed, steady load and measured drain; skip fault cases')
    args=parser.parse_args()
    if args.steady_seconds<1 or args.recovery_seconds<1 or not 1<=args.rounds<=50 or not 1024<=args.base_port<=55000:
        parser.error('positive durations, rounds 1..50, base port 1024..55000 required')
    fixture=None
    def interrupted(signum,frame): raise KeyboardInterrupt
    signal.signal(signal.SIGTERM,interrupted)
    try:
        fixture=Onebox(args); fixture.run()
    except BaseException as exc:
        if fixture and fixture.report['status']!='passed':
            fixture.report.update(status='failed',error=repr(exc)); fixture.persist()
        raise
    finally:
        signal.signal(signal.SIGTERM,signal.SIG_IGN); signal.signal(signal.SIGINT,signal.SIG_IGN)
        if fixture: fixture.close()


if __name__=='__main__': main()
