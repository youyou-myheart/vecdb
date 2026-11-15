"""Foreground local multi-process cluster. Only manages children it starts."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import socketserver
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parent.parent


class LocalCluster:
    def __init__(self, namespace, partitions=32, dimension=16, base_port=18200,
                 directory=None, environment=None, grpc_port=None, replicas=1, scheduler="hash"):
        if not namespace or not 1 <= partitions <= 64 or not 1 <= dimension <= 4096:
            raise ValueError('namespace required; partitions 1..64; dimension 1..4096')
        if not 1024 <= base_port <= 65500:
            raise ValueError('base port must be 1024..65500')
        if grpc_port is not None and (not 1024 <= grpc_port <= 65535 or base_port <= grpc_port <= base_port + 34):
            raise ValueError("gRPC port must be 1024..65535 and outside the HTTP port range")
        if replicas not in (1,2):
            raise ValueError("replicas must be 1 or 2")
        if scheduler not in ("hash", "load") or (scheduler == "load" and replicas != 2):
            raise ValueError("load scheduler requires replicas=2")
        self.scheduler = scheduler
        self.replicas = replicas
        self.urls = {}
        self.grpc_port = grpc_port
        self.env = dict(os.environ if environment is None else environment)
        self.env.pop("GRPC_LISTEN_ADDR", None)  # Only the Gateway receives the dedicated listener.
        for key in ('API_TOKEN', 'CLUSTER_TOKEN'):
            if len(self.env.get(key, '')) < 16:
                raise ValueError(key + ' must contain at least 16 characters')
        self.env.update(CLUSTER_MODE='1', NAMESPACE=namespace,
                        PARTITIONS=str(partitions), DIMENSION=str(dimension), REPLICAS=str(replicas), SCHEDULER=scheduler)
        self.namespace, self.partitions, self.base_port = namespace, partitions, base_port
        self.directory = Path(directory or ROOT / '.state' / ('cluster-' + uuid.uuid4().hex))
        self.children = {}
        self.workers = []
        self.prefix = uuid.uuid4().hex[:10]
        self.generation = 0
        self.gateway = 'http://127.0.0.1:' + str(base_port + 2)

    def request(self, path, method='GET', data=None, url=None):
        req = urllib.request.Request((url or self.gateway) + path, method=method,
            headers={'Authorization': 'Bearer ' + self.env['API_TOKEN'],
                     'Content-Type': 'application/json'},
            data=None if data is None else json.dumps(data).encode())
        with urllib.request.urlopen(req, timeout=35) as response:
            return json.load(response)

    def launch(self, role, slot):
        port = self.base_port + slot
        # Check before spawn; the server remains authoritative if another process races us.
        with socket.socket() as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind(('127.0.0.1', port))
        self.generation += 1
        node = f'{self.prefix}-{role}-{slot}-{self.generation}'
        url = f'http://127.0.0.1:{port}'
        self.directory.mkdir(parents=True, exist_ok=True)
        log = (self.directory / (node + '.log')).open('a')
        try:
            proc = subprocess.Popen([str(ROOT / '.tools/vector-server')], cwd=ROOT,
                env=dict(self.env, GRPC_LISTEN_ADDR=(f'127.0.0.1:{self.grpc_port}' if role == 'gateway' and self.grpc_port else ''), NODE_ID=node, ROLES=role,
                         LISTEN_ADDR=f'127.0.0.1:{port}', ADVERTISE_URL=url),
                stdout=log, stderr=log)
        finally:
            log.close()
        self.children[node] = proc
        self.urls[node] = url
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    raise RuntimeError(f'{node} exited ({proc.returncode}); see {self.directory}')
                try:
                    health = self.request('/healthz', url=url)
                    if health['namespace'] != self.namespace:
                        raise RuntimeError('unexpected process on ' + url)
                    return node
                except (OSError, urllib.error.URLError):
                    time.sleep(.1)
            raise TimeoutError('process startup: ' + node)
        except BaseException:
            self.stop(node)
            raise

    def start(self, workers=2):
        self.validate_workers(workers)
        if self.children:
            raise RuntimeError('cluster already started')
        try:
            self.launch('master', 0)
            self.launch('master', 1)
            self.launch('gateway', 2)
            self.scale(workers)
        except BaseException:
            self.close()
            raise

    @staticmethod
    def validate_workers(count):
        if not isinstance(count, int) or not 0 <= count <= 32:
            raise ValueError('workers must be 0..32')

    def scale(self, count):
        self.validate_workers(count)
        while len(self.workers) > count:
            node = self.workers[-1]
            if self.replicas > 1 and count > 0:
                self.request('/v1/drain', 'POST', {}, url=self.urls[node])
                deadline = time.monotonic() + 90
                while True:
                    state = self.status()
                    plans = state.get('replica_plans', {})
                    if state['ready'] and len(plans) == self.partitions and all(
                            all(a['node'] != node for a in p['serving']) for p in plans.values()):
                        break
                    if time.monotonic() >= deadline:
                        raise TimeoutError('drain incomplete; Worker kept running: ' + node)
                    time.sleep(.2)
            self.stop(node)
            self.workers.pop()
        while len(self.workers) < count:
            self.workers.append(self.launch('worker', 3 + len(self.workers)))

    def status(self):
        state = self.request('/v1/cluster')
        state['processes'] = {n: {'pid': p.pid, 'exit_code': p.poll()}
                              for n, p in self.children.items()}
        state['desired_workers'] = list(self.workers)
        return state

    def wait_ready(self, timeout=60):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            last = self.status()
            owners, assignments = last['owners'], last['assignments']
            if self.replicas > 1:
                plans = last.get('replica_plans', {})
                replicas = last.get('replica_owners', {})
                if (self.workers and last['ready'] and last['master_available']
                    and len(plans) == self.partitions
                    and all(len(p['desired']) == min(self.replicas,len(self.workers))
                            and {a['partition'] for a in p['serving']} == {a['partition'] for a in p['desired']}
                            and all(a['node'] in self.workers for a in p['desired']) for p in plans.values())
                    and all(len(replicas.get(i,{}).get('owners',[])) >= min(self.replicas,len(self.workers))
                            and plans[i]['publisher'] in {o['partition'] for o in replicas.get(i,{}).get('owners',[])} for i in plans)):
                    return last
                time.sleep(.2)
                continue

            if (self.workers and last['ready'] and last['master_available']
                and len(owners) == self.partitions and len(assignments) == self.partitions
                and all(a['node'] == max(self.workers, key=lambda node:
                        hashlib.sha256((index + '\0' + node).encode()).digest())
                        for index, a in assignments.items())
                and all(o['node'] == assignments[i]['node']
                        and o['session'] == assignments[i]['session']
                        for i, o in owners.items())
                and {n['id'] for n in last['nodes'] if 'worker' in n['roles']} == set(self.workers)):
                return last
            time.sleep(.2)
        raise TimeoutError('cluster did not converge: ' + json.dumps(last))

    def stop(self, node):
        proc = self.children[node]
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)

    def close(self):
        # Signal every child first, then reap; no PID files or unrelated processes.
        for proc in self.children.values():
            if proc.poll() is None:
                proc.terminate()
        for node in reversed(list(self.children)):
            self.stop(node)


class ControlServer(socketserver.UnixStreamServer):
    """Single-threaded commands serialize lifecycle changes; socket is owner-only."""
    def __init__(self, path, cluster):
        self.cluster = cluster
        self.stopping = False
        super().__init__(str(path), ControlHandler)
        os.chmod(path, 0o600)
        self.timeout = .5

    def handle_error(self, request, client_address):
        # A disconnected CLI must not abort accepted lifecycle operations.
        pass


class ControlHandler(socketserver.StreamRequestHandler):
    def handle(self):
        self.connection.settimeout(5)
        try:
            raw = self.rfile.readline(4097)
            if len(raw) > 4096 or not raw.endswith(b'\n'):
                raise ValueError('request must be one JSON line, at most 4096 bytes')
            request = json.loads(raw)
            if not isinstance(request, dict):
                raise ValueError('request must be an object')
            action = request.get('action')
            allowed = {'action', 'workers'} if action == 'scale' else {'action'}
            if set(request) - allowed:
                raise ValueError('unknown request fields')
            cluster = self.server.cluster
            if action == 'local-status':
                result = cluster.status()
            elif action == 'scale':
                count = request.get('workers')
                if type(count) is not int:
                    raise ValueError('workers must be an integer')
                cluster.scale(count)
                result = cluster.wait_ready() if count else {'desired_workers': [], 'ready': False}
            elif action == 'stop':
                self.server.stopping = True
                result = {'stopping': True}
            else:
                raise ValueError('unknown control action')
            reply = {'ok': True, 'result': result}
        except Exception as exc:
            reply = {'ok': False, 'error': str(exc)}
        self.wfile.write((json.dumps(reply) + '\n').encode())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--namespace', required=True)
    parser.add_argument('--workers', type=int, default=2)
    parser.add_argument('--partitions', type=int, default=32)
    parser.add_argument('--dimension', type=int, default=16)
    parser.add_argument('--base-port', type=int, default=18200)
    parser.add_argument('--scheduler', choices=('hash','load'), default='hash')
    parser.add_argument('--replicas', type=int, default=1)
    parser.add_argument('--grpc-port', type=int, help='optional Gateway gRPC port')
    parser.add_argument('--control-socket', help='Unix socket for vectorctl; foreground service mode')
    args = parser.parse_args()
    cluster = LocalCluster(args.namespace, args.partitions, args.dimension, args.base_port, grpc_port=args.grpc_port, replicas=args.replicas, scheduler=args.scheduler)
    def interrupted(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    server = None
    control_path = None
    try:
        if args.control_socket:
            control_path = Path(args.control_socket).absolute()
            control_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            # Never remove an existing socket: it may belong to another live supervisor.
            server = ControlServer(control_path, cluster)
        cluster.start(args.workers)
        if args.workers:
            cluster.wait_ready()
        print(f'Gateway: {cluster.gateway}\nLogs: {cluster.directory}', flush=True)
        if args.grpc_port:
            print(f'gRPC: 127.0.0.1:{args.grpc_port}', flush=True)
        if server is not None:
            print(f'Control socket: {control_path}', flush=True)
            while not server.stopping:
                server.handle_request()
            return
        print('Commands: status | scale N (0..32) | quit; EOF/Ctrl-C stops children.', flush=True)
        for line in sys.stdin:
            command = line.strip().split()
            try:
                if command == ['quit']:
                    break
                if command == ['status']:
                    print(json.dumps(cluster.status(), indent=2), flush=True)
                elif len(command) == 2 and command[0] == 'scale':
                    count = int(command[1])
                    cluster.scale(count)
                    if count:
                        cluster.wait_ready()
                    print(f'Workers: {count}', flush=True)
                elif command:
                    print('Commands: status | scale N | quit', flush=True)
            except (ValueError, OSError, RuntimeError, TimeoutError) as exc:
                print(str(exc), file=sys.stderr, flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        # A second interrupt must not prevent child cleanup.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        cluster.close()
        if server is not None:
            server.server_close()
            control_path.unlink(missing_ok=True)


if __name__ == '__main__':
    main()
