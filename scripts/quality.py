"""Fail-closed quality gates with unique reports, deadlines and child cleanup."""
import argparse
from dataclasses import dataclass, field
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parent.parent


@dataclass
class Stage:
    name: str
    command: list[str]
    timeout: float = 600
    env: dict = field(default_factory=dict)
    artifacts: tuple[str, ...] = ()


def save(path, report):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(report, indent=2) + '\n')
    temporary.replace(path)


def stop_group(process):
    # Stages own new POSIX sessions, including their descendants. A stage may
    # exit before its workers, or leave a fault-injected worker in SIGSTOP.
    try:
        os.killpg(process.pid, signal.SIGTERM)
        os.killpg(process.pid, signal.SIGCONT)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=5)


def run_stage(stage, directory, root=ROOT):
    result = dict(name=stage.name, command=stage.command, status='running', timeout_seconds=stage.timeout)
    started = time.monotonic()
    process = None
    try:
        for artifact in stage.artifacts:
            (root / artifact).unlink(missing_ok=True)
        env = dict(os.environ, INTEGRATION='0', ONEBOX_ACCEPTANCE='0', ANN_ACCEPTANCE='0',
                   CAPACITY_ACCEPTANCE='0', STRUCTURED_ACCEPTANCE='0')
        env.update(stage.env)
        with (directory / (stage.name + '.log')).open('wb') as log:
            process = subprocess.Popen(stage.command, cwd=root, env=env,
                stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            result['exit_code'] = process.wait(timeout=stage.timeout)
        if result['exit_code']:
            raise RuntimeError(f"exit code {result['exit_code']}")
        for artifact in stage.artifacts:
            data = json.loads((root / artifact).read_text())
            if data.get('status') != 'passed':
                raise RuntimeError(f'{artifact}: not passed')
            for cleanup in ('all_children_stopped', 'all_instances_stopped'):
                if cleanup in data and data[cleanup] is not True:
                    raise RuntimeError(f'{artifact}: cleanup failed')
        result['status'] = 'passed'
    except subprocess.TimeoutExpired:
        result.update(status='failed', error='stage timeout', timed_out=True)
    except (Exception, KeyboardInterrupt) as exc:
        result.update(status='failed', error=repr(exc))
    finally:
        if process is not None:
            try:
                stop_group(process)
            except OSError as exc:
                result.update(status='failed', cleanup_error=repr(exc))
        for artifact in stage.artifacts:
            path = root / artifact
            if path.exists():
                shutil.copy2(path, directory / path.name)
        result['seconds'] = round(time.monotonic() - started, 3)
    return result


def stages_for(lane, python, directory):
    go = ['bash', 'scripts/go.sh']
    stages = [
        Stage('unit-race', go + ['test', '-race', '-count=1', '-timeout=8m', '-coverprofile='+str(directory/'unit.cover'), './...']),
        Stage('vet', go + ['vet', './...']),
        Stage('python-harness', [python, '-m', 'unittest', 'discover', '-s', 'scripts', '-p', 'test_*.py', '-v']),
        Stage('python-sdk', [python, '-m', 'unittest', 'discover', '-s', 'sdk/python/tests', '-v'], env={'PYTHONPATH': 'sdk/python'}),
    ]
    if lane == 'smoke':
        stages += [
            Stage('storage-integration', go + ['test', '-race', '-count=1', '-timeout=8m', './...'], env={'INTEGRATION': '1'}),
            Stage('seven-services', go + ['test', '-race', '-count=1', '-timeout=12m', '-run',
                  '^Test(OneProcess|GraphOneProcess|RetentionOneProcess|ClusteredOneProcess)Pipeline$', '-v', './cmd/vector-server'],
                  timeout=780, env={'ONEBOX_ACCEPTANCE': '1'}, artifacts=tuple('reports/'+p+'onebox-pipeline.json' for p in ('', 'graph-', 'retention-', 'clustered-'))),
            Stage('build-server', go + ['build', '-o', '.tools/vector-server', './cmd/vector-server']),
            Stage('build-cli', go + ['build', '-o', '.tools/vectorctl', './cmd/vectorctl']),
            Stage('dispatch-faults', [python, 'scripts/verify_dispatch.py'], artifacts=('reports/dispatch-processes.json',)),
            Stage('retention-faults', [python, 'scripts/verify_retention.py'], timeout=300, artifacts=('reports/retention-processes.json',)),
        ]
    return stages


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--lane', choices=('unit', 'smoke'), default='unit')
    args = parser.parse_args()
    run = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8]
    directory = ROOT / 'reports/quality' / run
    directory.mkdir(parents=True)
    # Shared binaries and legacy report paths require workspace serialization.
    with (ROOT/'reports/quality/.lock').open('w') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.exit(2, 'another quality run is active in this workspace\n')
        def interrupted(signum, frame):
            raise InterruptedError(f'signal {signum}')
        signal.signal(signal.SIGTERM, interrupted)
        report = dict(run_id=run, lane=args.lane, status='running', stages=[],
            commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
            dirty=bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT)))
        path, latest = directory/'report.json', ROOT/'reports/quality/latest.json'
        save(path, report); save(latest, report)
        try:
            for stage in stages_for(args.lane, str(ROOT/'.venv/bin/python'), directory):
                print(f'[{stage.name}] running; {directory/stage.name}.log', flush=True)
                report['active_stage'] = stage.name
                save(path, report); save(latest, report)
                result = run_stage(stage, directory)
                report['stages'].append(result)
                print(f"[{stage.name}] {result['status']} ({result['seconds']}s)", flush=True)
                if result['status'] != 'passed':
                    raise RuntimeError(stage.name+': '+result.get('error', 'failed'))
            report['status'] = 'passed'
        except (Exception, KeyboardInterrupt) as exc:
            report.update(status='failed', error=repr(exc))
        finally:
            report.pop('active_stage', None)
            save(path, report); save(latest, report)
        print(path, flush=True)
        return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    sys.exit(main())
