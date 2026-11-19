"""Native onebox: steady load, then compare cached SDK routing with all shards."""
import argparse
import hashlib
import json
import math
import signal
import time

from onebox_e2e import Onebox, ROOT, percentile, vector_for
from quality import save
from vectorstore import Client


def summary(values):
    return dict(samples=len(values), p50_ms=percentile(values, .5)*1000,
                p95_ms=percentile(values, .95)*1000, p99_ms=percentile(values, .99)*1000)


def run(fixture, args):
    fixture.run()
    # Do not claim success until the SDK checks below finish as well.
    fixture.report['status'] = 'running'
    fixture.persist()
    routed, broadcast, recalls, fanouts, differences = [], [], [], [], []
    oracle = fixture.oracle.snapshot()
    with Client(f'127.0.0.1:{args.base_port+180}', fixture.c.env['API_TOKEN'],
                search_refresh_interval=1) as client:
        deadline = time.monotonic()+15
        while True:
            ranked = client._search_routes(vector_for(512, 1), 3)
            if ranked and all(row[2] is not None for row in ranked):
                break
            fixture.check_background()
            if time.monotonic() >= deadline:
                raise TimeoutError('heartbeat record counts not available to SDK')
            time.sleep(.1)
        original = client.stub.Search
        selected = []
        def observe(request, **kwargs):
            selected.append(list(request.index_partitions))
            return original(request, **kwargs)
        client.stub.Search = observe
        # Warm both paths; no background mutations during this comparison.
        for _ in range(4):
            client.search(vector_for(512, 1), k=10)
            client.search(vector_for(512, 1), k=10, all_partitions=True)
        for n in range(args.samples):
            vector = vector_for(512+n % 128, 1)
            expected = client.search(vector, k=10, exact=True)
            paths = [('routed', {}), ('all', dict(all_partitions=True))]
            if n % 2:
                paths.reverse()
            responses = {}
            for name, options in paths:
                selected.clear()
                started = time.monotonic()
                response = client.search(vector, k=10, **options)
                elapsed = time.monotonic()-started
                assert len(response.hits) == 10 and len({h.id for h in response.hits}) == 10
                for hit in response.hits:
                    record = oracle[hit.id]
                    assert 0 < hit.revision <= record['revision']
                    assert math.isfinite(hit.distance) and hit.distance >= 0
                    if hit.revision == record['revision']:
                        distance = sum((x-y)**2 for x, y in zip(vector, record['vector']))
                        assert math.isclose(hit.distance, distance, rel_tol=1e-5, abs_tol=1e-6)
                (routed if name == 'routed' else broadcast).append(elapsed)
                if name == 'routed':
                    fanouts.append(len(selected[-1]))
                    assert len(selected[-1]) == 1, selected
                responses[name] = response
            truth = {h.id for h in expected.hits}
            recalls.append(len(truth & {h.id for h in responses['routed'].hits})/len(truth))
            if recalls[-1] < 1 and len(differences) < 10:
                differences.append(dict(query=vector, agreement=recalls[-1],
                    routed=[dict(id=h.id, revision=h.revision, distance=h.distance,
                                 current_revision=oracle[h.id]['revision']) for h in responses['routed'].hits],
                    exact_all=[dict(id=h.id, revision=h.revision, distance=h.distance) for h in expected.hits]))
        by_id = client.search_by_id('v-0512', k=10)
        assert len(by_id.hits) == 10
    return dict(status='passed', source_commit=fixture.report['commit'],
                source_dirty=fixture.report['dirty'],
                binary_sha256=hashlib.sha256((ROOT/'.tools/vector-server').read_bytes()).hexdigest(),
                routed=summary(routed), all_partitions=summary(broadcast),
                searched_partition_counts=sorted(set(fanouts)),
                minimum_agreement_with_all_exact_at_10=min(recalls),
                mean_agreement_with_all_exact_at_10=sum(recalls)/len(recalls),
                result_difference_samples=differences,
                agreement_note='Diagnostic comparison of independent eventual views; nearest-cluster routing does not promise global recall.',
                search_by_id='passed',
                scope='Interleaved warmed read-only SDK calls after steady load; not saturation throughput.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--steady-seconds', type=int, default=120)
    parser.add_argument('--samples', type=int, default=120)
    parser.add_argument('--base-port', type=int, default=21400)
    args = parser.parse_args()
    if args.steady_seconds <= 0 or args.samples < 1:
        parser.error('positive duration and samples required')
    args.recovery_seconds = 1
    args.rounds = 1
    args.latency_only = True
    args.keep_running = False
    fixture = None
    result = dict(status='failed')
    def interrupted(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    try:
        fixture = Onebox(args)
        result = run(fixture, args)
        fixture.report['status'] = 'passed'
    except BaseException as exc:
        result.update(status='failed', error=repr(exc))
        if fixture:
            fixture.report.update(status='failed', error=repr(exc))
        raise
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        if fixture:
            fixture.close()
            result.update(onebox_report=str(fixture.logs/'report.json'),
                          all_children_stopped=fixture.report['all_children_stopped'])
            save(fixture.logs/'sdk-query-routing.json', result)
            save(ROOT/'reports/sdk-query-routing-latest.json', result)
            print(json.dumps(result, indent=2), flush=True)


if __name__ == '__main__':
    main()
