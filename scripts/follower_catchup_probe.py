"""Time ordinary FollowerSearch responses against a certified post-write graph frontier."""
import hashlib
import time

from quality import save


def final_frontier(state, records, indexes, partitions):
    # Each accepted logical revision contributes one data event. This trial owns
    # a fresh namespace, and all writers are joined before the probe begins.
    expected = {f'data-{n:04d}': 0 for n in range(partitions)}
    for key, record in records.items():
        shard = int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], 'big') % partitions
        expected[f'data-{shard:04d}'] += record['revision']
    progress = state['data_progress']
    if set(progress) != set(expected) or any(progress[k]['head'] != n for k, n in expected.items()):
        raise AssertionError('durable data heads differ from the acknowledged workload')
    if any(progress[k]['completed_lsn'] != n for k, n in expected.items()):
        return None
    # ClusterStatus reads graph heads AFTER the source completion observations.
    # A later control-only graph entry can make this frontier conservative.
    heads = state['graph_heads']
    if set(heads) != indexes:
        raise AssertionError('incomplete graph heads')
    return {index: head['committed'] for index, head in heads.items()}


def covers(position, target):
    if position['sequence'] == target['sequence'] and position != target:
        raise AssertionError('different graph positions at the same sequence')
    return position['sequence'] >= target['sequence']


def summarize_capture(samples, final):
    indexes = set(final)
    behind = {}
    for sample in samples:
        reads = sample['reads']
        if len(reads) != len(indexes) or {r['index'] for r in reads} != indexes:
            raise AssertionError('incomplete or duplicate follower partitions')
        for r in reads:
            if not r['replica'] or not r['session'] or not r['epoch'] or not covers(r['applied'], r['target']):
                raise AssertionError('invalid follower read barrier')
        qualified = True
        identities = []
        for r in reads:
            identity = (r['index'], r['replica'], r['session'], r['epoch'])
            identities.append(identity)
            goal = final[r['index']]
            if not covers(r['target'], goal):
                qualified = False
                # The leader sampled its graph after this request's queue wait.
                offset = r['queue_ms']
                if not covers(r['applied'], goal):
                    # The local graph was still behind when the query viewed it.
                    # This occurred AFTER capture and permission to query.
                    offset += r['capture_ms'] + r['wait_apply_ms']
                behind[identity] = max(behind.get(identity, 0), sample['sent_ms'] + offset)
        if qualified:
            # All worker queries started after permission and completed before
            # receipt. Subtract ONLY each worker's own query duration.
            upper = max(sample['received_ms'] - r['query_ms'] for r in reads)
            return dict(first_confirmed_ms=sample['received_ms'], applied_upper_ms=upper,
                        applied_lower_ms=max(behind.get(key, 0) for key in identities), first_probe_ms=samples[0]['sent_ms'],
                        confirming_attempt=sample['attempt'], reads=reads, hit=sample['hit'])
    return None


def capture(trial, client, name, anchor):
    t0 = trial.oracle.last_ack
    query = trial.oracle.vector_factory(anchor, 1)
    index_ids = {f'index-{n:04d}' for n in range(len(trial.centers))}
    samples = []
    frontier = None
    next_check = t0 + .20
    path = trial.logs / (name + '-follower.json')
    def position(p):
        return dict(sequence=p.sequence, hash=p.hash, data_offset=p.data_offset)
    try:
        while time.monotonic() - t0 < 10:
            trial.check_background()
            sent = time.monotonic()
            response = client.follower_search(query, k=1, timeout=2)
            received = time.monotonic()
            reads = [dict(index=r.index_partition, replica=r.replica, node=r.node, session=r.session,
                          epoch=r.owner_epoch, target=position(r.target), applied=position(r.applied),
                          queue_ms=r.queue_ms, capture_ms=r.leader_capture_ms,
                          wait_apply_ms=r.wait_apply_ms, query_ms=r.query_ms)
                     for r in response.follower_reads]
            hit = [(h.id, h.revision, h.distance) for h in response.hits]
            samples.append(dict(attempt=len(samples)+1, sent_ms=(sent-t0)*1000,
                                received_ms=(received-t0)*1000, reads=reads, hit=hit))
            # Validate early samples retrospectively. Never count certification
            # or report generation as time to catch up.
            if not frontier and received >= next_check:
                frontier = final_frontier(trial.state(), trial.oracle.snapshot(), index_ids, 16)
                next_check = time.monotonic() + .10
            if frontier:
                result = summarize_capture(samples, frontier)
                if result:
                    if result['hit'] != [(f'v-{anchor:04d}', 1, 0.0)]:
                        raise AssertionError(('anchor mismatch on covering follower read', result['hit']))
                    return result
            time.sleep(.005)
        raise TimeoutError('FollowerSearch did not confirm final graph positions within 10s')
    finally:
        save(path, dict(last_ack_monotonic=t0, final_graph_positions=frontier, samples=samples))
