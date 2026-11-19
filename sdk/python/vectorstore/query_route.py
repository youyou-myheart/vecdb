"""Approximate nearest-centroid selection from periodic cluster heartbeats."""
import math


def ranked_partitions(state, vector):
    centers = state.routing.centers
    if not 1 <= len(centers) <= 64 or state.routing.version != 1:
        return []
    if not all(math.isfinite(x) and abs(x) <= 1e15 for x in vector):
        return []  # Let the server return the normal query validation error.
    nodes = {n.id: n for n in state.nodes}
    ranked = []
    for i, center in enumerate(centers):
        if len(center.vector) != len(vector):
            return []
        id = f'index-{i:04d}'
        counts = []
        owners = state.replica_owners.get(id)
        if owners:
            for owner in owners.owners:
                node = nodes.get(owner.node)
                if not owner.ready or node is None or node.session != owner.session:
                    continue
                load = node.load.get(id)
                if load is not None and load.records_known:
                    counts.append(load.live_records)
        # Replicas are copies: use max, never sum their counts.
        count = max(counts) if counts else None
        ranked.append((sum((x-y)**2 for x, y in zip(vector, center.vector)), id, count))
    return sorted(ranked)


def initial_fanout(ranked, k):
    # Unknown counts and old servers safely fall back to all partitions.
    if not ranked or any(row[2] is None for row in ranked):
        return len(ranked)
    total = 0
    for n, (_, _, count) in enumerate(ranked, 1):
        total += count
        if total >= k:
            return n
    return len(ranked)
