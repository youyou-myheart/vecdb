"""Stable, bounded test buckets; service search K remains capped at 1000."""
import math

MAX_SEARCH_K = 1000
BUCKET_FIELD = 'acceptance_bucket'


class QueryBuckets:
    def __init__(self, id_limit, width=500):
        if not 1 <= width <= MAX_SEARCH_K//2 or id_limit < 1:
            raise ValueError('positive ID domain and bucket width at most 500 required')
        self.id_limit, self.width = id_limit, width
        # The last bucket deliberately has no assigned IDs, including future writes.
        self.count = math.ceil(id_limit/width)+1

    def number(self, id):
        if not id.startswith('v-') or not id[2:].isdigit():
            raise ValueError('noncanonical test ID: '+id)
        n=int(id[2:])
        if id != f'v-{n:04d}' or not 0 <= n < self.id_limit:
            raise ValueError('test ID outside declared domain: '+id)
        return n

    def label(self, number):
        if not 0 <= number < self.id_limit: raise ValueError('ID outside bucket domain')
        return f'{number//self.width:04d}'

    def payload(self, number, revision):
        return dict(group=str(number%8),revision=str(revision),**{BUCKET_FIELD:self.label(number)})

    def partition(self, expected):
        buckets=[{} for _ in range(self.count)]
        for id,r in expected.items():
            label=self.label(self.number(id))
            if not r['deleted']:
                if r['payload'].get(BUCKET_FIELD)!=label:
                    raise AssertionError('oracle payload does not match immutable ID bucket')
                buckets[int(label)][id]=r
        assert sum(map(len,buckets))==sum(not r['deleted'] for r in expected.values())
        assert all(len(b)<=self.width for b in buckets)
        assert not buckets[-1]
        return [(f'{n:04d}',{BUCKET_FIELD:f'{n:04d}'},records) for n,records in enumerate(buckets)]
