"""Real S3 integration checks, with a synthetic 17 MiB index artifact."""
import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from botocore.exceptions import ClientError
from storage import ManifestStore, SegmentStore


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--restore-only', action='store_true')
    args = parser.parse_args()
    root = Path('.state')
    root.mkdir(exist_ok=True)
    store = SegmentStore()
    catalog = ManifestStore(root / 'manifest.db')
    checks = []
    started = time.monotonic()
    if not args.restore_only:
        try:
            store.s3.head_bucket(Bucket=store.bucket)
        except ClientError as e:
            if e.response['ResponseMetadata']['HTTPStatusCode'] != 404:
                raise
            store.s3.create_bucket(Bucket=store.bucket)
        source = root / 'source.segment'
        with source.open('wb') as f:
            for _ in range(17):
                f.write(os.urandom(1024*1024))
        old = catalog.get('demo/shard-0')
        manifest = store.upload_verified(source, 'demo/shard-0')
        checks.append('multipart_upload_and_full_sha256_readback')
        # Unpublished artifacts must not change the current manifest.
        assert catalog.get('demo/shard-0') == old
        catalog.publish('demo/shard-0', old['generation'] if old else None, manifest)
        try:
            catalog.publish('demo/shard-0', 'stale-writer', manifest)
            raise AssertionError('stale publication accepted')
        except ValueError:
            checks.append('stale_manifest_rejected')
        with source.open('rb') as f:
            f.seek(123)
            expected = f.read(4096)
        assert store.read_range(manifest, 123, 4218) == expected
        checks.append('range_read')
    manifest = catalog.get('demo/shard-0')
    if manifest is None:
        raise RuntimeError('Run initial verification before --restore-only')
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda i: store.restore(manifest, root / f'node-{i}' / 'index.segment'), range(4)))
    checks.append('four_parallel_verified_restores')
    destination = root / 'node-0' / 'index.segment'
    before = destination.read_bytes()
    bad = dict(manifest, sha256='0'*64)
    try:
        store.restore(bad, destination)
        raise AssertionError('invalid checksum accepted')
    except ValueError:
        assert destination.read_bytes() == before
        checks.append('checksum_failure_preserves_active_file')
    report = {'status': 'passed', 'mode': 'restore-only' if args.restore_only else 'full',
              'checks': checks, 'bytes': manifest['size'],
              'elapsed_seconds': round(time.monotonic()-started, 3),
              'scope': 'single-node SeaweedFS; synthetic artifact; local SQLite manifest, not TiKV'}
    Path('reports').mkdir(exist_ok=True)
    Path('reports/' + report['mode'] + '.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
