"""Immutable S3 segments; local manifest adapter is NOT a TiKV replacement."""
import hashlib
import json
import os
import sqlite3
import tempfile
import uuid
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config


def sha256(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


class ManifestStore:
    """Local transactional CAS adapter. Production must use TiKV transactions."""
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.path = str(path)
        with sqlite3.connect(self.path) as db:
            db.execute('CREATE TABLE IF NOT EXISTS manifest (name TEXT PRIMARY KEY, value TEXT NOT NULL)')

    def get(self, name):
        with sqlite3.connect(self.path) as db:
            row = db.execute('SELECT value FROM manifest WHERE name=?', (name,)).fetchone()
            return json.loads(row[0]) if row else None

    def publish(self, name, expected_generation, manifest):
        with sqlite3.connect(self.path) as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT value FROM manifest WHERE name=?', (name,)).fetchone()
            actual = json.loads(row[0])['generation'] if row else None
            if actual != expected_generation:
                raise ValueError('manifest generation conflict')
            db.execute('INSERT OR REPLACE INTO manifest VALUES (?, ?)', (name, json.dumps(manifest)))


class SegmentStore:
    def __init__(self, bucket='vector-index-poc'):
        self.bucket = bucket
        self.s3 = boto3.client('s3',
            endpoint_url=os.getenv('S3_ENDPOINT', 'http://127.0.0.1:18333'),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'poc-access'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'poc-secret-local-only'),
            region_name='us-east-1',
            config=Config(signature_version='s3v4', s3={'addressing_style': 'path'},
                retries={'mode': 'standard', 'max_attempts': 3},
                connect_timeout=5, read_timeout=30))

    def upload_verified(self, source, namespace):
        digest = sha256(source)
        generation = uuid.uuid4().hex
        key = f'{namespace}/{generation}/{digest}.segment'
        self.s3.upload_file(str(source), self.bucket, key,
            Config=TransferConfig(multipart_threshold=8*1024*1024,
                                  multipart_chunksize=8*1024*1024))
        manifest = {'generation': generation, 'key': key,
                    'size': Path(source).stat().st_size, 'sha256': digest}
        # Read back bytes; ETag is not a content hash for multipart uploads.
        with tempfile.TemporaryDirectory() as d:
            self.restore(manifest, Path(d) / 'verified.segment')
        return manifest

    def restore(self, manifest, destination):
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(dir=destination.parent, prefix='.loading-')
        os.close(fd)
        try:
            self.s3.download_file(self.bucket, manifest['key'], temporary)
            if Path(temporary).stat().st_size != manifest['size'] or sha256(temporary) != manifest['sha256']:
                raise ValueError('segment checksum mismatch')
            with open(temporary, 'rb') as f:
                os.fsync(f.fileno())
            os.replace(temporary, destination)
            return destination
        finally:
            Path(temporary).unlink(missing_ok=True)

    def read_range(self, manifest, start, end):
        response = self.s3.get_object(Bucket=self.bucket, Key=manifest['key'], Range=f'bytes={start}-{end}')
        try:
            return response['Body'].read()
        finally:
            response['Body'].close()
