import hashlib
import tempfile
import unittest
from pathlib import Path
from storage import ManifestStore, SegmentStore, sha256

class FakeS3:
    data = b'index-data'
    def upload_file(self, path, bucket, key, **kwargs):
        self.data = Path(path).read_bytes()
    def download_file(self, bucket, key, target):
        Path(target).write_bytes(self.data)
    def get_object(self, **kwargs):
        import io
        start, end = map(int, kwargs['Range'][6:].split('-'))
        return {'Body': io.BytesIO(self.data[start:end + 1])}

class StorageTests(unittest.TestCase):
    def test_manifest_cas(self):
        with tempfile.TemporaryDirectory() as d:
            m = ManifestStore(Path(d) / 'm.db')
            self.assertIsNone(m.get('a'))
            m.publish('a', None, {'generation': 'g1'})
            with self.assertRaises(ValueError):
                m.publish('a', None, {'generation': 'g2'})
            self.assertEqual(m.get('a')['generation'], 'g1')
            m.publish('a', 'g1', {'generation': 'g2'})
            self.assertEqual(ManifestStore(Path(d) / 'm.db').get('a')['generation'], 'g2')
    def test_upload_restore_and_corruption(self):
        with tempfile.TemporaryDirectory() as d:
            src, dst = Path(d) / 'source', Path(d) / 'target'
            src.write_bytes(b'index-data')
            s = SegmentStore.__new__(SegmentStore)
            s.bucket, s.s3 = 'test', FakeS3()
            m = s.upload_verified(src, 'ns')
            self.assertEqual(sha256(src), hashlib.sha256(b'index-data').hexdigest())
            s.restore(m, dst)
            self.assertEqual(dst.read_bytes(), src.read_bytes())
            self.assertEqual(s.read_range(m, 1, 3), b'nde')
            s.s3.data = b'corrupt'
            with self.assertRaises(ValueError):
                s.restore(m, dst)
            self.assertEqual(dst.read_bytes(), b'index-data')
            self.assertFalse(list(Path(d).glob('.loading-*')))

if __name__ == '__main__':
    unittest.main()
