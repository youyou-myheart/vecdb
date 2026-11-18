from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from probe_http import RequestJournal


class RequestJournalTest(unittest.TestCase):
    def test_concurrent_evidence_uses_one_handle_and_flushes_every_record(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'requests.jsonl'
            destination=SimpleNamespace(open=Mock(side_effect=path.open))
            with RequestJournal(destination) as journal:
                with ThreadPoolExecutor(max_workers=24) as pool:
                    list(pool.map(lambda n:journal.record(dict(request_id=str(n),http_status=200,
                        value={'private':'successful response body'})),range(250)))
                journal.flush()
                rows=[json.loads(line) for line in path.read_text().splitlines()]
                self.assertEqual({r['request_id'] for r in rows},{str(n) for n in range(250)})
                self.assertEqual(len(rows),250)
                self.assertTrue(all('value' not in r for r in rows))
                self.assertEqual(journal.summary()['records'],250)
            self.assertEqual(destination.open.call_count,1)

    def test_write_and_flush_failures_cannot_pass_acceptance(self):
        for operation in ('write','flush'):
            stream=Mock()
            getattr(stream,operation).side_effect=OSError('disk failed')
            destination=SimpleNamespace(open=Mock(return_value=stream))
            with self.assertRaisesRegex(OSError,'disk failed'):
                with RequestJournal(destination) as journal:
                    journal.record(dict(request_id='request',http_status=503,response_body='fenced'))
                    journal.flush()
            stream.close.assert_called_once()


if __name__=='__main__':unittest.main()
