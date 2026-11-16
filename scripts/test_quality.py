import json
from pathlib import Path
import sys
import tempfile
import unittest
from quality import Stage, run_stage


class QualityRunnerTest(unittest.TestCase):
    def run_case(self, code, **kwargs):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            output = root/'output'; output.mkdir()
            result = run_stage(Stage('test', [sys.executable, '-c', code], **kwargs), output, root)
            self.assertTrue((output/'test.log').exists())
            return result

    def test_nonzero_is_failure(self):
        result = self.run_case('raise SystemExit(7)')
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['exit_code'], 7)

    def test_timeout_is_failure(self):
        result = self.run_case('import time; time.sleep(30)', timeout=.1)
        self.assertEqual(result['status'], 'failed')
        self.assertTrue(result['timed_out'])

    def test_missing_artifact_is_failure(self):
        self.assertEqual(self.run_case('pass', artifacts=('result.json',))['status'], 'failed')

    def test_stale_artifact_cannot_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); output = root/'output'; output.mkdir()
            (root/'result.json').write_text(json.dumps({'status':'passed'}))
            result = run_stage(Stage('test', [sys.executable, '-c', 'pass'], artifacts=('result.json',)), output, root)
            self.assertEqual(result['status'], 'failed')
            self.assertFalse((root/'result.json').exists())

    def test_success_requires_cleanup(self):
        for stopped in (True, False):
            body = json.dumps({'status':'passed', 'all_children_stopped':stopped})
            result = self.run_case(f'from pathlib import Path; Path("result.json").write_text({body!r})', artifacts=('result.json',))
            self.assertEqual(result['status'], 'passed' if stopped else 'failed')

    def test_launch_failure_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = run_stage(Stage('missing', ['/no-such-quality-command']), Path(tmp), Path(tmp))
            self.assertEqual(result['status'], 'failed')
