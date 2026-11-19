import json
from pathlib import Path
import tempfile
import unittest

from overall_performance import input_sets, load_baselines, render


class ReportInputsTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)

    def source(self, name, **changes):
        row = dict(version=name, status="complete", region_control=True, cpu_quota="400000 100000",
                   dimensions=64, data_shards=4, index_shards=4, index_replicas=2, workers=3,
                   seed_records=8192, queries=[], writes={}, sdk_writes={})
        row.update(changes)
        path = self.root / f"{name}.json"
        path.write_text(json.dumps(row), encoding="utf-8")
        return path

    def test_legacy_default_requires_explicit_profile_and_cannot_conflict(self):
        path = self.source("legacy")
        with self.assertRaises(ValueError):
            input_sets([], [path])  # Embedded controlled profile.
        raw = json.loads(path.read_text())
        del raw["region_control"]
        path.write_text(json.dumps(raw))
        with self.assertRaises(ValueError):
            input_sets([path])
        data = load_baselines([], [path], "a" * 64)[False][1]
        self.assertFalse(data["region_control"])
        self.assertEqual(data["binary_sha256"], "a" * 64)
        self.assertNotIn("region_control", json.loads(path.read_text()))

    def test_different_worker_counts_omit_comparison_and_keep_evidence(self):
        current = self.source("current", writes={"put_c16": {"rps": 11, "p99_ms": 2}})
        old = self.source("previous", workers=1, writes={"put_c16": {"rps": 4, "p99_ms": 8}})
        output = render([current], [old], [], self.root / "report.md")
        report = output.read_text()
        self.assertIn("workers", report)
        self.assertIn("previous.json", report)
        self.assertIn("Gateway 分散写，16 并发 | — | 11.0 | —", report)
        with self.assertRaises(ValueError):
            render([current], [], [], current)
