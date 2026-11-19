import copy
import json
import tempfile
import unittest
from pathlib import Path

if __package__:
    from .perf_report_render import render_report
else:
    from perf_report_render import render_report


def matrix():
    return {
        "status": "complete",
        "binary_sha256": "a" * 64,
        "region_control": True,
        "cpu_quota": "200000 100000",
        "workers": 3,
        "data_shards": 4,
        "index_shards": 4,
        "index_replicas": 2,
        "seed_records": 256,
        "dimensions": 64,
        "query_settings": {"seed": 13, "k": 10, "query_ids": ["v1", "v2"], "live_records": 256},
        "queries": [
            {
                "scenario": "sdk_all_partitions",
                "concurrency": 16,
                "n": 100,
                "warmup": 10,
                "succeeded": 100,
                "errors": 0,
                "rps": 120,
                "p99_ms": 4,
            }
        ],
    }


class PerformanceReportTest(unittest.TestCase):
    def test_paired_query_and_conditions(self):
        current = matrix()
        baseline = copy.deepcopy(current)
        baseline["queries"][0].update(rps=100, p99_ms=None)
        report = render_report(current, baseline=baseline, current_source="a b.json")
        self.assertIn("[查看数据](a%20b.json)", report)
        self.assertIn("| 全片默认策略 C16 |", report)
        self.assertIn("| 120<br>100 |", report)
        self.assertIn("| 4<br>N/A |", report)
        self.assertIn("| +20.0% |", report)
        self.assertIn("已记录失败数的测量场景均为 0", report)
        baseline["region_control"] = False
        other = render_report(current, baseline=baseline)
        self.assertIn("| Region 受控 | TiKV 默认 Region 策略 |", other)
        self.assertNotIn("+20.0%", other)

    def test_window_ack_drop_and_failures(self):
        current = matrix()
        current.update(
            sdk_writes={
                "20": {
                    "target_qps": 20,
                    "seconds": 1,
                    "clients": 1,
                    "inflight_per_client": 2,
                    "offered": 20,
                    "invoked": 17,
                    "durable_acks_in_window": 14,
                    "after_window_acks": 2,
                    "ack_qps_in_window": 14,
                    "dropped_unsent": 3,
                    "rpc_failure_count": 1,
                    "examples": ["write failed"],
                }
            },
            failover={"setup_error": "owner unavailable"},
        )
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "matrix.json"
            source.write_text(json.dumps(current), encoding="utf-8")
            report = render_report(source)
        self.assertIn("| 20 | 1 | 1 × 2 | 20 | 17 | 14 | 2 | 14 | 3 | 1 |", report)
        self.assertIn("write failed", report)
        self.assertIn("owner unavailable", report)
        self.assertNotIn("已记录失败数的测量场景均为 0", report)
        self.assertIn("未记录测量阶段的失败计数", render_report({"status": "running"}))

    def test_supplements_use_the_same_binary(self):
        current = matrix()
        supplement = {
            "label": "小分片对照",
            "status": "passed",
            "binary_sha256": "a" * 64,
            "quality": {"queries": 2, "overlap": 17, "recall_at_10": 0.85},
        }
        accepted = render_report(current, supplements=[supplement])
        self.assertIn("#### 小分片对照", accepted)
        self.assertIn("17 / 20", accepted)
        self.assertIn("85.00%", accepted)
        supplement["binary_sha256"] = "b" * 64
        rejected = render_report(current, supplements=[supplement])
        self.assertIn("已跳过该专项数据", rejected)
        self.assertNotIn("85.00%", rejected)

    def test_metadata_and_global_tikv_rpc_counters(self):
        row = matrix()
        row["metadata"] = {
            "git_rev": "abcdef0",
            "started_at": "2026-11-19T10:00:00Z",
            "overrides": {"GOMAXPROCS": "4"},
            "failover_probe": "probe fallback",
        }
        row["failover"] = {"epoch_probe": "actual target-shard probe"}
        row["resources"] = {
            "reads": {
                "counters": {
                    "tikv": {
                        "tikv_grpc_msg_duration_seconds_count|type=raw_batch_put": 0,
                        "tikv_grpc_msg_duration_seconds_count|type=kv_commit": 7,
                    }
                }
            }
        }
        report = render_report(row)
        self.assertIn("| 采集时源码 Git 提交 | abcdef0 |", report)
        self.assertIn("| 开始时间 | 2026-11-19T10:00:00Z |", report)
        self.assertIn("| 服务端 GOMAXPROCS 环境覆盖 | 4 |", report)
        self.assertIn("| 接管任期探测方式 | actual target-shard probe |", report)
        self.assertIn("| 0 · 7 |", report)
        del row["failover"]
        self.assertIn("| 接管任期探测方式 | probe fallback |", render_report(row))

    def test_reject_invalid_input(self):
        for data in (
            {},
            {"queries": "broken"},
            {"queries": [{}]},
            {"writes": {"row": 1}},
            {"status": "complete", "binary_sha256": "not-a-sha"},
        ):
            with self.subTest(data=data), self.assertRaises(ValueError):
                render_report(data)
        with self.assertRaises(ValueError):
            render_report(matrix(), current_sha="b" * 64)
        with self.assertRaises(ValueError):
            render_report(matrix(), supplements=1)


if __name__ == "__main__":
    unittest.main()
