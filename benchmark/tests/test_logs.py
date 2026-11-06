import contextlib
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmark.config import NodeParameters
from benchmark.logs import LogParser, ParseError

HEADER = (
    "[INFO] 2026/10/26 00:00:00.000000 pool.go:1: Transaction pool tx size set to 16\n"
    "[INFO] 2026/10/26 00:00:00.000000 pool.go:1: Transaction pool batch size set to 10\n"
    "[INFO] 2026/10/26 00:00:00.000000 pool.go:1: Transaction pool tx rate set to 200\n"
)


def record(index, leader, payload, batch, timestamp):
    data = {
        "index": index,
        "leader": leader,
        "payloads": [payload],
        "batches": [batch],
        "transactions": 10,
    }
    return f"[INFO] 2026/10/26 00:00:00.{timestamp:06d} node.go:1: Tuna_v2 commit {json.dumps(data)}\n"


def log(node, second=True):
    result = (
        HEADER + f"[INFO] 2026/10/26 00:00:00.000000 pool.go:1: Received Batch {node}\n"
    )
    result += record(1, "leader1", "payload0", 0, 100000 + node * 20000)
    if second:
        result += record(2, "leader2", "payload1", 1, 200000 + node * 20000)
    result += '[INFO] 2026/10/26 00:00:00.300000 runtime.go:1: Tuna_v2 stats {"Undecided": 2, "Pending": 0}\n'
    return result


class LogTests(unittest.TestCase):
    def test_actual_commits_and_local_latency(self):
        parser = LogParser([log(0), log(1)])
        self.assertEqual(parser.common_indices, [1, 2])
        result = parser.result()
        self.assertIn("Committed transactions in common window: 20", result)
        self.assertIn("160.00 ms", result)
        self.assertIn("Latest undecided slots per node: [2, 2]", result)

    def test_asynchronous_tails_use_shared_prefix(self):
        parser = LogParser([log(0), log(1, second=False)])
        self.assertEqual(parser.common_indices, [1])

    def test_conflicting_commits_rejected(self):
        with self.assertRaises(ParseError):
            LogParser([log(0), log(1).replace("leader1", "wrong-leader")])

    def test_duplicate_payload_rejected(self):
        with self.assertRaises(ParseError):
            LogParser([log(0).replace("payload1", "payload0")])

    def test_gaps_and_invalid_json_rejected(self):
        with self.assertRaises(ParseError):
            LogParser([log(0).replace('"index": 2', '"index": 3')])
        with self.assertRaises(ParseError):
            LogParser([HEADER + "[INFO] Tuna_v2 commit {bad json}"])

    def test_no_commit_is_zero_throughput(self):
        self.assertIn(
            "Common committed index window: none", LogParser([HEADER]).result()
        )
        with self.assertRaises(ParseError):
            LogParser([])

    def test_consensus_defaults_added_to_benchmark_config(self):
        parameters = NodeParameters(
            {
                "pool": {"tx_size": 16, "max_queue_size": 1000},
                "mempool": {"sync_timeout": 0},
            }
        )
        self.assertEqual(parameters.json["consensus"]["leader_count"], 2)

    def test_generic_sequence_comparison_supports_prefixes(self):
        spec = importlib.util.spec_from_file_location(
            "comparison", Path(__file__).resolve().parents[1] / "consistency.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as temporary:
            first, second = Path(temporary) / "a.log", Path(temporary) / "b.log"
            first.write_text("item one\nitem two\n")
            second.write_text("item one\n")
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertTrue(
                    module.compare_sequences(
                        [str(first), str(second)], r"item (\w+)", allow_prefix=True
                    )
                )
                self.assertFalse(
                    module.compare_sequences([str(first), str(second)], r"item (\w+)")
                )
                second.write_text("item other\n")
                self.assertFalse(
                    module.compare_sequences(
                        [str(first), str(second)], r"item (\w+)", allow_prefix=True
                    )
                )


if __name__ == "__main__":
    unittest.main()
