from datetime import datetime
from glob import glob
from json import JSONDecodeError, loads
from os.path import join
from re import findall, search
from statistics import mean


class ParseError(Exception):
    pass


class LogParser:
    """Measure durable Tuna_v2 commits and verify agreement on overlapping indices."""

    def __init__(self, nodes):
        if not nodes:
            raise ParseError("No node logs found")
        self.node_count = len(nodes)
        self.batches = {}
        self.deliveries = 0
        self.configs = None
        self.node_commits = []
        self.node_batches = []
        self.last_stats = []
        for log in nodes:
            if search(r"panic|DATA RACE", log):
                raise ParseError("Node failed or reported a data race")
            config = {}
            for key, label in [
                ("tx_size", "tx size"),
                ("batch_size", "batch size"),
                ("rate", "tx rate"),
            ]:
                match = search(rf"Transaction pool {label} set to (\d+)", log)
                if match is None:
                    raise ParseError(f"Missing transaction pool {key} in node log")
                config[key] = int(match.group(1))
            if self.configs is not None and self.configs != config:
                raise ParseError(
                    "Inconsistent transaction pool configuration across nodes"
                )
            self.configs = config
            batches = {}
            for timestamp, batch_id in findall(
                r"\[INFO] (.*?) pool\.go:\d+: Received Batch (\d+)", log
            ):
                value = self._timestamp(timestamp)
                batches[batch_id] = min(value, batches.get(batch_id, value))
                self.batches[batch_id] = min(value, self.batches.get(batch_id, value))
            self.node_batches.append(batches)
            self.deliveries += len(
                set(findall(r"receive batch (\d+) (?:author|from)", log))
            )
            commits, stats, delivered = {}, {}, set()
            previous = None
            for line in log.splitlines():
                if "Tuna_v2 stats " in line:
                    stats = self._json(line.split("Tuna_v2 stats ", 1)[1])
                if "Tuna_v2 commit " not in line:
                    continue
                prefix, payload = line.split("Tuna_v2 commit ", 1)
                record = self._json(payload)
                index = record.get("index")
                if not isinstance(index, int) or index < 1 or not record.get("leader"):
                    raise ParseError("Malformed commit identity")
                if (
                    not isinstance(record.get("transactions"), int)
                    or record["transactions"] < 0
                ):
                    raise ParseError("Malformed committed transaction count")
                if previous is not None and index != previous + 1:
                    raise ParseError(
                        "Commit output contains repeated or missing indices"
                    )
                previous = index
                refs = record.get("payloads") or []
                if len(set(refs)) != len(refs) or delivered.intersection(refs):
                    raise ParseError("A payload was delivered more than once")
                delivered.update(refs)
                match = search(
                    r"\[INFO] (\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d+)", prefix
                )
                if match is None:
                    raise ParseError("Missing commit timestamp")
                commits[index] = (record, self._timestamp(match.group(1)))
            self.node_commits.append(commits)
            self.last_stats.append(stats)
        shared = set(self.node_commits[0])
        for commits in self.node_commits[1:]:
            shared.intersection_update(commits)
        self.common_indices = sorted(shared)
        for index in self.common_indices:
            expected = self._identity(self.node_commits[0][index][0])
            if any(
                self._identity(commits[index][0]) != expected
                for commits in self.node_commits[1:]
            ):
                raise ParseError(f"Nodes disagree at commit index {index}")

    @staticmethod
    def _identity(record):
        return (
            record["leader"],
            tuple(record.get("payloads") or []),
            record["transactions"],
        )

    @staticmethod
    def _json(value):
        try:
            return loads(value)
        except JSONDecodeError as error:
            raise ParseError(f"Invalid structured log: {error}") from error

    @staticmethod
    def _timestamp(value):
        try:
            return datetime.strptime(value, "%Y/%m/%d %H:%M:%S.%f").timestamp()
        except ValueError as error:
            raise ParseError(f"Invalid log timestamp: {error}") from error

    def result(self):
        transactions = sum(
            self.node_commits[0][index][0]["transactions"]
            for index in self.common_indices
        )
        rates, latencies = [], []
        for batches, commits in zip(self.node_batches, self.node_commits):
            if not self.common_indices:
                continue
            end = commits[self.common_indices[-1]][1]
            start = (
                min(batches.values()) if batches else commits[self.common_indices[0]][1]
            )
            duration = end - start
            rates.append(transactions / duration if duration > 0 else 0)
            for index in self.common_indices:
                record, committed_at = commits[index]
                for batch in record.get("batches") or []:
                    created_at = batches.get(str(batch))
                    if created_at is not None and committed_at >= created_at:
                        latencies.append((committed_at - created_at) * 1000)
        prefix = (
            f"{self.common_indices[0]}..{self.common_indices[-1]}"
            if self.common_indices
            else "none"
        )
        return (
            "\nTuna_v2 consensus benchmark\n"
            f" Nodes: {self.node_count}\n"
            f' Input rate per node: {self.configs["rate"]:,} tx/s\n'
            f' Transaction size: {self.configs["tx_size"]:,} B\n'
            f' Batch size: {self.configs["batch_size"]:,} tx/batch\n'
            f" Generated batches: {len(self.batches):,}\n"
            f" Payload deliveries across peers: {self.deliveries:,}\n"
            f" Common committed index window: {prefix}\n"
            f" Common commits: {len(self.common_indices):,}\n"
            f" Committed transactions in common window: {transactions:,}\n"
            f" Committed TPS (minimum across nodes): {min(rates, default=0):,.0f} tx/s\n"
            f" Mean end-to-end latency (local batches): {mean(latencies) if latencies else 0:.2f} ms\n"
            f' Latest undecided slots per node: {[stats.get("Undecided", 0) for stats in self.last_stats]}\n'
            f' Latest pending blocks per node: {[stats.get("Pending", 0) for stats in self.last_stats]}\n'
        )

    def print(self, filename):
        with open(filename, "a") as output:
            output.write(self.result())

    @classmethod
    def process(cls, directory):
        nodes = []
        for filename in sorted(glob(join(directory, "node-info-*.log"))):
            with open(filename, "r") as source:
                nodes.append(source.read())
        return cls(nodes)
