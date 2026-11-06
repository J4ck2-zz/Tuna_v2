from json import dump, load


class ConfigError(Exception):
    pass


class Key:
    def __init__(self, pubkey, prikey):
        self.pubkey = pubkey
        self.prikey = prikey

    @classmethod
    def from_file(cls, filename):
        assert isinstance(filename, str)
        with open(filename, "r") as f:
            data = load(f)
        return cls(data["public"], data["private"])


class TSSKey:
    def __init__(self, N, T, pub, share):
        self.N = N
        self.T = T
        self.pub = pub
        self.share = share

    @classmethod
    def from_file(cls, filename):
        assert isinstance(filename, str)
        with open(filename, "r") as f:
            data = load(f)
        return cls(data["N"], data["T"], data["pub"], data["share"])


class Committee:
    def __init__(self, pubkeys, ids, addresses, mempool_addr):
        inputs = [pubkeys, addresses, mempool_addr]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert len({len(x) for x in inputs}) == 1

        self.pubkeys = pubkeys
        self.ids = ids
        self.addresses = addresses
        self.mempool = mempool_addr

        self.json = self._build()

    def _build(self):
        node = {}
        for a, n, id, ma in zip(self.addresses, self.pubkeys, self.ids, self.mempool):
            node[id] = {"name": n, "addr": a, "node_id": id, "mempool_addr": ma}
        return node

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, "w") as f:
            dump(self.json, f, indent=4, sort_keys=True)

    def size(self):
        return len(self.json)


class LocalCommittee(Committee):
    def __init__(self, pubkeys, ids, port):
        assert isinstance(pubkeys, list) and all(isinstance(x, str) for x in pubkeys)
        assert isinstance(port, int)
        size = len(pubkeys)
        addresses = [f"127.0.0.1:{port + i}" for i in range(size)]
        mempool = [f"127.0.0.1:{port + i + size}" for i in range(size)]
        super().__init__(pubkeys, ids, addresses, mempool)


class NodeParameters:
    def __init__(self, json):
        try:
            self.sync_timeout = int(json["mempool"]["sync_timeout"])
            self.tx_size = int(json["pool"]["tx_size"])
            int(json["pool"]["max_queue_size"])
        except KeyError as e:
            raise ConfigError(f"Malformed parameters: missing key {e}")
        except (ValueError, TypeError) as e:
            raise ConfigError(f"Invalid parameters type: {e}")
        self.json = json
        consensus = self.json.setdefault("consensus", {})
        for key, value in {
            "leader_count": 2,
            "leader_timeout_ms": 300,
            "min_block_delay_ms": 20,
            "sync_interval_ms": 100,
            "max_payloads_per_block": 32,
            "max_parents": 1024,
            "max_block_bytes": 1048576,
            "max_pending_blocks": 10000,
            "cache_rounds": 100,
        }.items():
            consensus.setdefault(key, value)

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, "w") as f:
            dump(self.json, f, indent=4, sort_keys=True)


class BenchParameters:
    def __init__(self, json):
        try:
            nodes = json["nodes"]
            nodes = nodes if isinstance(nodes, list) else [nodes]
            if not nodes or any(x <= 0 for x in nodes):
                raise ConfigError("Missing or invalid number of nodes")

            batch_szie = json["batch_size"]
            batch_szie = batch_szie if isinstance(batch_szie, list) else [batch_szie]
            if not batch_szie:
                raise ConfigError("Missing batch_size")

            rate = json["rate"]
            rate = rate if isinstance(rate, list) else [rate]
            if not rate:
                raise ConfigError("Missing rate")

            self.nodes = [int(x) for x in nodes]
            self.log_level = int(json["log_level"])
            self.rate = [int(x) for x in rate]
            self.batch_szie = int(json["batch_size"])
            self.duration = int(json["duration"])
            self.runs = int(json["runs"]) if "runs" in json else 1
            self.node_instance = (
                int(json["node_instance"]) if "node_instance" in json else 1
            )

        except KeyError as e:
            raise ConfigError(f"Malformed bench parameters: missing key {e}")

        except ValueError:
            raise ConfigError("Invalid parameters type")
