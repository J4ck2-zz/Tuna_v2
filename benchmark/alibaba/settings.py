from json import JSONDecodeError, load


class SettingsError(Exception):
    pass


class Settings:
    def __init__(
        self,
        key_name,
        key_path,
        accesskey_path,
        node_port,
        mempool_port,
        instance_type,
        aws_regions,
    ):
        regions = aws_regions if isinstance(aws_regions, list) else [aws_regions]
        inputs_str = [key_name, key_path, instance_type]
        inputs_str += regions
        inputs_int = [node_port]
        ok = all(isinstance(x, str) for x in inputs_str)
        ok &= all(isinstance(x, int) for x in inputs_int)
        ok &= len(regions) > 0
        if not ok:
            raise SettingsError("Invalid settings types")

        self.key_name = key_name
        self.key_path = key_path
        self.accesskey_path = accesskey_path

        self.node_port = node_port
        self.mempool_port = mempool_port

        self.instance_type = instance_type
        self.aws_regions = regions

    @classmethod
    def load(cls, filename):
        try:
            with open(filename, "r") as f:
                data = load(f)

            return cls(
                data["key"]["name"],
                data["key"]["path"],
                data["key"]["accesskey"],
                data["ports"]["node"],
                data["ports"]["mempool"],
                data["instances"]["type"],
                data["instances"]["regions"],
            )
        except (OSError, JSONDecodeError) as e:
            raise SettingsError(str(e))

        except KeyError as e:
            raise SettingsError(f"Malformed settings: missing key {e}")
