import hashlib


def parse_partition_config(config):
    if config is None:
        return None
    if not isinstance(config, dict):
        raise ValueError("job_partition must be a TOML table")

    supported_keys = {"count", "index"}
    unknown_keys = set(config) - supported_keys
    if unknown_keys:
        unknown_keys = ", ".join(sorted(unknown_keys))
        raise ValueError(f"Unknown job_partition keys: {unknown_keys}")

    try:
        partition_count = config["count"]
        partition_index = config["index"]
    except KeyError as e:
        raise ValueError(f"Missing job_partition.{e.args[0]}") from e

    if not isinstance(partition_count, int) or isinstance(partition_count, bool):
        raise ValueError("job_partition.count must be an integer")
    if not isinstance(partition_index, int) or isinstance(partition_index, bool):
        raise ValueError("job_partition.index must be an integer")
    if partition_count < 1:
        raise ValueError("job_partition.count must be at least 1")
    if partition_index < 0 or partition_index >= partition_count:
        raise ValueError(
            "job_partition.index must be greater than or equal to 0 and less than job_partition.count"
        )

    return partition_count, partition_index


def job_partition_index(job_name, partition_count):
    digest = hashlib.sha256(job_name.encode("utf-8")).digest()
    return int.from_bytes(digest, byteorder="big") % partition_count
