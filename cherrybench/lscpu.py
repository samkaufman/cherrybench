"""Gathers the memory and CPU topology of the current host."""

import glob
import json
import subprocess
import dataclasses

@dataclasses.dataclass
class Table:
    logical_cpus: list["LogicalCpu"]
    cache_names: list[str]


@dataclasses.dataclass
class LogicalCpu:
    id: int
    core: int
    numa_node: int
    caches: list[int]


def system_topology() -> Table:
    output = subprocess.run(["lscpu", "--json", "-a", "-e"], check=True, capture_output=True)
    return table_from_lscpu_json(output.stdout.decode('utf-8'))


def table_from_lscpu_json(stdout: str) -> Table:
    logical_cpus = []
    cpu_objs = json.loads(stdout)["cpus"]
    if not cpu_objs:
        return Table([], [])

    cache_levels = _cache_levels_from_cpu_object(cpu_objs[0])
    caches_header = ":".join(cache_levels)
    logical_ids_seen = set()
    for cpu_obj in cpu_objs:
        caches=cpu_obj[caches_header].split(":")
        caches = [int(c) for c in caches]

        assert cpu_obj["cpu"] not in logical_ids_seen
        logical_ids_seen.add(cpu_obj["cpu"])

        logical_cpus.append(
            LogicalCpu(
                id=int(cpu_obj["cpu"]),
                core=int(cpu_obj["core"]),
                numa_node=int(cpu_obj["node"]),
                caches=caches))
    return Table(logical_cpus, cache_names=cache_levels)


def _cache_levels_from_cpu_object(cpu_obj) -> list[str]:
    possible_keys = [k for k in cpu_obj.keys() if ":" in k]
    assert len(possible_keys) == 1
    key = possible_keys[0]
    assert key == key.lower()
    levels = key.split(":")
    assert len(levels) == len(set(levels)) 
    return levels


def _get_governor_settings() -> list[str]:
    govs = []
    for path in glob.glob("/sys/devices/system/cpu/cpu*/cpufreq/scaling_governor"):
        with open(path, "r") as fo:
            govs.append(fo.read().strip())
    assert govs
    return govs


def _check_environment():
    # TODO: Check irqbalance
    # Check choice of governor
    if any(g != "performance" for g in _get_governor_settings()):
        raise Exception("Clock rate governor not set to 'performance'")
    # # TODO: Check CPU frequency range
    # if any(mi != ma for mi, ma in _get_clock_rates()):
    #     raise Exception("CPU clock rates should be fixed (min == max)")
    # TODO: Check that we're realtime
