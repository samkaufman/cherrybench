import contextlib
import pathlib
import re

CPU_ROOT = pathlib.Path("/sys/devices/system/cpu")
CPU_DIR_RE = re.compile(r"cpu(\d+)")

assert CPU_ROOT.is_dir()


@contextlib.contextmanager
def configure_machine():
    original_governors = _get_governors()
    original_frequencies = _get_scaling_frequency_ranges()
    cpu_frequencies = _get_cpu_frequency_ranges()
    for cpu_id in range(len(original_governors)):
        _set_governor(cpu_id, "performance")
        _, max_freq = cpu_frequencies[cpu_id]
        _set_scaling_freq_range(cpu_id, max_freq, max_freq)
    try:
        yield
    finally:
        for cpu_id, o in enumerate(original_governors):
            _set_governor(cpu_id, o)
            _set_scaling_freq_range(cpu_id, *original_frequencies[cpu_id])


def _get_governors() -> list[str]:
    return _read_cpu_files(pathlib.Path("cpufreq") / "scaling_governor")


def _get_scaling_frequency_ranges() -> list[tuple[int, int]]:
    lows = _read_cpu_files(pathlib.Path("cpufreq") / "scaling_min_freq")
    highs = _read_cpu_files(pathlib.Path("cpufreq") / "scaling_max_freq")
    return list(zip(map(int, lows), map(int, highs)))


def _get_cpu_frequency_ranges() -> list[tuple[int, int]]:
    lows = _read_cpu_files(pathlib.Path("cpufreq") / "cpuinfo_min_freq")
    highs = _read_cpu_files(pathlib.Path("cpufreq") / "cpuinfo_max_freq")
    return list(zip(map(int, lows), map(int, highs)))


def _set_governor(cpu_id: int, governor: str) -> None:
    _set_cpu_file(pathlib.Path("cpufreq") / "scaling_governor", cpu_id, governor)


def _set_scaling_freq_range(cpu_id: int, low: int, high: int) -> None:
    _set_cpu_file(pathlib.Path("cpufreq") / "scaling_min_freq", cpu_id, str(low))
    _set_cpu_file(pathlib.Path("cpufreq") / "scaling_max_freq", cpu_id, str(high))


def _read_cpu_files(suffix: pathlib.Path) -> list[str]:
    cpu_dirs = {}
    for cpu_dir in CPU_ROOT.iterdir():
        dir_name_match = CPU_DIR_RE.match(cpu_dir.name)
        if cpu_dir.is_dir() and dir_name_match:
            cpu_dirs[int(dir_name_match.group(1))] = cpu_dir
    cpu_dirs = [cpu_dirs[i] for i in sorted(cpu_dirs.keys())]

    governors = []
    for cpu_dir in cpu_dirs:
        path = cpu_dir / suffix
        with path.open("r") as fo:
            governors.append(fo.read().strip())
    return governors


def _set_cpu_file(suffix: pathlib.Path, cpu_id: int, contents: str) -> None:
    path = CPU_ROOT / f"cpu{cpu_id}" / suffix
    with path.open("w") as fo:
        fo.write(contents)
