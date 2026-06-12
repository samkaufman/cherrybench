import contextlib
import csv
import datetime
import io
import pathlib
import platform
import sys
import types
import unittest

from . import _median_gflops_per_sec

STDOUT_COLUMNS = [
    "start_time",
    "hostname",
    "job_name",
    "job_size",
    "batch_size",
    "backend_name",
    "runtime_secs",
    "median_gflops_per_sec",
    "runtime_samples",
    "is_rt",
]


class StdoutReporter:
    def __init__(self):
        self.hostname = platform.node()
        self._wrote_header = False

    def log_result(
        self,
        start_time,
        job,
        runtime_secs: float,
        runtime_samples,
        is_rt: bool,
        local_dir: pathlib.Path,
    ):
        writer = csv.writer(sys.stdout, lineterminator="\n")
        if not self._wrote_header:
            writer.writerow(STDOUT_COLUMNS)
            self._wrote_header = True

        median_gflops_per_sec = _median_gflops_per_sec(job.gflops, runtime_samples)
        row = [
            str(start_time),
            self.hostname,
            job.name,
            job.size,
            job.batch_size,
            job.backend_name,
            runtime_secs,
            "" if median_gflops_per_sec is None else median_gflops_per_sec,
            ", ".join(f"{s:.8f}" for s in runtime_samples),
            str(is_rt),
        ]
        writer.writerow(row)
        sys.stdout.flush()

    def has_existing_entry(self, job) -> bool:
        return False


class StdoutReporterTest(unittest.TestCase):
    def test_log_result_writes_header_and_row(self):
        output = io.StringIO()
        reporter = StdoutReporter()
        reporter.hostname = "host"
        job = types.SimpleNamespace(
            name="job",
            size=128,
            batch_size=4,
            backend_name="backend",
            gflops=20.0,
        )

        with contextlib.redirect_stdout(output):
            reporter.log_result(
                datetime.datetime(2026, 1, 2, 3, 4, 5),
                job,
                1.0,
                [2.0, 1.0, 4.0],
                False,
                pathlib.Path("."),
            )

        rows = list(csv.reader(io.StringIO(output.getvalue())))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0], STDOUT_COLUMNS)
        self.assertEqual(
            rows[1],
            [
                "2026-01-02 03:04:05",
                "host",
                "job",
                "128",
                "4",
                "backend",
                "1.0",
                "10.0",
                "2.00000000, 1.00000000, 4.00000000",
                "False",
            ],
        )
