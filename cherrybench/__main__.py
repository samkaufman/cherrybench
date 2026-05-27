import argparse
import datetime
import logging
import math
import pathlib
import random
import tempfile
import tomllib

from . import host_config, reporting
from .jobs import DockerfileJob

MIN_SAMPLES = 5
MIN_RUNTIME = 10  # seconds
JOB_CHUNK_SIZE = 4  # TODO: Derive from core count

logger = logging.getLogger(__name__)

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("-v", "--verbose", action="store_true")
arg_parser.add_argument(
    "--filter",
    action="append",
    help="Only run jobs whose name matches all of the given filters (substring match). Can be specified multiple times.",
)
arg_parser.add_argument("CONFIG", type=pathlib.Path)
args = arg_parser.parse_args()


logging.basicConfig(level=(logging.DEBUG if args.verbose else logging.INFO))


def load_config(input_file, job_filters=None):
    with input_file.open("rb") as fo:
        data = tomllib.load(fo)

        jobs = []
        for job_entry in data["jobs"]:
            if job_filters and not all(
                f in job_entry["name"] or f in job_entry["backend_name"]
                for f in job_filters
            ):
                continue
            jobs.append(
                DockerfileJob(
                    name=job_entry["name"],
                    size=job_entry["size"],
                    batch_size=int(job_entry["batch_size"]),
                    backend_name=job_entry["backend_name"],
                    docker_path=pathlib.Path(job_entry["docker_path"]),
                    docker_build_args=job_entry.get("docker_build_args", {}),
                    command=job_entry["command"],
                    gflops=job_entry.get("gflops"),
                    num_cores=job_entry.get("num_cores", 1),
                    enable_perf=job_entry.get("enable_perf", False),
                )
            )

        reporters = []
        for reporter_key, reporter_entry in data["reporters"].items():
            if reporter_key == "google_sheets":
                reporters.append(
                    reporting.GSheetsReporter(
                        google_key_file=pathlib.Path(reporter_entry["key_file"]),
                        gsheet_name=reporter_entry["sheet_name"],
                        remote_root_name=reporter_entry["folder_name"],
                    )
                )
            else:
                raise ValueError(f"Unknown reporter type {reporter_entry['type']}")

        # Load optional max_work_time (in seconds)
        max_work_time = data.get("max_work_time")
        if max_work_time is not None:
            max_work_time = float(max_work_time)

        # Process jobs according to `order`
        order = data.get("order")
        if order == "random":
            random.shuffle(jobs)
            logger.debug("Shuffling jobs in random order")
        elif order == "random-new-first":
            new_jobs = []
            existing_jobs = []

            for job in jobs:
                is_new = True
                for reporter in reporters:
                    if reporter.has_existing_entry(job):
                        is_new = False
                        break

                if is_new:
                    new_jobs.append(job)
                else:
                    existing_jobs.append(job)

            random.shuffle(new_jobs)
            random.shuffle(existing_jobs)

            jobs = new_jobs + existing_jobs
            logger.debug(
                "Ordered jobs with random-new-first: %d new jobs, %d existing jobs",
                len(new_jobs),
                len(existing_jobs),
            )
        elif order is not None and order != "sequential":
            raise ValueError(
                f"Unknown order value '{order}'. Supported values are 'random', 'sequential', and 'random-new-first'"
            )

    return (jobs, reporters, max_work_time)


def run_job_to_sufficiency(job, output_dir, cherrybench_dir):
    inner_loop_count = MIN_SAMPLES
    samps = None
    # TODO: Tell user and quit if increasing loop count doesn't increase time.
    while True:
        samps = job.run(output_dir, inner_loop_count, cherrybench_dir)
        fastest_sample = min(samps)
        fastest_total_runtime = fastest_sample * inner_loop_count
        if fastest_total_runtime >= MIN_RUNTIME:
            break
        inner_loop_count = max(
            inner_loop_count + 1,
            math.ceil(
                inner_loop_count * min(10, MIN_RUNTIME / fastest_total_runtime)
            ),
        )
        logger.debug(
            "fastest sample was %s seconds, increasing loop count to %s",
            fastest_sample,
            inner_loop_count,
        )
    return samps


def run(jobs, reporters, max_work_time=None) -> None:
    process_start_time = datetime.datetime.now()
    if max_work_time is not None:
        logger.info("Maximum work time set to %.1f seconds", max_work_time)

    with tempfile.TemporaryDirectory() as cherrybench_dir:
        cherrybench_dir = pathlib.Path(cherrybench_dir)
        logger.info("Created temporary cherrybench directory: %s", cherrybench_dir)

        for job_chunk_idx in range(0, len(jobs), JOB_CHUNK_SIZE):
            chunk = jobs[job_chunk_idx : job_chunk_idx + JOB_CHUNK_SIZE]
            # TODO: Parallelize the prepare calls below
            for job in chunk:
                logger.info(
                    "Preparing job %s,%s,%s", job.name, job.size, job.backend_name
                )
                job.prepare()
            with host_config.configure_machine():
                for job in chunk:
                    # Check if we should stop starting new jobs due to time limit
                    if max_work_time is not None:
                        elapsed_time = (
                            datetime.datetime.now() - process_start_time
                        ).total_seconds()
                        if elapsed_time >= max_work_time:
                            logger.info(
                                "Maximum work time (%.1f seconds) reached. Stopping before job %s",
                                max_work_time,
                                job.name,
                            )
                            return

                    logger.info("Running job %s,%s", job.name, job.backend_name)
                    with tempfile.TemporaryDirectory() as output_dir:
                        output_dir = pathlib.Path(output_dir)
                        logger.debug("Temporary output directory is %s", output_dir)
                        start_time = datetime.datetime.now()
                        runtime_samples = run_job_to_sufficiency(
                            job, output_dir, cherrybench_dir
                        )
                        for reporter in reporters:
                            reporter.log_result(
                                start_time,
                                job,
                                min(runtime_samples),
                                runtime_samples,
                                is_rt=False,  # TODO: Change when RT is supported
                                local_dir=output_dir,
                            )


run(*load_config(args.CONFIG, job_filters=args.filter))
