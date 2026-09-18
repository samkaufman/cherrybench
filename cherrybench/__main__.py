import argparse
import dataclasses
import datetime
import logging
import math
import pathlib
import random
import tempfile
import tomllib

from . import host_config, partition, reporting
from .jobs import DockerfileJob

DEFAULT_MIN_LOOP_STEPS = 5
DEFAULT_MIN_RUNTIME = 10  # seconds
JOB_CHUNK_SIZE = 4  # TODO: Derive from core count
STDOUT_REPORT_DEFAULT = True

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class RunControl:
    min_loop_steps: int = DEFAULT_MIN_LOOP_STEPS
    min_runtime: float = DEFAULT_MIN_RUNTIME

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("-v", "--verbose", action="store_true")
arg_parser.add_argument(
    "--filter",
    action="append",
    help="Only run jobs whose name matches all of the given filters (substring match). Can be specified multiple times.",
)
arg_parser.add_argument("CONFIG", type=pathlib.Path)


def _filter_jobs_by_partition(jobs, partition_config):
    parsed_config = partition.parse_partition_config(partition_config)
    if parsed_config is None:
        return jobs

    partition_count, partition_index = parsed_config
    selected_jobs = [
        job
        for job in jobs
        if (
            partition.job_partition_index(job.name, partition_count)
            == partition_index
        )
    ]
    logger.info(
        "Selected job partition %d/%d: %d of %d jobs",
        partition_index,
        partition_count,
        len(selected_jobs),
        len(jobs),
    )
    return selected_jobs


def _parse_run_control(run_control_entry):
    if not isinstance(run_control_entry, dict):
        raise ValueError("run_control must be a table")

    run_control = RunControl(
        min_loop_steps=run_control_entry.get(
            "min_loop_steps", DEFAULT_MIN_LOOP_STEPS
        ),
        min_runtime=run_control_entry.get("min_runtime", DEFAULT_MIN_RUNTIME),
    )
    if run_control.min_loop_steps <= 0:
        raise ValueError("run_control.min_loop_steps must be positive")
    if run_control.min_runtime < 0:
        raise ValueError("run_control.min_runtime must be nonnegative")
    return run_control


def _parse_job_image_config(job_entry):
    has_docker_path = "docker_path" in job_entry
    has_image_ref = "image_ref" in job_entry
    job_name = job_entry.get("name", "<unknown>")

    if has_docker_path and has_image_ref:
        raise ValueError(
            f"Job {job_name!r} must not provide both docker_path and image_ref"
        )
    if not has_docker_path and not has_image_ref:
        raise ValueError(
            f"Job {job_name!r} must provide either docker_path or image_ref"
        )

    docker_path = pathlib.Path(job_entry["docker_path"]) if has_docker_path else None
    image_ref = job_entry["image_ref"] if has_image_ref else None
    return docker_path, image_ref


def load_config(input_file, job_filters=None):
    with input_file.open("rb") as fo:
        data = tomllib.load(fo)
        run_control = _parse_run_control(data.get("run_control", {}))

        jobs = []
        for job_entry in data["jobs"]:
            docker_path, image_ref = _parse_job_image_config(job_entry)
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
                    docker_path=docker_path,
                    image_ref=image_ref,
                    docker_build_args=job_entry.get("docker_build_args", {}),
                    command=job_entry["command"],
                    gflops=job_entry.get("gflops"),
                    num_cores=job_entry.get("num_cores", 1),
                    enable_perf=job_entry.get("enable_perf", False),
                )
            )

        jobs = _filter_jobs_by_partition(jobs, data.get("job_partition"))

        reporters = []
        reporter_entries = data.get("reporters", {})
        stdout = reporter_entries.get("stdout", {}).get(
            "enabled", STDOUT_REPORT_DEFAULT
        )
        if not isinstance(stdout, bool):
            raise ValueError("reporters.stdout.enabled must be a boolean")
        if stdout:
            reporters.append(reporting.StdoutReporter())

        for reporter_key, reporter_entry in reporter_entries.items():
            if reporter_key == "stdout":
                continue
            if reporter_key == "google_sheets":
                reporters.append(
                    reporting.GSheetsReporter(
                        google_key_file=pathlib.Path(reporter_entry["key_file"]),
                        gsheet_name=reporter_entry.get("sheet_name"),
                        gsheet_key=reporter_entry.get("sheet_key"),
                        remote_root_name=reporter_entry["folder_name"],
                    )
                )
            else:
                raise ValueError(f"Unknown reporter type {reporter_key}")

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

    return (jobs, reporters, max_work_time, run_control)


def run_job_to_sufficiency(job, output_dir, cherrybench_dir, run_control=None):
    if run_control is None:
        run_control = RunControl()
    inner_loop_count = run_control.min_loop_steps
    samps = None
    # TODO: Tell user and quit if increasing loop count doesn't increase time.
    while True:
        samps = job.run(output_dir, inner_loop_count, cherrybench_dir)
        fastest_sample = min(samps)
        fastest_total_runtime = fastest_sample * inner_loop_count
        if fastest_total_runtime >= run_control.min_runtime:
            break
        inner_loop_count = max(
            inner_loop_count + 1,
            math.ceil(
                inner_loop_count
                * min(10, run_control.min_runtime / fastest_total_runtime)
            ),
        )
        logger.debug(
            "fastest sample was %s seconds, increasing loop count to %s",
            fastest_sample,
            inner_loop_count,
        )
    return samps


def run(jobs, reporters, max_work_time=None, run_control=None) -> None:
    if run_control is None:
        run_control = RunControl()
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
                            job, output_dir, cherrybench_dir, run_control
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


def main():
    args = arg_parser.parse_args()
    logging.basicConfig(level=(logging.DEBUG if args.verbose else logging.INFO))
    run(*load_config(args.CONFIG, job_filters=args.filter))


if __name__ == "__main__":
    main()
