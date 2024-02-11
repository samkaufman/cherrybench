import argparse
import datetime
import logging
import pathlib
import tempfile
import tomllib

from . import host_config, lscpu, reporting
from .jobs import DockerfileJob

MIN_SAMPLES = 5
MIN_RUNTIME = 5  # seconds

logger = logging.getLogger(__name__)

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("-v", "--verbose", action="store_true")
arg_parser.add_argument("CONFIG", type=pathlib.Path)
args = arg_parser.parse_args()

logging.basicConfig(level=(logging.DEBUG if args.verbose else logging.INFO))


def load_config(input_file):
    with input_file.open("rb") as fo:
        data = tomllib.load(fo)

        jobs = []
        for job_entry in data["jobs"]:
            jobs.append(
                DockerfileJob(
                    name=job_entry["name"],
                    size=job_entry["size"],
                    batch_size=int(job_entry["batch_size"]),
                    backend_name=job_entry["backend_name"],
                    docker_path=pathlib.Path(job_entry["docker_path"]),
                    docker_build_args=job_entry.get("docker_build_args", {}),
                    command=job_entry["command"],
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

    return (jobs, reporters)


def run_job_to_sufficiency(job, output_dir, logical_cpus):
    inner_loop_count = MIN_SAMPLES
    samps = None
    # TODO: During timing phase, just run the job with outer loop count of 1.
    while not samps or any((r * inner_loop_count) < MIN_RUNTIME for r in samps):
        samps = job.run(output_dir, inner_loop_count, logical_cpus)
        print(f"inner_loop_count={inner_loop_count}, samps={samps}")
        inner_loop_count *= min(100, max(2, MIN_RUNTIME / samps[0]))
    return samps


def run(jobs, reporters):
    # Find the logical cores corresponding to the first physical core.
    first_cpus = {c.id for c in lscpu.system_topology().logical_cpus if c.core == 0}
    assert first_cpus
    logger.info("First physical core corresponds to logical CPUs: %s", first_cpus)

    for job in jobs:
        logger.info("Preparing job %s", job.name)
        job.prepare()
    with host_config.configure_machine():
        for job in jobs:
            logger.info("Running job %s", job.name)
            with tempfile.TemporaryDirectory() as output_dir:
                output_dir = pathlib.Path(output_dir)
                logger.debug("Temporary output directory is %s", output_dir)
                start_time = datetime.datetime.now()
                runtime_samples = run_job_to_sufficiency(job, output_dir, first_cpus)
                for reporter in reporters:
                    reporter.log_result(
                        start_time,
                        job,
                        min(runtime_samples),
                        runtime_samples,
                        is_rt=False,  # TODO: Change when RT is supported
                        local_dir=output_dir,
                    )


run(*load_config(args.CONFIG))
