import dataclasses
import logging
import pathlib
import platform
import sys
from typing import Optional

import docker
import docker.models
import docker.models.containers
import docker.types

from . import lscpu

_DOCKER_CLIENT: docker.DockerClient = None  # type: ignore
_DOCKER_EXCEPTION_STOP_TIMEOUT = 2
_ASSIGN_CORES = False

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class DockerfileJob:
    name: str
    size: int
    batch_size: int
    backend_name: str
    docker_path: pathlib.Path
    docker_build_args: dict[str, str]
    command: list[str]
    gflops: Optional[float] = None
    num_cores: int = 1  # Number of physical cores to use
    enable_perf: bool = False

    def __post_init__(self):
        # Initialize the global Docker client if needed.
        global _DOCKER_CLIENT
        if _DOCKER_CLIENT is None:
            _DOCKER_CLIENT = docker.from_env()

    def prepare(self):
        global _DOCKER_CLIENT
        build_args = dict(self.docker_build_args)
        if self.enable_perf:
            build_args["CHERRYBENCH_HOST_LINUX_VERSION"] = platform.release()
        image, build_logs = _DOCKER_CLIENT.images.build(
            path=str(self.docker_path), rm=False, buildargs=build_args
        )

        # Log build output
        for log_entry in build_logs:
            if "stream" in log_entry:
                logger.info(log_entry["stream"].rstrip())
            elif "error" in log_entry:
                logger.error(log_entry["error"].rstrip())
            elif "status" in log_entry:
                logger.debug(log_entry["status"].rstrip())

        self.image = image
        logger.info("Built image: %s", self.image.id)  # TODO: Downgrade to DEBUG

    def run(self, output_dir: pathlib.Path, inner_steps: int) -> list[float]:
        global _DOCKER_CLIENT
        assert output_dir.is_dir()

        if _ASSIGN_CORES:
            topology = lscpu.system_topology()
            available_cores = set(c.core for c in topology.logical_cpus)
            if self.num_cores > len(available_cores):
                raise ValueError(
                    f"Job {self.name} requested {self.num_cores} cores, but only {len(available_cores)} physical cores are available"
                )

            # Select the first num_cores physical cores
            selected_cores = sorted(available_cores)[: self.num_cores]
            logical_cpus = {
                c.id for c in topology.logical_cpus if c.core in selected_cores
            }

            # Log which cores this job is using
            logger.info(
                "Job %s using %d physical cores (%s) with logical CPUs: %s",
                self.name,
                self.num_cores,
                selected_cores,
                logical_cpus,
            )

        e = {
            "CHERRYBENCH_OUTPUT_DIR": "/cherrybench_output",
            "CHERRYBENCH_LOOP_STEPS": str(inner_steps),
        }
        v = {str(output_dir): {"bind": "/cherrybench_output", "mode": "rw"}}
        run_kwargs = {
            "environment": e,
            "volumes": v,
            "detach": True,
            "cap_add": ["SYS_NICE"],
            "privileged": self.enable_perf,
        }
        if _ASSIGN_CORES:
            run_kwargs["cpuset_cpus"] = ",".join(str(c) for c in logical_cpus)
        container = _DOCKER_CLIENT.containers.run(
            self.image.id, self.command, **run_kwargs
        )  # type: ignore
        assert isinstance(container, docker.models.containers.Container)
        try:
            exit_code = container.wait()["StatusCode"]

            with (output_dir / "stderr.log").open("wb") as fo:
                for b in container.logs(stdout=False, stderr=True, stream=True):
                    fo.write(b)

            if exit_code != 0:
                print(
                    container.logs(stdout=False, stderr=True).decode("utf8"),
                    file=sys.stderr,
                )
                raise Exception(f"Exit code was {exit_code}")

            r = container.logs(stdout=True, stderr=False)
            assert isinstance(r, bytes)
            r = r.decode("utf-8").strip()
            runtime_secs = []
            for line in r.splitlines():
                if line.endswith("ns"):
                    line = line[:-2]
                    runtime_secs.append(float(line) / (inner_steps * 1_000_000_000))
                else:
                    if line.endswith("s"):
                        line = line[:-1]
                    runtime_secs.append(float(line) / inner_steps)
            return runtime_secs
        finally:
            container.stop(timeout=_DOCKER_EXCEPTION_STOP_TIMEOUT)
            container.remove()
