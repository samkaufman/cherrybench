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

CHERRYBENCH_MOUNT_PATH = "/cherrybench"

_DOCKER_CLIENT: docker.DockerClient = None  # type: ignore
_DOCKER_EXCEPTION_STOP_TIMEOUT = 2
_PREPARED_IMAGE_REPOSITORY = "cherrybench-prepared"
_ASSIGN_CORES = False

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class DockerfileJob:
    name: str
    size: int
    batch_size: int
    backend_name: str
    docker_path: Optional[pathlib.Path]
    docker_build_args: dict[str, str]
    command: list[str]
    gflops: Optional[float] = None
    num_cores: int = 1  # Number of physical cores to use
    enable_perf: bool = False
    image_ref: Optional[str] = None
    _container_image: Optional[str] = dataclasses.field(init=False, default=None)

    def __post_init__(self):
        if (self.docker_path is None) == (self.image_ref is None):
            raise ValueError("Exactly one of docker_path or image_ref must be provided")

        # Initialize the global Docker client if needed.
        global _DOCKER_CLIENT
        if _DOCKER_CLIENT is None:
            _DOCKER_CLIENT = docker.from_env()

    def prepare(self):
        global _DOCKER_CLIENT
        if self.image_ref is not None:
            try:
                _DOCKER_CLIENT.images.get(self.image_ref)
            except docker.errors.ImageNotFound as e:
                raise ValueError(
                    f"Image reference {self.image_ref!r} was not found locally"
                ) from e
            self._container_image = self.image_ref
            logger.info("Using image reference: %s", self.image_ref)
            return

        assert self.docker_path is not None
        build_args = dict(self.docker_build_args)
        if self.enable_perf:
            build_args["CHERRYBENCH_HOST_LINUX_VERSION"] = platform.release()
        try:
            image, _ = _DOCKER_CLIENT.images.build(
                path=str(self.docker_path), rm=False, buildargs=build_args
            )
        except docker.errors.BuildError as e:
            for log_entry in e.build_log:
                if "stream" in log_entry:
                    logger.error(log_entry["stream"])
                elif "errorDetail" in log_entry:
                    logger.error(log_entry["errorDetail"]["message"])
                else:
                    logger.error(str(log_entry))
            raise
        # Tag the build. A chunk of jobs is prepared up front but run one at a
        # time, so an untagged image can sit dangling for hours behind a slow
        # sibling job and be reclaimed before its own job starts.
        image_digest = image.id.removeprefix("sha256:")
        self._container_image = f"{_PREPARED_IMAGE_REPOSITORY}:{image_digest}"
        image.tag(_PREPARED_IMAGE_REPOSITORY, tag=image_digest)
        logger.debug("Built image: %s (tagged as %s)", image.id, self._container_image)

    def run(
        self,
        output_dir: pathlib.Path,
        inner_steps: int,
        cherrybench_dir: Optional[pathlib.Path] = None,
    ) -> list[float]:
        global _DOCKER_CLIENT
        assert output_dir.is_dir()
        if self._container_image is None:
            raise RuntimeError(f"Job {self.name} must be prepared before it can run")

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
        if cherrybench_dir is not None:
            v[str(cherrybench_dir)] = {"bind": CHERRYBENCH_MOUNT_PATH, "mode": "rw"}
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
            self._container_image, self.command, **run_kwargs
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
