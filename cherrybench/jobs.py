import dataclasses
import pathlib
import sys

import docker
import docker.models
import docker.models.containers
import docker.types

_DOCKER_CLIENT: docker.DockerClient = None # type: ignore
_DOCKER_EXCEPTION_STOP_TIMEOUT = 2

@dataclasses.dataclass
class DockerfileJob:
    name: str
    size: int
    batch_size: int
    backend_name: str
    docker_path: pathlib.Path
    docker_build_args: dict[str, str]
    command: list[str]

    def __post_init__(self):
        # Initialize the global Docker client if needed.
        global _DOCKER_CLIENT
        if _DOCKER_CLIENT is None:
            _DOCKER_CLIENT = docker.from_env()

    def prepare(self):
        global _DOCKER_CLIENT
        image, _ = _DOCKER_CLIENT.images.build(
            path=str(self.docker_path), rm=False,
            buildargs=self.docker_build_args
        )  # type: ignore
        self.image = image

    def run(self, output_dir: pathlib.Path, inner_steps: int, logical_cpus: set[int]) -> list[float]:
        global _DOCKER_CLIENT
        assert output_dir.is_dir()
        e = {
            "CHERRYBENCH_OUTPUT_DIR": "/cherrybench_output",
            "CHERRYBENCH_LOOP_STEPS": str(inner_steps),
        }
        v = {str(output_dir): {"bind": "/cherrybench_output", "mode": "rw"}}
        container = _DOCKER_CLIENT.containers.run(
            self.image.id, self.command, environment=e, volumes=v, detach=True,
            cap_add=["SYS_NICE"],
            cpuset_cpus=','.join(str(c) for c in logical_cpus),
            #
            # TODO: Run at REALTIME priority
            # cpu_rt_runtime=1000000,
            # ulimits=[docker.types.Ulimit(name='rtprio', soft=99, hard=99)],
            # privileged=True,
        )
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
