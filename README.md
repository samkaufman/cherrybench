# cherrybench

cherrybench is a simple tool for running sets of performance benchmarks.
It was written to evaluate the performance of [Morello's](https://github.com/samkaufman/morello)
generated code.

## Job Configuration

Each `[job]` in a config. file supports the following fields:

- `name` (string): Unique identifier for the job.
- `size` (integer): Problem size parameter for the benchmark.
- `batch_size` (integer): Number of iterations per batch.
- `backend_name` (string): Identifier of the backend under test.
- `docker_path` (string): Path to the directory containing the Dockerfile.
- `docker_build_args` (table): Map of build-time arguments passed to `docker build`.
- `command` (array[string]): Command and arguments to run inside the container.
- `gflops` (number, optional): Total numbers of GFLOPS to complete the job.
- `num_cores` (integer, default=1): Number of physical CPU cores to bind to the
   container.