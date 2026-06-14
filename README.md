# cherrybench

cherrybench is a simple tool for running sets of performance benchmarks.
It was written to evaluate the performance of [Morello's](https://github.com/samkaufman/morello)
generated code.

## Job Configuration

Each `[[jobs]]` entry in a config file supports the following fields:

- `name` (string): Unique identifier for the job.
- `size` (integer): Problem size parameter for the benchmark.
- `batch_size` (integer): Number of iterations per batch.
- `backend_name` (string): Identifier of the backend under test.
- `docker_path` (string): Path to the directory containing the Dockerfile. Required
  when `image_ref` is absent.
- `image_ref` (string): Image reference to run directly, such as `repo/name:tag`.
  Required when `docker_path` is absent.
- `docker_build_args` (table): Map of build-time arguments passed to `docker build`
  when cherrybench builds an image from `docker_path`.
- `command` (array[string]): Command and arguments to run inside the container.
- `gflops` (number, optional): Total numbers of GFLOPS to complete the job.
- `num_cores` (integer, default=1): Number of physical CPU cores to bind to the
   container.
- `enable_perf` (boolean, default=false): Whether to enable Linux perf integration.

Set exactly one of `docker_path` or `image_ref`. Existing configs that use
`docker_path` continue to build an image before running the benchmark. Configs that
use `image_ref` skip the build and run the container from that image reference. This
lets artifact repositories ship exact Docker image archives, which evaluators can
load with `docker image load` before running cherrybench.

## Reporting

Benchmark results are sent to each configured reporter.

By default, cherrybench prints benchmark result rows as CSV to standard output.
To disable stdout reporting, set:

```toml
[reporters.stdout]
enabled = false
```

### Google Sheets
cherrybench also supports reporting to Google Sheets. To enable that, add:

```toml
[reporters.google_sheets]
key_file = "service-account.json"
sheet_name = "Benchmark Results"
folder_name = "Benchmark Artifacts"
```

## Job Partitioning

To split the same job config across multiple machines, add a top-level
`[job_partition]` table:

```toml
[job_partition]
count = 4
index = 0
```

`count` is the total number of partitions, and `index` is the zero-based partition
to execute on this machine. Cherrybench assigns each job to a partition using a
deterministic SHA-256 hash of the job's `name`, so every job name maps to the same
partition on every machine. Jobs with the same `name` always run in the same
partition.

## Run Control

By default, cherrybench starts each job with `CHERRYBENCH_LOOP_STEPS=5` and
increases that value until the fastest sample represents at least 10 seconds of
work. You can adjust these parameters with a top-level `[run_control]` table:

```toml
[run_control]
min_loop_steps = 1
min_runtime = 0
```

- `min_loop_steps` (positive integer, default=5): Initial value for
  `CHERRYBENCH_LOOP_STEPS`.
- `min_runtime` (nonnegative number, default=10): Minimum runtime in seconds
  represented by the fastest sample before cherrybench stops increasing
  `CHERRYBENCH_LOOP_STEPS`.

### Linux Perf Integration

Jobs can optionally enable Linux perf integration by setting the `enable_perf` to
`true`. This supports Docker containers running Linux perf by increasing permissions,
passing in the host's Linux version (for matching), and automatically parsing Linux perf
output for the benefit of reporters.

- cherrybench will build the Docker image with the `CHERRYBENCH_HOST_LINUX_VERSION`
  build argument set to the host's Linux version.
