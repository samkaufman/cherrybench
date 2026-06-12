def median_gflops_per_sec(
    gflops: float | None, runtime_samples: list[float]
) -> float | None:
    """Return median achieved GFLOP/s, or None when it cannot be computed."""
    if gflops is None or not runtime_samples:
        return None

    gflops_per_sec_samples = [gflops / s for s in runtime_samples if s > 0]
    n = len(gflops_per_sec_samples)
    if not n:
        return None

    sorted_samples = sorted(gflops_per_sec_samples)
    mid = n // 2
    if n % 2 == 1:
        return sorted_samples[mid]
    return (sorted_samples[mid - 1] + sorted_samples[mid]) / 2
