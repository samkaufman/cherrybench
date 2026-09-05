import dataclasses
from typing import TypeAlias


@dataclasses.dataclass(frozen=True)
class SuccessResult:
    """Carry timing samples for a successful benchmark."""

    runtime_samples: tuple[float, ...]

    def __post_init__(self):
        if not self.runtime_samples:
            raise ValueError("A successful benchmark must have timing samples")

    @property
    def fastest_runtime_secs(self) -> float:
        """Return the fastest timing sample."""
        return min(self.runtime_samples)


@dataclasses.dataclass(frozen=True)
class UnsatisfiableResult:
    """Report that the configured runner cannot satisfy this benchmark."""

    reason: str | None = None


BenchmarkResult: TypeAlias = SuccessResult | UnsatisfiableResult
