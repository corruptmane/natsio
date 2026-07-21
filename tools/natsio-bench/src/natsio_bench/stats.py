"""Metric math: rates, throughput, latency percentiles — stdlib only.

No numpy: percentiles come from sorted-list indexing (nearest-rank), rates and
throughput from plain division. Kept deliberately tiny so the numbers in the
report are trivially auditable.
"""

import math
from collections.abc import Sequence
from statistics import median

__all__ = [
    "aggregate",
    "latency_percentiles",
    "mb_per_s",
    "msgs_per_s",
    "percentile",
]


def msgs_per_s(count: int, seconds: float) -> float:
    """Messages (or ops) per second over an elapsed wall-clock window."""
    return count / seconds if seconds > 0 else 0.0


def mb_per_s(total_bytes: int, seconds: float) -> float:
    """Megabytes (10^6 bytes) per second — the unit users compare disks in."""
    return (total_bytes / 1_000_000) / seconds if seconds > 0 else 0.0


def percentile(samples: Sequence[float], p: float) -> float:
    """The ``p``-th percentile (0-100) by nearest-rank on a sorted copy.

    Nearest-rank (not interpolated): with N samples the p-th percentile is the
    value at 1-based rank ``ceil(p/100 * N)``. Deterministic and allocation-cheap,
    which is all a benchmark report needs.
    """
    if not samples:
        return 0.0
    ordered = sorted(samples)
    if p <= 0:
        return ordered[0]
    if p >= 100:
        return ordered[-1]
    rank = math.ceil(p / 100 * len(ordered))
    index = min(max(rank - 1, 0), len(ordered) - 1)
    return ordered[index]


def latency_percentiles(samples_ms: Sequence[float]) -> dict[str, float]:
    """p50/p90/p99/max (milliseconds) from raw per-sample latencies."""
    return {
        "p50": percentile(samples_ms, 50),
        "p90": percentile(samples_ms, 90),
        "p99": percentile(samples_ms, 99),
        "max": max(samples_ms) if samples_ms else 0.0,
    }


def aggregate(values: Sequence[float]) -> dict[str, float]:
    """Median / min / max across per-repeat metric values (the runner's summary)."""
    if not values:
        return {"median": 0.0, "min": 0.0, "max": 0.0}
    return {"median": float(median(values)), "min": min(values), "max": max(values)}
