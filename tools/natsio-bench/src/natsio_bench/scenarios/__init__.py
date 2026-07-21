"""Benchmark scenarios and their registry.

Importing this package populates :data:`SCENARIOS` (via the ``@register``
decorators in :mod:`core` and :mod:`jetstream`) — insertion order is report
order.
"""

# Imported for their registration side effects. Kept last, and referenced in
# __all__, so linters neither reorder nor flag them as unused.
from natsio_bench.scenarios import core, jetstream
from natsio_bench.scenarios.base import (
    SCENARIOS,
    BenchConfig,
    Result,
    Scenario,
)

__all__ = [
    "SCENARIOS",
    "BenchConfig",
    "Result",
    "Scenario",
    "core",
    "jetstream",
]
