"""Scenario contract, registry, and the shared payloads every client reuses.

A scenario is ``async fn(adapter, server_url, config) -> Result``: it does its
own warmup fraction, times only the measured phase, and returns one metric. The
runner handles repeats. Payload byte objects are module-level constants so every
client is handed the *same* ``bytes`` — a fairness requirement, not an accident.
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from natsio_bench.adapters import Adapter, Capability

__all__ = [
    "JS_PAYLOAD",
    "KV_VALUE",
    "PAYLOAD_1K",
    "PAYLOAD_16B",
    "PAYLOAD_64K",
    "SCENARIOS",
    "BenchConfig",
    "Result",
    "Scenario",
    "count",
    "register",
    "warmup_count",
]

_KIB = 1024
_MIB = 1024 * 1024

# Shared, immutable payloads — the identical bytes object goes to every client.
PAYLOAD_16B = bytes(16)
PAYLOAD_1K = bytes(_KIB)
PAYLOAD_64K = bytes(64 * _KIB)
JS_PAYLOAD = bytes(128)
KV_VALUE = bytes(128)
OS_SIZE_FULL = 16 * _MIB
OS_SIZE_QUICK = 4 * _MIB


@dataclass(slots=True)
class BenchConfig:
    """Knobs a scenario reads to size its work."""

    quick: bool = False
    warmup_frac: float = 0.1


@dataclass(slots=True)
class Result:
    """One measured phase: the headline metric plus supporting detail.

    ``higher_is_better`` drives the report's natsio-relative ratio and its
    good/bad colouring; ``detail`` carries secondary numbers (latency tail,
    per-direction throughput) that are shown in JSON and, for latency, the table.
    """

    value: float
    unit: str
    higher_is_better: bool = True
    detail: dict[str, float] = field(default_factory=dict)
    ops: int = 0
    seconds: float = 0.0


type ScenarioFn = Callable[[Adapter, str, BenchConfig], Awaitable[Result]]


@dataclass(slots=True)
class Scenario:
    name: str
    capability: Capability
    group: str
    fn: ScenarioFn


SCENARIOS: dict[str, Scenario] = {}


def register(name: str, *, capability: Capability, group: str) -> Callable[[ScenarioFn], ScenarioFn]:
    """Decorator: add a scenario to the global registry under ``name``."""

    def decorate(fn: ScenarioFn) -> ScenarioFn:
        SCENARIOS[name] = Scenario(name=name, capability=capability, group=group, fn=fn)
        return fn

    return decorate


def count(config: BenchConfig, full: int, quick: int) -> int:
    """Pick the full or the reduced (``--quick``) count."""
    return quick if config.quick else full


def warmup_count(config: BenchConfig, timed: int) -> int:
    """A warmup batch sized as a fraction of the timed work (at least one)."""
    return max(1, int(timed * config.warmup_frac))
