"""Sequential benchmark runner: fresh server per pair, repeats, hard isolation.

For every (scenario, client) pair the runner starts a brand-new JetStream-enabled
``nats-server`` (isolated tmp store), connects the adapter, runs R repeats, and
records the per-repeat metric plus a median/min/max summary. Nothing overlaps —
timing purity demands one server, one connection, one measured phase at a time.

Isolation is absolute: an unsupported capability becomes a recorded *skip*, and
any exception (server start, connect, or a repeat) becomes a recorded *error*.
Neither stops the run; the report always accounts for every pair.
"""

import subprocess
import traceback
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path

# natsio.testing is a separate distribution grafted into the natsio namespace via
# a .pth; ty can't follow the split editable install (same as natsio/tests/server.py).
from natsio.testing import NatsServerProcess, find_server_binary  # ty: ignore[unresolved-import]

from natsio_bench.adapters import ADAPTERS, Adapter
from natsio_bench.scenarios import SCENARIOS, BenchConfig
from natsio_bench.scenarios.base import Scenario
from natsio_bench.stats import aggregate

__all__ = [
    "PairResult",
    "ProgressCallback",
    "RunResult",
    "discover_server_binary",
    "run",
    "server_version",
]

type ProgressCallback = Callable[[str], None]


@dataclass(slots=True)
class RepeatSample:
    value: float
    detail: dict[str, float]
    ops: int
    seconds: float


@dataclass(slots=True)
class PairResult:
    """The outcome of one (scenario, client) pair."""

    scenario: str
    client: str
    status: str  # "ok" | "skip" | "error"
    unit: str = ""
    higher_is_better: bool = True
    summary: dict[str, float] = field(default_factory=dict)  # median/min/max of value
    repeats: list[RepeatSample] = field(default_factory=list)
    skip_reason: str | None = None
    error: str | None = None

    @property
    def value(self) -> float | None:
        """The headline (median) metric, or None when skipped/errored."""
        return self.summary.get("median") if self.status == "ok" else None


@dataclass(slots=True)
class RunResult:
    scenarios: list[str]
    clients: list[str]
    repeats: int
    quick: bool
    pairs: list[PairResult]


def discover_server_binary() -> str:
    """Locate ``nats-server``: ``NATS_SERVER_BIN`` / PATH, else the repo's tools/.bin.

    natsio-testing already honours ``NATS_SERVER_BIN`` and PATH; this adds the
    repo-local ``tools/.bin/nats-server`` fallback by walking up from the cwd, so
    the bench tool works from a checkout without any env setup.
    """
    found = find_server_binary()
    if found is not None:
        return found
    for parent in [Path.cwd(), *Path.cwd().parents]:
        candidate = parent / "tools" / ".bin" / "nats-server"
        if candidate.is_file():
            return str(candidate)
    raise FileNotFoundError(
        "nats-server not found: set NATS_SERVER_BIN, put it on PATH, or run from a "
        "checkout containing tools/.bin/nats-server"
    )


def server_version(binary: str) -> str:
    """``nats-server --version`` output, trimmed (best-effort)."""
    try:
        out = subprocess.run(
            [binary, "--version"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    return (out.stdout or out.stderr).strip() or "unknown"


async def _run_pair(
    adapter_cls: type[Adapter],
    scenario: Scenario,
    binary: str,
    repeats: int,
    config: BenchConfig,
) -> PairResult:
    adapter = adapter_cls()
    if not adapter.supports(scenario.capability):
        return PairResult(
            scenario=scenario.name,
            client=adapter_cls.name,
            status="skip",
            skip_reason=f"no {scenario.capability.value} capability",
        )

    # A fresh, JetStream-enabled server per pair — no cross-contamination, and an
    # isolated store dir the process cleans up on stop().
    server = NatsServerProcess(binary, jetstream=True)
    samples: list[RepeatSample] = []
    unit = ""
    higher_is_better = True
    try:
        await server.start()
        await adapter.connect(server.url)
        for _ in range(repeats):
            result = await scenario.fn(adapter, server.url, config)
            unit = result.unit
            higher_is_better = result.higher_is_better
            samples.append(
                RepeatSample(value=result.value, detail=result.detail, ops=result.ops, seconds=result.seconds)
            )
    except Exception:
        return PairResult(
            scenario=scenario.name,
            client=adapter_cls.name,
            status="error",
            unit=unit,
            higher_is_better=higher_is_better,
            repeats=samples,
            error=traceback.format_exc(limit=6).strip(),
        )
    finally:
        # Teardown must never mask the real result (or raise from the finally).
        with suppress(Exception):
            await adapter.close()
        await server.stop()

    return PairResult(
        scenario=scenario.name,
        client=adapter_cls.name,
        status="ok",
        unit=unit,
        higher_is_better=higher_is_better,
        summary=aggregate([s.value for s in samples]),
        repeats=samples,
    )


async def run(
    *,
    clients: list[str],
    scenarios: list[str],
    repeats: int,
    config: BenchConfig,
    binary: str,
    progress: ProgressCallback | None = None,
) -> RunResult:
    """Run every requested (scenario, client) pair sequentially."""
    pairs: list[PairResult] = []
    total = len(scenarios) * len(clients)
    done = 0
    for scenario_name in scenarios:
        scenario = SCENARIOS[scenario_name]
        for client_name in clients:
            done += 1
            if progress is not None:
                progress(f"[{done}/{total}] {scenario_name} :: {client_name}")
            pair = await _run_pair(ADAPTERS[client_name], scenario, binary, repeats, config)
            if progress is not None:
                progress(f"    -> {_summarize(pair)}")
            pairs.append(pair)
    return RunResult(scenarios=scenarios, clients=clients, repeats=repeats, quick=config.quick, pairs=pairs)


def _summarize(pair: PairResult) -> str:
    if pair.status == "skip":
        return f"n/s ({pair.skip_reason})"
    if pair.status == "error":
        first_line = (pair.error or "error").splitlines()[-1]
        return f"ERROR: {first_line}"
    return f"{pair.summary['median']:.1f} {pair.unit}"
