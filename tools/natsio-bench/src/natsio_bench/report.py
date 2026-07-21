"""Rendering: a console table (natsio-relative) and a full JSON dump.

The table shows one median metric per (scenario, client) cell with a ratio versus
natsio, normalized so **>1x always means "beats natsio"** regardless of whether
the metric is higher-better (throughput) or lower-better (latency). The JSON
carries every raw repeat plus machine/env metadata for reproducibility.
"""

import platform
import sys
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as pkg_version

from natsio_bench.runner import PairResult, RunResult, server_version
from natsio_bench.scenarios import SCENARIOS

__all__ = ["build_json", "collect_metadata", "render_console"]

_NATSIO = "natsio"


def _version(dist: str) -> str:
    try:
        return pkg_version(dist)
    except PackageNotFoundError:
        return "unknown"


def collect_metadata(binary: str) -> dict[str, object]:
    """Machine / environment facts that make a run reproducible."""
    return {
        "python": sys.version.split()[0],
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "processor": platform.processor() or "unknown",
        # The CLI drives every client with asyncio.run() and the default policy —
        # no uvloop for anyone, so the comparison is loop-neutral.
        "event_loop": "asyncio (stdlib DefaultEventLoopPolicy, no uvloop)",
        "nats_server": server_version(binary),
        "server_binary": binary,
        "clients": {
            "natsio": _version("natsio"),
            "nats-py": _version("nats-py"),
            "nats-core": _version("nats-core"),
        },
    }


# -- console -----------------------------------------------------------------


def _fmt_value(value: float, unit: str) -> str:
    if unit == "ms":
        return f"{value:.3f}"
    if unit == "MB/s":
        return f"{value:,.1f}"
    return f"{value:,.0f}"


def _ratio(pair: PairResult, natsio_value: float | None) -> str:
    """natsio-relative ratio, normalized so >1x means this client beats natsio."""
    if pair.status != "ok" or natsio_value is None or natsio_value <= 0 or pair.value is None:
        return ""
    if pair.value <= 0:
        return ""
    ratio = pair.value / natsio_value if pair.higher_is_better else natsio_value / pair.value
    return f"{ratio:.2f}x"


def _cell(pair: PairResult | None, natsio_value: float | None) -> str:
    if pair is None:
        return "-"
    if pair.status == "skip":
        return "n/s"
    if pair.status == "error":
        return "ERR"
    assert pair.value is not None
    ratio = _ratio(pair, natsio_value)
    body = _fmt_value(pair.value, pair.unit)
    return f"{body} ({ratio})" if ratio else body


def render_console(run: RunResult, metadata: dict[str, object]) -> str:
    index: dict[tuple[str, str], PairResult] = {(p.scenario, p.client): p for p in run.pairs}
    clients = run.clients

    header = ["scenario", "unit", *clients]
    rows: list[list[str]] = []
    row_groups: list[str] = []

    for name in run.scenarios:
        scenario = SCENARIOS[name]
        natsio_pair = index.get((name, _NATSIO))
        natsio_value = natsio_pair.value if natsio_pair is not None else None
        unit = ""
        cells: list[str] = []
        for client in clients:
            pair = index.get((name, client))
            if pair is not None and pair.unit:
                unit = pair.unit
            cells.append(_cell(pair, natsio_value))
        rows.append([name, unit, *cells])
        row_groups.append(scenario.group)

    widths = [len(h) for h in header]
    for row in rows:
        for i, value in enumerate(row):
            widths[i] = max(widths[i], len(value))

    def line(cells: list[str]) -> str:
        first = cells[0].ljust(widths[0])
        rest = [cells[i].rjust(widths[i]) for i in range(1, len(cells))]
        return "  ".join([first, *rest])

    out: list[str] = []
    out.append(f"natsio-bench  ({'quick' if run.quick else 'full'} mode, {run.repeats} repeats, median shown)")
    out.append("")
    out.extend(_meta_lines(metadata))
    out.append("")
    out.append(line(header))
    out.append("-" * len(line(header)))

    current_group = ""
    for row, group in zip(rows, row_groups, strict=True):
        if group != current_group:
            out.append(f"[{group}]")
            current_group = group
        out.append(line(row))

    out.append("")
    out.append("ratios are vs natsio; >1.00x means the client beats natsio.  n/s = not supported, ERR = failed.")
    if any(p.status == "error" for p in run.pairs):
        out.append("")
        out.append("errors:")
        for pair in run.pairs:
            if pair.status == "error":
                first_line = (pair.error or "").splitlines()[-1] if pair.error else "unknown"
                out.append(f"  {pair.scenario} :: {pair.client}: {first_line}")
    return "\n".join(out)


def _meta_lines(metadata: dict[str, object]) -> list[str]:
    clients = metadata.get("clients", {})
    versions = ", ".join(f"{k} {v}" for k, v in clients.items()) if isinstance(clients, dict) else str(clients)
    return [
        f"python {metadata['python']} ({metadata['implementation']}) on {metadata['platform']}",
        f"server: {metadata['nats_server']}",
        f"clients: {versions}",
        f"loop: {metadata['event_loop']}",
    ]


# -- json --------------------------------------------------------------------


def build_json(run: RunResult, metadata: dict[str, object]) -> dict[str, object]:
    return {
        "metadata": metadata,
        "run": {
            "quick": run.quick,
            "repeats": run.repeats,
            "clients": run.clients,
            "scenarios": run.scenarios,
        },
        "results": [
            {
                "scenario": pair.scenario,
                "group": SCENARIOS[pair.scenario].group if pair.scenario in SCENARIOS else None,
                "client": pair.client,
                "status": pair.status,
                "unit": pair.unit,
                "higher_is_better": pair.higher_is_better,
                "summary": pair.summary,
                "skip_reason": pair.skip_reason,
                "error": pair.error,
                "repeats": [
                    {"value": s.value, "detail": s.detail, "ops": s.ops, "seconds": s.seconds} for s in pair.repeats
                ],
            }
            for pair in run.pairs
        ],
    }
