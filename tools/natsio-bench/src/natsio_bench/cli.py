"""``natsio-bench`` command line: pick clients/scenarios, run, print, dump JSON."""

import argparse
import asyncio
import json
import sys
from pathlib import Path

from natsio_bench import runner
from natsio_bench.adapters import ADAPTERS
from natsio_bench.report import build_json, collect_metadata, render_console
from natsio_bench.scenarios import SCENARIOS, BenchConfig

__all__ = ["main"]


def _csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="natsio-bench",
        description="Fair, reproducible benchmarks: natsio vs nats-py vs nats-core.",
    )
    parser.add_argument(
        "--clients",
        type=_csv,
        default=None,
        help=f"comma-separated subset of {', '.join(ADAPTERS)} (default: all)",
    )
    parser.add_argument(
        "--scenarios",
        type=_csv,
        default=None,
        help="comma-separated subset of scenario names (default: all)",
    )
    parser.add_argument("--repeats", type=int, default=3, help="timed repeats per pair (default: 3)")
    parser.add_argument("--json", type=Path, default=None, metavar="PATH", help="write full raw results + metadata")
    parser.add_argument("--quick", action="store_true", help="smaller counts for a fast smoke run")
    parser.add_argument("--list", action="store_true", help="list clients and scenarios, then exit")
    return parser


def _resolve(requested: list[str] | None, available: list[str], label: str) -> list[str]:
    if requested is None:
        return list(available)
    unknown = [name for name in requested if name not in available]
    if unknown:
        raise SystemExit(f"unknown {label}: {', '.join(unknown)}\navailable: {', '.join(available)}")
    return requested


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    if args.list:
        print("clients:")
        for name in ADAPTERS:
            print(f"  {name}")
        print("scenarios:")
        for name, scenario in SCENARIOS.items():
            print(f"  {name:20s} [{scenario.group}/{scenario.capability.value}]")
        return 0

    if args.repeats < 1:
        raise SystemExit("--repeats must be >= 1")

    clients = _resolve(args.clients, list(ADAPTERS), "client")
    scenarios = _resolve(args.scenarios, list(SCENARIOS), "scenario")

    try:
        binary = runner.discover_server_binary()
    except FileNotFoundError as exc:
        raise SystemExit(str(exc)) from None

    config = BenchConfig(quick=args.quick)
    metadata = collect_metadata(binary)

    def progress(message: str) -> None:
        print(message, file=sys.stderr, flush=True)

    result = asyncio.run(
        runner.run(
            clients=clients,
            scenarios=scenarios,
            repeats=args.repeats,
            config=config,
            binary=binary,
            progress=progress,
        )
    )

    print(render_console(result, metadata))

    if args.json is not None:
        args.json.write_text(json.dumps(build_json(result, metadata), indent=2))
        print(f"\nwrote {args.json}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
