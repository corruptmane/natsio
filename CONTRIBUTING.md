# Contributing to natsio

Thanks for your interest. natsio is a zero-dependency asyncio NATS client;
the bar is high on correctness and low on ceremony.

## Setup

The repo is a [uv](https://docs.astral.sh/uv/) workspace. One command:

```bash
uv sync
```

Everything else runs through [`just`](https://github.com/casey/just) — CI runs
the exact same recipes, so if `just gates` is green locally, CI will be too:

```bash
just gates          # format check, lint, types (ty), full test suite — the bar for every change
just unit           # fast, hermetic tests only
just integration    # tests against a real nats-server
just docs           # build the docs site (strict: broken links fail)
just bench --quick  # benchmark against nats-py / nats-core
```

Integration and benchmark tests need a `nats-server` binary. Drop the 2.14
binary at `tools/.bin/nats-server`, or point `NATS_SERVER_BIN` at one.

## Ground rules

These are load-bearing — read [`docs/decisions.md`](docs/decisions.md) before
changing anything non-trivial. In short:

- **Zero runtime dependencies.** Stdlib only; no C extensions. Ed25519 is an
  optional extra.
- **Wire contracts are pinned** to nats.go / the ADRs, often byte-level.
  Changing emitted bytes is an interop event, not a refactor.
- **Loud failure.** Nothing drops silently; every parked `await` has a wake
  path; `CancelledError` is never suppressed.
- **Tests for everything.** Unit for local logic, live-server for anything that
  touches the wire. Event-driven waits, never `sleep`. Name a regression test
  for its finding.
- **Docstrings are plain markdown** (mkdocstrings renders them) — never Sphinx
  `:meth:`/`:class:` roles.
- **Suppressions are visible and justified.** No blanket `# type: ignore`; a
  `# noqa` carries a reason. Lazy imports are banned in source except for a
  documented circular/optional break.

## Pull requests

Branch from `main`, keep the change focused, and fill in the PR checklist.
Update `CHANGELOG.md`'s `## Unreleased` section for anything user-visible — the
release tooling turns that into the dated release notes.

## Extensions

New capabilities that aren't core-shaped belong in an extension distribution
(`natsio-<name>`, imported as `natsio.<name>`) — see
[`extensions/README.md`](extensions/README.md).
