# natsio

A zero-dependency, pure-asyncio NATS client for modern Python — built from scratch for
Python 3.13+ and NATS server 2.14+.

> ⚠️ **Status: ground-up rewrite in progress.** Nothing here is usable yet.
> The previous implementation (core NATS + legacy-generation JetStream/KV) is preserved
> on the [`legacy`](../../tree/legacy) branch.

## Design

- **Zero runtime dependencies** — stdlib only. NKey/JWT auth is the single exception and
  delegates Ed25519 to an audited external backend (see below); natsio ships no crypto.
- **Sans-io protocol core** — a buffer-fed pull parser with no asyncio imports, fuzz- and
  property-tested for chunk-boundary correctness, reusable under future transports (WebSocket).
- **Structured concurrency** — TaskGroup-supervised connection lifecycle; no silent task death.
- **Modern JetStream only** — the ADR-37 simplified consumer API (`fetch()` / `next()` /
  `consume()`, ordered consumers). No deprecated push/pull-subscribe workflow.
- **Loud backpressure** — bounded queues with configurable policies; nothing is dropped silently.
- **Fully typed** — PEP 695 generics, `py.typed`, checked with `ty`.

## Dependencies

The core client has **no runtime dependencies**. The one exception is NKey and
JWT (`.creds`) authentication, which needs Ed25519 — natsio deliberately ships
no cryptography of its own:

```bash
pip install 'natsio[nkeys]'         # PyNaCl (recommended)
pip install 'natsio[cryptography]'  # if you already depend on `cryptography`
```

Either backend works; they are verified to produce identical keys and
signatures. If your keys live in a KMS or HSM, you need neither — supply your
own signer via `CallbackAuth` and natsio stays dependency-free.

## Workspace layout

This repository is a [uv](https://docs.astral.sh/uv/) workspace:

| Path | What |
|---|---|
| `natsio/` | The core client — the only published distribution for now |
| `extensions/natsio-*` | Orbit-style extension packages (independent versioning; see `extensions/README.md`) |
| `tools/` | Development-only utilities (test-server management, benchmarks) |

## Development

```bash
uv sync              # create venv, install workspace + dev tools
uv run pytest        # run tests
uv run ruff format . # format
uv run ruff check .  # lint
uv run ty check      # type-check
```

## License

[Apache-2.0](LICENSE)
