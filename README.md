# natsio

A zero-dependency, pure-asyncio NATS client for modern Python — built from scratch for
Python 3.13+ and NATS server 2.14+.

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

## Getting started

- **[Documentation](https://corruptmane.github.io/natsio/)** — guides, API
  reference, design decisions, and a page per extension.
- **[`examples/`](examples/)** — a dozen runnable, commented scripts from
  hello-world pub/sub through JetStream, KV, Object Store, microservices,
  WebSocket, and graceful shutdown. Start a local server with `just server`,
  then `python examples/01_hello_pubsub.py`. Each extension has its own
  `examples/basic.py` under `extensions/natsio-*/`.
- **[Migrating from nats-py](docs/migration-from-nats-py.md)** — side-by-side
  API mappings, an error-type table, the JetStream generational shift, and an
  honest list of behavioral differences.

## Workspace layout

This repository is a [uv](https://docs.astral.sh/uv/) workspace:

| Path | What |
|---|---|
| `natsio/` | The core client — the primary published distribution |
| `extensions/natsio-*` | Orbit-style extension packages (independent versioning; see `extensions/README.md`) |
| `tools/` | Development-only utilities (test-server management, benchmarks) |
| `examples/` | Runnable, teaching-oriented example scripts |
| `docs/` | Guides, API reference, and design decisions ([published site](https://corruptmane.github.io/natsio/)) |

## Development

```bash
uv sync              # create venv, install workspace + dev tools
just gates           # format check, lint, types, full test suite
just bench --quick   # benchmark against nats-py / nats-core
just release 0.12.0  # version bump + gates; prints the tag/publish ritual
```

## License

[Apache-2.0](LICENSE)
