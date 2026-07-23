# natsio

A zero-dependency, pure-asyncio NATS client for modern Python — built from
scratch for Python 3.13+ and NATS server 2.14+, with nats.go as the behavioral
oracle.

```bash
pip install natsio            # or: uv add natsio
```

```python
import asyncio
import natsio

async def main() -> None:
    async with await natsio.connect("nats://localhost:4222") as nc:
        sub = await nc.subscribe("greet.*")
        await nc.publish("greet.world", b"hello")
        async for msg in sub:
            print(msg.subject, msg.data)   # greet.world b'hello'
            break

asyncio.run(main())
```

## Why natsio

- **Zero runtime dependencies** — stdlib only. NKey/JWT auth is the single
  exception and delegates Ed25519 to an audited external backend (below);
  natsio ships no cryptography of its own. No C extensions.
- **Sans-io protocol core** — a buffer-fed pull parser with no asyncio imports,
  fuzz- and property-tested for chunk-boundary correctness, reused under the
  in-house WebSocket transport (RFC 6455, also stdlib-only).
- **Structured concurrency** — a TaskGroup-supervised connection lifecycle;
  no silent task death, every parked await has a wake path from close.
- **Modern JetStream only** — the ADR-37 simplified consumer API
  (`fetch()` / `next()` / `consume()`, ordered consumers). No deprecated
  push/pull-subscribe workflow. Key-Value and Object Store included.
- **Loud failure** — bounded queues with configurable policies; nothing is
  dropped silently, hostile input is a typed error, reads are digest-verified.
- **Fully typed** — PEP 695 generics, `py.typed`, checked with `ty`.

Across the in-repo benchmark harness natsio leads nats-py on 11 of 13 scenarios
and ties the other two — see the
[benchmarks](https://github.com/corruptmane/natsio/blob/main/docs/benchmarks.md).

## Authentication backends

The core client has no runtime dependencies. NKey and JWT (`.creds`)
authentication needs Ed25519, which natsio deliberately does not ship:

```bash
pip install 'natsio[nkeys]'         # PyNaCl (recommended)
pip install 'natsio[cryptography]'  # if you already depend on `cryptography`
```

Either backend produces identical keys and signatures (they are
differential-tested against the reference). If your keys live in a KMS or HSM
you need neither — supply your own signer via `CallbackAuth` and natsio stays
dependency-free.

## Documentation & examples

- **Docs:** guides, API reference, and design decisions —
  <https://github.com/corruptmane/natsio> (see `docs/`).
- **Examples:** a dozen runnable, commented scripts from hello-world pub/sub
  through JetStream, KV, Object Store, WebSocket, microservices, and graceful
  shutdown — in
  [`examples/`](https://github.com/corruptmane/natsio/tree/main/examples).
- **Migrating from nats-py:** side-by-side API mappings, an error-type table,
  and an honest list of behavioral differences —
  [`docs/migration-from-nats-py.md`](https://github.com/corruptmane/natsio/blob/main/docs/migration-from-nats-py.md).

## Extensions

Optional features ship as separate distributions that install *into* the
`natsio` namespace (the [orbit](https://github.com/synadia-io/orbit.py) model):
message schedules, JetStream fast-ingest batch publishing, a `$SYS` monitoring
client, partitioned consumer groups, distributed counters, KV codecs, NATS CLI
context files, OpenTelemetry, and a test-server manager. See
[`extensions/`](https://github.com/corruptmane/natsio/tree/main/extensions).

## License

[Apache-2.0](https://github.com/corruptmane/natsio/blob/main/LICENSE)
