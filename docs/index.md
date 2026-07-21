# natsio

A **zero-dependency, pure-asyncio NATS client** for modern Python — built from
scratch for Python 3.13+ and NATS Server 2.14+.

natsio is stdlib-only at its core, with a sans-io protocol engine, structured
concurrency throughout, and a single modern JetStream API. Nothing is dropped
silently, everything is typed, and there is no C extension to compile.

```python
import asyncio
import natsio


async def main() -> None:
    async with await natsio.connect("nats://localhost:4222") as nc:
        async with nc.subscribe("greet.*") as sub:
            await nc.publish("greet.world", b"hello")
            async for msg in sub:
                print(msg.subject, msg.data)   # greet.world b'hello'
                break


asyncio.run(main())
```

## Why natsio

- **Zero runtime dependencies.** Stdlib only. Ed25519 for NKey/JWT auth is the
  single, *optional* exception — natsio ships no cryptography of its own (see
  [Authentication & TLS](guide/auth-tls.md)).
- **Sans-io protocol core.** A buffer-fed pull parser with no asyncio imports,
  fuzz- and property-tested for chunk-boundary correctness.
- **Structured concurrency.** A `TaskGroup`-supervised connection lifecycle: no
  silently dying background tasks.
- **Modern JetStream only.** The ADR-37 simplified consumer API
  (`fetch()` / `next()` / `consume()`, ordered consumers) — plus Key-Value and
  Object Store. No deprecated push/pull-subscribe workflow.
- **Microservices built in.** The ADR-32 service framework — endpoints, groups,
  and the standard `$SRV` discovery/stats control plane the `nats` CLI speaks.
- **WebSocket transport.** `ws://` and `wss://` with the same client API, over an
  in-house zero-dependency RFC 6455 core — for browsers, edges, and firewalls.
- **Loud backpressure.** Bounded delivery queues with configurable policies;
  dropped messages are always counted and reported, never lost in silence.
- **Fully typed.** PEP 695 generics, `py.typed`, checked with `ty`.

## Performance

natsio leads nats-py on **11 of 13** benchmark scenarios (core throughput,
request/reply, and JetStream publish/consume), and targets nats.go parity.
See the [Benchmarks](benchmarks.md) page for the harness, methodology, and the
two scenarios where it does not lead.

## Install

```bash
uv add natsio            # or: pip install natsio
```

NKey seeds and JWT/`.creds` credentials need an Ed25519 backend — install the
extra:

```bash
uv add "natsio[nkeys]"        # PyNaCl (recommended)
uv add "natsio[cryptography]" # if you already depend on `cryptography`
```

Token and user/password auth — and custom `CallbackAuth` signing (KMS/HSM) —
need nothing extra.

## Where to next

- **[Getting started](getting-started.md)** — start a server, connect,
  publish/subscribe, request/reply, in one small program.
- **[Core messaging](guide/core-messaging.md)** — subscriptions in depth,
  queue groups, backpressure, request/reply.
- **[Connection & lifecycle](guide/connection.md)** — `ConnectOptions`,
  reconnect behavior, events, drain vs close.
- **[Authentication & TLS](guide/auth-tls.md)** — every credential scheme and
  TLS via `ssl.SSLContext`.
- **[JetStream](guide/jetstream.md)**, **[Key-Value](guide/key-value.md)**,
  **[Object Store](guide/object-store.md)** — the persistence layer.
- **[Microservices](guide/micro.md)** — request/reply services with `$SRV`
  discovery, and **[WebSocket](guide/websocket.md)** — the same client over `ws`.
- **[Migrating from nats-py](migration-from-nats-py.md)** — side-by-side API
  mappings and the behavioral differences that will actually bite.

Coming from nats-py? The [migration guide](migration-from-nats-py.md) maps
every common API surface. Ten runnable, commented scripts also live in
[`examples/`](https://github.com/corruptmane/natsio/tree/main/examples).
