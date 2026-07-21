# natsio examples

Runnable, self-contained scripts that teach natsio from first connect to
graceful shutdown. Read them in order — each builds on the last — or jump to
the feature you need.

## Quickstart

```bash
just server                                  # start a JetStream-enabled nats-server
python examples/01_hello_pubsub.py           # run any example (defaults to nats://127.0.0.1:4222)
```

Every script reads the `NATS_URL` environment variable (default
`nats://127.0.0.1:4222`), so point it anywhere:

```bash
NATS_URL=nats://127.0.0.1:4222 python examples/06_jetstream_streams.py
```

## Index

| # | Script | Demonstrates |
|---|--------|--------------|
| 01 | `01_hello_pubsub.py` | Connect, subscribe (`async with` + `async for`), publish, respond |
| 02 | `02_request_reply.py` | Request/reply RPC; no-responders fast-fail vs. request timeout |
| 03 | `03_subscriptions.py` | Callback vs. iterator mode, queue groups, `unsubscribe_after`, pending limits/backpressure, drain |
| 04 | `04_lifecycle_events.py` | `events()` stream, `error_cb`, reconnect tuning, `force_reconnect`, close vs. drain |
| 05 | `05_auth_tls.py` | user/pass, token, NKey seed, `.creds` file, TLS (guarded by env vars) |
| 06 | `06_jetstream_streams.py` | Create a stream, publish with acks/dedup/expectations, the async publish window, info/purge |
| 07 | `07_jetstream_consumers.py` | ADR-37 `fetch()`/`next()`/`consume()`, ack/nak/term, ordered consumer, consumer CRUD |
| 08 | `08_key_value.py` | KV put/get, `create`/`update` CAS, delete/purge, history, watch (the `None` marker), per-key TTL |
| 09 | `09_object_store.py` | Chunked put/get streaming, `get_bytes`, links, `update_meta` rename, watch/list, seal |
| 10 | `10_graceful_shutdown.py` | Drain semantics, in-flight work on close, signal-driven shutdown (`asyncio.Runner` + signal handlers) |

Examples 06–10 require a JetStream-enabled server (`just server` enables it).
Example 05's credential-specific sections only run when the matching
environment variable is set (`NATS_USER`/`NATS_PASSWORD`, `NATS_TOKEN`,
`NATS_NKEY_SEED`, `NATS_CREDS`, `NATS_TLS_URL`); NKey and `.creds` auth also
need the `natsio[nkeys]` extra.
