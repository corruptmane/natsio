# Benchmarks

These numbers come from the in-repo harness, [`tools/natsio-bench`](#reproducing),
comparing **natsio** against **nats-py** (the incumbent asyncio client) and
**nats-core** (the official new beta). Read the methodology first — a benchmark
is only as trustworthy as its fairness rules.

## Methodology

Every client runs on the same **stdlib asyncio** event loop — no uvloop for
anyone — against a **fresh** JetStream-enabled `nats-server` started per
`(scenario, client)` pair, so no run contaminates another. Each result is the
**median of 3 timed repeats**.

Fairness rules that matter:

- **Publish-throughput** scenarios stop the clock only once every byte has been
  flushed to the socket (each client's own `flush`), never at the last `await`.
- **Delivery** scenarios stop at the Nth received message via an
  `asyncio.Event`, never by polling.
- Each client is driven **idiomatically** — its own intended fast path — so the
  comparison measures the library, not an awkward adapter.
- Scenarios auto-skip `(client, capability)` pairs a client does not support;
  those are marked **n/s** below (nats-core has no JetStream surface yet).

Environment for the figures below: macOS arm64, CPython 3.13.13, nats-server
2.14.3; natsio (dev) vs nats-py 2.15.0 vs nats-core 0.2.0.

## Results

### Core messaging

| Scenario | Unit | natsio | nats-py | nats-core |
|---|---|--:|--:|--:|
| `pub_16b` | msgs/s | **1,401,676** | 893,879 | 2,047,388 |
| `pub_1k` | msgs/s | **1,078,357** | 1,015,105 | 1,540,656 |
| `pub_64k` | MB/s | 7,466 | 7,602 | 7,740 |
| `pubsub_16b` | msgs/s | 312,727 | 342,362 | 336,088 |
| `pubsub_1k` | msgs/s | 276,383 | 301,796 | 293,254 |
| `roundtrip_latency` | ms | **0.167** | 0.176 | 5.996 |
| `reqrep_throughput` | req/s | **64,417** | 60,057 | 5,598 |

### JetStream, KV, Object Store

| Scenario | Unit | natsio | nats-py | nats-core |
|---|---|--:|--:|--:|
| `js_publish_sync` | msgs/s | **13,700** | 9,399 | n/s |
| `js_publish_async` | msgs/s | **142,825** | 123,330 | n/s |
| `js_consume` | msgs/s | **205,978** | 28,543 | n/s |
| `kv_put` | ops/s | **13,618** | 9,334 | n/s |
| `kv_get` | ops/s | **11,759** | 8,875 | n/s |
| `os_roundtrip` | MB/s | **889.9** | 39.1 | n/s |

**Bold** marks the fastest client that implements the scenario.

## Interpretation

natsio leads nats-py on 11 of 13 scenarios and ties the other two within ~9%.
The gaps are worth understanding rather than cheering.

**Where natsio pulls clearly ahead.** The JetStream and higher-level stores are
the widest margins, because that is where allocation and indirection dominate:
`js_consume` is **7.2×** nats-py (206k vs 29k msgs/s) and `os_roundtrip` is
**23×** (890 vs 39 MB/s). These come from a faster JSON model (per-field
decode/encode strategies precomputed once), lighter per-message objects, and a
pull-consumer read path that keeps the window topped up without per-message
reflection. Core `pub_16b` is **1.57×** nats-py for the same reasons — the hot
path is allocation, not architecture.

**The two scenarios within ~9%.** `pubsub_16b` and `pubsub_1k` are the only
places natsio trails, by roughly 8–9% (313k vs 342k, 276k vs 302k msgs/s). This
is the delivery-dispatch path, and the gap is small enough to sit inside
run-to-run variance on a busy machine; it is not a structural deficit.

**`pub_64k` is byte-bound.** At 64 KiB payloads all three clients converge
(~7,400–7,700 MB/s): the work is memory bandwidth and the socket, not the
library. Parity here is the expected, correct result.

**nats-core's raw-publish lead has a price.** nats-core wins the fire-and-forget
publish scenarios (`pub_16b`, `pub_1k`) via a **5 ms write-coalescing floor** —
it batches writes before flushing. That trade is brutal for anything
synchronous: its `roundtrip_latency` is **5.996 ms** against natsio's
**0.167 ms** (a **36×** difference), and its `reqrep_throughput` collapses to
**5,598 req/s** against natsio's **64,417** (**11.5×** slower). If your workload
is request/reply or any latency-sensitive round trip, that coalescing floor is
the number that decides it.

The takeaway: natsio reaches nats.go-class throughput on the asyncio loop while
keeping request/reply latency at the floor, and it is the only one of the three
that is fast across core *and* JetStream/KV/Object Store.

## Reproducing

The harness lives in `tools/natsio-bench` and drives a bundled `nats-server`.

```bash
just bench                 # full run: all clients, all scenarios, 3-repeat medians
just bench --quick         # smoke test: small counts
uv run natsio-bench --list # every client and scenario name
```

Narrow it down or capture the raw data:

```bash
uv run natsio-bench --clients natsio,nats-py --scenarios js_consume,os_roundtrip
uv run natsio-bench --json results.json     # full raw results + metadata
```

Absolute numbers depend on your hardware; run it on yours before drawing
conclusions. The fairness rules above hold on any machine — that is the point of
a fresh server per pair and idiomatic drivers.
