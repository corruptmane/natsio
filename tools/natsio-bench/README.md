# natsio-bench

A fair, reproducible benchmark harness comparing **natsio** against
**nats-py** (the incumbent asyncio client) and **nats-core** (the official new
beta).

```
just bench --quick          # smoke: small counts, all clients, all scenarios
just bench                  # full run, 3 repeats, median reported
uv run natsio-bench --clients natsio,nats-py --scenarios pub_throughput --json out.json
```

## Fairness

Every client runs on the same stdlib asyncio event loop (no uvloop for anyone),
against a **fresh** JetStream-enabled `nats-server` started per (scenario,
client) pair so no run contaminates another. Publish-throughput scenarios stop
the clock only once every byte has been flushed to the socket (each client's
own `flush`), and delivery scenarios stop at the Nth received message via an
`asyncio.Event`, never by polling. Each client is driven *idiomatically* — its
own intended fast path — so the comparison measures the library, not an
awkward adapter.

Scenarios auto-skip `(client, capability)` pairs a client does not support; the
report marks those `n/s`.
