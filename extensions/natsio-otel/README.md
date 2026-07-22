# natsio-otel

OpenTelemetry adapter over natsio's zero-dependency instrumentation seam.
Exports **metrics** for a natsio connection, plus **trace-context propagation
helpers** for message headers. Distribution `natsio-otel`, imported as
`natsio.otel`. Pre-1.0, no API-stability promises.

```python
import natsio
from natsio.otel import OtelInstrumentation

nc = await natsio.connect(
    "nats://localhost",
    instrumentation=OtelInstrumentation(),  # meter_provider=None -> global
)
```

`OtelInstrumentation(meter_provider=None, *, record_subject=False)` implements
the core `Instrumentation` protocol. It records into the given meter provider
(or the global one). Every hook is allocation-light and cannot raise.

## Metrics

Instrument names follow the OpenTelemetry [messaging semantic
conventions](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/)
where the spec covers the concept; everything else is namespaced
`nats.client.*` and marked **custom** below. All instruments carry
`messaging.system=nats`.

| Instrument | Type | Unit | Source hook | Notes |
|---|---|---|---|---|
| `messaging.client.sent.messages` | Counter | `{message}` | `on_message_published` | semconv; `messaging.operation.type=send` |
| `messaging.client.consumed.messages` | Counter | `{message}` | `on_message_delivered` | semconv; `messaging.operation.type=receive` |
| `nats.client.sent.bytes` | Counter | `By` | `on_message_published` | custom; application payload bytes (no framing) |
| `nats.client.consumed.bytes` | Counter | `By` | `on_message_delivered` | custom; application payload bytes (no framing) |
| `nats.client.sent.message.size` | Histogram | `By` | `on_message_published` | custom; payload-size distribution |
| `nats.client.consumed.message.size` | Histogram | `By` | `on_message_delivered` | custom; payload-size distribution |
| `nats.client.network.sent.bytes` | Counter | `By` | `on_bytes_sent` | custom; whole-flush wire bytes, includes framing |
| `nats.client.network.received.bytes` | Counter | `By` | `on_bytes_received` | custom; wire bytes, includes framing |
| `nats.client.connects` | Counter | `{event}` | `on_connect` | custom |
| `nats.client.reconnects` | Counter | `{event}` | `on_reconnect` | custom |
| `nats.client.disconnects` | Counter | `{event}` | `on_disconnect` | custom; `error.type` when the drop carried an error |
| `nats.client.closes` | Counter | `{event}` | `on_close` | custom |
| `nats.client.errors` | Counter | `{error}` | `on_error` | custom; keyed by `error.type` |
| `nats.client.slow_consumer.drops` | Counter | `{message}` | `on_slow_consumer` | custom; one increment per dropped message |

Semantic-convention attribute keys (`messaging.system`,
`messaging.destination.name`, `messaging.operation.type`, `error.type`) are
hardcoded strings rather than imported from
`opentelemetry-semantic-conventions`, which is not a runtime dependency.

### Subject cardinality (`record_subject`)

The message subject maps to `messaging.destination.name`, but NATS subjects —
especially `_INBOX.*` reply subjects generated per request — are effectively
unbounded and will explode a metric backend's cardinality. So subjects are
**not** recorded by default. Pass `record_subject=True` only when your subject
space is known to be small and static:

```python
OtelInstrumentation(record_subject=True)
```

This adds one dict allocation per message on the hot path (the default reuses a
shared attribute dict and allocates nothing per message).

## Trace-context propagation

```python
from natsio.otel import inject, extract
```

- `inject(headers=None, *, context=None) -> Headers` — returns a **fresh**
  `natsio.Headers` (the caller's mapping is never mutated) with `traceparent` /
  `tracestate` (and any other configured propagators) written in, ready to pass
  to `publish`.
- `extract(source, *, context=None) -> Context` — pulls a trace `Context` out
  of a received `Msg` (or a `Headers`/mapping, or `None`).

```python
from opentelemetry import trace
from opentelemetry.trace import SpanKind

tracer = trace.get_tracer("myapp")

# producer
with tracer.start_as_current_span("send order", kind=SpanKind.PRODUCER):
    await nc.publish("orders", body, headers=inject())

# consumer
async def handler(msg):
    ctx = extract(msg)
    with tracer.start_as_current_span("process order", context=ctx, kind=SpanKind.CONSUMER):
        ...
```

## The spans story (honest version)

**This extension ships metrics, not automatic spans.** That is a limitation of
the instrumentation seam as it stands, not a design choice, and the propagation
helpers above are the clean subset that *is* expressible.

The seam is a set of fire-and-forget **point-event** hooks
(`on_message_published(subject, size) -> None`, etc.). Real producer/consumer
spans need three things the hooks cannot provide:

1. **A place to inject context into an outgoing publish.** `on_message_published`
   fires *after* the frame is already encoded and buffered, and receives only
   `(subject, size)` — never the headers, which it also could not mutate. There
   is no publish-wrapping seam, so a `traceparent` cannot be attached
   automatically. Header injection must be done by the caller via `inject()`.
2. **A scope around message processing.** `on_message_delivered` fires at parse
   time, before dispatch and before the user callback runs, and carries only
   `(subject, size)` — no headers, no reply. It cannot bracket the handler, so a
   consumer/process span cannot be started as the handler's parent, nor its
   duration measured. Extraction must be done by the caller via `extract()`.
3. **Operation timing.** The semconv histograms `messaging.client.operation.duration`
   and `messaging.process.duration` need paired start/stop (or an elapsed
   argument). The hooks are instantaneous and unpaired, so these two core
   metrics are **not** implemented here.

See `CHANGELOG.md` and the task's friction report for concrete proposals to the
core (header-carrying delivery hook, publish start/complete pair, a
handler-wrapping seam).

## Requirements

- `natsio`, `opentelemetry-api>=1.20`
- Tests additionally use `opentelemetry-sdk` and a `nats-server` binary.

Part of the natsio extension tier. If adopted into orbit.py the move is
mechanical (`natsio/otel/` -> `orbit/otel/`).
