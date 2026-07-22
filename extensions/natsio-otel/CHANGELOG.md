# Changelog

All notable changes to `natsio-otel` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project is
pre-1.0 and makes no API-stability promises.

## [0.1.0] - 2026-07-22

Initial release.

### Added

- `OtelInstrumentation(meter_provider=None, *, record_subject=False)` —
  implements the natsio `Instrumentation` protocol and records OpenTelemetry
  **metrics** through `opentelemetry-api`:
  - Semconv-conformant message counters `messaging.client.sent.messages` and
    `messaging.client.consumed.messages`.
  - Custom `nats.client.*` instruments for payload bytes (counters + size
    histograms), network wire bytes, connection lifecycle events (connect,
    reconnect, disconnect, close), background errors (keyed by `error.type`),
    and slow-consumer drops.
  - All instruments carry `messaging.system=nats`. Subject attribution
    (`messaging.destination.name`) is opt-in via `record_subject=True` to avoid
    cardinality blow-up from `_INBOX.*` reply subjects.
  - Hooks are allocation-light (shared attribute dicts on the hot path) and
    never raise.
- `inject(headers=None, *, context=None) -> Headers` and
  `extract(source, *, context=None) -> Context` — W3C trace-context propagation
  bridged to natsio `Headers`, driven explicitly by the caller.

### Known limitations (seam friction)

Automatic producer/consumer **spans are not shipped** — the instrumentation
seam cannot express them. The point-event hooks provide no publish-time header
access, no scope around the subscriber callback, and no operation timing, so
`messaging.client.operation.duration` / `messaging.process.duration` and
auto-instrumented spans are out of reach from the hooks alone. Proposals to the
core to close the gap:

- A delivery hook that carries the message's headers (and reply), so a consumer
  span's parent context can be extracted without user code.
- A publish start/complete hook pair (or a publish-wrapping seam) exposing the
  outgoing header block, so `traceparent` can be injected and send duration
  measured.
- A handler-wrapping seam so process spans can bracket the user callback.
- Optionally, an elapsed-time argument on the publish/deliver hooks for the
  semconv duration histograms.
