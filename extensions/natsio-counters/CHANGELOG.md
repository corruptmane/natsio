# Changelog

All notable changes to `natsio-counters` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project is
pre-1.0 and makes no API-stability promises.

## [0.1.0] - 2026-07-22

Initial release. Distributed counters over JetStream counter streams
([ADR-49](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-49.md)),
mirroring [`orbit.go/counters`](https://github.com/synadia-io/orbit.go/tree/main/counters).
Requires nats-server 2.12+ (`allow_msg_counter`). Stdlib only.

### Added

- **Stream helpers**: `create_counter(js, config)` (creates the backing stream
  with `allow_msg_counter` + `allow_direct` forced on), `get_counter(js, name)`
  (binds by name; `CounterNotFoundError` when absent), and
  `counter_from_stream(js, stream)` (wraps an already-fetched `Stream`). Mirrors
  orbit's `NewCounterFromStream` / `GetCounter`.
- **`Counter` handle**:
  - `add(subject, delta) -> int` — publishes the `Nats-Incr` increment and
    returns the new total from the PubAck `val` (no follow-up read). Negative and
    zero deltas per ADR semantics; arbitrary-precision via native Python `int`.
    Rejects non-int / `bool` deltas.
  - `load(subject) -> int` — current value via Direct Get `last_by_subj`.
  - `get(subject) -> CounterEntry` — value + last increment + parsed sources.
  - `get_multiple(subjects) -> AsyncIterator[CounterEntry]` — one batch Direct
    Get (`multi_last`, ADR-31) with wildcard support, over the core's
    `request_many` (ADR-47). Mirrors orbit's `GetMultiple`.
- **`CounterConfig`** — counter-stream configuration (subjects, storage,
  replicas, limits, placement, compression, metadata). Omits per-message-TTL and
  schedule knobs, which ADR-49 declares incompatible with counters.
- **`CounterEntry`** — `subject`, `value`, `sources`, `incr`.
- **Wire-contract constants** `COUNTER_INCREMENT_HEADER` (`Nats-Incr`) and
  `COUNTER_SOURCES_HEADER` (`Nats-Counter-Sources`), plus `parse_counter_value`
  and `parse_sources` helpers for the `{"val": "..."}` payload and the sources
  header.
- **Typed errors** under `natsio.jetstream.JetStreamError`:
  `CounterNotEnabledError`, `DirectAccessRequiredError`, `CounterNotFoundError`,
  `CounterSubjectNotInitializedError`, `InvalidCounterValueError`.
- **Tests**: wire-contract/unit tests with header and payload vectors ported
  from orbit's `counter_test.go`; live end-to-end tests against a real
  `nats-server` (concurrent adds from two clients, value survival across a
  server restart on a persisted `store_dir`, negative/zero deltas, cross-subject
  counters, wildcard `get_multiple`).

### Known limitations (core seam friction)

Surfaced while building this extension from outside the core. All are in the
natsio core, not in the counters module:

- **No batch Direct Get in the `Stream` API.** `Stream.get_msg` does only
  single-message Direct Get; `get_multiple` had to hand-build the
  `$JS.API.DIRECT.GET.<stream>` `multi_last` request and drive it through
  `client.request_many`, re-parsing `Nats-Subject`/`Nats-Sequence` headers and
  skipping the `204 EOB` frame — duplicating logic that lives privately in
  `Stream._direct_get` / `StoredMsg`. *Proposal:* a public
  `Stream.get_last_msgs_for(subjects)` (or `get_batch`) returning `StoredMsg`s.
- **No structured "counter increment missing" error.** Publishing to a counter
  stream without `Nats-Incr` surfaces as a generic `APIError` (description
  `message counter increment is missing`) — no dedicated `err_code` mapping,
  so extensions can only match on the string. *Proposal:* register the counter
  err_codes in `natsio.jetstream.errors`.
- **`Counter` must hold both `JetStreamContext` and `Stream`.** Increment goes
  through `ctx.publish` while reads go through `stream.get_msg`; there is no
  single object owning both, so the handle threads them manually (as KV/OS do).
  Minor, but every stream-over-JetStream module re-derives this pairing.
