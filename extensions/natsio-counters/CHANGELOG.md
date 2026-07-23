# Changelog

All notable changes to `natsio-counters` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project is
pre-1.0 and makes no API-stability promises.

## [0.1.1] - 2026-07-23

### Fixed

- Ship the `py.typed` marker. The distribution declares `Typing :: Typed` and
  is fully annotated, but the marker file was missing, so `mypy`/`ty` in a
  consuming project could not see the package's inline types. Type checkers now
  pick them up.

### Added

- A runnable `examples/basic.py` (create a counter, increment, read one, and
  enumerate many with `get_multiple`).

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
    Get with wildcard support, over the core's `Stream.get_last_msgs_for`
    (`multi_last`, ADR-31). Mirrors orbit's `GetMultiple`.
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

- ~~**No batch Direct Get in the `Stream` API.**~~ *Resolved in core:*
  `get_multiple` now drives the public `Stream.get_last_msgs_for(subjects)`
  (batch `multi_last` Direct Get, ADR-31), which returns `StoredMsg`s and
  reuses the core's `Nats-Subject`/`Nats-Sequence` header parsing and `204 EOB`
  termination — no more hand-built request or duplicated framing.
- ~~**No structured "counter increment missing" error.**~~ *Resolved in core:*
  the counter err_codes are registered in `natsio.jetstream.errors` as
  `CounterIncrementMissingError` (`10169`) and `CounterIncrementInvalidError`
  (`10171`), so a bare publish to a counter stream raises the typed error
  instead of a string-matched generic `APIError`.
- **`Counter` must hold both `JetStreamContext` and `Stream`.** Increment goes
  through `ctx.publish` while reads go through `stream.get_msg`; there is no
  single object owning both, so the handle threads them manually (as KV/OS do).
  Minor, but every stream-over-JetStream module re-derives this pairing.
