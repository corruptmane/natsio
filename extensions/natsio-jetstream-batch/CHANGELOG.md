# Changelog

All notable changes to `natsio-jetstream-batch` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project is
pre-1.0 and makes no API-stability promises.

## [0.1.0] - 2026-07-23

Initial release. JetStream fast-ingest batch publishing and batch reads,
mirroring [`orbit.go/jetstreamext`](https://github.com/synadia-io/orbit.go/tree/main/jetstreamext).
Requires nats-server 2.14+ (`allow_batched`) for publishing and 2.11+ with
`allow_direct` for reads. Stdlib only.

### Added

- **`fast_publisher(js, *, flow_control, continue_on_gap, error_handler)`** —
  opens a fast-ingest batch. Performs no I/O, so `await` is optional
  (`__await__` returns self); the ack inbox is subscribed on the first message.
- **`FastPublisher`**:
  - `add(subject, payload, **expectations) -> FastPubAck` — publishes (and
    persists) one message. The first call opens the batch and waits for the
    server's opening ack, so an unusable stream fails on message 1 rather than
    message 1000. Later calls stall once `flow * max_outstanding_acks` messages
    are unacked.
  - `commit(subject, payload, **expectations) -> BatchAck` — final message plus
    end of batch; valid as the very first call (probe-confirmed: the server
    accepts a commit that opens its own batch).
  - `close() -> BatchAck | None` — end-of-batch marker with no message.
    Idempotent, returns `None` on an untouched publisher (the server rejects an
    EOB that would open a batch; orbit.go returns `ErrEmptyBatch` there), and
    re-raises a server-side abandon.
  - `async with` support: the exit commits nothing but `close()`s the batch, and
    on an exception abandons it best-effort without masking the original error.
  - Introspection: `is_closed`, `batch_id`, `size`, `ack_sequence`, `ack`.
- **`FlowControl(flow=100, max_outstanding_acks=2, ack_timeout=None)`** — frozen,
  validated config for ack cadence, back-pressure window, and the total ack
  deadline (defaults to the JetStream context timeout). Includes the ping/stall
  recovery loop: pings at a third of the deadline re-request the latest flow ack,
  so a lost ack costs a third of a deadline instead of the batch.
- **`get_batch(js, stream, batch, *, seq, start_time, subject, max_bytes,
  timeout) -> BatchGet`** — batch Direct Get from a sequence, a time, or the
  lowest match, optionally subject-filtered (wildcards) and byte-capped. Options
  are validated eagerly in the caller's frame; iteration yields core
  `StoredMsg`s and terminates on the server's `204 EOB`. `await` is optional.
- **Wire-contract API**: `build_reply_prefix` / `build_reply_subject` (the
  `<inbox>.<flow>.<gap>.<seq>.<op>.$FI` encoding), `parse_ack_frame` (the
  `ack`/`gap`/`err`/terminal frame union), `build_message_headers`, and the
  `OP_*` / `GAP_*` / `ALLOW_BATCHED` constants.
- **Typed errors** under `natsio.jetstream.JetStreamError` via
  `JetStreamBatchError`: `BatchClosedError`, `BatchAbandonedError`,
  `BatchAckTimeoutError`, `ConcurrentBatchUseError`, `BatchReentrantUseError`,
  `BatchGapError`, `BatchMessageError`, `InvalidBatchAckError`, `BatchGetError`,
  `BatchGetUnsupportedError`, `BatchGetIncompleteError`. The four server
  err_codes emitted by 2.14.3 (10205-10208) are registered into core's registry
  as `FastBatchNotEnabledError`, `FastBatchInvalidPatternError`,
  `FastBatchInvalidIdError`, `FastBatchUnknownIdError` — with a WARNING if a
  code was already bound to a different class, since the registry is a process
  global that core owns.
- **Loud-failure choices** beyond the oracle:
  - An **abandoned batch never returns as a successful ack.** The server ends a
    batch it abandoned (rejected message, or a hole in `gap=fail` mode) with a
    normal-shaped terminal ack; the short `count` is the discriminator, and a
    parked `commit()`/`close()` gets `BatchAbandonedError` — with the rejection
    or gap that caused it as `__cause__` — instead of a `BatchAck` for messages
    that were dropped. The failure is sticky: later calls re-raise it.
  - Concurrent use of one publisher is *detected* (`ConcurrentBatchUseError`)
    for the window that can corrupt the batch — a caller parked inside
    `add`/`commit`/`close` while another calls in.
  - Driving the publisher from its own `error_handler` raises
    `BatchReentrantUseError` at once: the handler runs on the ack-reader task,
    so `close()`/`commit()` from there would wait for an ack only that (now
    blocked) task can deliver. Dispatch it to another task instead.
  - A batch read that ends without the `204 EOB` raises `BatchGetIncompleteError`
    instead of returning a partial answer that looks whole.
  - **Hostile ack frames are typed, never fatal.** A non-object `error` field
    (`"error":"boom"` / `"error":null` — legal JSON any account client can put
    on the batch's ack inbox) used to raise a bare `AttributeError` that escaped
    the reader and killed it silently, so the publisher went on blind. It is now
    an `InvalidBatchAckError` the reader reports and survives (invariant 5).
  - **A 503 on the ack inbox is diagnosed correctly and fast.** Publishing to a
    subject no stream captures (a typo, the likeliest user error) returns an
    immediate empty-bodied 503; the reader used to feed it to the JSON parser,
    report a bogus "not valid JSON", and then cost the whole `ack_timeout`. It
    now ends the batch at once with `NoStreamResponseError` naming the subject.
  - **`close()` after a failed or cancelled `add` re-raises, never returns
    `None`.** A cancelled `add` ends the batch with messages already persisted
    but no EOB; `close()` took the empty-batch branch and returned the "nothing
    added" sentinel, hiding the abort. It now re-raises the failure (a real
    exception as-is; a bare cancellation wrapped in `BatchClosedError` naming how
    many messages persisted).
- **Tests**: 75 unit tests (wire constants, reply-subject encoding, frame
  parsing against payloads captured from 2.14.3, err_code registration and its
  conflict warning, flow-control/stall/ping recovery, abandon detection on both
  the parked and unparked path, error-handler re-entrancy, termination and
  concurrency, request-body shapes) driven by a fake
  client and a hand-rolled fast-ingest server; 32 live tests against the pinned
  nats-server 2.14.3 (1000-message batch persisted in order, stall path with a
  one-message ack window, close-without-commit, gap handling in both modes,
  rejected-message abandon against a parked commit, batch reads by
  sequence/time/subject/max_bytes plus the empty and truncated cases). The live
  suite skips loudly on a server that does not echo `allow_batched`.

### Known limitations (core seam friction)

Surfaced while building this extension from outside the core. All are in the
natsio core, not in this module:

- **`PubAck.batch_id` / `PubAck.batch_size` never populate.** They map to wire
  keys `batch_id`/`batch_size`, but the server sends `batch`/`count` (verified
  against 2.14.3 and orbit.go's `BatchAck` struct tags), so both land in
  `extra`. This module declares its own `BatchAck` with the correct names.
- **`StreamConfig` does not model `allow_batched`** (2.14), so fast-ingest
  streams must be created with `extra={"allow_batched": True}`.
- **The Direct Get reply -> `StoredMsg` conversion is private**
  (`Stream._stored_from_direct`), so the batch reader re-implements it.
- **`Client.request_many` swallows the 503 no-responders terminator**, making
  "nobody answered" indistinguishable from "no messages" — hence the
  `allow_direct` pre-check and the EOB-or-raise rule here.
