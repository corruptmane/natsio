# Changelog

All notable changes to the `natsio` core client are documented here.
Extension packages under `extensions/` keep their own changelogs.

## Unreleased

- `await` is now optional (a no-op) on the session-factory returns —
  `subscribe()`, `consume()`, `ordered_consumer()`, KV/Object Store
  `watch()` — so nats-py muscle memory like `await nc.subscribe(...)`
  works unchanged. `ObjectStore.get()` deliberately stays non-awaitable:
  `await obj.get(...)` looks like it should yield bytes, and a loud
  `TypeError` beats silently handing back a result object.

## 0.9.0 — 2026-07-21

Ground-up rewrite. The previous implementation is retired to the `legacy` branch.

### API polish

- Per-message TTLs (ADR-43) accept `timedelta` everywhere a TTL is taken
  (`publish`, `publish_async`, KV `put`/`create`/`purge`), alongside the
  existing whole-second ints and `"never"`. The wire stays second-granular;
  sub-second timedeltas are rejected loudly instead of rounded.
- `create_or_update_stream()` is public: the idempotent way to assert a
  stream from scripts and services (was internal, used by the KV/Object
  Store management APIs).
- `OrderedConsumer.messages(until_drained=True)`: finite reads end the
  iteration normally the moment the consumer is caught up (exact, via the
  server's per-delivery `num_pending` — no timeout wait, no
  `NoMessagesError` to catch). Resumable: a second drained read continues
  from the consumer's position; a purge mid-drain ends the read instead of
  hanging.

### Performance

Profiling against nats-py 2.15.0 and nats-core 0.2.0 (see `tools/natsio-bench`)
found the hot-path gaps were allocation and indirection, not architecture.
Five contract-preserving fixes, each patch-measured before landing:

- `validate_subject` fast-path: compiled-regex fast-reject for plain dotted
  subjects; any special character falls through to the unchanged full scan,
  so every rejection and error message is byte-identical (differential-tested
  against the previous implementation). Internally generated reply inboxes
  (request/publish_async) skip re-validation.
- The instrumentation seam costs nothing when unused: the default Noop is no
  longer wrapped, and real backends get their guard wrappers pre-bound once
  instead of a closure per call ("a broken metrics backend cannot kill the
  connection" still holds and is still tested).
- `max_payload` is a cached int refreshed from INFO (including async INFO,
  which previously did not update the publish ceiling), not a per-publish
  `server_info` dict copy.
- The per-message `Msg` / parser-event objects dropped `frozen=True` (kept
  slots and identity equality): read-only by convention, ~420ns cheaper per
  delivered message.
- The JSON model precomputes per-field decode/encode strategies at class-plan
  time — zero `typing` reflection per message; `PubAck.from_wire` is 1.9x
  faster, and every JetStream/KV/ObjectStore entity benefits.

Result (same machine, full bench): pub 16B 724k → 1.40M msgs/s, pubsub 16B
211k → 313k, request/reply 48k → 64k req/s (ahead of nats-py), JS async
publish 87k → 143k msgs/s (ahead), JS consume 133k → 206k msgs/s. natsio now
leads nats-py on 11 of 13 scenarios and ties the rest within ~9%; nats-core's
remaining raw-publish lead comes from a 5ms write-coalescing floor that costs
it 36x on request/reply latency.

### nats.go parity features

Feature-surface parity with nats.go, each mirrored from its source contract:

- Core: `Client.force_reconnect()` (deliberate drop, backoff-bypassing,
  never counted as a server failure); `retry_on_failed_connect` (initial
  failure returns a reconnecting client; first success fires `Connected`);
  dedicated `reconnect_buf_size` (8MB default, `-1` disables buffering and
  fails/reports loudly — `ReconnectBufExceededError`);
  `permission_err_on_subscribe` (denied subscriptions raise
  `PermissionsViolationError` and close, using nats.go's exact error
  regexes).
- KV: per-key TTLs on `put`/`create` (ADR-43), `purge(key, last=...)` CAS,
  `purge_deletes(older_than=...)` (30-min default marker threshold),
  multi-key `watch(*keys)`, `watch(resume_from_revision=...)`, status
  `metadata`/`description`.
- Object Store: `update_meta` with the full rename contract (CAS-gated on
  both subjects — stricter than nats.go's unguarded writes), public
  `show_deleted` on `info`/`get`.
- JetStream: ADR-42 priority groups — `priority_groups`/`priority_policy`
  consumer config, `group`/`min_pending`/`min_ack_pending` on
  fetch/next/consume, pinned-client `Nats-Pin-Id` lifecycle with 423
  recovery, `Consumer.unpin()`; async publish window (`publish_async` →
  future, pending cap 4000 with 200ms stall wait,
  `publish_async_complete()`, in-flight futures fail on disconnect);
  `expected_last_subject_seq_subject` (2.12) on both publish paths;
  message-schedule stream config + header constants (2.12).
- Management: `update_key_value` / `create_or_update_key_value` /
  `key_value_store_names` / `key_value_stores` and the Object Store
  equivalents, with nats.go's dual subject+prefix listing filters.

### nats.go parity audit

The full nats.go test suite (~54k lines, the de-facto client conformance
oracle) was mined for behavioral edge cases across eight domains; every
claimed divergence was reproduced live before being fixed. Highlights:

- Liveness: a pending `request()` is woken by `close()` with
  `ConnectionClosedError` instead of hanging out its timeout; JetStream
  `consume()`/ordered iteration/`fetch()` surface `ConnectionClosedError` on
  connection close or reconnect exhaustion instead of parking forever; KV
  watchers/`keys()`/`history()` complete their initial snapshot (bounded idle)
  instead of deadlocking when the bucket is purged mid-snapshot;
  `unsubscribe()`/`drain()` called from inside a subscription callback no
  longer abort the callback or deadlock.
- Auth: URL userinfo now takes precedence over option credentials (nats.go
  contract); permanent config errors (missing creds file, bad seed, missing
  Ed25519 backend) fail fast instead of being masked by other pool servers;
  a repeated identical auth rejection during reconnect aborts to Closed
  (2-strikes, like nats.go — opt out with `ignore_auth_error_abort=True`);
  auth errors additionally reach `error_cb`.
- Pool: discovered servers absent from a later gossiped `connect_urls` are
  pruned (explicit and currently-connected servers always kept).
- Validation: stream/consumer names are validated client-side (a dotted name
  used to hang the full timeout); KV keys reject `..` and non-terminal `>`;
  `fetch()` rejects non-positive batch and negative timeout; binding
  `key_value()` to a stream that doesn't cover the bucket's keyspace raises
  `BucketNotFoundError`; empty `api_prefix`/`domain` are rejected.
- Parser: PING/PONG/+OK tolerate trailing bytes (nonconforming-peer
  robustness); `-ERR` quote normalization matches nats.go.
- Semantics: sync `next_msg()` converts a payload-less 503 into
  `NoRespondersError` and refuses callback-mode subscriptions; malformed
  `$JS.ACK` replies raise `NotJSMessageError`; Object Store `get()` detects
  chunks appended beyond the recorded count (`DigestMismatchError`);
  `drain()` timeout surfaces `DrainTimeoutError` through the error callback;
  `streams()` gained a `subject` filter.

### JetStream (ADR-37 simplified API)

- `nc.jetstream()` → `JetStreamContext` with domain/api-prefix routing; typed
  `$JS.API` plumbing mapping `{code, err_code, description}` errors onto an
  extensible `err_code`-keyed exception registry.
- Stream CRUD, purge, paged listings; stored-message reads over Direct Get
  (when the stream allows it) or `STREAM.MSG.GET`; message delete.
- JetStream publish with PubAck, `Nats-Msg-Id` dedup, `Nats-Expected-*`
  expectations, per-message TTL (ADR-43), and brief 503 retry per ADR-22
  before `NoStreamResponseError`.
- Pull consumers only: `fetch()` (bounded single pull with precise
  404/408/409/423 status classification), `next()`, and `consume()` — a
  continuous session with token-correlated overlapping pulls, threshold
  refills, ADR-9 idle-heartbeat stall recovery, and reconnect re-pull.
- Ordered consumer (ADR-17): ephemeral, `ack_policy=none`, judged on consumer
  sequence contiguity; self-heals from gaps, stalls, and consumer deletion by
  recreating at the next unseen stream sequence
  (`deliver_policy=by_start_sequence` always paired with `opt_start_seq`).
- Full ack surface: `ack`, `ack_sync` (double-ack, un-marks on failure so it
  can be retried), `nak(delay=...)`, `term(reason)`, `in_progress`; v1/v2
  ack-reply metadata parsing.
- Consumer pause/resume (2.11+), API-level introspection
  (`js.api_level()` — 3 on 2.14).
- Entities are slotted dataclasses over a zero-dependency Annotated-converter
  JSON model: `timedelta` ↔ nanoseconds, aware `datetime` ↔ RFC 3339, enums as
  `StrEnum`, `None` omitted (never `null`), and unknown server fields captured
  and round-tripped so newer-server configs are never destroyed.

### Key-Value (ADR-8)

- `natsio.kv`: buckets over `KV_<bucket>` streams — `get`/`put`/`create`/
  `update` (CAS)/`delete`/`purge`, `history`, `keys`/`iter_keys` (streaming),
  and self-healing `watch()` with a single `None` initial-state marker and
  snapshot dedup (an ordered-consumer heal mid-snapshot can never duplicate
  keys or resurrect stale values).
- No default TTL: buckets never expire data unless asked (the legacy client
  defaulted to 120s and silently lost data). Client-side floor at the
  server's 100ms `max_age` minimum.
- Direct Get reads (ADR-31), ADR-48 limit markers (read as purge-class, like
  other clients), per-key TTL purge markers (`purge(key, ttl=...)` with
  `allow_msg_ttl`), `limit_marker_ttl`.
- `create()` resolves marker churn by CAS-ing against the marker's revision —
  correct for brand-new, deleted, and concurrently-recreated keys.
- `KeyCodec`/`ValueCodec` protocol seam (identity by default) so codec packs
  (ADR-54 path notation, encryption) plug in without breaking changes;
  wildcard watches refuse a key codec loudly instead of matching nothing.
- Typed errors: `BucketNotFoundError`, `BucketExistsError`,
  `KeyNotFoundError` / `KeyDeletedError(revision)`, `KeyExistsError(revision)`.

### Object Store (ADR-20)

- `natsio.objectstore`: chunked blobs over `OBJ_<bucket>` streams — `put`
  (bytes or async iterables, re-chunked to 128KiB by default, streaming
  SHA-256), `get` (async iterator with link resolution and mandatory
  digest+size verification — a completed read is a verified read),
  `get_bytes`, `info`, `delete`, `list`/`watch`, object and bucket links,
  `seal`, `status`.
- Every metadata write is CAS-gated on the meta subject (probe-confirmed
  necessity): a put that loses a concurrent same-name race purges its own
  chunks instead of leaking them forever, and a delete racing a put can never
  report success while leaving data live.
- Failed puts purge their published chunks (cancellation-safe, best-effort);
  replacing an object purges the replaced revision's chunks; reads of
  truncated objects fail with a bounded timeout instead of hanging.
- Wire-compatible with nats.go/nats.py (padded-base64url names and digests,
  rollup metas, `mtime` emission, object-level `headers`, delete-marker
  shape) — verified bidirectionally against hand-crafted foreign metas.

### Extensions

- Extension distributions now import as `natsio.<name>` (pkgutil-style shared
  namespace; wheels ship only their subpackage). First extension:
  `natsio-testing` — the real-server process manager (start/stop, configs,
  free ports, readiness probing, JetStream store dirs, SIGKILL fault
  injection) used by natsio's own integration suite.

### Project

- uv workspace: `natsio/` (core client), `extensions/natsio-*` (orbit-style
  extension distributions), `tools/`.
- Apache-2.0, PEP 639 metadata (`License-Expression` + `license-files`).
- Tooling: ruff, `ty`, pytest + pytest-asyncio (auto mode), hypothesis.
- CI: lint/typecheck, unit matrix (Python 3.13/3.14 × ubuntu/macOS),
  live-server integration against nats-server 2.14, wheel build; tag-triggered
  release via PyPI trusted publishing.

### Protocol core (sans-io)

- `Parser`: h11-style pull parser (`receive_data` / `next_event` / `NEED_DATA`)
  with no asyncio imports. Single `bytearray` buffer with offset tracking,
  amortized compaction, and exactly one payload copy per message.
- Framing violations are fatal and terminal: `max_control_line` / `max_payload`
  enforced, malformed control lines raise `ParserError`, and the parser refuses
  reuse afterwards. A corrupt but length-delimited HMSG header block stays
  non-fatal — the message is delivered with `headers_error` set.
- `Headers`: multi-value, case-preserving map. Encoding rejects CR/LF in keys
  and values (wire-injection safe). Inline status exposes both numeric code and
  full description.
- Wire builders for PUB/HPUB/SUB/UNSUB/CONNECT/PING/PONG; CONNECT hard-sets
  `protocol=1`, `headers=true`, `no_responders=true`.
- Server `-ERR` classification distinguishes fatal from stay-connected errors.
- Tested for chunk-boundary invariance at every split point of a reference
  stream, one byte at a time, over random multi-way splits, and under Hypothesis
  frame×partition generation, plus a raw-bytes fuzz target.

### Transport, connection, auth

- `Transport` seam (structural protocol) with a TCP implementation over a custom
  `asyncio.Protocol`; WebSocket can be added without touching parser or
  connection.
- Connection lifecycle: single `ConnectionState` enum, INFO → optional TLS
  (in-place upgrade or handshake-first) → CONNECT → PING/PONG-verified
  handshake. Cluster topology is seeded from the first INFO and updated from
  async INFO; lame-duck triggers migration.
- Write path: coalescing buffer with a single flusher that swaps before writing,
  high-water-mark backpressure, and carry-over of unflushed publishes across a
  reconnect.
- Reconnect: jittered exponential backoff with a per-server minimum interval,
  consecutive-failure budgets, subscription replay with `UNSUB` remainder math,
  and buffering of frames issued while disconnected.
- Inbound dispatch is synchronous and non-blocking; per-session flusher and
  pinger run under a `TaskGroup` that collapses on connection loss.
- Auth: pluggable `Authenticator` re-invoked on every (re)connect —
  user/password, token, NKey, `.creds` (re-read for rotation), and callback.
  Ed25519 is delegated to an external backend (`natsio[nkeys]` → PyNaCl, or
  `natsio[cryptography]`); natsio ships no cryptography of its own. Configuring
  NKey/JWT auth without a backend fails at construction with an actionable
  error rather than mid-handshake. The NKey codec (base32 + CRC-16 + role
  prefixes) is ours and is differential-tested against the reference `nkeys`
  package across every role.
- Zero-dependency instrumentation seam (`Instrumentation` protocol, no-op by
  default) for metrics/tracing exporters shipped outside the core.
