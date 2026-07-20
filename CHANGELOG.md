# Changelog

All notable changes to the `natsio` core client are documented here.
Extension packages under `extensions/` keep their own changelogs.

## Unreleased

Ground-up rewrite. The previous implementation is retired to the `legacy` branch.

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
