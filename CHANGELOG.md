# Changelog

All notable changes to the `natsio` core client are documented here.
Extension packages under `extensions/` keep their own changelogs.

## Unreleased

Ground-up rewrite. The previous implementation is retired to the `legacy` branch.

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
