# Design decisions

The reasoning behind natsio's non-obvious choices, recorded so they survive
the people (and AI sessions) that made them. Format: the decision, the why,
and — where one exists — the scar that taught it.

## Architecture

### Zero runtime dependencies, and no crypto of our own

The core installs nothing. The single exception is Ed25519 for NKey/JWT
auth, deliberately **delegated** to an optional extra (`natsio[nkeys]` →
PyNaCl, or `natsio[cryptography]`) through a small signer seam — natsio
ships no cryptography and takes no responsibility for any. The NKey *codec*
(base32, CRC-16/XMODEM, role prefixes) is ours, differential-tested against
the reference `nkeys` package; both Ed25519 backends are verified to produce
identical signatures. Keys in a KMS/HSM need neither extra: `CallbackAuth`
takes your signer.

### Sans-io protocol cores

The NATS parser and the WebSocket (RFC 6455) client are pull-based state
machines with no asyncio imports: `receive_data()` / `next_event()` /
`NEED_DATA`. Why: they become *provable* — the test suites assert the event
stream is identical under **every byte-split** of the input, plus mutation
fuzzing with hang detection — and they are reusable under any transport
(the WebSocket transport wraps the same untouched NATS parser).

This design costs a few hundred ns/message versus a monolithic regex loop
(nats-py's approach). Profiling measured that gap and we **kept the
sans-io design anyway**: the remaining delivery deficit (~9%) is not worth
collapsing the correctness story. Do not "optimize" this away.

### Structured concurrency and the termination rules

- No reader task: inbound dispatch happens synchronously inside
  `data_received`; nothing on that path may block.
- One flusher per session, swap-before-write; per-session tasks live under a
  TaskGroup that collapses when the session is lost.
- Closure is signalled by `asyncio.Event` latches, **never in-band `None`
  sentinels** — a sentinel can be dropped by a full queue, consumed by the
  wrong waiter, or block the closer.
- **Every parked await must have a wake path from the `Closed` lifecycle
  event.** The nats.go conformance audit found four "hangs forever" bugs
  (pending `request()` through `close()`, JetStream consume/ordered
  iteration, KV snapshot on a purged bucket, callback self-teardown) that
  were all this one lesson.
- `CancelledError` is never suppressed; teardown code detects
  "the reader is the current task" instead of cancelling itself.

### JetStream: ADR-37 simplified API only

`fetch()` / `next()` / `consume()` and the ordered consumer. No push
consumers, no legacy pull-subscribe — deprecated upstream, and carrying two
generations doubles every surface forever. Migration guidance exists in
`docs/migration-from-nats-py.md` for push users.

### Extension namespace: `natsio.<name>`

Extensions are separate distributions (`natsio-testing`, future
`natsio-otel`…) that import as `natsio.<name>` — the orbit.py model with the
natsio brand. Mechanism: the core stays a *regular* package whose
`__init__.py` pkgutil-extends its own `__path__`; extension wheels ship only
their subpackage (never a top-level `__init__.py`). Same hybrid Airflow uses
for provider packages. If a module is ever adopted into official orbit.py,
the transplant is mechanical.

### The instrumentation seam

Observability hooks are a zero-dep protocol (`Instrumentation`) so an OTel
adapter can be an extension, not a core dependency. Two rules with teeth:
a broken metrics backend must not kill the connection (guards are pre-bound
once, and *that* contract is tested), and the seam must cost nothing when
unused (the default Noop is not wrapped at all — this was ~17% of publish
throughput before it was fixed).

## Behavioral contracts

### Loud failure, everywhere

The through-line of the whole client: **nothing drops silently.**
Backpressure drops are counted and reported; disabling the reconnect buffer
(`-1`) makes racing publishes *report* their loss; Object Store reads verify
digest and size ("a completed read is a verified read"), and chunks appended
by a hostile writer are a `DigestMismatchError`; a hostile WebSocket frame
is a typed teardown, not unbounded buffering (64 MiB cap, nats.go parity);
subject/name validation happens client-side with **byte-stable error
messages** — the performance fast path is differential-tested against the
reference validator so no rejection ever changes shape.

### No default KV TTL

Buckets never expire data unless asked. The pre-rewrite implementation
defaulted to a 120-second TTL and silently lost data; that bug is pinned by
a regression test and this sentence.

### Every rollup meta write is CAS-gated

KV and Object Store metadata live on rollup subjects. Unguarded rollup
writes were probe-proven to (a) permanently orphan the losing writer's
chunks under concurrent puts and (b) let a delete report success while the
data stayed live. Every meta write therefore carries
`expected_last_subject_seq` and re-anchors on conflict — losers *know* they
lost and clean up after themselves. This is stricter than nats.go, on
purpose.

### Ordered-consumer snapshots

The ordered consumer self-heals by replaying from its last good position —
correct for live streams, hazardous for snapshots. Hence: snapshot phases
dedupe by (key, revision); snapshot phases are idle-bounded so a bucket
purged mid-snapshot terminates instead of deadlocking; `until_drained=True`
ends finite reads exactly (via each delivery's `num_pending`, re-checked on
every consumer recreation) rather than by timeout.

### Optional await

`subscribe()`, `consume()`, `ordered_consumer()`, KV/Object Store `watch()`,
and micro registration all return objects whose `__await__` is a
no-suspension generator returning `self`: `await` is tolerated as a no-op.
Why: subscribing performs no I/O here (frames are buffered), but every other
NATS client awaits these calls — muscle memory (human and LLM) writes
`await`, and fighting it costs more than accepting it. Two lines drawn
deliberately: the await is **not** a hidden flush (that would give two
spellings different failure semantics), and `ObjectStore.get()` is excluded
(`await obj.get(...)` reads like it yields bytes; the immediate `TypeError`
is the better teacher).

### Auth and reconnect semantics (nats.go contracts)

URL credentials beat option credentials. Permanent config errors (missing
creds file, bad seed, missing Ed25519 backend) fail fast instead of being
masked by other pool servers. Two identical auth rejections from a server
abort the reconnect loop (`ignore_auth_error_abort` opts out). Discovered
servers absent from later gossip are pruned; over WebSocket, gossiped URLs
are *unconditionally* re-schemed onto the connection's scheme so a TCP
address can never smuggle into a ws pool.

### Durations

Config durations are `timedelta`. Per-message TTLs (ADR-43) additionally
accept whole-second ints and `"never"` because that is the wire's
granularity — and a sub-second `timedelta` is **rejected loudly** rather
than rounded, because the wire genuinely cannot express it.

### Microservices (ADR-32)

Monitoring subjects (`$SRV.*`) are subscribed queue-group-less so every
instance answers discovery, while endpoints share a queue group (default
`"q"`) and load-balance — that asymmetry *is* the protocol. Unhandled
handler exceptions auto-respond with a 500-class error and are counted; a
deliberate `respond_error` followed by a raise keeps the handler's error as
`last_error`. `stop()` drains rather than cuts, so in-flight handlers
finish.

### WebSocket scope (v1)

permessage-deflate is declined (no extension negotiation → reserved bits are
a hard error), proxy paths and custom upgrade headers are not offered.
Deliberate simplicity; revisit on demand.

## Performance doctrine

The 2x-vs-nats-py publish win came from five contract-preserving fixes
(validation fast path, unwrapped Noop instrumentation, cached `max_payload`,
un-frozen per-message dataclasses, precomputed JSON codec plans) — all
**allocation and indirection**, none architectural. Method is part of the
doctrine: profile with evidence (ns/op, call counts), patch-measure-revert
before landing, name the competitor's mechanism, and record what is *not*
worth chasing (the sans-io parser rewrite; the 0.01%-hit `asyncio.wait`
race). nats-core beats us on raw publish via a 5 ms write-coalescing floor
that costs it 37x on request/reply latency — we deliberately do not copy
that trade.

## Process

- **nats.go is the conformance oracle.** Parity claims are verified against
  its *source and tests*, never from memory. The full-suite audit (8
  domains, every finding live-reproduced, then adversarially refuted) is the
  template for behavioral work.
- **Reviews**: adversarial reviewers with mandatory live probes, then
  independent refutation. Findings travel as markdown — strict structured
  schemas made review agents emit degenerate stubs.
- **Docs**: docstrings and pages are plain markdown (mkdocstrings does not
  render Sphinx roles — they leak as literal text); builds are strict;
  non-trivial snippets are run against a live server before publication.
  Context7 indexes `docs/` + `examples/`, with `rules` steering agents away
  from nats-py idiom bleed-through.
- **CI** runs the same `just` recipes as local dev (no split brain); merges
  gate on the pinned supported server; a daily workflow tests the latest
  server release and a from-source build of server `main` as an early-warning
  radar, not a gate.
- **Versioning**: fix → patch, addition → minor, 1.0 declares the API
  freeze. `just release X.Y.Z` performs the bump (and refuses without a
  changelog section). Commits are signed by the owner; `git add` uses
  explicit paths only — a `.env` with a live key once reached the public
  repo via `git add -A`, and the history rewrite that followed is not an
  experience worth repeating.
