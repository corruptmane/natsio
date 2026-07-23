# Changelog

All notable changes to `natsio-schedules` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project is
pre-1.0 and makes no API-stability promises.

## [0.1.0] - 2026-07-23

Initial release. JetStream message schedules
([ADR-51](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-51.md)),
pinned to nats.go `jetstream/message.go` + `jetstream/jetstream_options.go` and
verified end-to-end against nats-server 2.14.3. Requires server 2.12+ for the
feature, 2.14+ for cron time zones and `Nats-Schedule-Rollup`. Stdlib only.

### Added

- **Expression builders** producing the exact `Nats-Schedule` bytes nats.go
  emits:
  - `at(datetime)` -> `@at <RFC3339>` — timezone-aware only, converted to UTC,
    truncated to whole seconds (Go's `time.RFC3339` layout carries no fraction).
    A past instant is accepted: ADR-51 defines it as "fire immediately".
  - `after(timedelta)` -> `at(now + delay)`.
  - `every(timedelta | str)` -> `@every <duration>` — `timedelta`s are formatted
    as Go's `time.Duration.String()` (`1m30s`, `1h0m0s`); strings are validated
    against `time.ParseDuration` and passed through verbatim. Minimum `1s`.
  - `cron(str)` — the 6-field form (seconds first) with `*`, `?`, ranges, lists,
    `/step`, and three-letter month/day names, matching the server's parser on
    a leading `*`/`?` in a range (`*-*` is accepted, `5-*` is not) and rejecting
    day-of-month/month pairs no calendar can satisfy (`0 0 0 31 2 *`), plus the
    predefined aliases
    `@yearly`/`@annually`/`@monthly`/`@weekly`/`@daily`/`@midnight`/`@hourly`
    exported as `YEARLY` … `HOURLY` and `PREDEFINED`.
  - `parse_schedule` to validate/classify an already-formed expression, and
    `format_go_duration` / `parse_go_duration` as the Go-duration seam.
- **`publish_schedule(js, subject, schedule, *, target, ...)`** and
  `build_schedule_headers(...)` — the full ADR-51 client-set header block
  (`Nats-Schedule`, `-Target`, `-Source`, `-TTL`, `-Time-Zone`, `-Rollup`) with
  pre-flight validation: wildcard/empty target and source rejected, time zone
  rejected on non-cron schedules and for fixed UTC offsets, sub-second TTLs
  rejected. Publish expectations (`msg_id`, `expected_last_subject_seq`) pass
  through to the core.
- **`Schedules` handle** over one stream: `create`, `get`, `list` (batch Direct
  Get filtered to messages carrying `Nats-Schedule`), `cancel` (reads the
  definition, refuses subjects holding anything else, purges only up to it, and
  is loud when nothing was there), `cancel_many` (unguarded wildcard purge),
  `cancel_by_sequence`, and `stop_and_publish` — ADR-51's atomic
  stop-and-publish (`Nats-Schedule-Next: purge` + `Nats-Scheduler`), CAS-gated
  on the definition's sequence via
  `Nats-Expected-Last-Subject-Sequence(-Subject)`: explicitly with
  `expected_schedule_seq`, or implicitly from a read (`require_existing`, the
  default) so that stopping an already-fired schedule raises instead of
  publishing and cancelling nothing. Binding is `await`-optional.
- **`list()` enumerates completely or raises.** One batch Direct Get cannot
  answer more than `MAX_SUBJECTS_PER_BATCH` (1024, exported) matching subjects —
  the server refuses the request with `413 Too Many Results`, which is the
  *expected* shape for ADR-51's one-subject-per-schedule layout. Above the cap
  `list()` reads the matching subjects from `STREAM.INFO` and fetches them page
  by page; a truncated batch (dead connection, deadline) propagates rather than
  passing for a complete enumeration. There is deliberately no `batch=`
  parameter: the server caps on matching subjects, not on the requested batch
  size, so it could only ever truncate the answer without saying so.
- **`list()` default filters are read live, not from a bind-time snapshot.**
  With no ``subjects`` argument the stream's subjects are fetched from
  `STREAM.INFO` on each call, so a subject added to the stream after the handle
  was bound still enumerates — reading the cached snapshot silently dropped it.
- **`get()` distinguishes a missing schedule from a gone stream.** Core reuses
  `MessageNotFoundError` for "direct get unavailable" (a deleted stream / direct
  get disabled), so `get()` confirms the miss against `STREAM.INFO` before
  translating: a real infra fault (`StreamNotFoundError`, `APIError`) now
  surfaces as itself instead of being reported as "no such schedule".
- **`encode_schedule_ttl` treats a `timedelta` and a duration string alike.**
  The `timedelta` branch rejected any sub-second precision (`timedelta(seconds=
  1.5)`) while the string branch accepted `"1.5s"` — yet the server accepts
  `1.5s` and rejects only `< 1s`. Both forms now apply the single `>= 1s` rule
  and emit the same Go duration (nats.go `WithScheduleTTL` parity).
- **Stream helpers**: `create_schedule_stream(js, ScheduleStreamConfig(...))`
  (forces `allow_msg_schedules` + `allow_direct`, defaults `allow_msg_ttl` on
  and `discard` to old, which ADR-51 requires), `schedules(js, name)`,
  `schedules_from_stream(js, stream)`.
- **`ScheduleEntry`** — a stored definition (`subject`, `sequence`, `schedule`,
  `target`, `source`, `ttl`, `time_zone`, `rollup`, `payload`, `time`,
  `headers`) with `is_one_shot`, `fires_at`, `interval` and `expression`. The
  read accessors never re-run the write-path validator, so a definition written
  by the `nats` CLI or nats.go is readable even where the local grammar is
  stricter; `headers` carries the stored block with the Direct Get envelope
  (`TRANSPORT_HEADERS`) removed, and is excluded from `==`/`hash()` so entries
  compare equal across read paths and can be put in a `set`.
- **`delivery_info(msg)` / `is_scheduled(msg)`** — read the server's stamps
  (`Nats-Scheduler`, `Nats-Schedule-Next`, mirrored `Nats-TTL`/`Nats-Rollup`)
  off a core `Msg`, a `JsMsg`, or a `StoredMsg`, as a typed `ScheduleDelivery`
  with the `purge` sentinel surfaced as `final=True`.
- **`natsio.schedules.headers`** — every ADR-51 header name, re-exported from
  the core plus `SCHEDULE_ROLLUP` (`Nats-Schedule-Rollup`), which the core does
  not carry yet.
- **Typed errors** rooted at `ScheduleError` (a `JetStreamError`), in two
  branches: local validation (`ScheduleConfigError` and friends, also
  `natsio.errors.ConfigError`), and server-reported (`ScheduleAPIError` and
  friends, also `APIError`) bound to every ADR-51 `err_code` through the core's
  `register_error` hook — 10186/10187 (mirror/source), 10188 (disabled), 10189
  (pattern), 10190 (target), 10191 (TTL), 10192 (rollup), 10203 (source), 10212
  (scheduler), 10223 (time zone), all probe-verified on 2.14.3.
- **Tests**: 195 unit tests over the emitted strings, every rejection path and
  the `list()` paging/failure paths (scripted `Stream` stand-ins), plus 90 live
  tests against a real `nats-server` — `@at` delivery and self-purge, `@every`
  repetition with monotonic `Nats-Schedule-Next`, cron, subject sampling, user
  headers travelling to the target, list/get/replace, enumeration of more
  schedules than one batch Direct Get can answer, CAS create, all four
  cancellation paths (including a cancel aimed at a non-schedule message),
  atomic stop-and-publish (including the stale-sequence and missing-schedule
  refusals), every server err_code, and a **grammar-parity suite** that asserts
  the local validator's verdict matches the pinned server's, expression by
  expression.

### Known limitations (core seam friction)

Surfaced while building this extension from outside the core. All are in the
natsio core, not in the schedules module:

- **`natsio.jetstream.headers` is missing `Nats-Schedule-Rollup`** (ADR-51
  rev 4, server 2.14) and the `@annually` / `@midnight` cron aliases, both of
  which the pinned 2.14.3 accepts. Proposal: add
  `SCHEDULE_ROLLUP: Final = "Nats-Schedule-Rollup"` and
  `SCHEDULE_ANNUALLY`/`SCHEDULE_MIDNIGHT` next to the existing constants.
  (nats.go has not caught up either — this one is ahead of the Go oracle.)
- **No Go-duration codec in the core.** `natsio.jetstream.headers.encode_ttl`
  emits bare seconds (`"300"`) for `Nats-TTL`, while ADR-51 and nats.go both use
  Go duration strings (`"5m"`) for `Nats-Schedule-TTL`; the server accepts both.
  This package ships its own `format_go_duration`/`parse_go_duration` rather
  than reach into the core's TTL encoder. Proposal: promote a shared Go-duration
  codec into `natsio._internal` so TTL, schedules, and any future
  duration-valued header agree on one implementation.
- **The schedule err_codes are not in the core registry.** Worked around from
  outside using the documented `natsio.jetstream.errors.register_error` hook,
  which did the job cleanly (the `natsio-jetstream-batch` extension reached for
  the same hook independently). Residual friction: the core already owns
  `StreamConfig.allow_msg_schedules` and every `Nats-Schedule*` header
  constant, so owning the ten matching err_codes would be consistent — and the
  registry is process-global, so two extensions claiming one code would
  silently fight. Proposal: move 10186-10192 / 10203 / 10212 / 10223 into
  `natsio.jetstream.errors` next to the counter codes, and make
  `register_error` refuse to rebind a code that is already registered to a
  different type.
- **`Stream.get_last_msgs_for` reads an empty result as a truncated one.**
  2.14.3 answers a `multi_last` request that matches *nothing at all* with a
  single `404 No Results` and no `204 EOB` — per-subject misses are simply
  omitted from the reply, so a 404 is terminal, not skippable. The core skips
  it and then raises "ended without the 204 EOB terminator", i.e. every empty
  batch get is reported as truncation (after blocking for the full request
  timeout, since no `stall` is passed). `Schedules.list` works around it by
  confirming against `STREAM.INFO` before deciding a range is empty — one extra
  round-trip, and it cannot tell a genuinely empty range from one that was
  truncated *and* concurrently emptied. Proposal: treat `404` as a clean
  terminator in `get_last_msgs_for` (keeping the missing-EOB error for the case
  where data was yielded and the stream just stopped).
- **No paging seam for batch Direct Get.** The server caps one `multi_last`
  request at 1024 *matching subjects* and refuses the whole request above it
  (`413 Too Many Results`); the request's `batch` field does not raise that cap,
  and there is no `min_seq`/cursor to walk with. Paging therefore has to be done
  by enumerating subjects out of `STREAM.INFO` and re-querying them in chunks,
  which every caller of `get_last_msgs_for` will have to reinvent. Proposal:
  either page inside `get_last_msgs_for`, or expose `stream_info`'s
  `subjects_filter` paging (`offset`/`total`, which the core's `StreamInfo` does
  not model) so callers can do it without guessing.
- **`Stream` handles cannot publish.** Like KV/OS and counters, `Schedules`
  must hold both the `JetStreamContext` (to publish definitions) and the
  `Stream` (to read and purge them). Every stream-over-JetStream module
  re-derives this pairing.
