# natsio-schedules

JetStream **message schedules**
([ADR-51](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-51.md))
for [natsio](https://github.com/corruptmane/natsio): schedule-expression
builders, a scheduled-publish helper, and read/cancel operations over the
definitions a stream holds. Distribution `natsio-schedules`, imported as
`natsio.schedules`. Zero runtime dependencies beyond `natsio`. Pre-1.0, no
API-stability promises.

Requires **nats-server 2.12+** for the feature itself, **2.14+** for cron time
zones and `Nats-Schedule-Rollup`. Verified against the pinned **2.14.3**.

## What it is

A stream created with `allow_msg_schedules` can hold *schedule definitions*:
ordinary stored messages carrying a `Nats-Schedule` header. The server
republishes each definition's body to its `Nats-Schedule-Target` on the
schedule — a one-shot delayed publish, a repeating interval, a cron expression,
or a periodic sample of another subject's latest message.

A definition's subject is its identity. The server stores it as a
`Nats-Rollup: sub` message, so there is exactly one schedule per subject and
re-publishing replaces it.

> **Generated messages are published *inside* the stream.** A plain core-NATS
> subscription on the target subject will not see them (unless the stream has
> `republish` configured). Consume them with a JetStream consumer.

## Usage

```python
from datetime import timedelta

import natsio
from natsio.schedules import (
    HOURLY,
    ScheduleStreamConfig,
    after,
    create_schedule_stream,
    cron,
    delivery_info,
    every,
)

nc = await natsio.connect("nats://localhost")
js = nc.jetstream()

# `subjects` must cover the schedule subjects AND every target subject.
sched = await create_schedule_stream(
    js, ScheduleStreamConfig(name="SCHED", subjects=["schedules.>", "orders.>"])
)

# One-shot: publish `orders.reminder` five minutes from now, then self-destruct.
await sched.create(
    "schedules.orders.r1", after(timedelta(minutes=5)), target="orders.reminder", payload=b"ping"
)

# Repeating, with a TTL on each generated message.
await sched.create(
    "schedules.heartbeat", every(timedelta(seconds=30)), target="orders.tick", ttl="5m"
)

# Cron (6 fields: sec min hour dom month dow), evaluated in a named zone.
await sched.create(
    "schedules.report", cron("0 0 5 * * *"), target="orders.report", time_zone="Europe/Amsterdam"
)
await sched.create("schedules.hourly", HOURLY, target="orders.hourly")

# Subject sampling: republish the latest `sensors.raw` reading every minute.
await sched.create(
    "schedules.sample", every("1m"), target="orders.sampled", source="sensors.raw"
)

# Inspect / enumerate / cancel.
entry = await sched.get("schedules.heartbeat")
print(entry.schedule, entry.target, entry.interval)

async for entry in sched.list("schedules.>"):
    print(entry.subject, entry.schedule)

await sched.cancel("schedules.heartbeat")
```

On the consuming side:

```python
consumer = await sched.stream.create_consumer(ConsumerConfig(filter_subject="orders.>"))
msg = await consumer.next()

info = delivery_info(msg)     # None if this wasn't produced by a schedule
if info is not None:
    print(info.scheduler)     # "schedules.heartbeat" — the definition's subject
    print(info.next_run)      # next firing, or None ...
    print(info.final)         # ... when this was the last one (`Nats-Schedule-Next: purge`)
```

### Cancelling atomically

ADR-51's "advanced" stop: remove the schedule *only if* a message on a
different subject is persisted — the way to fire a delayed publish early
without letting the schedule also fire it, or to record a cancellation where
consumers can see it. CAS-gated on the definition's sequence, so a schedule
that already fired is never "cancelled" twice.

```python
entry = await sched.get("schedules.orders.r1")
await sched.stop_and_publish(
    "schedules.orders.r1",
    publish_to="orders.reminder",        # or any other subject except the schedule's own
    payload=b"sent early",
    expected_schedule_seq=entry.sequence,
)
```

Without an explicit `expected_schedule_seq` the definition is read first and
its sequence used as the gate, so stopping a schedule that already fired (or
never existed) raises `ScheduleNotFoundError` instead of publishing the message
and cancelling nothing — the server accepts that no-op happily. Pass
`require_existing=False` for a one-round-trip, fire-and-forget stop.

`cancel()` is the plain version: it reads the definition, refuses subjects that
hold something other than a schedule, and purges only up to the message it
read. `cancel_many()` is **not** guarded — it is a raw wildcard purge and will
happily delete generated messages and ordinary traffic under the filter.

## API

| Symbol | Purpose |
|---|---|
| `at(datetime)` | `@at <RFC3339>` — one-shot at an instant (tz-aware, UTC, whole seconds). |
| `after(timedelta)` | `at(now + delay)`. |
| `every(timedelta \| str)` | `@every <go-duration>` — repeating, minimum `1s`. |
| `cron(str)` | 6-field cron, or a predefined `@` alias. |
| `YEARLY` / `ANNUALLY` / `MONTHLY` / `WEEKLY` / `DAILY` / `MIDNIGHT` / `HOURLY` | The predefined aliases as `Schedule`s. |
| `parse_schedule(str \| Schedule)` | Validate/classify an already-formed expression. |
| `format_go_duration` / `parse_go_duration` | Go `time.Duration` ↔ `timedelta`. |
| `create_schedule_stream(js, config)` | Create a schedule-capable stream, return its handle. |
| `schedules(js, name)` | Bind an existing one by name. |
| `schedules_from_stream(js, stream)` | Wrap an already-fetched `Stream` (no I/O; `await` optional). |
| `Schedules.create(subject, schedule, *, target, ...)` | Store/replace a definition. |
| `Schedules.get(subject)` | The stored `ScheduleEntry`. |
| `Schedules.list(subjects=None)` | Enumerate definitions (batch Direct Get, paged past the server's subject cap). |
| `Schedules.cancel(subject)` | Purge one schedule; loud if the subject held no definition. |
| `Schedules.cancel_many(filter)` | Unconditional wildcard purge — anything under the filter, schedule or not. |
| `Schedules.cancel_by_sequence(seq)` | Delete the definition by stream sequence. |
| `Schedules.stop_and_publish(...)` | Atomic stop + publish, CAS-gated on the definition by default. |
| `publish_schedule(js, ...)` | The same publish, without a handle. |
| `build_schedule_headers(...)` | Just the `Nats-Schedule*` header dict. |
| `delivery_info(msg)` / `is_scheduled(msg)` | Read the server's stamps off a delivered message. |
| `ScheduleEntry`, `ScheduleDelivery`, `ScheduleStreamConfig`, `Schedule` | Entities. |
| `natsio.schedules.headers` | Every ADR-51 header name as a constant. |

### Errors

Everything roots at `ScheduleError` (a `natsio.jetstream.JetStreamError`), in
two branches:

- **Local, pre-flight** — `ScheduleExpressionError`, `ScheduleTargetError`,
  `ScheduleSourceError`, `ScheduleTimeZoneError`, `ScheduleTTLError`,
  `SchedulesNotEnabledError`. All also subclass `natsio.errors.ConfigError`, so
  they are `ValueError`s. (`ScheduleNotFoundError` covers "no definition
  there".)
- **Server-reported** — every ADR-51 `err_code`, bound to a typed error through
  the core's `register_error` hook (registration happens on `import
  natsio.schedules`). They are also plain `APIError`s, so existing
  `except APIError` code keeps working.

| `err_code` | Error |
|---|---|
| 10186 | `MirrorWithMsgSchedulesError` |
| 10187 | `SourceWithMsgSchedulesError` |
| 10188 | `MessageSchedulesDisabledError` |
| 10189 | `SchedulePatternInvalidError` |
| 10190 | `ScheduleTargetInvalidError` |
| 10191 | `ScheduleTTLInvalidError` |
| 10192 | `ScheduleRollupInvalidError` |
| 10203 | `ScheduleSourceInvalidError` |
| 10212 | `SchedulerInvalidError` |
| 10223 | `ScheduleTimeZoneInvalidError` |

## Wire contract

| Header | Set by | Value |
|---|---|---|
| `Nats-Schedule` | client | `@at <RFC3339>`, `@every <duration>`, 6-field cron, or an `@` alias |
| `Nats-Schedule-Target` | client | concrete subject in the same stream (required) |
| `Nats-Schedule-Source` | client | concrete subject to sample; no wildcards |
| `Nats-Schedule-TTL` | client | Go duration or `never`; needs `allow_msg_ttl` |
| `Nats-Schedule-Time-Zone` | client | IANA name / `UTC` / `Local`; **cron only** |
| `Nats-Schedule-Rollup` | client | `sub` only |
| `Nats-Scheduler` | server | the schedule's subject |
| `Nats-Schedule-Next` | server | next firing (RFC3339), or `purge` |
| `Nats-TTL`, `Nats-Rollup` | server | mirrored from the `Nats-Schedule-*` pair |

Two things the table cannot show:

- `Nats-Schedule-Next` is stamped **in the schedule's own time zone** when
  `Nats-Schedule-Time-Zone` is set (`2026-07-23T02:56:46+05:30`), not in UTC.
  `delivery_info` keeps the offset, so `ScheduleDelivery.next_run` is an aware
  `datetime` in that zone — compare instants, not wall clocks.
- `ScheduleEntry.headers` holds the *stored* block only. Direct Get adds
  `Nats-Stream` / `Nats-Subject` / `Nats-Sequence` / `Nats-Time-Stamp` (and,
  on batch replies, `Nats-Num-Pending` / `Nats-Last-Sequence`); those are
  stripped (`TRANSPORT_HEADERS`), so an entry reads the same from `get()` and
  `list()` and its headers can be handed straight back to `create()`.

Pinned against nats.go `jetstream/message.go` (header constants),
`jetstream/jetstream_options.go` (`WithScheduleAt` / `WithScheduleEvery` /
`WithScheduleCron` / `WithScheduleTTL` / `WithScheduleTimeZone` value
formatting), `jetstream/errors.go` (err_codes) and ADR-51 itself; every
accept/reject boundary is additionally probed against the pinned 2.14.3 server
in `TestGrammarParity`. Note that `Nats-Schedule-Rollup` (ADR-51 rev 4) and the
`@annually` / `@midnight` aliases exist on the 2.14.3 server but not yet in
nats.go — for those the ADR and the server are the oracle.

## Scope limits

- **No local next-fire calculation.** Cron/interval evaluation lives in the
  server; this package never predicts when a schedule will run. Read the
  server's own answer from `Nats-Schedule-Next` on a delivered message.
- **The cron validator is a pre-flight, not a re-implementation.** It matches
  the server on everything the parity suite covers (field count, ranges, names,
  steps, `?`, `@` aliases, `0-6` day-of-week, a leading `*`/`?` swallowing the
  rest of a range, and day-of-month/month pairs that can never occur) and is
  *stricter* in exactly one place: an empty `Nats-Schedule` is rejected here,
  while the server accepts it and simply stores a normal message. Two known
  gaps in the other direction — cases the server rejects and this validator
  lets through, so they come back as an `APIError` rather than a local one:
  a cron expression whose *only* firing is further out than the server's search
  horizon (e.g. `0 0 0 29 2 *` evaluated in the late 2090s), and anything else
  its evaluator dislikes. The server stays the authority.
- **No stream-lifecycle management beyond creation.** Enabling
  `allow_msg_schedules` on an *existing* stream is a core `update_stream` call;
  this package will not do it implicitly.
- **`list()` requires `allow_direct`** (set by `create_schedule_stream`). It is
  one batch Direct Get over the stream's subjects, filtered to messages that
  actually carry `Nats-Schedule`. A single request cannot answer more than
  `MAX_SUBJECTS_PER_BATCH` (1024) matching subjects — the server refuses the
  whole thing with `413 Too Many Results` — so above that `list()` reads the
  matching subjects from `STREAM.INFO` and fetches them page by page. That is
  transparent but not free: narrow the filter (`list("schedules.>")`) on
  streams that also carry high-cardinality target subjects to stay on the
  single-request path. Enumeration is complete or it raises; a truncated read
  is never returned as if it were the whole set.
- **Retention interactions are the operator's problem.** ADR-51's `WorkQueue` /
  `Interest` caveats (a consumer ack can silently delete a schedule) are
  documented in the ADR, not enforced here; `ScheduleStreamConfig` defaults to
  `Limits`, which is the recommended policy.

## Example

A runnable script is at [`examples/basic.py`](https://github.com/corruptmane/natsio/blob/main/extensions/natsio-schedules/examples/basic.py) — start a server with `just server`, then:

```bash
python extensions/natsio-schedules/examples/basic.py
```
