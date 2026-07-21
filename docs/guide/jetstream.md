# JetStream

Core NATS is fire-and-forget. **JetStream** adds persistence: a *stream*
captures messages published on its subjects and stores them, and every publish
is acknowledged with a `PubAck` carrying the assigned
sequence. That ack is what lets JetStream offer guarantees core NATS cannot —
deduplication, optimistic concurrency, and durable, replayable consumption.

natsio implements the **ADR-37 simplified API** only: pull consumers with three
read shapes (`fetch`, `next`, `consume`), plus a first-class ordered consumer.
There is no legacy push/subscribe surface to learn or misuse.

```python
import natsio
from natsio.jetstream import StreamConfig, ConsumerConfig

async with await natsio.connect("nats://localhost:4222") as nc:
    js = nc.jetstream()
    stream = await js.create_stream(StreamConfig(name="ORDERS", subjects=["orders.>"]))
    await js.publish("orders.new", b'{"id": 1}')

    consumer = await stream.create_consumer(ConsumerConfig(durable_name="worker"))
    async with consumer.consume() as messages:
        async for msg in messages:
            await msg.ack()
```

`nc.jetstream()` returns a lightweight [`JetStreamContext`][natsio.jetstream.JetStreamContext]
over the connection; it talks to the server's `$JS.API` control plane and takes
optional `domain=` / `api_prefix=` routing and the `publish_async_*` window
knobs described below.

## Streams

A `StreamConfig` mirrors the JSON API. The
essentials: a `name`, the `subjects` it captures, a
`RetentionPolicy` (`LIMITS` by default —
keep by age/size; `WORK_QUEUE` and `INTEREST` for consumed-once semantics), a
`StorageType` (`FILE` or `MEMORY`), and limits
such as `max_msgs`, `max_bytes`, `max_age`, and `num_replicas`.

### Assert a stream idempotently

In scripts and services you usually want *"make the stream look like this"*,
not *"create, and fail if it exists"*.
[`create_or_update_stream`][natsio.jetstream.JetStreamContext.create_or_update_stream]
is the idiomatic assert — it updates an existing stream to your config (a no-op
when identical) or creates a missing one, and never raises
`StreamNameInUseError`. It is race-tolerant under concurrent creators.

```python
from natsio.jetstream import StreamConfig, RetentionPolicy

stream = await js.create_or_update_stream(
    StreamConfig(
        name="ORDERS",
        subjects=["orders.>"],
        retention=RetentionPolicy.LIMITS,
        max_msgs=1_000_000,
        max_age=None,  # never expire (a timedelta caps message age)
    )
)
```

Use `create_stream` when you
*want* a collision to raise, and
[`stream("ORDERS")`][natsio.jetstream.JetStreamContext.stream] to get a handle
to an existing one. Either way you get a [`Stream`][natsio.jetstream.Stream] —
the handle you create consumers from.

### Inspect, purge, list

```python
info = await stream.info()
print(info.state.messages, info.state.first_seq, info.state.last_seq)

purged = await stream.purge(subject="orders.stale")   # returns count purged
await js.delete_stream("ORDERS")

async for name in js.stream_names():   # auto-paged
    print(name)
```

### Read a stored message directly

A stream is addressable by sequence or subject without a consumer.
[`get_msg`][natsio.jetstream.Stream.get_msg] uses **Direct Get** when the stream
allows it (`StreamConfig(allow_direct=True)` — the 2.14-era default read path),
falling back to the `STREAM.MSG.GET` API otherwise.

```python
msg = await stream.get_msg(subject="orders.a.1")   # last message on that subject
first = await stream.get_msg(1)                     # by sequence
print(msg.subject, msg.seq, msg.payload)
```

## Publishing

Every JetStream publish awaits a `PubAck`:

```python
ack = await js.publish("orders.new", b'{"id": 1}')
print(ack.stream, ack.seq, ack.duplicate)
```

### Deduplication

Tag a publish with `msg_id` and the server drops duplicates seen inside the
stream's duplicate window — at-least-once retries become effectively-once. The
duplicate is recognised as the *same* sequence, with `duplicate=True`, and
nothing new is stored.

```python
a1 = await js.publish("orders.new", b'{"id": 2}', msg_id="order-2")
a2 = await js.publish("orders.new", b'{"id": 2}', msg_id="order-2")
assert a2.seq == a1.seq and a2.duplicate
```

### Publish expectations (optimistic concurrency)

Make a publish conditional on the stream's current state. A violation raises
`WrongLastSequenceError` instead of
appending — these are first-class keyword arguments, not hand-built headers.

```python
from natsio.jetstream import WrongLastSequenceError

info = await stream.info()
await js.publish("orders.new", b"...", expected_last_seq=info.state.last_seq)
try:
    await js.publish("orders.new", b"...", expected_last_seq=info.state.last_seq)  # now stale
except WrongLastSequenceError:
    ...  # another writer moved the stream on
```

The full set: `msg_id`, `expected_stream`, `expected_last_seq`,
`expected_last_subject_seq`, `expected_last_subject_seq_subject` (server 2.12+),
`expected_last_msg_id`.

!!! note "Subject-scoped sequence checks (2.12)"
    `expected_last_subject_seq` asserts the last sequence *on the subject you
    publish to*. Server 2.12 adds `expected_last_subject_seq_subject`, which
    scopes that check to a different subject filter — publish to `orders.a.2`
    while asserting the last sequence across `orders.a.*`:

    ```python
    await js.publish(
        "orders.a.2", b"...",
        expected_last_subject_seq=last_seq_on_wildcard,
        expected_last_subject_seq_subject="orders.a.*",
    )
    ```

    The two are coupled — passing the subject without the sequence raises
    `ConfigError` up front.

### Per-message TTL (ADR-43)

A single message can self-expire, independent of the stream's `max_age`. The
stream must be created with `allow_msg_ttl=True`. `ttl` accepts a `timedelta`,
whole seconds, or the string `"never"`:

```python
from datetime import timedelta

stream = await js.create_or_update_stream(
    StreamConfig(name="ORDERS", subjects=["orders.>"], allow_msg_ttl=True)
)
await js.publish("orders.session", token, ttl=timedelta(minutes=30))
await js.publish("orders.session", token, ttl=300)          # whole seconds
```

The wire format is second-granular, so a sub-second `timedelta` is rejected
loudly rather than silently rounded.

## The async publish window

Publishing one ack at a time is simple but serial. For throughput,
[`publish_async`][natsio.jetstream.JetStreamContext.publish_async] fires without
waiting and returns a future; you await the futures (or the whole batch) later.
It takes the same expectation and TTL keywords as `publish`.

```python
# fire many publishes; each returns a future resolving to its PubAck
futures = [await js.publish_async("orders.batch", f"item-{i}".encode()) for i in range(1000)]
print(js.publish_async_pending, "acks outstanding")

await js.publish_async_complete(timeout=10)   # wait for the whole batch
acks = [f.result() for f in futures]
```

Outstanding acks are capped at `publish_async_max_pending` (default 4000). When
the window is full, `publish_async` waits up to `publish_async_stall_wait`
(default 200 ms) for it to drain, then raises
`TooManyStalledMsgsError` — back off
and retry. On disconnect, outstanding futures fail with `ConnectionClosedError`
rather than hanging, because the ack for a pre-disconnect publish is lost and a
blind resend could duplicate. Tune the window on the context:

```python
js = nc.jetstream(publish_async_max_pending=8000, publish_async_stall_wait=0.5)
```

## Consumers

A stream stores messages; a **consumer** is a stateful cursor that delivers them
and tracks what you have acknowledged. Create one from a
[`Stream`][natsio.jetstream.Stream] with a
`ConsumerConfig`. A `durable_name` makes it
survive restarts (its ack state lives on the server); omit it for an ephemeral
consumer bounded by `inactive_threshold`.

```python
from datetime import timedelta
from natsio.jetstream import ConsumerConfig, AckPolicy

consumer = await stream.create_consumer(
    ConsumerConfig(
        durable_name="worker",
        ack_policy=AckPolicy.EXPLICIT,       # every message must be acked
        ack_wait=timedelta(seconds=30),      # redeliver if unacked for 30s
        filter_subject="orders.new",         # or filter_subjects=[...]
    )
)
```

Creating with the same name and config again is idempotent. Consumer CRUD lives
on both the stream and the handle:

```python
info = await consumer.info()                 # or stream.consumer_info("worker")
print(info.num_pending, info.num_ack_pending)
async for name in stream.consumer_names():
    ...
await consumer.delete()                       # or stream.delete_consumer("worker")
```

### Three ways to read

=== "fetch"

    Pull up to `n` messages, once. Returns what arrived — an **empty list is a
    normal outcome**. Best for batch/worker loops.

    ```python
    batch = await consumer.fetch(10, timeout=5)
    for msg in batch:
        await msg.ack()
    ```

=== "next"

    Pull exactly one; raises `NoMessagesError`
    on timeout. `next()` is `fetch(1)` with a friendlier signature.

    ```python
    from natsio.jetstream import NoMessagesError

    try:
        msg = await consumer.next(timeout=5)
        await msg.ack()
    except NoMessagesError:
        ...  # nothing within the deadline
    ```

=== "consume"

    A continuous, self-refilling stream. Best for daemons. It keeps up to
    `max_messages` requested from the server, re-pulling before the buffer
    drains, and self-heals across reconnects.

    ```python
    async with consumer.consume(max_messages=500) as messages:
        async for msg in messages:
            await process(msg.data)
            await msg.ack()
    ```

A delivered message is a [`JsMsg`][natsio.jetstream.JsMsg]: it wraps the core
message (`.data`, `.subject`, `.headers`) and adds the ack surface. Its
`.metadata` decodes the `$JS.ACK` reply subject — stream/consumer sequence,
delivery count, and how many messages remain pending:

```python
print(msg.metadata.stream_seq, msg.metadata.num_delivered, msg.metadata.num_pending)
```

### Acknowledgement

The ack is a decision, and `ack`/`nak`/`term` are **terminal** — a second
terminal ack raises
`MessageAlreadyAckedError`.

| Call | Meaning |
|---|---|
| `await msg.ack()` | Done — never redeliver. |
| `await msg.ack_sync()` | Ack and wait for the server to confirm it was recorded. |
| `await msg.nak(delay=5)` | Failed — redeliver (optionally after `delay` seconds or a `timedelta`). |
| `await msg.term("reason")` | Poisoned — never redeliver, stop trying. |
| `await msg.in_progress()` | Still working; reset the ack-wait timer (may be sent any number of times). |

### Heartbeats and liveness

Long-lived reads ask the server for periodic `100`-status heartbeat frames so a
silent stream is distinguishable from a dead connection. `consume()` sets a
sensible `idle_heartbeat` automatically; `fetch()` takes one explicitly. If the
connection closes or reconnection is exhausted, a parked `fetch`, `consume`, or
`next` raises [`ConnectionClosedError`][natsio.errors.ConnectionClosedError]
instead of hanging forever.

## The ordered consumer

An **ordered consumer** is an ephemeral, always-in-order, self-healing view of
the stream — no name, no acks. It is backed by an `ack_policy=none` consumer
that judges delivery by *consumer* sequence contiguity; on any gap, missed
heartbeat, or consumer loss it silently recreates itself at the next unseen
stream sequence. It is the engine under KV and Object Store watchers.

[`messages()`][natsio.jetstream.OrderedConsumer.messages] has two modes, and the
distinction matters:

=== "Live tail (idle_timeout)"

    `idle_timeout` bounds the wait *per message*. On a quiet stream it raises
    `NoMessagesError` instead of
    self-healing forever — that is how a caller tells *"drained"* from *"still
    coming"* on an open-ended read.

    ```python
    from natsio.jetstream import NoMessagesError

    async with stream.ordered_consumer() as ordered:
        try:
            async for msg in ordered.messages(idle_timeout=30):
                print(msg.data, msg.metadata.stream_seq)
        except NoMessagesError:
            pass  # no new message within 30s
    ```

=== "Finite read (until_drained)"

    `until_drained=True` ends the iteration **normally** the moment the consumer
    is caught up — exact and immediate, via the server's per-delivery
    `num_pending`, with no timeout wait and no exception to catch. Messages
    published while draining are still delivered ("caught up" means caught up at
    delivery time); an empty stream yields nothing. A second drained read
    resumes from where the first stopped.

    ```python
    async with stream.ordered_consumer() as ordered:
        async for msg in ordered.messages(until_drained=True):
            archive(msg.data)
        # falls through here once the stream is fully read
    ```

Combine the two when you want a finite read *with* a liveness bound while
draining. `ordered_consumer()` also takes `filter_subjects`, `deliver_policy`,
`opt_start_seq`/`opt_start_time`, and `headers_only`.

## Priority groups (ADR-42)

Priority groups let several clients share one consumer with server-enforced
delivery gating. Configure the consumer with `priority_groups` and a
`PriorityPolicy`; every `fetch`/`next`/
`consume` then names its `group`.

**Overflow** gating (`min_pending` / `min_ack_pending`) serves a request only
when the consumer has at least that many pending (or unacked) messages —
useful for spilling backlog to extra workers only when it builds up:

```python
from natsio.jetstream import ConsumerConfig, PriorityPolicy

consumer = await stream.create_consumer(
    ConsumerConfig(
        durable_name="workers",
        priority_policy=PriorityPolicy.OVERFLOW,
        priority_groups=["backlog"],
    )
)
# this overflow worker only pulls once 100+ messages have piled up
batch = await consumer.fetch(50, timeout=5, group="backlog", min_pending=100)
```

**Pinned client** (`PriorityPolicy.PINNED_CLIENT`) routes a group's deliveries
to a single client at a time — the server pins one puller and stamps its
`Nats-Pin-Id` on every message; natsio replays it so you stay pinned.
[`unpin`][natsio.jetstream.Consumer.unpin] releases it so the next puller in the
group takes over:

```python
consumer = await stream.create_consumer(
    ConsumerConfig(
        durable_name="primary",
        priority_policy=PriorityPolicy.PINNED_CLIENT,
        priority_groups=["hot"],
    )
)
async with consumer.consume(group="hot") as messages:
    async for msg in messages:
        await msg.ack()
# hand the group over to another client
await consumer.unpin("hot")
```

A consumer with priority groups requires every pull to name a valid one, and a
plain consumer rejects a `group` it cannot honour — both validated client-side.

## Advanced: message schedules

Server 2.12 can generate messages on a schedule. Enable it on the stream with
`StreamConfig(allow_msg_schedules=True)`, then publish a *definition* message
carrying the schedule headers from
`natsio.jetstream.headers` — `Nats-Schedule` (an
`@at <RFC3339>` / `@every <duration>` / cron expression, or an alias like
`@daily`), `Nats-Schedule-Target` (where generated messages land), and
optionally `Nats-Schedule-TTL` / `Nats-Schedule-Time-Zone`.

```python
from natsio.jetstream import headers as js_headers

await js.publish(
    "sched.nightly", b"",
    headers={
        js_headers.SCHEDULE: js_headers.SCHEDULE_DAILY,
        js_headers.SCHEDULE_TARGET: "jobs.run",
    },
)
```

The server stamps its own `Nats-Scheduler` / `Nats-Schedule-Next` headers on the
generated messages — don't set those yourself. See the header module for the
full constant set.

## See also

- [Key-Value](key-value.md) and [Object Store](object-store.md) — higher-level
  stores built on streams, consumers, and the ordered consumer.
- [JetStream API reference](../reference/jetstream.md) — every field and method.
- [Migrating from nats-py](../migration-from-nats-py.md) — if you are porting a
  push/pull-subscribe codebase.
