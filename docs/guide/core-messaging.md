# Core messaging

Publish, subscribe, and request/reply — the verbs you use in almost every
natsio program. This page assumes a connected client; snippets are fragments of
one script:

```python
import asyncio
import natsio


async def main() -> None:
    async with await natsio.connect("nats://localhost:4222") as nc:
        ...   # the snippets below go here


asyncio.run(main())
```

## Publishing

`publish` sends a message and returns once the frame is buffered locally — it
does **not** wait for delivery. Payloads are `bytes`; a `str` is UTF-8 encoded
for you.

```python
await nc.publish("events.user.created", b'{"id": 42}')
await nc.publish("events.user.created", "text is encoded for you")
```

A payload larger than the server's `max_payload` raises
`MaxPayloadExceededError` before anything hits the socket — read the limit from
`nc.max_payload`.

### Headers

Pass `headers=` a plain mapping. Values may be a single string or a sequence of
strings for **multi-value** headers:

```python
await nc.publish(
    "events.user.created",
    b"...",
    headers={
        "Content-Type": "application/json",
        "Nats-Msg-Id": "42",          # e.g. JetStream dedup
        "X-Tag": ["red", "blue"],     # multi-value: two X-Tag lines on the wire
    },
)
```

On the receiving side, `msg.headers` is a `Headers` map (or `None` if the
message carried no header block). Lookup is exact-match and case-preserving;
indexing returns the **first** value, `get_all` returns every value:

```python
msg.headers["Content-Type"]        # 'application/json'  (first value)
msg.headers.get("Missing")         # None
msg.headers.get_all("X-Tag")       # ['red', 'blue']
```

!!! warning "Header injection is rejected"
    Keys must be printable ASCII without `:`; values may not contain CR or LF.
    An unsafe header raises `BadHeadersError` at publish time — a header block
    can never break wire framing.

## Subscriptions

`nc.subscribe(subject)` returns a `Subscription`. It is synchronous by design:
registration takes effect immediately, so no message can be missed between
subscribing and consuming. Subjects may contain wildcards (`*` for one token,
`>` for the rest).

A subscription has **two consumption modes, and they are mutually exclusive**:
async-iterator (the default) or callback.

### Iterator mode

No `cb=`. You pull messages yourself. Best when consumption is linear and you
want natural backpressure from simply not iterating. As an async context
manager it unsubscribes on exit:

```python
async with nc.subscribe("orders.>") as sub:
    async for msg in sub:
        print(msg.subject, msg.data)
        if done:
            break
```

For a single message with its own deadline, use `next_msg` — it raises
`TimeoutError` on expiry and `SubscriptionClosedError` if the subscription is
gone:

```python
sub = nc.subscribe("orders.new")
try:
    msg = await sub.next_msg(timeout=1.0)
except natsio.TimeoutError:
    print("nothing within 1s")
```

### Callback mode

Pass `cb=`. natsio spawns a background reader task that hands each message to
your callback (sync or async). This is the natural shape for a long-lived
responder or worker:

```python
async def handle(msg: natsio.Msg) -> None:
    await do_work(msg.data)

sub = nc.subscribe("orders.new", cb=handle)
await nc.flush()   # ensure the SUB reached the server before you rely on it
```

!!! warning "The modes are exclusive"
    A callback subscription cannot also be iterated (and `next_msg` on it
    raises `SubscriptionClosedError`). Pick one mode per subscription. A
    callback that raises does not kill the reader — the error is routed to the
    client's [error callback](connection.md#lifecycle-events).

### Queue groups

Subscribers sharing a `queue=` name form a group: the server delivers each
message to **exactly one** member. This is how you scale a worker pool — every
worker subscribes to the same subject with the same queue name.

```python
async def worker(msg: natsio.Msg) -> None:
    await process(msg.data)

nc.subscribe("work.jobs", queue="pool", cb=worker)
nc.subscribe("work.jobs", queue="pool", cb=worker)   # another instance
# ...each job goes to only one of them, load-balanced.
```

### Unsubscribing

- `await sub.unsubscribe()` — stop delivery immediately and discard anything
  still queued.
- `await sub.unsubscribe_after(n)` — ask the server to stop after `n` **total**
  deliveries (the `UNSUB <sid> <max>` contract), then the subscription closes
  itself. Ideal for "collect exactly N replies":

```python
sub = nc.subscribe("demo.limited")
await sub.unsubscribe_after(3)
for i in range(5):                       # publish more than the limit
    await nc.publish("demo.limited", str(i).encode())

got = [msg.data async for msg in sub]    # loop ends when the sub auto-closes
assert len(got) == 3
```

### Pending limits and backpressure

A subscription is a bounded delivery queue fed synchronously from the socket
read path. That path must never block, so when the queue exceeds its **pending
limits** a `PendingLimitPolicy` decides what gives — and every policy is loud:
drops are counted in `sub.dropped` and reported to the error callback.

Limits default to `pending_msgs_limit=65_536` and `pending_bytes_limit=64 MiB`
(from [`ConnectOptions`](connection.md)); override them per subscription:

| Policy | Behavior when limits are exceeded |
|---|---|
| `DROP_NEW` *(default)* | Discard the arriving message. |
| `DROP_OLD` | Evict the oldest queued messages to make room (last-value-wins). |
| `BLOCK` | Pause reading from the socket until the consumer catches up. Nothing is dropped; under sustained pressure the *server* eventually declares this client a slow consumer. |
| `ERROR` | Fail the subscription: stop delivering and raise `SlowConsumerError` to the consumer. |

```python
sub = nc.subscribe(
    "telemetry.firehose",
    pending_msgs_limit=1000,
    policy=natsio.PendingLimitPolicy.DROP_OLD,
)
# ...later, observe loss:
if sub.dropped:
    print(f"dropped {sub.dropped} messages under load")
```

`sub.pending_msgs`, `sub.pending_bytes`, and `sub.delivered` give live counters.

### Draining a subscription

`unsubscribe()` is abrupt — queued messages are discarded. `drain()` is
graceful: it stops **new** delivery, waits for the already-queued backlog to be
handled, *then* closes. Use it for clean shutdown where in-flight work must not
be lost:

```python
sub = nc.subscribe("jobs.work", cb=slow_handler)
# ...
await sub.drain()   # returns only once every queued message has been handled
```

`drain()` is unbounded by itself — bound it with your own `asyncio.timeout`, or
use [`nc.drain()`](connection.md#drain-vs-close) to drain the whole client under
`drain_timeout`.

## Request/reply

`request` publishes to a subject with a private reply inbox and awaits a single
answer. natsio muxes all replies through one wildcard inbox internally, so a
request is just an `await`:

```python
reply = await nc.request("svc.echo", b"hello", timeout=2.0)
print(reply.data)
```

Two failure modes, and the distinction matters:

- **`NoRespondersError`** — nobody is subscribed to the subject. The server
  tracks interest and returns a 503, so the request fails *immediately* instead
  of waiting out the timeout. This fast-fail makes request/reply safe as a
  health probe.
- **`TimeoutError`** — a responder exists but did not answer within the
  deadline.

```python
try:
    await nc.request("svc.maybe", b"?", timeout=1.0)
except natsio.NoRespondersError:
    ...   # fires at once — nobody listening
except natsio.TimeoutError:
    ...   # a responder existed but was too slow
```

### Responding

Inside a handler, `msg.respond()` publishes back to the message's reply
subject. Responding to a message that has no reply subject raises
`NoReplySubjectError`:

```python
async def handler(msg: natsio.Msg) -> None:
    await msg.respond(b"pong", headers={"X-Handled-By": "svc-1"})
```

### request_many

`request_many` sends one request and yields **every** reply (ADR-47). It
completes on whichever comes first: `max_msgs` replies, a `stall`-second gap
between replies, or the overall `timeout`. A no-responders status ends the
stream without yielding:

```python
replies = [
    msg.data
    async for msg in nc.request_many(
        "svc.scatter", b"who is there?", max_msgs=5, stall=0.25, timeout=2.0
    )
]
```

## See also

- [Connection & lifecycle](connection.md) — reconnect, events, drain vs close.
- [Client API reference](../reference/client.md) and
  [Errors reference](../reference/errors.md).
- [Migrating from nats-py](../migration-from-nats-py.md) — subscription and
  request API mappings.
