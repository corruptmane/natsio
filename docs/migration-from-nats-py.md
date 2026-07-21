# Migrating from nats-py to natsio

This guide is for teams running **nats-py 2.x** (verified against `nats-py`
2.15.0) who want to move to **natsio** — a zero-dependency asyncio NATS client
for Python 3.13+ targeting NATS Server 2.14. It maps every commonly-used API
surface side by side, calls out the behavioral differences that will actually
bite you, and lists the handful of things nats-py lets you do that natsio
deliberately does not.

natsio is not a drop-in shim. The concepts line up, but the method shapes,
option names, and error types are different by design. Budget an afternoon for a
small service, and read [§8 Behavioral differences](#8-behavioral-differences-that-will-bite)
before you ship.

---

## 1. Why migrate, and how to install

**Why:**

- **Zero runtime dependencies.** NKey/JWT signing is an optional extra
  (`natsio[nkeys]`); everything else — core, JetStream, KV, Object Store — is
  pure stdlib asyncio.
- **One JetStream API, not two.** natsio implements only the ADR-37 *simplified*
  pull model (`stream.create_consumer(...)` → `consumer.fetch()/consume()`).
  nats-py 2.15 still ships the older push (`js.subscribe`) and
  `js.pull_subscribe`/`fetch` surfaces. Fewer footguns, one thing to learn.
- **Typed, client-side validation.** Bad subjects, bad consumer names, and
  conflicting options raise a typed `ConfigError` *before* a round-trip, instead
  of surfacing as an opaque server `-ERR`.
- **nats.go feature and performance parity.** See [§9](#9-performance-notes).

**Install** (natsio requires Python ≥ 3.13 and is tested against NATS Server 2.14):

```bash
uv add natsio            # or: pip install natsio
uv add "natsio[nkeys]"   # add an Ed25519 backend for NKey / JWT (.creds) auth
```

The `[nkeys]` extra pulls in a signing backend (PyNaCl or cryptography). You
only need it for NKey seeds or JWT/`.creds` credentials. Token and
user/password auth — and custom `CallbackAuth` signing — need nothing extra.

```python
import natsio

async with await natsio.connect("nats://localhost:4222") as nc:
    await nc.publish("greet.world", b"hello")
```

---

## 2. Connecting

nats-py folds all connection settings into `nats.connect(**kwargs)`. natsio does
the same, but the kwargs are a typed mirror of a frozen `ConnectOptions`
dataclass — you can pass them as keywords or build a `ConnectOptions` once and
reuse it:

```python
# nats-py
import nats
nc = await nats.connect("nats://localhost:4222", name="svc", ping_interval=30)

# natsio — keywords
import natsio
nc = await natsio.connect("nats://localhost:4222", name="svc", ping_interval=30)

# natsio — reusable options object (keywords still override it)
from natsio import ConnectOptions
opts = ConnectOptions(servers=("nats://localhost:4222",), name="svc")
nc = await natsio.connect(options=opts, ping_interval=30)
```

Multiple servers are positional in natsio (`connect(url1, url2)`) or the
`servers=(...)` tuple; nats-py takes a `servers=[...]` list.

### Option mapping

| nats-py `connect()` kwarg | natsio equivalent | Notes |
|---|---|---|
| `servers="nats://..."` / `["...", "..."]` | positional `connect(*servers)` or `servers=(...,)` (**tuple**) | natsio default is `("nats://127.0.0.1:4222",)` |
| `name=None` | `name=None` | client name in CONNECT |
| `connect_timeout=2` | `connect_timeout=5.0` | **default differs** (natsio 5.0s) |
| `verbose=False` | `verbose=False` | |
| `pedantic=False` | `pedantic=False` | |
| `no_echo=False` | `echo=True` | **inverted**: `no_echo=True` ⇒ `echo=False` |
| `allow_reconnect=True` | `allow_reconnect=True` | |
| `max_reconnect_attempts=60` | `max_reconnect_attempts=60` | natsio: per-server consecutive failures; `-1` = unlimited. `0` is rejected — use `allow_reconnect=False` for connect-once |
| `reconnect_time_wait=2` | `reconnect_time_wait=2.0` | natsio adds `reconnect_time_wait_max=8.0`, `reconnect_jitter`, `reconnect_jitter_tls` for a capped backoff-with-jitter |
| `ping_interval=120` | `ping_interval=120.0` | |
| `max_outstanding_pings=2` | `max_outstanding_pings=2` | |
| `dont_randomize=False` | `no_randomize=False` | renamed |
| `connect_timeout` | `connect_timeout` | |
| `drain_timeout=30` | `drain_timeout=30.0` | |
| `user=None` / `password=None` | `user=` / `password=` | must be given together |
| `token=None` | `token=` | |
| `user_credentials=None` | `credentials=` | path to a `.creds` file (needs `[nkeys]`) |
| `nkeys_seed_str=None` | `nkey_seed=` | the seed **string** (needs `[nkeys]`) |
| `nkeys_seed=None` (**file path**) | `nkey_seed=Path(p).read_text()...` or a custom `NKeyAuth` | natsio has no seed-*file* kwarg; read it, or pass `authenticator=NKeyAuth(seed=...)` |
| `signature_cb=` + `user_jwt_cb=` | `authenticator=CallbackAuth(jwt_callback=, signature_callback=)` | see [auth](#authentication) below |
| `tls=None` (`ssl.SSLContext`) | `tls=TLSConfig(context=...)` | wrap in `TLSConfig` |
| `tls_hostname=None` | `tls=TLSConfig(hostname=...)` | |
| `tls_handshake_first=False` | `tls=TLSConfig(handshake_first=True)` | |
| `inbox_prefix=b"_INBOX"` | `inbox_prefix="_INBOX"` | **str, not bytes** |
| `pending_size=2097152` | `max_pending_size=2*1024*1024` | renamed; the outbound write buffer cap |
| `flush_timeout=None` | `flush_timeout=10.0` | natsio has a real default |
| `flusher_queue_size=1024` | *(none)* | natsio's write path has no separate flusher queue; there is no equivalent knob |
| `error_cb=` | `error_cb=` **and/or** `nc.events()` | callback still supported; `events()` is the richer stream — see below |
| `disconnected_cb=` | `nc.events()` → `Disconnected` | |
| `reconnected_cb=` | `nc.events()` → `Reconnected` | |
| `closed_cb=` | `nc.events()` → `Closed` | |
| `discovered_server_cb=` | `nc.events()` → `ServersDiscovered` | |
| `lame_duck_mode_cb=` | `nc.events()` → `LameDuck` | |
| `reconnect_to_server_handler=` | *(none)* | no custom server-selection hook — see [§8](#8-behavioral-differences-that-will-bite) |
| `retry_on_failed_connect` | `retry_on_failed_connect=False` | **removed from nats-py 2.15**; natsio still has it |
| *(nats-py: n/a)* | `ignore_auth_error_abort=False` | 2-strikes auth abort on reconnect — see [§8](#8-behavioral-differences-that-will-bite) |
| *(nats-py: n/a)* | `reconnect_buf_size=8MB` | dedicated cap for publishes buffered while disconnected |
| *(nats-py: n/a)* | `pending_msgs_limit` / `pending_bytes_limit` | per-connection subscription-queue defaults |
| *(nats-py: n/a)* | `permission_err_on_subscribe=False` | route a sub permission `-ERR` to the subscription — see [§8](#8-behavioral-differences-that-will-bite) |

### Callbacks vs. the events stream

nats-py registers one callback per lifecycle transition. natsio keeps the
`error_cb` (pass it to `connect(error_cb=...)`) but replaces the *other*
callbacks with a single typed async stream:

```python
# nats-py
async def on_disconnect():   print("disconnected")
async def on_reconnect():    print("reconnected")
nc = await nats.connect(disconnected_cb=on_disconnect, reconnected_cb=on_reconnect)

# natsio
import asyncio
from natsio import Disconnected, Reconnected, Closed, LameDuck, ServersDiscovered

async def watch(nc):
    async for event in nc.events():
        match event:
            case Disconnected():        print("disconnected")
            case Reconnected(server_url=url): print("reconnected to", url)
            case LameDuck():            print("server going into lame-duck")
            case Closed():              return   # stream ends after Closed
asyncio.create_task(watch(nc))
```

`error_cb` still works exactly as before for background errors:

```python
async def on_error(err):  # err is a natsio.NATSError
    log.warning("nats background error: %s", err)
nc = await natsio.connect("nats://localhost:4222", error_cb=on_error)
```

### Authentication

| nats-py | natsio |
|---|---|
| `user=`, `password=` | `user=`, `password=` |
| `token=` | `token=` |
| `user_credentials="app.creds"` | `credentials="app.creds"` |
| `nkeys_seed_str="SU..."` | `nkey_seed="SU..."` |
| `user_jwt_cb=` + `signature_cb=` | `authenticator=CallbackAuth(jwt_callback=..., signature_callback=...)` |

natsio also exposes the authenticators directly for full control — `NKeyAuth`,
`CredsAuth` (in-memory JWT+seed), `CredsFileAuth`, `TokenAuth`,
`UserPasswordAuth`, `CallbackAuth` — passed as `authenticator=`. Every
authenticator is re-invoked on each (re)connect, so rotated `.creds` files and
callback-produced tokens are picked up automatically. All flat auth fields
(`user/password/token/nkey_seed/credentials/authenticator`) are mutually
exclusive; supplying two raises `ConfigError`.

---

## 3. Core messaging

### Publish

```python
# nats-py
await nc.publish("subject", b"payload", reply="inbox", headers={"K": "v"})

# natsio — identical shape (payload may be str or bytes)
await nc.publish("subject", b"payload", reply="inbox", headers={"K": "v"})
```

### Subscribe — callback vs. iterator

The single biggest shape change: **`nc.subscribe()` is synchronous in natsio**
(it returns a `Subscription`, not a coroutine). The SUB frame is buffered
immediately, so no message is missed between subscribing and consuming.

```python
# nats-py — subscribe is a coroutine
sub = await nc.subscribe("greet.>")
async for msg in sub.messages:      # .messages property
    print(msg.data)

# natsio — subscribe is NOT awaited; iterate the Subscription directly
sub = nc.subscribe("greet.>")
async for msg in sub:               # the Subscription IS the async iterator
    print(msg.data)
```

Callback subscriptions:

```python
# nats-py
async def handler(msg): ...
await nc.subscribe("greet.>", cb=handler)

# natsio (subscribe still synchronous)
async def handler(msg): ...
nc.subscribe("greet.>", cb=handler)
```

A subscription is either a callback sub **or** an iterator sub, never both.
Iterating a callback sub — or calling `next_msg()` on one — raises
`SubscriptionClosedError` (see [§8](#8-behavioral-differences-that-will-bite)).

### `next_msg`

```python
# nats-py — default timeout 1.0s
msg = await sub.next_msg(timeout=1.0)

# natsio — timeout is required-by-habit (None = wait forever), raises natsio.TimeoutError
msg = await sub.next_msg(timeout=1.0)
```

### Queue groups

```python
# nats-py
await nc.subscribe("work", queue="workers", cb=handler)
# natsio
nc.subscribe("work", queue="workers", cb=handler)
```

### Auto-unsubscribe

```python
# nats-py
await sub.unsubscribe(limit=10)      # stop after 10 messages

# natsio — split into two explicit methods
await sub.unsubscribe()              # stop now, discard queued
await sub.unsubscribe_after(10)      # server stops delivery after 10 total
```

### Request / reply

```python
# nats-py — default timeout 0.5s; supports old_style=True
reply = await nc.request("svc", b"ping", timeout=0.5)

# natsio — default timeout is the connection's request_timeout (5.0s)
reply = await nc.request("svc", b"ping", timeout=2.0)
```

natsio also has **request-many** (ADR-47), which nats-py lacks:

```python
async for reply in nc.request_many("svc", b"ping", max_msgs=5, stall=0.1):
    print(reply.data)
```

### Respond

```python
# both
await msg.respond(b"pong")
```

natsio's `respond` also accepts `headers=`; responding to a message with no
reply subject raises `NoReplySubjectError` (nats-py raises a generic `Error`).

### Draining

```python
# nats-py
await sub.drain()      # drain one subscription
await nc.drain()       # drain the whole connection, then close

# natsio — same names
await sub.drain()      # unbounded on its own; bound it with asyncio.timeout()
await nc.drain()       # bounded by drain_timeout, then always closes
```

### Headers — single-value dict vs. multi-value `Headers`

nats-py headers are a plain `dict[str, str]`: one value per key, last-write-wins.
natsio delivers a **multi-value, case-preserving** `Headers` mapping (it *is* a
`Mapping[str, str]`, so `headers["K"]` and `headers.get("K")` return the *first*
value and existing dict-style code keeps working):

```python
# reading (natsio)
msg.headers["Trace-Id"]          # first value, like a dict
msg.headers.get("Trace-Id")      # first value or None
msg.headers.get_all("Link")      # ['a', 'b'] — every value for the key

# writing — pass a plain dict, or a dict of lists for repeats
await nc.publish("s", b"x", headers={"K": "v"})
await nc.publish("s", b"x", headers={"Link": ["a", "b"]})
```

`msg.headers` is `None` when the message carried no header block (nats-py
likewise gives `None`).

---

## 4. Error mapping

Every natsio exception derives from `natsio.NATSError`. Where it aids `except`
ergonomics, natsio subtrees also mix in the matching builtin — e.g.
`natsio.TimeoutError` is also a `builtins.TimeoutError`, so `except TimeoutError`
(the builtin) catches it.

### Core errors

| nats-py (`nats.errors.*`) | natsio (`natsio.*`) | Notes |
|---|---|---|
| `TimeoutError` | `TimeoutError` | both also subclass `asyncio.TimeoutError` |
| `ConnectionClosedError` | `ConnectionClosedError` | also a `ConnectionError` in natsio |
| `NoServersError` | `NoServersAvailableError` | subclass of `ConnectionClosedError` |
| `NoRespondersError` | `NoRespondersError` | server 503; raised by `request()` |
| `SlowConsumerError` | `SlowConsumerError` | natsio carries `.subject`, `.sid`, `.dropped`; see policies below |
| `StaleConnectionError` | `StaleConnectionError` | |
| `MaxPayloadError` | `MaxPayloadExceededError` | |
| `BadSubjectError` | `ConfigError` | natsio validates subjects **client-side** before send |
| `BadSubscriptionError` | `ConfigError` / `SubscriptionClosedError` | |
| `AuthorizationError` | `AuthorizationViolationError` | |
| *(n/a)* | `AuthenticationExpiredError` | creds expired mid-connection |
| *(n/a)* | `PermissionsViolationError` | subject pub/sub denied (non-fatal) |
| *(n/a)* | `MaxSubscriptionsExceededError` | account sub limit (non-fatal) |
| `ProtocolError` | `ProtocolError` | natsio adds `ParserError`, `MaxControlLineExceededError` |
| `DrainTimeoutError` | `DrainTimeoutError` | subclass of `TimeoutError` in both |
| `FlushTimeoutError` | `TimeoutError` | a flush that times out |
| `ConnectionDrainingError` | `ConnectionClosedError` | natsio surfaces "can't, we're closing" uniformly |
| `ConnectionReconnectingError` | *(buffered)* / `ReconnectBufExceededError` | publishes while disconnected are buffered up to `reconnect_buf_size` |
| `OutboundBufferLimitError` | `ReconnectBufExceededError` | disconnected-publish buffer overflow |
| `InvalidUserCredentialsError` | `ConfigError` / `MissingDependencyError` | bad creds file / missing `[nkeys]` backend |
| `InvalidCallbackTypeError` | *(n/a — typed signatures)* | |
| `MsgAlreadyAckdError` | `MessageAlreadyAckedError` (JetStream) | |
| `NotJSMessageError` | `NotJSMessageError` (JetStream) | |

### JetStream / KV / Object Store errors

| nats-py (`nats.js.errors.*`) | natsio |
|---|---|
| `APIError` | `natsio.jetstream.APIError` (keyed on `err_code`) |
| `NotFoundError` / stream 404 | `StreamNotFoundError` |
| `BadRequestError` | `APIError` |
| `NoStreamResponseError` | `NoStreamResponseError` |
| `ServiceUnavailableError` | `NoRespondersError` / `NoStreamResponseError` |
| `FetchTimeoutError` | empty `fetch()` list, or `NoMessagesError` from `next()` |
| `ConsumerSequenceMismatchError` | *(handled internally by the ordered consumer)* |
| `KeyNotFoundError` | `natsio.kv.KeyNotFoundError` |
| `KeyDeletedError` | `natsio.kv.KeyDeletedError` (subclass of `KeyNotFoundError`) |
| `KeyWrongLastSequenceError` | `natsio.jetstream.WrongLastSequenceError` |
| `NoKeysError` | `keys()` returns `[]`; `history()` raises `KeyNotFoundError` |
| `BucketNotFoundError` | `natsio.kv.BucketNotFoundError` |
| `InvalidBucketNameError` / `InvalidKeyError` | same names (subclass `ConfigError`) |
| `ObjectNotFoundError` | `natsio.objectstore.ObjectNotFoundError` |
| `ObjectDeletedError` | `natsio.objectstore.ObjectDeletedError` |
| `ObjectAlreadyExists` | `natsio.objectstore.ObjectExistsError` |
| `DigestMismatchError` | `natsio.objectstore.DigestMismatchError` |
| `LinkIsABucketError` | `natsio.objectstore.LinkError` |

```python
# nats-py
from nats.js.errors import KeyNotFoundError
try:
    await kv.get("missing")
except KeyNotFoundError:
    ...

# natsio
from natsio.kv import KeyNotFoundError
try:
    await kv.get("missing")
except KeyNotFoundError:
    ...
```

---

## 5. JetStream — the big one

This is where the two clients diverge most. nats-py 2.15 exposes the **legacy**
JetStream surface: push subscriptions (`js.subscribe`), pull subscriptions
(`js.pull_subscribe` + `PullSubscription.fetch`), and an ordered consumer via
`js.subscribe(ordered_consumer=True)`. **natsio implements only the ADR-37
simplified pull API** — `stream.create_consumer(...)` returning a `Consumer` you
`fetch()`, `next()`, or `consume()`, plus a first-class `stream.ordered_consumer()`.

### Getting a context

```python
# both
js = nc.jetstream()
```

natsio's `jetstream()` also takes `domain=`, `api_prefix=`, and the
`publish_async_*` window knobs.

### Publish (with expectations)

nats-py publishes with **plain kwargs for stream/ttl and headers for
expectations**; natsio promotes the ADR-2 publish expectations to **first-class
keyword arguments** (no hand-built `Nats-Expected-*` headers):

```python
# nats-py — expectations go in headers by hand
from nats.js.api import Header  # or literal strings
ack = await js.publish(
    "orders.new", b"...",
    headers={"Nats-Msg-Id": "o-1", "Nats-Expected-Last-Sequence": "7"},
    timeout=2, msg_ttl=60.0,
)

# natsio — expectations are typed kwargs
ack = await js.publish(
    "orders.new", b"...",
    msg_id="o-1",
    expected_last_seq=7,
    ttl=60,          # whole seconds (or "never"); needs stream allow_msg_ttl
    timeout=2,
)
print(ack.seq, ack.stream, ack.duplicate)
```

natsio publish-expectation kwargs: `msg_id`, `expected_stream`,
`expected_last_seq`, `expected_last_subject_seq`,
`expected_last_subject_seq_subject` (server 2.12+), `expected_last_msg_id`.

### Async publish

```python
# nats-py
future = await js.publish_async("s", b"x")   # note: coroutine returning a future
ack = await future
await js.publish_async_completed()

# natsio
future = await js.publish_async("s", b"x")   # also a coroutine returning a future
ack = await future
await js.publish_async_complete()             # note: complete, not completed
print(js.publish_async_pending)               # property (int)
```

natsio caps outstanding acks at `publish_async_max_pending` (default 4000);
when the window is full the call waits `publish_async_stall_wait` then raises
`TooManyStalledMsgsError`. On disconnect, outstanding futures fail with
`ConnectionClosedError` rather than hanging.

### Streams

```python
# nats-py
from nats.js.api import StreamConfig
await js.add_stream(StreamConfig(name="ORDERS", subjects=["orders.>"]))
await js.update_stream(config)
await js.delete_stream("ORDERS")
info = await js.stream_info("ORDERS")
await js.purge_stream("ORDERS", subject="orders.stale")

# natsio
from natsio.jetstream import StreamConfig
stream = await js.create_stream(StreamConfig(name="ORDERS", subjects=["orders.>"]))
await js.update_stream(config)
await js.delete_stream("ORDERS")
info = await js.stream_info("ORDERS")           # or: stream = await js.stream("ORDERS")
await stream.purge(subject="orders.stale")      # or js.purge_stream("ORDERS", ...)
```

`js.create_stream` returns a `Stream` **handle** (with `.cached_info`), which is
the object you create consumers from. natsio also has
`js.streams()` / `js.stream_names()` as async iterators (auto-paged).

### Pull consumers — `pull_subscribe`/`fetch` → `create_consumer`/`fetch`

```python
# nats-py
psub = await js.pull_subscribe("orders.>", durable="worker", stream="ORDERS")
msgs = await psub.fetch(batch=10, timeout=5)
for m in msgs:
    await m.ack()

# natsio
from natsio.jetstream import ConsumerConfig
stream = await js.stream("ORDERS")
consumer = await stream.create_consumer(ConsumerConfig(durable_name="worker"))
msgs = await consumer.fetch(10, timeout=5)      # first arg is max_messages
for m in msgs:
    await m.ack()
```

An empty `fetch()` list is a normal outcome (no messages within the deadline).
For "exactly one, or raise":

```python
# nats-py: fetch(1) then index, or catch FetchTimeoutError
# natsio:
msg = await consumer.next(timeout=5)            # raises NoMessagesError on expiry
```

### Continuous consumption — `consume()`

nats-py has no continuous simplified `consume()` loop; you build your own loop
around repeated `fetch()`, or use a push subscription. natsio gives you a
self-refilling stream:

```python
# natsio
async with consumer.consume(max_messages=500) as messages:
    async for msg in messages:
        await process(msg.data)
        await msg.ack()
```

`consume()` keeps the server topped up to `max_messages` in flight, re-pulling
before the buffer drains. It self-heals across reconnects.

### Push subscriptions — gone; here's what to use instead

nats-py's `js.subscribe(...)` (push, with `manual_ack`, `flow_control`,
`idle_heartbeat`) has **no natsio equivalent** — and that's deliberate. Push
consumers are effectively deprecated upstream: the server team steers everyone
to pull, which has proper flow control, backpressure, and no server-side
delivery state to lose. Replace them:

| nats-py push usage | natsio replacement |
|---|---|
| `js.subscribe("x.>", durable="d", cb=handler)` | durable pull `consumer.consume()` loop |
| `js.subscribe("x.>", ordered_consumer=True)` | `stream.ordered_consumer()` |
| `js.subscribe(..., manual_ack=True)` | pull consumers always give you explicit `msg.ack()` |
| `js.subscribe(..., flow_control=True, idle_heartbeat=...)` | built into `consume()` (heartbeats + pull windowing) |

### Ordered consumer

```python
# nats-py
sub = await js.subscribe("orders.>", ordered_consumer=True)
async for msg in sub.messages:
    print(msg.data)     # no ack needed (ack_none)

# natsio
async with stream.ordered_consumer() as ordered:
    async for msg in ordered.messages(idle_timeout=30):
        print(msg.data)
```

natsio's ordered consumer is ephemeral and self-healing: on a gap, a missed
heartbeat, or consumer loss it silently recreates itself at the next unseen
sequence. `idle_timeout` lets you distinguish a quiet stream from a dead one
(raises `NoMessagesError` instead of self-healing forever).

### Acknowledgement

```python
# nats-py                        # natsio (identical names on JsMsg)
await msg.ack()                  await msg.ack()
await msg.ack_sync()             await msg.ack_sync()          # waits for server confirm
await msg.nak(delay=5)           await msg.nak(delay=5)        # seconds or timedelta
await msg.term()                 await msg.term("reason")      # natsio takes an optional reason
await msg.in_progress()          await msg.in_progress()
```

In natsio, `ack`/`nak`/`term` are **terminal**: sending a second terminal ack
raises `MessageAlreadyAckedError`. `in_progress()` may be sent any number of
times before a terminal ack.

### Consumer management

```python
# nats-py
await js.add_consumer("ORDERS", ConsumerConfig(durable_name="w"))
await js.consumer_info("ORDERS", "w")
await js.delete_consumer("ORDERS", "w")

# natsio — through the Stream handle
consumer = await stream.create_consumer(ConsumerConfig(durable_name="w"))
info = await stream.consumer_info("w")           # or await consumer.info()
await stream.delete_consumer("w")                # or await consumer.delete()
async for name in stream.consumer_names(): ...
```

`ConsumerConfig` fields line up closely (both derive from the JSON API), but
natsio's is **pull-only**: push fields aren't modeled (they round-trip via
`extra` if a foreign consumer carries them).

---

## 6. Key-Value

```python
# nats-py
kv = await js.create_key_value(bucket="config")   # kwargs, or KeyValueConfig
kv = await js.key_value("config")

# natsio
from natsio.kv.entities import KeyValueConfig
kv = await js.create_key_value(KeyValueConfig(bucket="config"))
kv = await js.key_value("config")
```

| Operation | nats-py | natsio |
|---|---|---|
| put | `await kv.put("k", b"v")` → revision | `await kv.put("k", b"v")` → revision (value may be `str`) |
| get | `await kv.get("k")` → `Entry` | `await kv.get("k")` → `KvEntry` |
| get revision | `await kv.get("k", revision=3)` | `await kv.get("k", revision=3)` |
| create | `await kv.create("k", b"v")` | `await kv.create("k", b"v")` → raises `KeyExistsError` |
| update (CAS) | `await kv.update("k", b"v", last=7)` | `await kv.update("k", b"v", last=7)` |
| delete | `await kv.delete("k")` | `await kv.delete("k")` |
| purge | `await kv.purge("k")` | `await kv.purge("k")` |
| purge_deletes | `await kv.purge_deletes(olderthan=1800)` (**seconds**) | `await kv.purge_deletes(older_than=timedelta(minutes=30))` |
| history | `await kv.history("k")` | `await kv.history("k")` → `list[KvEntry]` |
| keys | `await kv.keys()` | `await kv.keys()` → `list[str]` (empty `[]`, never raises) |
| status | `await kv.status()` | `await kv.status()` → `KeyValueStatus` |
| watch | `kv.watch("k")` / `kv.watchall()` | `kv.watch("k")` / `kv.watch()` (no keys = whole bucket) |

`KvEntry` fields: `.key`, `.value` (bytes), `.revision`, `.operation`
(`Operation.PUT` / `DELETE` / `PURGE`), `.created`, `.delta`. Use
`entry.is_marker` to test for a delete/purge tombstone.

### Watchers and the `None` init marker

**Both clients** emit a single `None` sentinel once the current state has been
fully delivered — everything before `None` existed when the watch started;
everything after is a live update. This is verified behavior in both nats-py's
`KeyWatcher` (the "nil marker") and natsio's `KvWatcher`.

```python
# nats-py
w = await kv.watchall()
async for entry in w:
    if entry is None:
        break            # caught up with current state
    print(entry.key, entry.value)

# natsio — same protocol; watcher is an async context manager
async with kv.watch() as w:                 # no args = whole bucket
    async for entry in w:
        if entry is None:
            break        # caught up with current state
        print(entry.key, entry.value)
```

natsio watch options: `include_history`, `updates_only`, `ignore_deletes`,
`meta_only`, `resume_from_revision`. With `updates_only`, the `None` marker
arrives immediately (there is no initial state to replay).

### Per-key TTL

natsio requires the bucket to opt into per-message TTLs before a per-key `ttl`
is accepted (`KeyValueConfig(allow_msg_ttl=True)` or `limit_marker_ttl=`),
otherwise `put(..., ttl=...)` raises `ConfigError` client-side:

```python
kv = await js.create_key_value(KeyValueConfig(bucket="sessions", allow_msg_ttl=True))
await kv.put("token", b"...", ttl=300)      # whole seconds, or "never"
```

There is **no default bucket TTL** in natsio — `ttl=None` means keys never
expire (see [§8](#8-behavioral-differences-that-will-bite)).

---

## 7. Object Store

```python
# nats-py
obj = await js.create_object_store("assets")          # or config
obj = await js.object_store("assets")

# natsio
from natsio.objectstore.entities import ObjectStoreConfig, ObjectMeta
obj = await js.create_object_store(ObjectStoreConfig(bucket="assets"))
obj = await js.object_store("assets")
```

| Operation | nats-py | natsio |
|---|---|---|
| put | `await obj.put("name", data, meta=ObjectMeta(...))` | `await obj.put("name", data)` or `await obj.put(ObjectMeta(name=...), data)` — **meta first** |
| get (buffered) | `res = await obj.get("name")`; `res.data` | `await obj.get_bytes("name")` → bytes |
| get (stream) | `await obj.get("name", writeinto=fh)` | `async with obj.get("name") as res: async for chunk in res: ...` |
| info | `await obj.get_info("name")` | `await obj.info("name")` |
| delete | `await obj.delete("name")` | `await obj.delete("name")` |
| list | `await obj.list()` | `await obj.list()` → `list[ObjectInfo]` |
| watch | `await obj.watch()` | `obj.watch()` (async ctx manager; same `None` marker) |
| update_meta | `await obj.update_meta("name", meta)` | `await obj.update_meta("name", meta)` (also renames) |
| seal | `await obj.seal()` | `await obj.seal()` |
| status | `await obj.status()` | `await obj.status()` → `ObjectStoreStatus` |
| add link | *(not exposed — read-only link follow)* | `await obj.add_link("alias", target_info)` |
| add bucket link | *(not exposed)* | `await obj.add_bucket_link("dir", "other-bucket")` |

Key differences:

- **`put()` argument order is reversed** and takes `ObjectMeta`/name first,
  data second. `data` may be bytes or an **async iterable of byte chunks**;
  natsio re-chunks to `meta.chunk_size` (default 128 KiB) and SHA-256-digests
  as it streams.
- **`get()` streams and verifies.** natsio's `get()` returns an `ObjectResult`
  async-iterator/context-manager; the digest and size are checked after the
  final chunk, so a completed iteration is a *verified* read (mismatch →
  `DigestMismatchError`). Use `get_bytes()` for the whole thing in one buffer.
- **Link creation is a natsio addition.** nats-py 2.15 can *follow* links on
  `get()` but has no public API to create them; natsio adds `add_link` /
  `add_bucket_link`.

```python
# natsio streaming write + verified read
async def chunks():
    yield b"part-1"; yield b"part-2"
info = await obj.put(ObjectMeta(name="big.bin"), chunks())

async with obj.get("big.bin") as result:
    print(result.info.size)
    async for chunk in result:
        sink.write(chunk)
```

---

## 8. Behavioral differences that will bite

Read this section. These are the places where code that "looks migrated" behaves
differently at runtime.

1. **Subjects are validated client-side.** natsio checks subjects, queue groups,
   stream names, consumer names, and KV keys *before* the wire and raises a typed
   `ConfigError` (or `InvalidKeyError`, a `ConfigError` subclass). nats-py mostly
   lets the server reject them as `BadSubjectError`/`-ERR`. If you relied on
   catching `BadSubjectError`, catch `ConfigError` instead.

2. **`subscribe()` is synchronous.** No `await`. It returns a `Subscription`
   immediately (the SUB is buffered). `await nc.subscribe(...)` will fail —
   you'd be awaiting a `Subscription`.

3. **A subscription is callback OR iterator, never both.** Passing `cb=` and then
   iterating raises `SubscriptionClosedError`; so does calling `next_msg()` on a
   callback sub. In nats-py the surfaces overlap more loosely.

4. **URL credentials take precedence over options** (nats.go semantics). If a
   server URL embeds `nats://user:pass@host`, those win over `user=`/`password=`
   passed to `connect()`. nats-py's precedence differs — double-check any URL
   that carries inline credentials.

5. **2-strikes auth abort on reconnect.** By default (`ignore_auth_error_abort=
   False`), two *identical* auth rejections from the same server finalize the
   connection as `Closed` instead of retrying forever against credentials that
   will never work. Set `ignore_auth_error_abort=True` to get the old
   keep-retrying behavior.

6. **No default KV TTL, and per-key TTL must be enabled.** `ttl=None` means
   never expire. A per-key `put(..., ttl=...)` against a bucket that wasn't
   created with `allow_msg_ttl` / `limit_marker_ttl` raises `ConfigError` up
   front (the server would reject it anyway).

7. **Pending-limit policy is explicit.** Each subscription has
   `pending_msgs_limit` / `pending_bytes_limit` (default 65 536 msgs / 64 MiB)
   and a `PendingLimitPolicy`: `DROP_NEW` (default), `DROP_OLD`, `BLOCK`
   (backpressure the socket), or `ERROR` (raise `SlowConsumerError` to the
   consumer). Drops are always counted and reported through `error_cb` /
   `events()` — never silent. nats-py's slow-consumer handling is less
   configurable; if you depended on its exact drop behavior, pick a policy
   deliberately.

   ```python
   from natsio import PendingLimitPolicy
   sub = nc.subscribe("firehose.>", policy=PendingLimitPolicy.BLOCK,
                      pending_msgs_limit=10_000)
   ```

8. **`request()` fails fast when closed.** Calling `request()` on a closing or
   closed connection raises `ConnectionClosedError` immediately rather than
   timing out. In-flight requests fail the same way when the connection drops.

9. **Drain timeouts surface as `DrainTimeoutError`.** `nc.drain()` is bounded by
   `drain_timeout` and *always* closes the connection afterward; if the deadline
   is hit, a `DrainTimeoutError` is reported as a background error (via
   `error_cb`/`events()`) and the client still closes.

10. **No push consumers.** As covered in [§5](#5-jetstream-the-big-one) — port
    `js.subscribe(...)` push flows to `consume()` or `ordered_consumer()`.

11. **No old-style request.** nats-py's `request(..., old_style=True)`
    (one-shot reply subscription per request) does not exist; natsio always uses
    a single muxed reply inbox. This is faster and shouldn't change semantics,
    but there is no toggle.

12. **No custom server-selection hook.** nats-py's
    `reconnect_to_server_handler` has no natsio equivalent — use `servers=(...)`,
    `no_randomize`, and `ignore_discovered_servers` to shape reconnect behavior.

13. **Subscription permission denials.** By default a denied subscription stays
    registered and is re-sent (and re-denied) on every reconnect, surfacing only
    as a background error. Set `permission_err_on_subscribe=True` to *latch* the
    denial (nats.go behavior): iteration / `next_msg()` then raise
    `PermissionsViolationError` and the subscription is terminated.

---

## 9. Performance notes

natsio reaches nats.go-class throughput and matches (or beats) nats-py across
the board, at identical round-trip latency. Representative figures from the
in-repo harness (`tools/natsio-bench`, natsio vs nats-py, same machine and
server): core publish of 16-byte messages **~1.40M vs ~894k msgs/s**; JetStream
`consume()` **~206k vs ~29k msgs/s**; request/reply **~64k vs ~60k req/s** with
the same per-request latency. Run the harness yourself for numbers on your
hardware — see `tools/natsio-bench`.

---

## 10. Quick-reference cheat sheet

| Task | nats-py | natsio |
|---|---|---|
| Connect | `await nats.connect("url", **kw)` | `await natsio.connect("url", **kw)` |
| Lifecycle callbacks | `disconnected_cb=`, `reconnected_cb=`, ... | `error_cb=` + `async for e in nc.events()` |
| Publish | `await nc.publish("s", b"x", headers={...})` | `await nc.publish("s", b"x", headers={...})` |
| Subscribe (iter) | `sub = await nc.subscribe("s")`; `async for m in sub.messages` | `sub = nc.subscribe("s")`; `async for m in sub` |
| Subscribe (cb) | `await nc.subscribe("s", cb=h)` | `nc.subscribe("s", cb=h)` |
| Next message | `await sub.next_msg(timeout=1.0)` | `await sub.next_msg(timeout=1.0)` |
| Queue group | `await nc.subscribe("s", queue="q")` | `nc.subscribe("s", queue="q")` |
| Auto-unsub | `await sub.unsubscribe(limit=n)` | `await sub.unsubscribe_after(n)` |
| Request | `await nc.request("s", b"x", timeout=0.5)` | `await nc.request("s", b"x", timeout=2.0)` |
| Request many | *(n/a)* | `async for r in nc.request_many("s", b"x")` |
| Respond | `await msg.respond(b"y")` | `await msg.respond(b"y", headers=...)` |
| Multi-value header | *(dict, last wins)* | `msg.headers.get_all("K")` |
| Drain sub / conn | `await sub.drain()` / `await nc.drain()` | `await sub.drain()` / `await nc.drain()` |
| Headers | `Dict[str, str]` | `Headers` (multi-value `Mapping`) |
| JS context | `nc.jetstream()` | `nc.jetstream()` |
| Add stream | `await js.add_stream(cfg)` | `await js.create_stream(cfg)` → `Stream` |
| JS publish | `await js.publish("s", b"x", headers={...})` | `await js.publish("s", b"x", msg_id=..., expected_last_seq=...)` |
| Async publish | `await js.publish_async(...)`; `publish_async_completed()` | `await js.publish_async(...)`; `publish_async_complete()` |
| Pull consumer | `await js.pull_subscribe("s", durable="d")` | `await stream.create_consumer(ConsumerConfig(durable_name="d"))` |
| Fetch batch | `await psub.fetch(batch=n, timeout=t)` | `await consumer.fetch(n, timeout=t)` |
| Next (one) | `await psub.fetch(1)` | `await consumer.next(timeout=t)` |
| Continuous | *(manual fetch loop / push)* | `async with consumer.consume() as m: async for x in m` |
| Ordered | `await js.subscribe("s", ordered_consumer=True)` | `async with stream.ordered_consumer() as o: async for x in o.messages()` |
| Ack | `await msg.ack()` / `nak()` / `term()` | `await msg.ack()` / `nak()` / `term()` |
| KV bucket | `await js.create_key_value(bucket="b")` | `await js.create_key_value(KeyValueConfig(bucket="b"))` |
| KV put/get | `await kv.put("k", b"v")` / `await kv.get("k")` | `await kv.put("k", b"v")` / `await kv.get("k")` |
| KV watch | `await kv.watchall()` (yields `None` marker) | `kv.watch()` (yields `None` marker) |
| KV keys | `await kv.keys()` | `await kv.keys()` |
| Obj store | `await js.create_object_store("b")` | `await js.create_object_store(ObjectStoreConfig(bucket="b"))` |
| Obj put | `await obj.put("n", data)` | `await obj.put("n", data)` (or `ObjectMeta` first) |
| Obj get | `res = await obj.get("n"); res.data` | `await obj.get_bytes("n")` / `async with obj.get("n")` |
| Close | `await nc.close()` | `await nc.close()` (or `async with await natsio.connect(...)`) |
```
