# Connection & lifecycle

Everything about the connection *itself*: how to configure it, how it reconnects
when the network drops, how to observe its lifecycle, and how to shut it down
cleanly.

## ConnectOptions

Every connection setting lives on a single frozen, typed `ConnectOptions`
dataclass. You can pass its fields directly to `connect` as keyword arguments,
or build a `ConnectOptions` once and reuse it:

```python
import natsio

# keyword form — the kwargs are a typed mirror of ConnectOptions
nc = await natsio.connect(
    "nats://n1:4222", "nats://n2:4222",
    name="my-service",
    ping_interval=30.0,
)

# object form — build once, reuse
opts = natsio.ConnectOptions(
    servers=("nats://n1:4222", "nats://n2:4222"),
    name="my-service",
    connect_timeout=5.0,
    ping_interval=30.0,
)
nc = await natsio.connect(options=opts)
```

Passing both `options=` and keyword arguments is allowed — the keywords override
individual fields of the object. `ConnectOptions` also has a `.replace(**changes)`
helper that returns a modified copy.

### Servers and connect

| Field | Default | What it does |
|---|---|---|
| `servers` | `("nats://127.0.0.1:4222",)` | The server pool. Positional args to `connect` fill this in. |
| `name` | `None` | Client name reported to the server (shows in monitoring). |
| `connect_timeout` | `5.0` | Per-server deadline for the initial handshake. |
| `no_randomize` | `False` | Try servers in listed order instead of shuffling. |
| `ignore_discovered_servers` | `False` | Ignore extra cluster URLs the server advertises. |
| `echo` | `True` | When `False`, the server won't echo this client's own messages back to its matching subscriptions. |
| `inbox_prefix` | `"_INBOX"` | Prefix for the request/reply reply inboxes. Must be a non-empty subject with no trailing dot. |
| `verbose` / `pedantic` | `False` | Protocol-level `+OK` acks / strict subject checks (rarely needed). |

### Liveness and limits

| Field | Default | What it does |
|---|---|---|
| `ping_interval` | `120.0` | Seconds between client PINGs that keep the link warm. |
| `max_outstanding_pings` | `2` | Unanswered PINGs before the connection is declared stale. |
| `max_payload` | *(server-set)* | Read-only via `nc.max_payload` — the server's cap, enforced client-side before publish. |
| `max_control_line` | `4096` | Max protocol control-line length. |
| `pending_msgs_limit` | `65_536` | Default per-subscription pending message limit. |
| `pending_bytes_limit` | `64 MiB` | Default per-subscription pending byte limit. |
| `max_pending_size` | `2 MiB` | Positive encoded-frame admission limit on the connected publish path. |
| `request_timeout` | `5.0` | Default deadline for `request` / `request_many`. |

The pending limits set the *defaults* for new subscriptions; override them per
subscription with a `PendingLimitPolicy` — see
[backpressure](core-messaging.md#pending-limits-and-backpressure).

## Reconnect

When the connection drops, natsio reconnects automatically and transparently:
subscriptions are replayed, and publishes issued while disconnected are buffered
and flushed on reconnect.

```python
opts = natsio.ConnectOptions(
    servers=("nats://n1:4222", "nats://n2:4222"),
    allow_reconnect=True,        # default; set False to connect exactly once
    max_reconnect_attempts=60,   # consecutive failures PER SERVER; -1 = forever
    reconnect_time_wait=2.0,     # base backoff between attempts
    reconnect_time_wait_max=8.0, # backoff ceiling
    reconnect_jitter=0.1,        # random jitter added to each wait (plaintext)
    reconnect_jitter_tls=1.0,    # larger jitter for TLS reconnects
)
```

### Buffering while disconnected

Publishes issued during a reconnect are buffered up to `reconnect_buf_size` and
replayed once the link is back:

- **default (`8 MiB`)** — buffer up to 8 MiB of disconnected publishes; a
  publish past that raises `ReconnectBufExceededError` (non-fatal — reconnect
  continues, only that publish is rejected).
- **`0`** — use the 8 MiB default.
- **`-1`** — disable buffering entirely; *any* publish while disconnected raises
  `ReconnectBufExceededError` immediately.

```python
opts = natsio.ConnectOptions(reconnect_buf_size=-1)   # fail fast, never buffer
```

### Retry on the very first connect

By default, if the initial connect can't reach any server it raises. Set
`retry_on_failed_connect=True` to instead return a client already in the
`RECONNECTING` state — the first successful connect then fires `Connected`:

```python
nc = await natsio.connect(
    "nats://not-up-yet:4222",
    retry_on_failed_connect=True,
    max_reconnect_attempts=-1,
)
assert nc.status is natsio.ConnectionState.RECONNECTING   # returned, not raised
```

### The 2-strikes auth abort

If reconnect to a server fails with the **same authentication error twice in a
row**, natsio stops trying that path and finalizes the connection `Closed` —
retrying a genuinely-wrong credential forever is pointless. Set
`ignore_auth_error_abort=True` to keep retrying through repeated auth failures
instead.

### Forcing a reconnect

`force_reconnect()` deliberately drops the current transport and reconnects
immediately — bypassing backoff, and not counting the drop as a server failure.
Subscriptions replay and buffered publishes survive; `Disconnected` then
`Reconnected` fire as usual. It is non-blocking: it returns once the drop is
scheduled, not once the link is back.

```python
await nc.force_reconnect()
```

## Lifecycle events

Two channels expose the connection's own state, independent of message traffic.

**`error_cb`** — a callback passed to `connect`, invoked for background errors
not tied to any single caller: a benign server `-ERR`, a slow-consumer report,
a crashed subscription callback. These are informational — the connection keeps
running.

```python
async def on_error(err: natsio.NATSError) -> None:
    log.warning("nats background error: %s", err)

nc = await natsio.connect("nats://localhost:4222", error_cb=on_error)
```

**`events()`** — an async stream of typed lifecycle events. Consume it in a
background task; the stream ends (returns) when the client is closed, so the
task exits on its own at shutdown.

```python
async def watch(nc: natsio.Client) -> None:
    async for event in nc.events():
        match event:
            case natsio.Connected(server_url=url) | natsio.Reconnected(server_url=url):
                print("up:", url)
            case natsio.Disconnected(error=err):
                print("down:", err)
            case natsio.LameDuck(server_url=url):
                print("server going down:", url)
            case natsio.ServersDiscovered(urls=urls):
                print("cluster grew:", urls)
            case natsio.ErrorOccurred(error=err):
                print("background error:", err)
            case natsio.Closed():
                print("closed")

watcher = asyncio.create_task(watch(nc))
```

The event types are `Connected`, `Disconnected`, `Reconnected`, `LameDuck`,
`ServersDiscovered`, `ErrorOccurred`, and `Closed`. `events()` streams are
bounded and drop *oldest* on overflow, so a slow watcher can never stall the
connection.

### Introspection

At any time you can read the connection's live state:

```python
nc.status          # a ConnectionState enum (CONNECTED, RECONNECTING, CLOSED, ...)
nc.is_connected    # bool
nc.connected_url   # the server currently attached to, or None
nc.server_info     # the raw INFO dict from the server
nc.max_payload     # server's max payload in bytes
```

## Connected write admission

`max_pending_size` must be a positive integer. Each connected publish is
admitted only if its **complete encoded PUB/HPUB frame** fits: subject, reply
inbox, headers, protocol framing and payload all count. An individually
oversized frame raises `ConfigError` immediately, even if the server accepts
its payload size. Increase the limit to accommodate the application's largest
encoded frame. This does not change the independent server `max_payload` check.

When an otherwise admissible frame waits for space, `flush_timeout` bounds the
whole admission wait, including repeated wake-ups and competing publishers.
Timeout raises `natsio.TimeoutError`; cancellation propagates and closing or
losing the session wakes waiters. Internal control frames bypass this admission
limit, and disconnected buffering retains its separate `reconnect_buf_size`
policy (`0` selects the default, `-1` disables it). The connected limit has no
unlimited `0`/`-1` mode. Subscription limits still treat non-positive values as
unlimited. Timeout values must be finite and positive; reconnect jitter may be
zero but must be finite and non-negative.

## ClientStatistics

`nc.stats` returns a point-in-time `ClientStatistics` snapshot of the client's
counters:

```python
st = nc.stats
print(st.in_msgs, st.out_msgs, st.in_bytes, st.out_bytes, st.reconnects, st.errors)
```

## Drain vs close

Two teardown paths, and choosing correctly is the whole point:

- **`await nc.close()`** — stop *now*. Pending writes are flushed, but anything
  still queued for a subscription is discarded, and an in-flight `request`
  raises `ConnectionClosedError`. Use it when you are aborting.
- **`await nc.drain()`** — stop *cleanly*. Unsubscribe everything (so no new
  messages arrive), let already-queued messages run through their handlers,
  *then* close. This is what a well-behaved worker does on shutdown. It is
  bounded by `drain_timeout` (default 30s) — it will not hang forever, and the
  client is closed no matter what.

```python
# graceful shutdown of a long-lived worker
try:
    await stop_signal.wait()
finally:
    await nc.drain()
```

Both paths fire the final `Closed` event, which ends any `events()` stream.
Using the client as an `async with` context manager calls `close()` on exit;
call `drain()` explicitly when you need the graceful path.

!!! tip "Signals"
    For a service, wire `loop.add_signal_handler(signal.SIGTERM, stop.set)` and
    `await nc.drain()` on the stop event so SIGINT/SIGTERM drains in-flight work
    instead of dropping it. See `examples/10_graceful_shutdown.py`.

## See also

- [Core messaging](core-messaging.md) — subscriptions and backpressure.
- [Authentication & TLS](auth-tls.md) — the auth-related options.
- [Client API reference](../reference/client.md).
