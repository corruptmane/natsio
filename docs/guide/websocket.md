# WebSocket

NATS normally speaks its own protocol over a raw TCP socket. The **WebSocket
transport** carries that exact protocol inside RFC 6455 frames instead — same
client, same JetStream, same everything, reachable from places a bare TCP port is
not.

Reach for it when the network, not the application, dictates the transport:

- **Browser-adjacent infrastructure** — edge gateways, proxies, and load
  balancers that terminate HTTP/WebSocket but not arbitrary TCP.
- **Restrictive firewalls** — environments where only 443 is open outbound. `wss`
  looks exactly like HTTPS on the wire.
- **Shared ingress** — running NATS behind the same TLS-terminating front door as
  your web traffic.

If you control both ends and the network is open, plain `nats://` is simpler and
slightly leaner. WebSocket is the tool for when it is not.

## Connecting

The scheme is the entire opt-in. `ws://` for plaintext, `wss://` for TLS:

```python
import natsio

async with await natsio.connect("ws://localhost:8080") as nc:
    await nc.publish("greet", b"hello over websocket")
```

WebSocket listeners run on their own port — commonly 8080 for `ws` or 443 for
`wss` behind an ingress — so include it explicitly; there is no implied default.
A path and query string are honored (`ws://host:8080/nats`) for gateways that
route by path.

### TLS (`wss`)

WebSocket TLS is **transport-level**: the socket is wrapped with TLS *before* the
HTTP `Upgrade` request is sent, exactly like HTTPS. This is unlike `nats://` +
`tls`, where NATS negotiates an in-band upgrade after the server's `INFO`. There
is no STARTTLS-style handshake to interpose, so a `wss` connection configures TLS
the same way any TLS client does — through `TLSConfig`:

```python
import ssl
from natsio.options import TLSConfig

context = ssl.create_default_context(cafile="ca.pem")
async with await natsio.connect(
    "wss://nats.example.com:443",
    tls=TLSConfig(context=context, hostname="nats.example.com"),
) as nc:
    ...
```

With a publicly-trusted certificate you can omit `TLSConfig` entirely — the
default context validates against the system trust store. See
[Authentication & TLS](auth-tls.md) for the full `TLSConfig` surface.

### Server configuration

The server needs a `websocket { }` block. A minimal plaintext listener (for
local development or behind a TLS-terminating proxy):

```
websocket {
  host: "0.0.0.0"
  port: 8080
  no_tls: true
}
```

A TLS listener terminates `wss` at the server itself:

```
websocket {
  host: "0.0.0.0"
  port: 443
  tls {
    cert_file: "server-cert.pem"
    key_file:  "server-key.pem"
  }
}
```

## Everything else is identical

The transport is the *only* thing that changes. Once connected, there is no
WebSocket-specific API: subscriptions, request/reply, JetStream, Key-Value, and
Object Store all work exactly as over TCP.

```python
async with await natsio.connect("ws://localhost:8080") as nc:
    js = nc.jetstream()
    await js.create_stream(StreamConfig(name="ORDERS", subjects=["orders.>"]))
    await js.publish("orders.new", b'{"id": 1}')

    kv = await js.create_key_value(KeyValueConfig(bucket="config"))
    await kv.put("theme", b"dark")
```

Reconnection is identical too. If the connection drops, natsio re-runs the
WebSocket handshake and resubscribes transparently, with the same backoff,
jitter, and events as a TCP reconnect — see
[Connection & lifecycle](connection.md). A server-sent close frame is treated as
an ordinary connection loss, so it triggers reconnect like any TCP EOF.

!!! warning "Don't mix schemes in one pool"
    Every server in a connection is either WebSocket or not — the whole pool
    shares one transport, and gossiped cluster URLs inherit it. Mixing `ws`/`wss`
    with `nats`/`tls` in `servers=` raises `ConfigError` up front (parity with
    nats.go's `ErrMixingWebsocketSchemes`). Run a WebSocket-only or TCP-only pool.

## The transport itself

The RFC 6455 layer is in-house and sans-io — no `websockets`, no `aiohttp`, no
third-party dependency at all. It is the same engineering as the NATS protocol
core: a pull-parser fed opaque bytes, **chunk-boundary tested** so the decoded
frame sequence is invariant under any split of the byte stream, with framing
violations fatal (a WebSocket stream cannot be resynchronized mid-frame). Every
outbound NATS write becomes exactly one masked binary frame; inbound frame
payloads are handed straight to the NATS parser, which does its own message
framing. Server pings are answered transparently.

A declared frame length is capped at **64 MiB** — a hostile or broken server
announcing a giant frame gets a typed teardown, not unbounded buffering (the same
ceiling nats.go uses). Legitimate frames sit far below it, bounded by the
server's `max_payload`.

!!! note "v1 scope"
    The transport declines `permessage-deflate` compression (a deliberate
    simplicity choice — it keeps the codec allocation-light and branch-free), and
    does not yet support HTTP proxies or custom handshake headers. The connection
    path is `ws://host:port/path`; that is the whole configuration surface for v1.

## See also

- [Connection & lifecycle](connection.md) — reconnect behavior, events, and the
  server pool that WebSocket rides on.
- [Authentication & TLS](auth-tls.md) — the `TLSConfig` used by `wss`.
