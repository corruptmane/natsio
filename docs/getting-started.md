# Getting started

This page walks one small, coherent program: install natsio, start a server,
connect, publish and subscribe, and do a request/reply round-trip. Every
snippet is a fragment of the same script — assemble them and it runs.

## Install

natsio requires **Python 3.13+** and is tested against **NATS Server 2.14**.

```bash
uv add natsio            # or: pip install natsio
```

The core client has no runtime dependencies. You only need an extra for NKey or
JWT/`.creds` authentication — see [Authentication & TLS](guide/auth-tls.md).

## Start a server

You need a NATS server to talk to. The quickest way is Docker:

```bash
docker run --rm -p 4222:4222 nats:2.14 -js
```

Or download the [`nats-server`](https://github.com/nats-io/nats-server/releases)
binary and run it directly:

```bash
nats-server -js
```

The `-js` flag enables JetStream (persistence, Key-Value, Object Store). Plain
publish/subscribe below works without it, but you will want it for the
[JetStream guide](guide/jetstream.md).

## Connect

`natsio.connect` is a coroutine that returns a ready `Client`. Wrap it in
`async with` so the connection is closed — and its pending writes flushed —
even if the body raises.

```python
import asyncio
import natsio


async def main() -> None:
    async with await natsio.connect("nats://localhost:4222") as nc:
        print(f"connected to {nc.connected_url}")
        # ... everything below runs inside this block ...


if __name__ == "__main__":
    asyncio.run(main())
```

!!! tip "Multiple servers"
    Pass more than one URL for a cluster: `await natsio.connect(url1, url2)`.
    natsio randomizes and fails over between them. All the connection knobs
    live in [`ConnectOptions`](guide/connection.md).

## Publish and subscribe

A NATS subject has **no memory**: a message reaches only the subscriptions that
already exist when it is published. So subscribe *before* you publish.

`subscribe` is deliberately synchronous — the `SUB` frame takes effect
immediately, so nothing can slip through between creating the subscription and
consuming it. Consume it as an async iterator:

```python
async with nc.subscribe("greet.*") as sub:
    await nc.publish("greet.world", b"hello")

    async for msg in sub:
        # msg.data is bytes; msg.subject shows which token matched the wildcard
        print(f"{msg.subject}: {msg.data.decode()}")
        break   # otherwise the loop waits forever for the next message
```

`publish` returns once the frame is buffered locally, not once it is delivered
— it is non-blocking by design. Payloads are `bytes` (a `str` is UTF-8 encoded
for you).

## Request/reply

Request/reply turns NATS into an RPC bus: the requester awaits a single answer,
a responder does the work and calls `respond`. Stand up a responder in callback
mode, then make a request:

```python
async def handler(msg: natsio.Msg) -> None:
    await msg.respond(msg.data.decode().upper().encode())


# a background responder
nc.subscribe("svc.echo", cb=handler)
await nc.flush()   # make sure the SUB reached the server before requesting

reply = await nc.request("svc.echo", b"hello", timeout=2.0)
print(reply.data.decode())   # HELLO
```

Two failure modes are worth distinguishing:

- **`NoRespondersError`** — nobody is subscribed. The server knows instantly and
  the request fails *immediately*, not after the timeout. This fast-fail makes
  request/reply safe as a health probe.
- **`TimeoutError`** — a responder exists but did not answer within the
  deadline.

```python
try:
    await nc.request("svc.missing", b"?", timeout=1.0)
except natsio.NoRespondersError:
    print("nobody listening")     # fires at once
except natsio.TimeoutError:
    print("nobody answered in time")
```

## Where to go next

- **[Core messaging](guide/core-messaging.md)** — subscription modes, queue
  groups, pending limits and backpressure, `request_many`.
- **[Connection & lifecycle](guide/connection.md)** — reconnect, lifecycle
  events, `drain` vs `close`.
- **[Authentication & TLS](guide/auth-tls.md)** — user/pass, token, NKey,
  `.creds`, TLS.
- **[JetStream](guide/jetstream.md)** — durable streams and consumers.
- **[Migrating from nats-py](migration-from-nats-py.md)** — if you are coming
  from the older client.
