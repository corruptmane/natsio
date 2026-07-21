"""WebSocket transport: the same NATS protocol over ``ws://``.

NATS can run its protocol over a WebSocket instead of a raw TCP socket — the
frames are wrapped, but the CONNECT/PUB/SUB/MSG conversation above them is
byte-for-byte identical. So *nothing* in your code changes: you pass a
``ws://`` URL to ``connect`` and every verb — publish, subscribe,
request/reply, JetStream — behaves exactly as it does over TCP. That
interchangeability is the whole point of this example.

This script is **self-starting**: a WebSocket listener needs a server config
block (``websocket { ... }``), which a plain ``just server`` does not enable.
So unless you point it at an existing ws endpoint via ``NATS_WS_URL``, it spins
up a throwaway ``nats-server`` on a free port using ``natsio.testing`` — the
first natsio dev extension, which manages real server processes for tests and
demos. (It is a *dev* dependency; a production app never imports it.)

Also shown, in comments and a tiny try/except:

* ``wss://`` + ``TLSConfig`` — encrypted WebSockets reuse the same TLS knobs as
  ``tls://`` (see example 05); only the scheme changes.
* the mixed-scheme guard — a single connection is *either* WebSocket or not;
  mixing ``ws://`` and ``nats://`` in one pool is a ``ConfigError``.

Scope note: natsio v1 speaks the WebSocket framing but does *not* offer
permessage-deflate compression — payloads go over the wire uncompressed.

Run it (no server or env vars needed — it starts its own)::

    python examples/12_websocket.py
    NATS_WS_URL=ws://127.0.0.1:8080 python examples/12_websocket.py   # bring your own
"""

import asyncio
import contextlib
import os
from collections.abc import AsyncIterator

import natsio


@contextlib.asynccontextmanager
async def websocket_endpoint() -> AsyncIterator[str]:
    """Yield a ``ws://`` URL, starting a throwaway server if one isn't provided.

    If ``NATS_WS_URL`` is set we trust it and start nothing. Otherwise we import
    ``natsio.testing`` *here* — lazily, so the import cost (and the dev-only
    dependency) is incurred only when we actually need to self-start — and boot
    a nats-server whose only extra config is a plaintext WebSocket listener.
    """
    preset = os.environ.get("NATS_WS_URL")
    if preset:
        yield preset
        return

    # Dev extension: process management for tests/examples, not for production.
    # (ty can't see the editable-install namespace merge; the runtime import works.)
    from natsio.testing import NatsServerProcess, free_port  # ty: ignore[unresolved-import]

    ws_port = free_port()
    # The minimal config that turns on a ws listener. `no_tls: true` keeps it
    # plaintext (ws://, not wss://) so the example needs no certificates.
    config = f"websocket {{\n  port: {ws_port}\n  no_tls: true\n}}\n"
    server = NatsServerProcess(find_binary(), config=config)
    await server.start()
    try:
        yield f"ws://127.0.0.1:{ws_port}"
    finally:
        await server.stop()


def find_binary() -> str:
    """Locate the nats-server binary: NATS_SERVER_BIN, this repo's tools/.bin, then PATH."""
    from pathlib import Path

    from natsio.testing import find_server_binary  # ty: ignore[unresolved-import]

    # This repo vendors a server under tools/.bin for its own tests; prefer it so
    # the example runs out of the box from a checkout. A standalone user would
    # just have nats-server on PATH (the find_server_binary() fallback).
    local = Path(__file__).resolve().parent.parent / "tools" / ".bin" / "nats-server"
    binary = os.environ.get("NATS_SERVER_BIN") or (str(local) if local.is_file() else None) or find_server_binary()
    if binary is None:
        raise SystemExit(
            "nats-server binary not found. Put it on PATH, set NATS_SERVER_BIN, "
            "or point NATS_WS_URL at a running WebSocket endpoint."
        )
    return binary


async def demo(url: str) -> None:
    """Exercise the ordinary verbs over WebSocket — identical to the TCP path."""
    async with await natsio.connect(url) as nc:
        print(f"connected over WebSocket to {nc.connected_url}")

        # Pub/sub: unchanged. The subscription reads MSG frames that happen to
        # have travelled inside WebSocket frames; the API doesn't know or care.
        async with nc.subscribe("ws.demo") as sub:
            await nc.publish("ws.demo", b"hello over websocket")
            msg = await anext(aiter(sub))
            print(f"  pub/sub: {msg.subject} -> {msg.data.decode()!r}")

        # Request/reply: also unchanged — the reply-inbox muxing is protocol,
        # and the protocol is the same on either transport.
        async def responder(m: natsio.Msg) -> None:
            await m.respond(b"pong:" + m.data)

        sub = nc.subscribe("ws.echo", cb=responder)
        await nc.flush()
        reply = await nc.request("ws.echo", b"ping")
        print(f"  request/reply: ws.echo -> {reply.data.decode()!r}")
        await sub.unsubscribe()


def show_wss_reference() -> None:
    """For a *secure* WebSocket (wss://), wrap a stdlib SSLContext, as with tls://.

    We only build the config object here — there's no wss server running — to
    show that encrypted WebSockets reuse exactly the TLS machinery from
    example 05. The single difference from a plaintext run is the URL scheme.
    """
    import ssl

    ctx = ssl.create_default_context()
    _ = natsio.TLSConfig(context=ctx, hostname="nats.example.com")
    print("wss:// reference: connect('wss://host:443', tls=TLSConfig(context=ssl...)) — same knobs as tls://")


async def show_mixed_scheme_guard() -> None:
    """A pool is all-WebSocket or all-TCP; mixing the two is rejected up front."""
    try:
        await natsio.connect("ws://127.0.0.1:8080", "nats://127.0.0.1:4222")
    except natsio.ConfigError as exc:
        print(f"mixed-scheme guard: ws:// + nats:// in one pool -> ConfigError: {exc}")


async def main() -> None:
    async with websocket_endpoint() as url:
        await demo(url)
    show_wss_reference()
    await show_mixed_scheme_guard()
    print("done")


if __name__ == "__main__":
    asyncio.run(main())
