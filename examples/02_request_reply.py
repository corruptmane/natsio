"""Request/reply: a service, a client, and the two ways a request can fail.

Request/reply turns NATS into an RPC bus. The requester publishes to a subject
with a private reply inbox and awaits a single answer; a responder subscribed
to that subject does the work and calls ``respond``. natsio muxes all replies
through one wildcard inbox internally, so a request is just ``await``.

Two failure modes are worth distinguishing, and this script demonstrates both:

* **NoRespondersError** — nobody is subscribed to the subject. The server
  knows this instantly (it tracks interest) and sends a 503, so the request
  fails *immediately* instead of waiting out the timeout. This is the
  fast-fail that makes request/reply safe to use as a health probe.
* **TimeoutError** — a responder exists but did not answer within the
  deadline. Here the client waited the full timeout because interest existed;
  the work simply never completed.

Run it (start a server first with ``just server``)::

    python examples/02_request_reply.py
"""

import asyncio
import os

import natsio

NATS_URL = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")


async def run_service(nc: natsio.Client) -> natsio.Subscription:
    """Subscribe a 'time' service in callback mode and return its handle.

    Callback mode (``cb=``) spawns a background reader task that hands each
    message to the callback, which frees `main` to act as the client. This is
    the natural shape for a long-lived responder.
    """

    async def handle(msg: natsio.Msg) -> None:
        # Echo the request payload back, uppercased, as the reply.
        answer = msg.data.decode().upper().encode()
        await msg.respond(answer)

    sub = nc.subscribe("svc.echo", cb=handle)
    # Flush so the SUB frame has certainly reached the server before we start
    # sending requests to it — otherwise the first request could race the
    # subscription and spuriously get a NoRespondersError.
    await nc.flush()
    return sub


async def main() -> None:
    async with await natsio.connect(NATS_URL) as nc:
        # 1) Fast-fail: no responder on this subject yet. The server replies
        #    503 and natsio raises NoRespondersError right away.
        try:
            await nc.request("svc.echo", b"hello", timeout=2.0)
        except natsio.NoRespondersError:
            print("no responder yet -> NoRespondersError (immediate, not a timeout)")

        # 2) Now stand up the service and make a real request.
        sub = await run_service(nc)
        reply = await nc.request("svc.echo", b"hello", timeout=2.0)
        print(f"request 'hello' -> reply {reply.data.decode()!r}")

        # 3) Timeout: interest exists (a slow responder), but no answer comes
        #    within the deadline. This one waits the full timeout.
        slow = nc.subscribe("svc.slow")  # iterator mode, but we never answer
        await nc.flush()
        try:
            await nc.request("svc.slow", b"...", timeout=0.5)
        except natsio.TimeoutError:
            print("responder exists but never replied -> TimeoutError (waited the deadline)")
        await slow.unsubscribe()

        await sub.unsubscribe()

    print("done")


if __name__ == "__main__":
    asyncio.run(main())
