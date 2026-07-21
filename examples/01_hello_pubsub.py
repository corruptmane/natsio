"""Hello, NATS: connect, subscribe, publish, respond.

The smallest useful NATS program. It shows the four verbs you will use in
almost every script — connect, subscribe, publish, respond — and the two
idioms natsio leans on everywhere: ``async with`` for lifecycle and
``async for`` for consuming a subscription.

Why subscribe *before* publish? A NATS subject has no memory: a message is
delivered only to subscriptions that already exist when it is published. Core
NATS is fire-and-forget (JetStream, in later examples, is the durable story).
So the ordering below is not incidental — it is the whole contract.

Run it (start a server first with ``just server``)::

    python examples/01_hello_pubsub.py
    NATS_URL=nats://127.0.0.1:4222 python examples/01_hello_pubsub.py
"""

import asyncio
import os

import natsio

NATS_URL = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")


async def main() -> None:
    # `connect` is a coroutine that returns a ready Client; wrapping it in
    # `async with` guarantees the connection is closed (and its writes flushed)
    # even if the body raises.
    async with await natsio.connect(NATS_URL) as nc:
        print(f"connected to {nc.connected_url}")

        # `subscribe` is deliberately synchronous — the SUB frame is buffered
        # and takes effect immediately, so no message can slip through between
        # here and the first `async for`. The `async with` unsubscribes on exit.
        async with nc.subscribe("greet.*") as sub:
            # Publish two messages. `publish` returns once the frame is
            # buffered locally, not once it is delivered — it is non-blocking
            # by design. The second one carries a reply subject so the
            # subscriber can answer it.
            await nc.publish("greet.world", b"hello")
            await nc.publish("greet.ping", b"anyone there?", reply="_INBOX.demo")

            # Iterate the subscription like any async stream. We know exactly
            # two messages are coming, so we count and break — otherwise this
            # loop would wait forever for the next one.
            received = 0
            async for msg in sub:
                # `msg.data` is an alias for `msg.payload`; both are `bytes`.
                # A message on `greet.*` binds the trailing token, so
                # `msg.subject` tells us which one arrived.
                print(f"  <- {msg.subject}: {msg.data.decode()}")

                # Only messages published with a reply subject can be answered.
                if msg.reply:
                    await msg.respond(b"pong")
                    print(f"  -> replied on {msg.reply}")

                received += 1
                if received == 2:
                    break

    print("connection closed cleanly")


if __name__ == "__main__":
    asyncio.run(main())
