"""JetStream consumers: fetch / next / consume, acks, and the ordered view.

A stream stores messages; a *consumer* is a stateful cursor that delivers them
and tracks what you have acknowledged. natsio implements only the ADR-37
"simplified" API — pull-based, three read shapes, no deprecated push/subscribe
workflow:

* ``fetch(n)``   — pull up to n messages, once. Returns what arrived (an empty
                   list is normal). Best for batch/worker loops.
* ``next()``     — pull exactly one (raises ``NoMessagesError`` on timeout).
* ``consume()``  — a continuous, self-refilling stream. Best for daemons.

Each delivered message must be acknowledged, and the ack is a decision:

* ``ack()``  — done, never redeliver.
* ``nak()``  — failed, redeliver (optionally after a delay).
* ``term()`` — poisoned, never redeliver and stop trying.
* ``in_progress()`` — still working; reset the ack-wait timer.

The script also shows consumer CRUD and the **ordered consumer** — an
ephemeral, always-in-order, self-healing view used heavily by KV/Object Store.

Run it (needs a JetStream server: ``just server``)::

    python examples/07_jetstream_consumers.py
"""

import asyncio
import contextlib
import os
from datetime import timedelta

import natsio
from natsio.jetstream import (
    AckPolicy,
    Consumer,
    ConsumerConfig,
    JetStreamContext,
    NoMessagesError,
    Stream,
    StreamConfig,
    StreamNotFoundError,
)

NATS_URL = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")


async def setup_stream(js: JetStreamContext) -> Stream:
    with contextlib.suppress(StreamNotFoundError):
        await js.delete_stream("EVENTS")
    stream = await js.create_stream(StreamConfig(name="EVENTS", subjects=["events.>"]))
    for i in range(10):
        await js.publish("events.log", f"event-{i}".encode())
    return stream


async def consumer_crud(stream: Stream) -> Consumer:
    print("== consumer CRUD ==")
    # A durable_name makes the consumer survive restarts; its ack state lives on
    # the server. Creating with the same name and config again is idempotent.
    consumer = await stream.create_consumer(
        ConsumerConfig(
            durable_name="worker",
            ack_policy=AckPolicy.EXPLICIT,  # every message must be acked
            ack_wait=timedelta(seconds=30),  # redeliver if unacked for 30s
        )
    )
    print(f"  created consumer {consumer.name!r}")

    names = [name async for name in stream.consumer_names()]
    print(f"  stream consumers: {names}")

    info = await consumer.info()
    print(f"  pending={info.num_pending} ack_pending={info.num_ack_pending}")
    return consumer


async def fetch_and_next(consumer: Consumer) -> None:
    print("== fetch() a batch, then next() one ==")
    # fetch pulls up to max_messages within the timeout. Ack each as you go.
    batch = await consumer.fetch(3, timeout=2.0)
    for msg in batch:
        # JsMsg wraps the core message and adds the ack surface. metadata
        # decodes the $JS.ACK reply subject: stream/consumer seq, redelivery
        # count, how many messages remain pending.
        print(f"  fetch: {msg.data.decode()!r} (stream_seq={msg.metadata.stream_seq})")
        await msg.ack()

    # next is fetch(1) with a friendlier signature; it raises on an empty pull.
    try:
        one = await consumer.next(timeout=2.0)
        print(f"  next:  {one.data.decode()!r}")
        await one.ack()
    except NoMessagesError:
        print("  next:  (no message within the deadline)")


async def redelivery_with_nak(js: JetStreamContext, stream: Stream) -> None:
    print("== nak -> redelivery, then ack ==")
    # nak tells the server to redeliver. To make the redelivery unambiguous we
    # use a dedicated consumer over a subject holding exactly ONE message, so
    # the message that comes back after the nak is provably the same one — its
    # delivery count goes from #1 to #2.
    await js.publish("events.retry", b"needs-retry")
    consumer = await stream.create_consumer(
        ConsumerConfig(name="nak-demo", ack_policy=AckPolicy.EXPLICIT, filter_subject="events.retry")
    )
    msg = (await consumer.fetch(1, timeout=2.0))[0]
    print(f"  got {msg.data.decode()!r} delivery #{msg.metadata.num_delivered} -> nak")
    await msg.nak()

    redelivered = (await consumer.fetch(1, timeout=2.0))[0]
    print(f"  same message back, delivery #{redelivered.metadata.num_delivered} -> ack")
    await redelivered.ack()
    await consumer.delete()


async def consume_stream(consumer: Consumer) -> None:
    print("== consume(): continuous, self-refilling ==")
    # consume() keeps a buffer topped up from the server, re-pulling before it
    # drains. idle_heartbeat asks the server for periodic 100-status frames so
    # a silent stream is distinguishable from a dead connection. We stop after
    # draining the remainder so the demo terminates.
    handled = 0
    async with consumer.consume(idle_heartbeat=5.0) as messages:
        while True:
            try:
                # Consumption.next bounds the wait; without a timeout it would
                # block until the next message (correct for a real daemon).
                msg = await messages.next(timeout=1.5)
            except NoMessagesError:
                break  # nothing left within the window: the stream is drained
            print(f"  consume: {msg.data.decode()!r}")
            await msg.ack()
            handled += 1
    print(f"  consumed {handled} more; term() and in_progress() work the same way on any JsMsg")


async def ordered_consumer(stream: Stream) -> None:
    print("== ordered consumer (ephemeral, always in order, self-healing) ==")
    # An ordered consumer needs no name and no acks: it is a read-only,
    # in-order view that recreates itself on any gap or stall. `idle_timeout`
    # bounds the wait per message: on a quiet stream it raises NoMessagesError
    # instead of self-healing forever — that is how a caller tells "drained"
    # from "still coming", so we catch it to end the demo.
    seen = 0
    async with stream.ordered_consumer() as ordered:
        try:
            async for msg in ordered.messages(idle_timeout=1.5):
                seen += 1
                if seen <= 3:
                    print(f"  ordered: {msg.data.decode()!r} (stream_seq={msg.metadata.stream_seq})")
        except NoMessagesError:
            pass  # no new message within idle_timeout: the stream is exhausted
    print(f"  streamed {seen} messages in order, no acks required")


async def main() -> None:
    async with await natsio.connect(NATS_URL) as nc:
        js = nc.jetstream()
        stream = await setup_stream(js)

        consumer = await consumer_crud(stream)
        await fetch_and_next(consumer)
        await redelivery_with_nak(js, stream)
        await consume_stream(consumer)
        await ordered_consumer(stream)

        await consumer.delete()
        await js.delete_stream(stream.name)
        print("cleaned up")


if __name__ == "__main__":
    asyncio.run(main())
