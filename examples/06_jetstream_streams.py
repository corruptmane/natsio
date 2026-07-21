"""JetStream streams: create, publish with guarantees, and the async window.

Core NATS is fire-and-forget. JetStream adds persistence: a *stream* captures
messages on its subjects and stores them, and every publish is acknowledged
with a :class:`~natsio.jetstream.PubAck` carrying the assigned sequence. That
ack is what lets JetStream offer guarantees core NATS cannot:

* **dedup** — tag a publish with ``msg_id`` and the server drops duplicates
  seen inside the stream's duplicate window (at-least-once becomes
  effectively-once for retries).
* **optimistic concurrency** — ``expected_last_seq`` / ``expected_last_msg_id``
  make a publish conditional on the stream's current state; a violation raises
  :class:`~natsio.jetstream.WrongLastSequenceError` instead of appending.

Publishing one-ack-at-a-time is simple but serial. For throughput, the
**async publish window** fires many publishes without awaiting each ack, then
waits for them all with ``publish_async_complete`` — the pattern this script
finishes on.

Run it (needs a JetStream server: ``just server``)::

    python examples/06_jetstream_streams.py
"""

import asyncio
import contextlib
import os

import natsio
from natsio.jetstream import (
    RetentionPolicy,
    StreamConfig,
    StreamNotFoundError,
    WrongLastSequenceError,
)

NATS_URL = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")


async def main() -> None:
    async with await natsio.connect(NATS_URL) as nc:
        # The JetStream context is a lightweight view over the connection; it
        # talks to the server's `$JS.API` control plane.
        js = nc.jetstream()

        # Create a stream capturing everything on `orders.>`. Re-running this
        # script would collide, so delete any leftover first (idempotent setup).
        with_cleanup = "ORDERS"
        with contextlib.suppress(StreamNotFoundError):
            await js.delete_stream(with_cleanup)

        stream = await js.create_stream(
            StreamConfig(
                name=with_cleanup,
                subjects=["orders.>"],
                retention=RetentionPolicy.LIMITS,  # keep by age/size limits (the default)
                max_msgs=1000,
            )
        )
        print(f"created stream {stream.name!r}")

        # -- publish with an ack ------------------------------------------------
        ack = await js.publish("orders.new", b'{"id": 1}')
        print(f"published -> stream={ack.stream} seq={ack.seq}")

        # -- dedup via msg_id ---------------------------------------------------
        # Publish the same logical message twice with the same msg_id. The
        # second is recognised as a duplicate: same sequence, duplicate=True,
        # nothing new is stored.
        a1 = await js.publish("orders.new", b'{"id": 2}', msg_id="order-2")
        a2 = await js.publish("orders.new", b'{"id": 2}', msg_id="order-2")
        print(f"dedup: first seq={a1.seq}, retry seq={a2.seq} duplicate={a2.duplicate}")

        # -- publish expectations (optimistic concurrency) ---------------------
        # Assert the stream's last sequence before appending. If another writer
        # has moved it, the publish is rejected instead of racing.
        info = await stream.info()
        last = info.state.last_seq
        good = await js.publish("orders.new", b'{"id": 3}', expected_last_seq=last)
        print(f"conditional publish succeeded at seq={good.seq}")
        try:
            # `last` is now stale, so this expectation must fail.
            await js.publish("orders.new", b'{"id": 4}', expected_last_seq=last)
        except WrongLastSequenceError as exc:
            print(f"stale expectation correctly rejected: {exc}")

        # -- async publish window (throughput) ---------------------------------
        # Fire many publishes without awaiting each ack. Each returns a future;
        # the window caps how many acks may be outstanding at once. Then wait
        # for the whole batch with publish_async_complete.
        futures = [js.publish_async("orders.batch", f"item-{i}".encode()) for i in range(50)]
        pending = [await f for f in futures]  # kick off the sends
        print(f"async: {js.publish_async_pending} acks still outstanding")
        await js.publish_async_complete(timeout=10.0)
        acked = [fut.result() for fut in pending]
        print(f"async: all {len(acked)} publishes acked (last seq={acked[-1].seq})")

        # -- stream introspection + purge --------------------------------------
        info = await stream.info()
        print(f"stream now holds {info.state.messages} messages (seq {info.state.first_seq}..{info.state.last_seq})")

        purged = await stream.purge(subject="orders.batch")
        print(f"purged {purged} messages on orders.batch")
        info = await stream.info()
        print(f"after purge: {info.state.messages} messages remain")

        await js.delete_stream(stream.name)
        print(f"deleted stream {stream.name!r}")


if __name__ == "__main__":
    asyncio.run(main())
