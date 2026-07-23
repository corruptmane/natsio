"""natsio-counters: distributed counters over JetStream (ADR-49).

A counter stream turns messages into an atomic running total per subject —
increment/decrement with a delta, read the value straight from the server's
ack. No read-modify-write races: the server does the addition.

Run it (needs a JetStream server: `just server`):

    python extensions/natsio-counters/examples/basic.py
"""

import asyncio
import contextlib
import os

from natsio.counters import CounterConfig, create_counter, get_counter  # ty: ignore[unresolved-import]

import natsio


async def main() -> None:
    url = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")
    async with await natsio.connect(url) as nc:
        js = nc.jetstream()

        # allow_msg_counter + allow_direct are forced on for a counter stream.
        counter = await create_counter(js, CounterConfig(name="COUNTS", subjects=["events.>"]))
        try:
            # add() returns the new running total, straight from the PubAck.
            print("orders +1  ->", await counter.add("events.orders", 1))
            print("orders +10 ->", await counter.add("events.orders", 10))
            print("orders -4  ->", await counter.add("events.orders", -4))  # negatives are fine
            print("signups +1 ->", await counter.add("events.signups", 1))

            # Read one counter's current value.
            print("\nload(events.orders)  ->", await counter.load("events.orders"))

            # Full entry: value + most-recent increment.
            entry = await counter.get("events.orders")
            print("get(events.orders)   -> value:", entry.value, "last incr:", entry.incr)

            # Enumerate many counters in one batch Direct Get (wildcards allowed).
            print("\nall events.* counters:")
            async for e in counter.get_multiple(["events.>"]):
                print(f"  {e.subject} = {e.value}")

            # Binding an existing counter is a no-op round trip.
            same = await get_counter(js, "COUNTS")
            print("\nre-bound COUNTS, orders =", await same.load("events.orders"))
        finally:
            with contextlib.suppress(Exception):
                await js.delete_stream("COUNTS")


if __name__ == "__main__":
    asyncio.run(main())
