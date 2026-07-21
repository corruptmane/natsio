"""Subscriptions in depth: modes, queue groups, auto-unsubscribe, backpressure.

A subscription is a bounded delivery queue fed synchronously from the socket
read path. Because that path must never block, everything that could block —
your callback, your ``async for`` body — runs in the consumer's own task,
pulling from the queue. When the queue hits its pending limits, a
:class:`~natsio.PendingLimitPolicy` decides what gives, and every policy is
loud: drops are counted and reported, never silent.

This script walks through:

1. iterator mode vs callback mode — the two ways to consume,
2. queue groups — competing consumers that load-balance a subject,
3. ``unsubscribe_after`` — let the server auto-cancel after N messages,
4. pending limits + a backpressure policy — bound memory under a fast producer,
5. ``drain`` — stop new delivery but finish the backlog first.

Run it (start a server first with ``just server``)::

    python examples/03_subscriptions.py
"""

import asyncio
import os

import natsio

NATS_URL = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")


async def iterator_vs_callback(nc: natsio.Client) -> None:
    print("== iterator mode vs callback mode ==")

    # Iterator mode: no `cb=`. You pull messages yourself with `async for` or
    # `next_msg`. Best when consumption is linear and you want backpressure
    # from simply not iterating.
    async with nc.subscribe("demo.iter") as sub:
        await nc.publish("demo.iter", b"one")
        # `next_msg` is the single-shot pull, with its own timeout.
        msg = await sub.next_msg(timeout=1.0)
        print(f"  iterator: got {msg.data.decode()!r}")

    # Callback mode: pass `cb=`. natsio spawns a reader task that feeds each
    # message to the callback. You cannot also iterate a callback subscription
    # (it raises) — pick one mode per subscription.
    done = asyncio.Event()

    async def on_msg(msg: natsio.Msg) -> None:
        print(f"  callback: got {msg.data.decode()!r}")
        done.set()

    sub = nc.subscribe("demo.cb", cb=on_msg)
    await nc.publish("demo.cb", b"two")
    await done.wait()
    await sub.unsubscribe()


async def queue_groups(nc: natsio.Client) -> None:
    print("== queue groups (load balancing) ==")
    # Subscribers sharing a queue name form a group: the server delivers each
    # message to exactly ONE member, round-robin-ish. This is how you scale a
    # worker pool horizontally — every worker subscribes to the same subject
    # with the same queue.
    counts = {"w1": 0, "w2": 0}

    def make_worker(name: str) -> natsio.Callback:
        async def worker(_msg: natsio.Msg) -> None:
            counts[name] += 1

        return worker

    w1 = nc.subscribe("work.jobs", queue="pool", cb=make_worker("w1"))
    w2 = nc.subscribe("work.jobs", queue="pool", cb=make_worker("w2"))
    await nc.flush()  # ensure both are registered before we publish

    for i in range(10):
        await nc.publish("work.jobs", str(i).encode())
    await nc.flush()
    await asyncio.sleep(0.1)  # let the reader tasks drain the queues

    print(f"  distributed 10 jobs across the group: {counts}")
    await w1.unsubscribe()
    await w2.unsubscribe()


async def unsubscribe_after(nc: natsio.Client) -> None:
    print("== unsubscribe_after (server-side auto-cancel) ==")
    # `unsubscribe_after(n)` sends UNSUB <sid> <n>: the server stops delivering
    # once n TOTAL messages have been sent to this subscription, then the
    # subscription closes itself. Useful for "collect exactly N replies".
    sub = nc.subscribe("demo.limited")
    await sub.unsubscribe_after(3)
    for i in range(5):  # publish more than the limit
        await nc.publish("demo.limited", str(i).encode())

    got = [msg.data.decode() async for msg in sub]  # loop ends when the sub closes
    print(f"  received exactly {len(got)} then the subscription auto-closed: {got}")


async def backpressure(nc: natsio.Client) -> None:
    print("== pending limits + backpressure policy ==")
    # Every subscription has pending-message and pending-byte limits. When they
    # are exceeded, the PendingLimitPolicy chooses the behaviour:
    #   DROP_NEW  (default) — drop the arriving message
    #   DROP_OLD            — evict the oldest queued message (last-value-wins)
    #   BLOCK              — pause the socket until the consumer catches up
    #   ERROR              — fail the subscription with SlowConsumerError
    #
    # Here we set a tiny limit and DROP_NEW, then flood without consuming, so
    # the queue overflows and drops are counted. `sub.dropped` makes the loss
    # observable — that is the "loud" part.
    sub = nc.subscribe(
        "demo.flood",
        pending_msgs_limit=5,
        policy=natsio.PendingLimitPolicy.DROP_NEW,
    )
    for i in range(20):
        await nc.publish("demo.flood", str(i).encode())
    await nc.flush()
    await asyncio.sleep(0.05)  # let delivery settle

    print(f"  pending={sub.pending_msgs} dropped={sub.dropped} (limit was 5, sent 20)")
    await sub.unsubscribe()


async def drain_subscription(nc: natsio.Client) -> None:
    print("== drain (finish the backlog, then close) ==")
    # `unsubscribe()` is abrupt: queued messages are discarded. `drain()` is
    # graceful: it stops NEW delivery (UNSUB now), then waits for the already
    # queued messages to be handled before closing. Use drain for clean
    # shutdown where you must not lose in-flight work.
    handled = 0

    async def slow_handler(_msg: natsio.Msg) -> None:
        nonlocal handled
        await asyncio.sleep(0.01)
        handled += 1

    sub = nc.subscribe("demo.drain", cb=slow_handler)
    for i in range(10):
        await nc.publish("demo.drain", str(i).encode())
    await nc.flush()

    await sub.drain()  # returns only once all 10 have run through the callback
    print(f"  drained: all {handled} queued messages were handled before close")


async def main() -> None:
    async with await natsio.connect(NATS_URL) as nc:
        await iterator_vs_callback(nc)
        await queue_groups(nc)
        await unsubscribe_after(nc)
        await backpressure(nc)
        await drain_subscription(nc)
    print("done")


if __name__ == "__main__":
    asyncio.run(main())
