"""Graceful shutdown: drain vs close, in-flight work, and signal handling.

When a service stops it should not drop the messages it has already accepted.
NATS gives you two teardown paths, and choosing correctly is the whole point of
this example:

* ``close()`` — stop *now*. Pending writes are flushed, but anything still
  queued for a subscription is discarded, and an in-flight ``request`` raises
  :class:`~natsio.ConnectionClosedError`. Use it when you are aborting.
* ``drain()`` — stop *cleanly*. Unsubscribe everything (so no new messages
  arrive), let the already-queued messages run through their handlers, *then*
  close. This is what a well-behaved worker does on shutdown. It is bounded by
  ``drain_timeout`` — it will not hang forever.

The script demonstrates the difference head-on (abrupt close drops queued work;
drain finishes it), then wraps a worker in the real-world shutdown pattern:
``asyncio.Runner`` plus POSIX signal handlers, so Ctrl-C (SIGINT) or SIGTERM
triggers a graceful drain instead of a hard kill.

Run it (start a server first with ``just server``)::

    python examples/10_graceful_shutdown.py
    # then press Ctrl-C to watch the signal-driven drain (or let it self-stop)
"""

import asyncio
import contextlib
import os
import signal

import natsio

NATS_URL = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")


async def contrast_close_vs_drain() -> None:
    """Publish a burst, then tear down two ways to show what each keeps."""
    print("== close() drops queued work; drain() finishes it ==")

    for mode in ("close", "drain"):
        nc = await natsio.connect(NATS_URL)
        completed = 0

        async def worker(_msg: natsio.Msg) -> None:
            nonlocal completed
            await asyncio.sleep(0.02)  # each job takes real time to process
            completed += 1

        nc.subscribe("jobs.work", cb=worker)
        await nc.flush()

        for i in range(20):
            await nc.publish("jobs.work", str(i).encode())
        await nc.flush()  # all 20 are now queued on the subscription

        if mode == "close":
            # Abrupt: whatever the worker has not finished yet is discarded.
            await nc.close()
        else:
            # Graceful: every queued job runs to completion before the close.
            await nc.drain()

        print(f"  {mode:>5}: {completed}/20 jobs completed before teardown")


async def run_worker_until_signal() -> None:
    """A long-lived worker that drains on SIGINT/SIGTERM (the real pattern)."""
    print("== signal-driven graceful shutdown ==")

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    # add_signal_handler is the asyncio-safe way to catch signals: the handler
    # runs on the event loop, so it can touch asyncio objects directly. It is
    # POSIX-only — suppress on platforms (e.g. Windows) that lack it.
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    nc = await natsio.connect(NATS_URL, name="graceful-worker")
    processed = 0

    async def handle(msg: natsio.Msg) -> None:
        nonlocal processed
        await asyncio.sleep(0.05)
        processed += 1
        if msg.reply:
            await msg.respond(b"ok")

    nc.subscribe("tasks.>", cb=handle)
    await nc.flush()

    # Simulate incoming load in the background so there is in-flight work when
    # the shutdown fires.
    async def produce() -> None:
        i = 0
        while not stop.is_set():
            await nc.publish("tasks.do", str(i).encode())
            i += 1
            await asyncio.sleep(0.02)

    producer = asyncio.create_task(produce())

    # So the example terminates unattended, self-trigger the stop after a short
    # while. In a real service you would simply `await stop.wait()` forever and
    # let the operator's Ctrl-C / SIGTERM be the only trigger.
    loop.call_later(1.0, stop.set)

    print("  worker running; waiting for a shutdown signal (auto-stops in ~1s)...")
    await stop.wait()
    print("  shutdown requested -> draining")

    producer.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await producer

    # drain() is the graceful teardown: in-flight handlers finish first.
    await nc.drain()
    print(f"  drained cleanly; {processed} tasks fully processed, none dropped")


async def main() -> None:
    await contrast_close_vs_drain()
    await run_worker_until_signal()
    print("done")


if __name__ == "__main__":
    # asyncio.Runner gives explicit control over the loop's lifetime and pairs
    # naturally with signal handlers — the modern alternative to asyncio.run for
    # a process whose whole job is to run until told to stop.
    with asyncio.Runner() as runner:
        runner.run(main())
