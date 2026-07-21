"""Connection lifecycle: events, the error callback, and reconnect behaviour.

A production client has to reason about the connection *itself*, not just
messages: Did we drop? Did we come back? Was there a benign background error?
natsio exposes this two ways, and this script uses both:

* **error_cb** — a callback passed to ``connect``, invoked for background
  errors that aren't tied to any single caller (a benign server ``-ERR``, a
  slow-consumer report, a crashed subscription callback).
* **events()** — an async stream of typed lifecycle events: ``Connected``,
  ``Disconnected``, ``Reconnected``, ``LameDuck``, ``ServersDiscovered``,
  ``ErrorOccurred``, ``Closed``. Consume it in a background task.

It then triggers a real reconnect with ``force_reconnect()`` so you can watch
``Disconnected`` -> ``Reconnected`` actually fire, and closes on the two clean
shutdown paths (``close`` vs ``drain``). The reconnect *tuning* knobs are
documented inline — they are all ``connect`` options.

Run it (start a server first with ``just server``)::

    python examples/04_lifecycle_events.py
"""

import asyncio
import os

import natsio

NATS_URL = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")


async def main() -> None:
    async def on_error(err: natsio.NATSError) -> None:
        # Background errors surface here. In real code you would log these;
        # they are informational, not fatal (the connection keeps running).
        print(f"  [error_cb] {type(err).__name__}: {err}")

    # A few of the reconnect knobs, shown as a config object for documentation.
    # Passing them here is equivalent to `connect(NATS_URL, allow_reconnect=...)`.
    options = natsio.ConnectOptions(
        servers=(NATS_URL,),
        name="lifecycle-demo",
        allow_reconnect=True,  # reconnect automatically on connection loss
        max_reconnect_attempts=60,  # consecutive failures per server before giving up; -1 = forever
        reconnect_time_wait=2.0,  # base backoff between attempts
        # Publishes issued while disconnected are buffered up to this many bytes
        # and flushed on reconnect. 0 -> 8MB default; -1 -> disable buffering
        # (a disconnected publish then raises ReconnectBufExceededError at once).
        reconnect_buf_size=8 * 1024 * 1024,
        # Return a client in RECONNECTING instead of raising if the *initial*
        # connect can't reach any server; the first success fires Connected.
        # (Left False here so a truly-down server fails fast in this demo.)
        retry_on_failed_connect=False,
    )

    nc = await natsio.connect(options=options, error_cb=on_error)

    # Consume lifecycle events in the background. events() ends (returns) when
    # the client is closed, so this task exits on its own at shutdown.
    seen: list[str] = []

    async def watch_events() -> None:
        async for event in nc.events():
            seen.append(type(event).__name__)
            match event:
                case natsio.Connected(server_url=url) | natsio.Reconnected(server_url=url):
                    print(f"  [event] {type(event).__name__} -> {url}")
                case natsio.Disconnected(error=err):
                    print(f"  [event] Disconnected (error={err})")
                case natsio.LameDuck(server_url=url):
                    print(f"  [event] LameDuck (server going down) -> {url}")
                case _:
                    print(f"  [event] {type(event).__name__}")

    watcher = asyncio.create_task(watch_events())
    await asyncio.sleep(0.05)  # let the watcher attach

    print(f"status={nc.status.value} connected_url={nc.connected_url}")

    # force_reconnect deliberately drops the current transport and reconnects
    # immediately (bypassing backoff, not counted as a server failure). It is
    # the clean way to demonstrate — or trigger — a reconnect. It returns once
    # the drop is scheduled, so we wait for the client to report CONNECTED again.
    print("forcing a reconnect...")
    await nc.force_reconnect()
    for _ in range(50):
        await asyncio.sleep(0.1)
        if nc.is_connected and "Reconnected" in seen:
            break
    print(f"back up: status={nc.status.value}")

    # Two clean shutdowns:
    #   drain() — unsubscribe everything, let queued messages be handled, then
    #             close. Graceful; bounded by drain_timeout.
    #   close() — close immediately: flush pending writes, drop what's queued.
    # We use drain() here; both fire the final Closed event, which ends events().
    print("draining...")
    await nc.drain()
    await watcher  # the watcher task finishes when Closed is delivered

    print(f"lifecycle seen: {seen}")


if __name__ == "__main__":
    asyncio.run(main())
