"""natsio-schedules: server-side message schedules (ADR-51).

A schedule is a definition stored in a stream that makes the server publish a
message on a subject at a time you describe — one-shot (`@at`/`after`),
recurring (`@every`), or cron. The generated message carries scheduler headers
that `delivery_info()` reads back.

Run it (needs a JetStream server 2.14+: `just server`):

    python extensions/natsio-schedules/examples/basic.py
"""

import asyncio
import contextlib
import os
from datetime import timedelta

from natsio.schedules import (  # ty: ignore[unresolved-import]
    ScheduleStreamConfig,
    after,
    create_schedule_stream,
    delivery_info,
    every,
)

import natsio
from natsio.jetstream import ConsumerConfig


async def main() -> None:
    url = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")
    async with await natsio.connect(url) as nc:
        js = nc.jetstream()

        # `subjects` must cover the schedule subjects AND every target subject.
        sched = await create_schedule_stream(
            js, ScheduleStreamConfig(name="SCHED", subjects=["schedules.>", "orders.>"])
        )
        try:
            # A consumer over the generated (target) messages, so we can see them fire.
            target = await sched.stream.create_consumer(ConsumerConfig(name="TARGETS", filter_subject="orders.>"))

            # One-shot: fire ~1s from now, then the definition self-destructs.
            await sched.create("schedules.soon", after(timedelta(seconds=1)), target="orders.reminder", payload=b"ping")
            # Recurring: every second, each generated message gets a 5-minute TTL.
            await sched.create("schedules.tick", every(timedelta(seconds=1)), target="orders.tick", ttl="5m")

            print("waiting for scheduled deliveries...\n")
            for _ in range(3):
                msg = await target.next(timeout=10.0)
                await msg.ack()
                info = delivery_info(msg)
                assert info is not None
                print(
                    f"  fired {msg.subject!r} payload={msg.data!r} "
                    f"scheduler={info.scheduler!r} final={info.final} next_run={info.next_run}"
                )

            # The recurring definition survives its firings; list what remains.
            print("\nlive schedule definitions:")
            async for entry in sched.list():
                print(f"  {entry.subject}: {entry.schedule} -> {entry.target}")
        finally:
            with contextlib.suppress(Exception):
                await js.delete_stream("SCHED")


if __name__ == "__main__":
    asyncio.run(main())
