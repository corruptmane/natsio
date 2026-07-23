"""natsio-pcgroups: partitioned consumer groups (static).

A partitioned group spreads a stream's messages across members by a partition
token in the subject, so each message is processed by exactly one member and a
single subject's ordering is preserved. This shows the *static* flavour: the
partition number is already the first subject token (via a stream transform).

Run it (needs a JetStream server 2.11+: `just server`):

    python extensions/natsio-pcgroups/examples/basic.py
"""

import asyncio
import contextlib
import os

from natsio.pcgroups import (  # ty: ignore[unresolved-import]
    PartitionedMsg,
    create_static,
    delete_static,
    static_consume,
)

import natsio
from natsio.jetstream import AckPolicy, ConsumerConfig, StreamConfig, SubjectTransform


async def main() -> None:
    url = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")
    async with await natsio.connect(url) as nc:
        js = nc.jetstream()

        # Partition "orders.<id>" over 4 partitions on ingest: the transform
        # prepends a partition token the group filters on.
        await js.create_stream(
            StreamConfig(
                name="ORDERS",
                subjects=["orders.*"],
                subject_transform=SubjectTransform(src="orders.*", dest="{{partition(4,1)}}.orders.{{wildcard(1)}}"),
            )
        )
        try:
            # A single member owns all 4 partitions here (max_members=4, members=["m1"]).
            await create_static(js, "ORDERS", "cg", 4, ["orders.*"], ["m1"])

            received: list[str] = []
            done = asyncio.Event()

            async def handle(msg: PartitionedMsg) -> None:
                received.append(msg.subject)  # "orders.42" — the partition token is stripped
                await msg.ack()
                if len(received) >= 6:
                    done.set()

            context = await static_consume(
                js,
                "ORDERS",
                "cg",
                "m1",
                handle,
                ConsumerConfig(ack_policy=AckPolicy.EXPLICIT, max_ack_pending=1),
            )
            try:
                for i in range(6):
                    await js.publish(f"orders.{i}", f"order {i}".encode())
                async with asyncio.timeout(15):
                    await done.wait()
                print("member m1 handled:", sorted(received))
            finally:
                await context.stop()
                await delete_static(js, "ORDERS", "cg")
        finally:
            with contextlib.suppress(Exception):
                await js.delete_stream("ORDERS")


if __name__ == "__main__":
    asyncio.run(main())
