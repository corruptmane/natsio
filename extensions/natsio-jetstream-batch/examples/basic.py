"""natsio-jetstream-batch: fast-ingest batch publishing + batch reads (2.14).

Fast ingest streams many messages into one server-side batch with flow-control
acks instead of one ack per message, then commits the batch atomically. Batch
Direct Get reads a run of messages back by sequence in one request.

Run it (needs a JetStream server 2.14+: `just server`):

    python extensions/natsio-jetstream-batch/examples/basic.py
"""

import asyncio
import contextlib
import os

from natsio.jetstream_batch import ALLOW_BATCHED, fast_publisher, get_batch  # ty: ignore[unresolved-import]

import natsio
from natsio.jetstream import StreamConfig


async def main() -> None:
    url = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")
    async with await natsio.connect(url) as nc:
        js = nc.jetstream()

        # Fast ingest needs allow_batched. Core's StreamConfig doesn't model the
        # 2.14 field yet, so it rides in `extra` (which round-trips untouched).
        stream = await js.create_stream(
            StreamConfig(name="RAW", subjects=["raw.>"], allow_direct=True, extra={ALLOW_BATCHED: True})
        )
        try:
            # --- publish: one batch of 10,000 messages, one atomic commit ------
            async with fast_publisher(js) as fp:
                for i in range(10_000):
                    await fp.add("raw.events", f"event {i}".encode())
                final = await fp.commit("raw.events", b"done")
            print(f"committed batch {final.batch_id}: stream={final.stream} seq={final.seq} size={final.size}")

            info = await stream.info()
            print("stream now holds", info.state.messages, "messages")

            # --- read: pull the first 5 back in one batch Direct Get -----------
            print("\nfirst 5 messages via get_batch:")
            async for msg in get_batch(js, stream, 5):
                print(f"  seq={msg.seq} {msg.payload!r}")
        finally:
            with contextlib.suppress(Exception):
                await js.delete_stream("RAW")


if __name__ == "__main__":
    asyncio.run(main())
