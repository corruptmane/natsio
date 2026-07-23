"""natsio-kvcodec: key and value codecs for Key-Value buckets (ADR-54).

Key codecs let you store keys that are not subject-legal (slashes, spaces);
value codecs transform the bytes at rest and over the wire (compression,
encryption). Both are transparent — you read and write your original keys and
values, the codec sits in between.

Run it (needs a JetStream server: `just server`):

    python extensions/natsio-kvcodec/examples/basic.py
"""

import asyncio
import contextlib
import os

from natsio.kvcodec import PathKeyCodec, ZlibValueCodec  # ty: ignore[unresolved-import]

import natsio
from natsio.kv import KeyValueConfig


async def main() -> None:
    url = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")
    async with await natsio.connect(url) as nc:
        js = nc.jetstream()

        kv = await js.create_key_value(
            KeyValueConfig(bucket="cfg"),
            key_codec=PathKeyCodec(),  # "/app/db/url" stored on the wire as "app.db.url"
            value_codec=ZlibValueCodec(),  # values compressed at rest and over the wire
        )
        try:
            await kv.put("/app/db/url", b"postgres://localhost:5432/app")
            await kv.put("/app/cache/ttl", b"300")

            entry = await kv.get("/app/db/url")
            print("get('/app/db/url')  ->", entry.value.decode())
            print("entry.key           ->", entry.key)  # decoded back to the original

            # keys() returns DECODED keys, not the on-wire subject tokens.
            print("keys()              ->", sorted(await kv.keys()))

            # A wildcard watch works because PathKeyCodec is filterable.
            print("\nwatch '/app/db/>':")
            watcher = await kv.watch("/app/db/>")
            async for update in watcher:
                if update is None:  # end of the initial replay
                    break
                print(f"  {update.key} = {update.value.decode()}")
            await watcher.stop()
        finally:
            with contextlib.suppress(Exception):
                await js.delete_key_value("cfg")


if __name__ == "__main__":
    asyncio.run(main())
