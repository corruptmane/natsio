"""Object Store: large blobs on JetStream, chunked and digest-verified.

KV is for small values; the Object Store is for arbitrarily large ones. An
object is split into fixed-size chunks (128KiB by default) stored as separate
messages, with one rollup *metadata* message describing it. Reads stream the
chunks back through a self-healing ordered consumer and verify the SHA-256
digest at the end — so a ``get`` that completes is a *verified* read.

This script covers:

* ``put`` — from in-memory bytes and from an async iterable (streaming upload),
* ``get`` streaming — ``async with`` over the chunks, with ``result.info``,
* ``get_bytes`` — the whole object in one buffer,
* ``add_link`` — a pointer to another object, followed transparently on read,
* ``update_meta`` — edit metadata, and rename an object,
* ``watch`` / ``list`` — enumerate the bucket,
* ``seal`` — make the bucket permanently immutable.

Run it (needs a JetStream server: ``just server``)::

    python examples/09_object_store.py
"""

import asyncio
import contextlib
import os
from collections.abc import AsyncIterator

import natsio
from natsio.objectstore import BucketNotFoundError, ObjectMeta, ObjectStoreConfig

NATS_URL = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")


async def chunk_source() -> AsyncIterator[bytes]:
    """A fake streaming upload: yields bytes in pieces, never fully in memory.

    put() re-chunks whatever an async iterable yields into uniform chunks, so
    your producer's slicing does not have to match the stored chunk size.
    """
    for i in range(4):
        yield f"stream-part-{i};".encode()
        await asyncio.sleep(0)  # cooperatively yield, like real I/O would


async def main() -> None:
    async with await natsio.connect(NATS_URL) as nc:
        js = nc.jetstream()

        with contextlib.suppress(BucketNotFoundError):
            await js.delete_object_store("assets")
        obj = await js.create_object_store(ObjectStoreConfig(bucket="assets"))
        print("created object store 'assets'")

        # -- put from bytes -----------------------------------------------------
        # The simplest form: a name and a bytes payload. Returns the stored
        # ObjectInfo (size, chunk count, digest, nuid).
        info = await obj.put("readme.txt", b"hello object store")
        print(f"put readme.txt -> {info.size} bytes in {info.chunks} chunk(s)")

        # -- put from an async iterable (streaming) ----------------------------
        # Pass an ObjectMeta to attach description/metadata; data can be an
        # async iterable so you never hold the whole object in memory.
        streamed = await obj.put(
            ObjectMeta(name="upload.bin", description="a streamed upload"),
            chunk_source(),
        )
        print(f"put upload.bin (streamed) -> {streamed.size} bytes")

        # -- get streaming: async-with over chunks -----------------------------
        # The context manager guarantees the ephemeral read consumer is torn
        # down. result.info is the (link-resolved) metadata; iterating yields
        # the raw chunks, and the digest is verified after the last one.
        async with obj.get("upload.bin") as result:
            print(f"streaming upload.bin ({result.info.size} bytes):")
            collected = bytearray()
            async for chunk in result:
                collected += chunk
            print(f"  reassembled {len(collected)} bytes: {bytes(collected).decode()!r}")

        # -- get_bytes: whole object in one call -------------------------------
        data = await obj.get_bytes("readme.txt")
        print(f"get_bytes(readme.txt) -> {data.decode()!r}")

        # -- links --------------------------------------------------------------
        # A link is a named pointer to another object. get() on the link
        # transparently streams the target's data.
        await obj.add_link("readme.link", info)
        via_link = await obj.get_bytes("readme.link")
        print(f"get_bytes(readme.link) follows the link -> {via_link.decode()!r}")

        # -- update_meta: edit metadata, and rename ----------------------------
        # Changing meta.name renames the object: same data (same chunks), new
        # name. The old name then 404s.
        await obj.update_meta("readme.txt", ObjectMeta(name="README.md", description="renamed"))
        names = sorted(o.name for o in await obj.list())
        print(f"after rename readme.txt -> README.md, objects: {names}")

        # -- watch / list -------------------------------------------------------
        # watch replays current state, yields a single None "caught up" marker,
        # then streams live changes — identical shape to the KV watcher.
        async with obj.watch() as watcher:
            initial: list[str] = []
            async for item in watcher:
                if item is None:
                    break
                initial.append(item.name)
            print(f"watch initial state: {sorted(initial)}")

        # -- seal: make the bucket immutable -----------------------------------
        # After seal there are no more puts, deletes, or purges — ever. Only do
        # this with no writers in flight.
        await obj.seal()
        status = await obj.status()
        print(f"sealed: {status.sealed} (bucket is now read-only)")

        # A delete_object_store still works on a sealed bucket (it drops the
        # whole backing stream), so we can clean up.
        await js.delete_object_store("assets")
        print("deleted object store")


if __name__ == "__main__":
    asyncio.run(main())
