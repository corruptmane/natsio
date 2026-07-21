"""Key-Value: a versioned map built on JetStream.

A KV bucket is a JetStream stream in disguise: each key is a subject, each
write a message, and the message sequence *is* the key's revision. That
foundation is what gives KV its distinctive features — atomic
compare-and-set, full history, and live watches — for free.

This script covers:

* ``put`` / ``get`` — the basic map,
* ``create`` / ``update`` — atomic writes: create-if-absent and CAS-on-revision,
* ``delete`` / ``purge`` — tombstones vs. history-erasing rollups,
* ``history`` — every past revision of a key,
* ``watch`` — a live feed, including the ``None`` "initial state done" marker,
* per-key TTL — a single self-expiring revision (needs ``allow_msg_ttl``),
* ``purge_deletes`` — housekeeping that removes tombstones.

Run it (needs a JetStream server: ``just server``)::

    python examples/08_key_value.py
"""

import asyncio
import contextlib
import os
from datetime import timedelta

import natsio
from natsio.jetstream import WrongLastSequenceError
from natsio.kv import (
    BucketNotFoundError,
    KeyDeletedError,
    KeyExistsError,
    KeyNotFoundError,
    KeyValueConfig,
)

NATS_URL = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")


async def main() -> None:
    async with await natsio.connect(NATS_URL) as nc:
        js = nc.jetstream()

        # history=5 keeps the last 5 revisions per key; allow_msg_ttl lets an
        # individual write self-expire (used near the end). Idempotent re-create
        # returns the existing bucket, but we delete first for a clean run.
        with contextlib.suppress(BucketNotFoundError):
            await js.delete_key_value("config")
        kv = await js.create_key_value(KeyValueConfig(bucket="config", history=5, allow_msg_ttl=True))
        print("created bucket 'config'")

        # -- put / get ----------------------------------------------------------
        rev = await kv.put("theme", b"dark")
        entry = await kv.get("theme")
        print(f"put theme=dark -> revision {rev}; get -> {entry.value.decode()!r} (rev {entry.revision})")

        # -- create: only if the key has no live value -------------------------
        await kv.create("region", b"us-east")
        try:
            await kv.create("region", b"eu-west")  # already exists -> refused
        except KeyExistsError:
            print("create('region') twice -> KeyExistsError (create is create-if-absent)")

        # -- update: compare-and-set on the expected revision ------------------
        # This is optimistic locking. Pass the revision you read; if someone
        # else wrote in between, the server rejects your update.
        current = await kv.get("theme")
        new_rev = await kv.update("theme", b"solarized", last=current.revision)
        print(f"CAS update theme -> revision {new_rev}")
        try:
            await kv.update("theme", b"light", last=current.revision)  # stale revision
        except WrongLastSequenceError:
            print("CAS with a stale revision -> WrongLastSequenceError (someone else won the race)")

        # -- history: every stored revision, oldest first ----------------------
        history = await kv.history("theme")
        trail = [(e.revision, e.value.decode(), e.operation.value) for e in history]
        print(f"history('theme'): {trail}")

        # -- delete vs purge ---------------------------------------------------
        # delete writes a tombstone: get() now raises, but history is preserved.
        await kv.delete("region")
        try:
            await kv.get("region")
        except KeyDeletedError:
            print("after delete('region'): get -> KeyDeletedError (a delete marker sits on top)")
        # purge rolls up: the tombstone stays but the prior revisions are erased.
        await kv.purge("region")
        print("purge('region'): history below the marker is erased")

        # -- watch: live feed with the None initial-state marker ---------------
        # A watcher first replays the CURRENT state (one entry per live key),
        # then yields exactly one `None` to signal "you are now caught up",
        # then streams live updates. That None is the seam between snapshot and
        # live tail — the single most important thing to get right with watch.
        seen_initial: list[str] = []
        async with kv.watch() as watcher:
            async for change in watcher:
                if change is None:
                    print(f"watch: initial state delivered ({seen_initial}); now live")
                    break  # for the demo we stop at the snapshot boundary
                if not change.is_marker:
                    seen_initial.append(change.key)

        # -- per-key TTL: a single self-expiring revision ----------------------
        # ttl is whole seconds (or "never"); the bucket must allow msg TTLs.
        await kv.put("session", b"token123", ttl=1)
        print("put session with ttl=1s; waiting for it to expire...")
        await asyncio.sleep(1.5)
        try:
            await kv.get("session")
            print("session still present (server TTL granularity)")
        except KeyNotFoundError:
            print("session expired and is gone (per-key TTL)")

        # -- purge_deletes: housekeeping ---------------------------------------
        # Remove delete/purge markers (and any history beneath them). Pass
        # timedelta(0) to ignore the default age grace and clear them all.
        await kv.purge_deletes(older_than=timedelta(0))
        remaining = await kv.keys()
        print(f"after purge_deletes: live keys = {sorted(remaining)}")

        await js.delete_key_value("config")
        print("deleted bucket")


if __name__ == "__main__":
    asyncio.run(main())
