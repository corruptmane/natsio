"""End-to-end kvcodec tests against a real nats-server with -js.

Exercises the full seam: create_key_value(key_codec=..., value_codec=...) then
put/get/delete/history/keys/watch, verifying the stored (encoded) form on the
wire and the decoded form at the API.
"""

from __future__ import annotations

import asyncio

import pytest
from natsio.kvcodec import (  # ty: ignore[unresolved-import]
    Base64KeyCodec,
    ChainValueCodec,
    PathKeyCodec,
    ZlibValueCodec,
)

import natsio
from natsio.errors import ConfigError
from natsio.kv import KeyDeletedError, KeyValueConfig, Operation


async def _collect_initial(kv, *keys: str) -> list:
    """Drain a watcher's initial snapshot (up to the None marker)."""
    entries = []
    async with kv.watch(*keys) as watcher:
        async for entry in watcher:
            if entry is None:
                break
            entries.append(entry)
    return entries


class TestPathKeyCodecLive:
    async def test_put_get_keys_history_decoded(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        kv = await js.create_key_value(
            KeyValueConfig(bucket="PATHKV", history=5),
            key_codec=PathKeyCodec(),
        )

        await kv.put("/config/app/db", b"postgres://localhost")
        await kv.put("/config/app/cache", b"redis://localhost")

        # get() round-trips the path key.
        entry = await kv.get("/config/app/db")
        assert entry.key == "/config/app/db"
        assert entry.value == b"postgres://localhost"

        # The key is stored ENCODED on the wire: raw subject uses dots + _root_.
        stored = await kv._stream.get_msg(subject="$KV.PATHKV._root_.config.app.db")
        assert stored.payload == b"postgres://localhost"

        # keys() returns DECODED path keys (core decodes via _key_from_subject).
        keys = sorted(await kv.keys())
        assert keys == ["/config/app/cache", "/config/app/db"]

        # history() keys are decoded too.
        await kv.put("/config/app/db", b"postgres://prod")
        history = await kv.history("/config/app/db")
        assert [e.value for e in history] == [b"postgres://localhost", b"postgres://prod"]
        assert all(e.key == "/config/app/db" for e in history)

    async def test_delete_marker_decoded(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        kv = await js.create_key_value(KeyValueConfig(bucket="PATHDEL"), key_codec=PathKeyCodec())
        await kv.put("/a/b", b"1")
        await kv.delete("/a/b")
        with pytest.raises(KeyDeletedError):
            await kv.get("/a/b")
        # deleted key drops out of keys()
        assert await kv.keys() == []


class TestBase64KeyCodecLive:
    async def test_roundtrip_valid_keys(self, nc: natsio.Client) -> None:
        # NOTE: raw keys must themselves be NATS-valid because the core
        # pre-validates before the codec runs (see Core Friction). So we use
        # ordinary keys; base64 still transforms them on the wire.
        js = nc.jetstream()
        kv = await js.create_key_value(KeyValueConfig(bucket="B64KV"), key_codec=Base64KeyCodec())

        await kv.put("user.settings", b"v1")
        entry = await kv.get("user.settings")
        assert entry.key == "user.settings"
        assert entry.value == b"v1"

        # stored encoded: dXNlcg.c2V0dGluZ3M
        stored = await kv._stream.get_msg(subject="$KV.B64KV.dXNlcg.c2V0dGluZ3M")
        assert stored.payload == b"v1"

        assert await kv.keys() == ["user.settings"]

    async def test_raw_exotic_key_roundtrips_through_codec(self, nc: natsio.Client) -> None:
        """Core fix (kvcodec finding #1): with a key codec, only the ENCODED
        key must be subject-legal — exotic raw keys are the codec's purpose."""
        js = nc.jetstream()
        kv = await js.create_key_value(KeyValueConfig(bucket="B64EXOTIC"), key_codec=Base64KeyCodec())
        await kv.put("Acme Inc.contact", b"x")
        assert (await kv.get("Acme Inc.contact")).value == b"x"
        assert await kv.keys() == ["Acme Inc.contact"]


class TestValueCodecLive:
    async def test_zlib_value_roundtrip(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        kv = await js.create_key_value(KeyValueConfig(bucket="ZKV"), value_codec=ZlibValueCodec())

        payload = b'{"users": [' + b'{"name": "x"},' * 500 + b"]}"
        await kv.put("blob", payload)
        assert (await kv.get("blob")).value == payload

        # On the wire it is compressed (smaller than the original).
        stored = await kv._stream.get_msg(subject="$KV.ZKV.blob")
        assert len(stored.payload) < len(payload)

    async def test_key_and_value_codecs_together(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        kv = await js.create_key_value(
            KeyValueConfig(bucket="BOTHKV", history=3),
            key_codec=PathKeyCodec(),
            value_codec=ChainValueCodec(ZlibValueCodec()),
        )
        await kv.put("/svc/config", b"data" * 100)
        entry = await kv.get("/svc/config")
        assert entry.key == "/svc/config"
        assert entry.value == b"data" * 100
        # keys()/iter_keys() under a framing codec are covered by
        # test_keys_under_framing_value_codec below.
        history = await kv.history("/svc/config")
        assert [e.value for e in history] == [b"data" * 100]

    async def test_keys_under_framing_value_codec(self, nc: natsio.Client) -> None:
        """Core fix (kvcodec finding #2): meta_only deliveries arrive with the
        payload server-stripped; the core must not run the value codec on
        them. Was a strict xfail pinning the bug."""
        js = nc.jetstream()
        kv = await js.create_key_value(
            KeyValueConfig(bucket="FRAMEKV"),
            value_codec=ZlibValueCodec(),
        )
        await kv.put("k", b"payload" * 50)
        assert (await kv.get("k")).value == b"payload" * 50
        assert await kv.keys() == ["k"]


class TestWatchLive:
    async def test_watch_single_codec_key(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        kv = await js.create_key_value(KeyValueConfig(bucket="WKV"), key_codec=PathKeyCodec())
        await kv.put("/a/b", b"1")
        await kv.put("/a/c", b"2")

        # Watch a single decoded key; only /a/b updates arrive, decoded.
        seen: list = []
        async with kv.watch("/a/b") as watcher:
            async for entry in watcher:
                if entry is None:
                    break
                seen.append((entry.key, entry.value))
        assert seen == [("/a/b", b"1")]

    async def test_watch_whole_bucket_decoded(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        kv = await js.create_key_value(KeyValueConfig(bucket="WHOLEKV"), key_codec=PathKeyCodec())
        await kv.put("/x/y", b"1")
        await kv.put("/x/z", b"2")
        entries = await _collect_initial(kv)  # watch() with no keys -> ">"
        keys = sorted(e.key for e in entries)
        assert keys == ["/x/y", "/x/z"]
        assert all(e.operation is Operation.PUT for e in entries)

    async def test_wildcard_watch_under_codec_refused(self, nc: natsio.Client) -> None:
        # The documented refusal: a wildcard watch combined with a key codec
        # would silently match nothing in the encoded keyspace, so the core
        # raises rather than mislead. (kvcodec's Base64KeyCodec.encode_filter
        # COULD support this, but the core has no filter hook — see Core
        # Friction.)
        js = nc.jetstream()
        kv = await js.create_key_value(KeyValueConfig(bucket="WCKV"), key_codec=PathKeyCodec())
        with pytest.raises(ConfigError):
            kv.watch("/a/*")
        with pytest.raises(ConfigError):
            kv.watch("/a/>")

    async def test_live_update_after_snapshot(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        kv = await js.create_key_value(KeyValueConfig(bucket="LIVEKV"), key_codec=PathKeyCodec())
        await kv.put("/k", b"initial")
        async with kv.watch("/k") as watcher:
            it = watcher.__aiter__()
            first = await it.__anext__()
            assert first is not None
            assert first.key == "/k" and first.value == b"initial"
            marker = await it.__anext__()
            assert marker is None
            await kv.put("/k", b"updated")
            live = await asyncio.wait_for(it.__anext__(), timeout=5.0)
            assert live is not None
            assert live.key == "/k" and live.value == b"updated"
