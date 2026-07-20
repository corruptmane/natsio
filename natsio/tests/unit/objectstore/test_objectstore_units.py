import base64
import hashlib
from datetime import UTC, datetime, timedelta

import pytest

from natsio.errors import ConfigError
from natsio.jetstream.context import _obj_stream_config
from natsio.jetstream.entities import DiscardPolicy, StorageCompression, StorageType
from natsio.objectstore import (
    DEFAULT_CHUNK_SIZE,
    InvalidBucketNameError,
    InvalidObjectNameError,
    ObjectInfo,
    ObjectMeta,
    ObjectStoreConfig,
    encode_object_name,
    validate_bucket_name,
    validate_object_name,
)
from natsio.objectstore.store import _digest_value, _digests_equal, _iter_chunks


class TestValidation:
    @pytest.mark.parametrize("bucket", ["assets", "my-bucket", "B_2", "a"])
    def test_valid_buckets(self, bucket: str) -> None:
        validate_bucket_name(bucket)

    @pytest.mark.parametrize("bucket", ["", "with.dot", "with space", "star*", "gt>", "üni"])
    def test_invalid_buckets(self, bucket: str) -> None:
        with pytest.raises(InvalidBucketNameError):
            validate_bucket_name(bucket)

    @pytest.mark.parametrize("name", ["a", "with space", "path/to/file.png", "übject", ".hidden", "a.b.>"])
    def test_any_nonempty_object_name_is_valid(self, name: str) -> None:
        validate_object_name(name)

    def test_empty_object_name_rejected(self) -> None:
        with pytest.raises(InvalidObjectNameError):
            validate_object_name("")
        with pytest.raises(InvalidObjectNameError):
            ObjectMeta(name="")

    def test_chunk_size_must_be_positive(self) -> None:
        with pytest.raises(ConfigError):
            ObjectMeta(name="x", chunk_size=0)
        with pytest.raises(ConfigError):
            ObjectMeta(name="x", chunk_size=-1)

    def test_sub_100ms_ttl_rejected(self) -> None:
        with pytest.raises(ConfigError):
            ObjectStoreConfig(bucket="b", ttl=timedelta(milliseconds=50))

    def test_negative_ttl_rejected(self) -> None:
        """Review regression: a negative ttl silently mapped to 'never expires'."""
        with pytest.raises(ConfigError):
            ObjectStoreConfig(bucket="b", ttl=timedelta(seconds=-1))

    def test_negative_kv_ttl_rejected(self) -> None:
        from natsio.kv import KeyValueConfig

        with pytest.raises(ConfigError):
            KeyValueConfig(bucket="b", ttl=timedelta(seconds=-1))


class TestNameEncoding:
    @pytest.mark.parametrize("name", ["logo.png", "path/to/file", "with space", "ünïcode-☂"])
    def test_roundtrip(self, name: str) -> None:
        encoded = encode_object_name(name)
        assert base64.urlsafe_b64decode(encoded).decode() == name

    def test_encoding_is_subject_token_safe(self) -> None:
        encoded = encode_object_name("weird name/☂.bin")
        assert " " not in encoded
        assert "." not in encoded
        assert "*" not in encoded
        assert ">" not in encoded

    def test_matches_go_client_padding(self) -> None:
        # nats.go uses padded base64url; interop requires the same subject.
        assert encode_object_name("a") == "YQ=="


class TestDigest:
    def test_format(self) -> None:
        digest = hashlib.sha256(b"hello")
        expected = base64.urlsafe_b64encode(digest.digest()).decode()
        assert _digest_value(digest) == f"SHA-256={expected}"

    def test_equality_ignores_padding(self) -> None:
        digest = hashlib.sha256(b"data")
        value = _digest_value(digest)
        assert _digests_equal(value, value)
        assert _digests_equal(value, value.rstrip("="))
        assert not _digests_equal(value, "SHA-256=AAAA")

    def test_equality_tolerates_standard_alphabet(self) -> None:
        """Review regression: a standard-base64 (+/) digest from an exotic
        writer must not fail an otherwise-valid read."""
        raw = bytes(range(251, 256)) * 6  # produces both + and / in std base64
        std = "SHA-256=" + base64.b64encode(raw).decode()
        url = "SHA-256=" + base64.urlsafe_b64encode(raw).decode()
        assert std != url  # the fixture actually exercises the alphabets
        assert _digests_equal(std, url)


class TestRechunking:
    async def _collect(self, data, chunk_size: int) -> list[bytes]:
        return [chunk async for chunk in _iter_chunks(data, chunk_size)]

    async def test_bytes_exact_multiple(self) -> None:
        chunks = await self._collect(b"a" * 20, 10)
        assert [len(c) for c in chunks] == [10, 10]

    async def test_bytes_with_remainder(self) -> None:
        chunks = await self._collect(b"a" * 25, 10)
        assert [len(c) for c in chunks] == [10, 10, 5]

    async def test_empty(self) -> None:
        assert await self._collect(b"", 10) == []

    async def test_memoryview_chunk_size_is_bytes_not_elements(self) -> None:
        """Review regression: a memoryview over array('i') indexes in 4-byte
        elements; chunk_size must still mean bytes."""
        from array import array

        data = array("i", range(100))  # 400 bytes
        chunks = await self._collect(memoryview(data), 7)
        assert b"".join(chunks) == data.tobytes()
        assert len(chunks) == 58  # ceil(400/7), not ceil(100/7)
        assert all(len(c) == 7 for c in chunks[:-1])

    async def test_async_iterable_rechunked_uniformly(self) -> None:
        async def pieces():
            # Ragged input: chunk boundaries must not depend on it.
            for piece in (b"ab", b"cdefgh", b"i", b"jklmnopqrstuv"):
                yield piece

        chunks = await self._collect(pieces(), 5)
        assert b"".join(chunks) == b"abcdefghijklmnopqrstuv"
        assert [len(c) for c in chunks] == [5, 5, 5, 5, 2]


class TestObjectInfoWire:
    def test_roundtrip_preserves_unknown_fields(self) -> None:
        wire = {
            "name": "x",
            "bucket": "b",
            "nuid": "N1",
            "size": 3,
            "chunks": 1,
            "digest": "SHA-256=abc",
            "headers": {"X-Foo": ["bar"]},  # first-class since the interop review
            "options": {"max_chunk_size": 1024},
            "revved": {"future": "field"},  # unmodeled: must survive
        }
        info = ObjectInfo.from_wire(wire)
        assert info.options is not None and info.options.max_chunk_size == 1024
        assert info.headers == {"X-Foo": ["bar"]}
        assert not info.is_link and not info.is_deleted
        out = info.to_wire()
        assert out["headers"] == {"X-Foo": ["bar"]}
        assert out["revved"] == {"future": "field"}

    def test_link_detection(self) -> None:
        info = ObjectInfo.from_wire({"name": "l", "options": {"link": {"bucket": "other", "name": "t"}}})
        assert info.is_link
        assert info.options is not None and info.options.link is not None
        assert info.options.link.bucket == "other"
        bucket_link = ObjectInfo.from_wire({"name": "l", "options": {"link": {"bucket": "other"}}})
        assert bucket_link.is_link
        assert bucket_link.options is not None and bucket_link.options.link is not None
        assert not bucket_link.options.link.name

    def test_mtime_not_emitted_when_unset(self) -> None:
        assert "mtime" not in ObjectInfo(name="x").to_wire()
        stamped = ObjectInfo(name="x", mtime=datetime(2026, 1, 1, tzinfo=UTC))
        assert stamped.to_wire()["mtime"] == "2026-01-01T00:00:00Z"


class TestStreamMapping:
    def test_defaults_map_to_adr20_stream(self) -> None:
        config = _obj_stream_config(ObjectStoreConfig(bucket="assets"))
        assert config.name == "OBJ_assets"
        assert config.subjects == ["$O.assets.C.>", "$O.assets.M.>"]
        assert config.discard is DiscardPolicy.NEW
        assert config.allow_rollup_hdrs is True
        assert config.allow_direct is True
        assert config.max_age is None  # objects never expire by default
        assert config.duplicate_window == timedelta(minutes=2)
        assert config.max_msgs_per_subject == -1  # chunks share a subject-per-nuid

    def test_ttl_bounds_duplicate_window(self) -> None:
        config = _obj_stream_config(ObjectStoreConfig(bucket="b", ttl=timedelta(seconds=30)))
        assert config.max_age == timedelta(seconds=30)
        assert config.duplicate_window == timedelta(seconds=30)

    def test_sizing_and_compression(self) -> None:
        config = _obj_stream_config(
            ObjectStoreConfig(
                bucket="b",
                max_bytes=1 << 30,
                storage=StorageType.MEMORY,
                replicas=3,
                compression=True,
                metadata={"team": "infra"},
            )
        )
        assert config.max_bytes == 1 << 30
        assert config.storage is StorageType.MEMORY
        assert config.num_replicas == 3
        assert config.compression is StorageCompression.S2
        assert config.metadata == {"team": "infra"}

    def test_default_chunk_size_is_adr20_canonical(self) -> None:
        assert DEFAULT_CHUNK_SIZE == 128 * 1024
