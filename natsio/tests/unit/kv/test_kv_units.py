from datetime import timedelta

import pytest

from natsio.jetstream.context import _kv_stream_config
from natsio.jetstream.entities import (
    DiscardPolicy,
    StorageCompression,
    StorageType,
    StreamConfig,
    StreamInfo,
    StreamState,
)
from natsio.kv import (
    InvalidBucketNameError,
    InvalidKeyError,
    KeyValueConfig,
    KeyValueStatus,
    validate_bucket_name,
    validate_key,
)


class TestValidation:
    @pytest.mark.parametrize("bucket", ["settings", "my-bucket", "B_2", "a"])
    def test_valid_buckets(self, bucket: str) -> None:
        validate_bucket_name(bucket)

    @pytest.mark.parametrize("bucket", ["", "with.dot", "with space", "star*", "gt>", "üni"])
    def test_invalid_buckets(self, bucket: str) -> None:
        with pytest.raises(InvalidBucketNameError):
            validate_bucket_name(bucket)

    @pytest.mark.parametrize("key", ["theme", "a.b.c", "path/to/thing", "K-1_2=3", "0"])
    def test_valid_keys(self, key: str) -> None:
        validate_key(key)

    @pytest.mark.parametrize(
        "key",
        ["", ".leading", "trailing.", "sp ace", "st*ar", "g>t", "ключ", "foo..bar", "a..b.c"],
    )
    def test_invalid_keys(self, key: str) -> None:
        with pytest.raises(InvalidKeyError):
            validate_key(key)

    @pytest.mark.parametrize("key", ["a.*", "a.>", ">", "*.b", "a.*.b", "foo.*.bar.>"])
    def test_wildcards_allowed_for_watch(self, key: str) -> None:
        validate_key(key, wildcards=True)

    @pytest.mark.parametrize(
        "key",
        ["", "foo..bar", "a..b.c", "a.>.b", "foo.", ".foo", "foo.bar>baz", ">.a", "a.>b"],
    )
    def test_invalid_wildcard_watchers(self, key: str) -> None:
        """Mirror nats.go TestKeyValueWatch invalid watchers: consecutive dots
        and a non-terminal ``>`` must be rejected even with wildcards."""
        with pytest.raises(InvalidKeyError):
            validate_key(key, wildcards=True)

    def test_history_bounds(self) -> None:
        KeyValueConfig(bucket="b", history=64)
        with pytest.raises(InvalidBucketNameError):
            KeyValueConfig(bucket="b", history=0)
        with pytest.raises(InvalidBucketNameError):
            KeyValueConfig(bucket="b", history=65)


class TestStreamMapping:
    def test_defaults_map_to_adr8_stream(self) -> None:
        config = _kv_stream_config(KeyValueConfig(bucket="settings"))
        assert config.name == "KV_settings"
        assert config.subjects == ["$KV.settings.>"]
        assert config.max_msgs_per_subject == 1
        assert config.discard is DiscardPolicy.NEW
        assert config.allow_rollup_hdrs is True
        assert config.deny_delete is True
        assert config.allow_direct is True
        assert config.max_age is None  # keys never expire by default
        assert config.duplicate_window == timedelta(minutes=2)
        assert config.allow_msg_ttl is None

    def test_ttl_bounds_duplicate_window(self) -> None:
        config = _kv_stream_config(KeyValueConfig(bucket="b", ttl=timedelta(seconds=30)))
        assert config.max_age == timedelta(seconds=30)
        assert config.duplicate_window == timedelta(seconds=30)

    def test_history_and_sizing(self) -> None:
        config = _kv_stream_config(
            KeyValueConfig(
                bucket="b",
                history=12,
                max_value_size=1024,
                max_bytes=1 << 20,
                storage=StorageType.MEMORY,
                replicas=3,
            )
        )
        assert config.max_msgs_per_subject == 12
        assert config.max_msg_size == 1024
        assert config.max_bytes == 1 << 20
        assert config.storage is StorageType.MEMORY
        assert config.num_replicas == 3

    def test_compression_and_limit_markers(self) -> None:
        config = _kv_stream_config(KeyValueConfig(bucket="b", compression=True, limit_marker_ttl=timedelta(minutes=1)))
        assert config.compression is StorageCompression.S2
        assert config.allow_msg_ttl is True
        assert config.subject_delete_marker_ttl == timedelta(minutes=1)


class TestKeyValueStatus:
    def _status(self, config: StreamConfig) -> KeyValueStatus:
        info = StreamInfo(config=config, state=StreamState())
        return KeyValueStatus(
            bucket="b",
            values=0,
            history=1,
            ttl=None,
            bytes=0,
            storage=StorageType.FILE,
            stream_info=info,
        )

    def test_metadata_and_description_surface_from_stream_config(self) -> None:
        status = self._status(StreamConfig(name="KV_b", description="my bucket", metadata={"foo": "bar"}))
        assert status.metadata == {"foo": "bar"}
        assert status.description == "my bucket"

    def test_metadata_defaults_to_empty_and_description_to_none(self) -> None:
        status = self._status(StreamConfig(name="KV_b"))
        assert status.metadata == {}
        assert status.description is None
