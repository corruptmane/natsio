"""Unit tests for the pure store-status builders used by KV/OS listings.

These mirror ``KeyValue.status()`` / ``ObjectStore.status()`` but read from an
already-fetched ``StreamInfo`` so paged listings avoid a per-bucket round-trip.
"""

from datetime import timedelta

from natsio.jetstream.context import _kv_status_from_info, _obj_status_from_info
from natsio.jetstream.entities import (
    StorageType,
    StreamConfig,
    StreamInfo,
    StreamState,
)


class TestKvStatusFromInfo:
    def test_maps_every_field(self) -> None:
        info = StreamInfo(
            config=StreamConfig(
                name="KV_TEST",
                description="a bucket",
                max_msgs_per_subject=5,
                max_age=timedelta(minutes=10),
                storage=StorageType.MEMORY,
                metadata={"k": "v"},
            ),
            state=StreamState(messages=3, bytes=99),
        )
        status = _kv_status_from_info("TEST", info)
        assert status.bucket == "TEST"
        assert status.values == 3
        assert status.history == 5
        assert status.ttl == timedelta(minutes=10)
        assert status.bytes == 99
        assert status.storage is StorageType.MEMORY
        assert status.description == "a bucket"
        assert status.metadata == {"k": "v"}
        assert status.stream_info is info

    def test_normalizes_zero_ttl_to_none(self) -> None:
        info = StreamInfo(config=StreamConfig(name="KV_X", max_age=timedelta(0)))
        assert _kv_status_from_info("X", info).ttl is None

    def test_foreign_unlimited_history(self) -> None:
        # A non-KV-shaped stream reports max_msgs_per_subject=-1 (unlimited).
        info = StreamInfo(config=StreamConfig(name="KV_FOREIGN", max_msgs_per_subject=-1))
        assert _kv_status_from_info("FOREIGN", info).history == -1


class TestObjStatusFromInfo:
    def test_maps_every_field(self) -> None:
        info = StreamInfo(
            config=StreamConfig(
                name="OBJ_A",
                description="blob store",
                max_age=timedelta(hours=1),
                storage=StorageType.MEMORY,
                num_replicas=1,
                sealed=True,
                metadata={"env": "test"},
            ),
            state=StreamState(bytes=1234),
        )
        status = _obj_status_from_info("A", info)
        assert status.bucket == "A"
        assert status.description == "blob store"
        assert status.ttl == timedelta(hours=1)
        assert status.storage is StorageType.MEMORY
        assert status.replicas == 1
        assert status.sealed is True
        assert status.size == 1234
        assert status.metadata == {"env": "test"}
        assert status.stream_info is info

    def test_normalizes_zero_ttl_and_defaults_unsealed(self) -> None:
        info = StreamInfo(config=StreamConfig(name="OBJ_B", max_age=timedelta(0)))
        status = _obj_status_from_info("B", info)
        assert status.ttl is None
        assert status.sealed is False
