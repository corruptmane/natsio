from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Annotated

from natsio._internal.jsonmodel import NS_DURATION, RFC3339, JsonModel
from natsio.jetstream import (
    AckPolicy,
    ConsumerConfig,
    ConsumerInfo,
    DeliverPolicy,
    PubAck,
    RetentionPolicy,
    StorageType,
    StreamConfig,
    StreamInfo,
)


class TestConverters:
    def test_ns_duration_round_trip(self) -> None:
        assert NS_DURATION.to_wire(timedelta(seconds=30)) == 30_000_000_000
        assert NS_DURATION.from_wire(30_000_000_000) == timedelta(seconds=30)
        assert NS_DURATION.to_wire(timedelta(milliseconds=1)) == 1_000_000

    def test_rfc3339_round_trip(self) -> None:
        moment = datetime(2026, 7, 20, 12, 30, 45, 123456, tzinfo=UTC)
        wire = RFC3339.to_wire(moment)
        assert wire.endswith("Z")
        assert RFC3339.from_wire(wire) == moment

    def test_rfc3339_nanosecond_precision_truncates(self) -> None:
        parsed = RFC3339.from_wire("2026-07-20T12:30:45.123456789Z")
        assert parsed == datetime(2026, 7, 20, 12, 30, 45, 123456, tzinfo=UTC)

    def test_rfc3339_offset_form(self) -> None:
        parsed = RFC3339.from_wire("2026-07-20T14:30:45.5+02:00")
        assert parsed.astimezone(UTC).hour == 12


class TestJsonModel:
    def test_none_fields_are_omitted_zeros_are_kept(self) -> None:
        config = StreamConfig(name="S", max_msgs=0)
        wire = config.to_wire()
        assert wire["max_msgs"] == 0
        assert "description" not in wire
        assert "max_age" not in wire

    def test_enum_and_duration_encoding(self) -> None:
        config = StreamConfig(
            name="S",
            retention=RetentionPolicy.WORK_QUEUE,
            storage=StorageType.MEMORY,
            max_age=timedelta(hours=1),
        )
        wire = config.to_wire()
        assert wire["retention"] == "workqueue"
        assert wire["storage"] == "memory"
        assert wire["max_age"] == 3_600_000_000_000

    def test_unknown_fields_round_trip(self) -> None:
        wire = {"name": "S", "storage": "file", "some_2026_field": {"nested": True}}
        config = StreamConfig.from_wire(wire)
        assert config.extra == {"some_2026_field": {"nested": True}}
        assert config.to_wire()["some_2026_field"] == {"nested": True}

    def test_nested_models_decode(self) -> None:
        info = StreamInfo.from_wire(
            {
                "config": {"name": "S", "subjects": ["s.>"], "retention": "limits"},
                "state": {"messages": 5, "first_seq": 1, "last_seq": 5},
                "created": "2026-07-20T00:00:00Z",
            }
        )
        assert info.config.name == "S"
        assert info.state.messages == 5
        assert info.created is not None
        assert info.created.tzinfo is not None

    def test_consumer_config_full_round_trip(self) -> None:
        config = ConsumerConfig(
            durable_name="worker",
            ack_policy=AckPolicy.EXPLICIT,
            ack_wait=timedelta(seconds=30),
            backoff=[timedelta(seconds=1), timedelta(seconds=5)],
            deliver_policy=DeliverPolicy.BY_START_SEQUENCE,
            opt_start_seq=42,
            filter_subjects=["a.>", "b.*"],
            inactive_threshold=timedelta(minutes=5),
            metadata={"team": "x"},
        )
        back = ConsumerConfig.from_wire(config.to_wire())
        assert back == config

    def test_consumer_info_with_push_fields_in_extra(self) -> None:
        info = ConsumerInfo.from_wire(
            {
                "stream_name": "S",
                "name": "old-push",
                "config": {
                    "durable_name": "old-push",
                    "deliver_subject": "push.me",  # push-only: unmodeled
                    "ack_policy": "explicit",
                },
                "delivered": {"consumer_seq": 1, "stream_seq": 1},
                "ack_floor": {"consumer_seq": 0, "stream_seq": 0},
            }
        )
        assert info.config.extra["deliver_subject"] == "push.me"
        assert info.config.to_wire()["deliver_subject"] == "push.me"

    def test_pub_ack(self) -> None:
        ack = PubAck.from_wire({"stream": "S", "seq": 7, "duplicate": True, "domain": "hub"})
        assert (ack.stream, ack.seq, ack.duplicate, ack.domain) == ("S", 7, True, "hub")

    def test_pub_ack_round_trip_preserves_error_and_extra_keys(self) -> None:
        # Guards the plan-based decode fast path: unknown keys (including a nested
        # "error" object) land in extra and survive the round-trip unchanged.
        wire = {
            "stream": "S",
            "seq": 7,
            "duplicate": True,
            "error": {"code": 400, "description": "bad request"},
            "misc": "x",
        }
        ack = PubAck.from_wire(wire)
        assert (ack.stream, ack.seq, ack.duplicate) == ("S", 7, True)
        assert ack.extra["error"] == {"code": 400, "description": "bad request"}
        assert ack.extra["misc"] == "x"
        back = ack.to_wire()
        assert back["stream"] == "S"
        assert back["error"] == {"code": 400, "description": "bad request"}
        assert back["misc"] == "x"

    def test_item_level_annotations_in_lists(self) -> None:
        @dataclass(slots=True, kw_only=True)
        class WithTimes(JsonModel):
            stamps: list[Annotated[datetime, RFC3339]] | None = None

        moment = datetime(2026, 1, 1, tzinfo=UTC)
        model = WithTimes(stamps=[moment])
        wire = model.to_wire()
        assert wire["stamps"] == ["2026-01-01T00:00:00Z"]
        assert WithTimes.from_wire(wire).stamps == [moment]
