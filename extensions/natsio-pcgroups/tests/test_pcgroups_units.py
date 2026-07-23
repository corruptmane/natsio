"""Pure-logic and wire-contract tests for natsio-pcgroups.

The partition-assignment cases and the validation sequence are ported
one-for-one from the oracle's ``partitioned_consumer_groups_test.go``
(``TestBaseFunctions``); the wire section pins the JSON that goes into the KV
buckets, byte for byte, against what orbit.go writes.
"""

import json
from typing import Any, cast

import pytest
from natsio.pcgroups import (  # ty: ignore[unresolved-import]
    DEFAULT_ACK_WAIT,
    ELASTIC_BUCKET,
    PRIORITY_GROUP,
    STATIC_BUCKET,
    ConsumerGroupConfigError,
    ElasticConsumerGroupConfig,
    MemberMapping,
    PartitionedMsg,
    PartitioningFilter,
    StaticConsumerGroupConfig,
    compose_group_stream_name,
    compose_key,
    compose_static_consumer_name,
    elastic_get_partition_filters,
    generate_partition_filters,
    partitioning_transform_destination,
    static_get_partition_filters,
    strip_partition,
    validate_elastic_config,
    validate_static_config,
)

from natsio.jetstream import JsMsg
from natsio.message import Msg

FILTER = "foo.*.*.>"
WILDCARDS = [1, 2]


def elastic(max_members: int, **kwargs: Any) -> ElasticConsumerGroupConfig:
    kwargs.setdefault("partitioning_filters", [PartitioningFilter(filter=FILTER, partitioning_wildcards=WILDCARDS)])
    return ElasticConsumerGroupConfig(max_members=max_members, **kwargs)


class TestNaming:
    def test_key_is_stream_dot_group(self) -> None:
        assert compose_key("ORDERS", "cg") == "ORDERS.cg"

    def test_static_consumer_name_is_group_dash_member(self) -> None:
        assert compose_static_consumer_name("cg", "m1") == "cg-m1"

    def test_elastic_stream_name_is_stream_dash_group(self) -> None:
        assert compose_group_stream_name("ORDERS", "cg") == "ORDERS-cg"

    @pytest.mark.parametrize(
        ("stored", "original"),
        [
            ("3.foo.bar", "foo.bar"),
            ("0.foo", "foo"),
            ("12.a.b.c", "a.b.c"),
            ("nodot", "nodot"),  # nothing to strip: returned unchanged
            ("7.", ""),
        ],
    )
    def test_strip_partition(self, stored: str, original: str) -> None:
        assert strip_partition(stored) == original


class TestGeneratePartitionFilters:
    """The oracle's balanced-distribution expectations, exactly."""

    @pytest.mark.parametrize(
        ("max_members", "expected"),
        [
            (6, {"m1": ["0", "1"], "m2": ["2", "3"], "m3": ["4", "5"]}),
            (7, {"m1": ["0", "1", "6"], "m2": ["2", "3"], "m3": ["4", "5"]}),
            (8, {"m1": ["0", "1", "6"], "m2": ["2", "3", "7"], "m3": ["4", "5"]}),
        ],
    )
    def test_oracle_distributions(self, max_members: int, expected: dict[str, list[str]]) -> None:
        config = elastic(max_members, members=["m1", "m2", "m3"])
        for member, partitions in expected.items():
            assert elastic_get_partition_filters(config, member) == [f"{p}.{FILTER}" for p in partitions]

    def test_every_partition_is_owned_exactly_once(self) -> None:
        for max_members in range(1, 17):
            for count in range(1, max_members + 1):
                members = [f"m{i}" for i in range(count)]
                owned = [
                    partition
                    for member in members
                    for partition in generate_partition_filters(members, max_members, None, member, ">")
                ]
                assert sorted(owned) == sorted(f"{p}.>" for p in range(max_members)), (max_members, count)

    def test_membership_is_deduplicated_and_sorted(self) -> None:
        scrambled = generate_partition_filters(["m3", "m1", "m1", "m2"], 6, None, "m1", ">")
        ordered = generate_partition_filters(["m1", "m2", "m3"], 6, None, "m1", ">")
        assert scrambled == ordered == ["0.>", "1.>"]

    def test_membership_is_capped_to_max_members(self) -> None:
        # Only the first (sorted) max_members names get partitions at all.
        members = ["m1", "m2", "m3", "m4"]
        assert generate_partition_filters(members, 2, None, "m1", ">") == ["0.>"]
        assert generate_partition_filters(members, 2, None, "m2", ">") == ["1.>"]
        assert generate_partition_filters(members, 2, None, "m3", ">") == []

    def test_unknown_member_owns_nothing(self) -> None:
        assert generate_partition_filters(["m1", "m2"], 4, None, "ghost", ">") == []

    def test_no_members_and_no_mappings(self) -> None:
        assert generate_partition_filters(None, 4, None, "m1", ">") == []
        assert generate_partition_filters([], 4, [], "m1", ">") == []

    def test_member_mappings_ignore_the_subject_filter(self) -> None:
        # The oracle always emits "<partition>.>" from mappings; pinned as-is
        # because a Go-created group must produce identical consumer filters.
        mappings = [MemberMapping(member="m1", partitions=[0, 3]), MemberMapping(member="m2", partitions=[1, 2])]
        assert generate_partition_filters(None, 4, mappings, "m1", "foo.*") == ["0.>", "3.>"]
        assert generate_partition_filters(None, 4, mappings, "m2", "foo.*") == ["1.>", "2.>"]

    def test_members_win_over_mappings(self) -> None:
        mappings = [MemberMapping(member="m1", partitions=[0, 1])]
        assert generate_partition_filters(["m1"], 2, mappings, "m1", "x.*") == ["0.x.*", "1.x.*"]


class TestPerFlavourFilters:
    def test_static_multiplies_partitions_by_filters(self) -> None:
        config = StaticConsumerGroupConfig(max_members=2, filters=["foo.*", "bar.*"], members=["m1", "m2"])
        assert static_get_partition_filters(config, "m1") == ["0.foo.*", "0.bar.*"]
        assert static_get_partition_filters(config, "m2") == ["1.foo.*", "1.bar.*"]

    def test_static_without_filters_covers_the_whole_stream(self) -> None:
        config = StaticConsumerGroupConfig(max_members=2, members=["m1", "m2"])
        assert static_get_partition_filters(config, "m1") == ["0.>"]

    def test_mappings_with_several_filters_do_not_duplicate(self) -> None:
        # Regression: the mapping branch ignores the subject filter, so one
        # "<partition>.>" per configured filter would ask the server for the
        # same filter twice — rejected with err_code 10138 ("consumer subject
        # filters cannot overlap"), which makes mappings + multiple filters
        # unusable. We deduplicate; the oracle does not.
        mappings = [MemberMapping(member="m1", partitions=[0]), MemberMapping(member="m2", partitions=[1])]
        static = StaticConsumerGroupConfig(max_members=2, filters=["foo.*", "bar.*"], member_mappings=mappings)
        assert static_get_partition_filters(static, "m1") == ["0.>"]
        elastic_config = ElasticConsumerGroupConfig(
            max_members=2,
            partitioning_filters=[
                PartitioningFilter(filter="foo.*", partitioning_wildcards=[1]),
                PartitioningFilter(filter="bar.*", partitioning_wildcards=[1]),
            ],
            member_mappings=mappings,
        )
        assert elastic_get_partition_filters(elastic_config, "m2") == ["1.>"]

    def test_elastic_without_filters_covers_the_whole_stream(self) -> None:
        config = ElasticConsumerGroupConfig(max_members=2, members=["m1", "m2"])
        assert elastic_get_partition_filters(config, "m2") == ["1.>"]


class TestTransformDestination:
    def test_oracle_case(self) -> None:
        assert (
            partitioning_transform_destination(FILTER, WILDCARDS, 4)
            == "{{Partition(4,1,2)}}.foo.{{Wildcard(1)}}.{{Wildcard(2)}}.>"
        )

    def test_single_wildcard(self) -> None:
        assert partitioning_transform_destination("foo.*", [1], 10) == "{{Partition(10,1)}}.foo.{{Wildcard(1)}}"

    def test_whole_subject_when_no_partitioning_wildcards(self) -> None:
        assert partitioning_transform_destination(">", [], 4) == "{{Partition(4)}}.>"
        assert partitioning_transform_destination("foo.*", None, 4) == "{{Partition(4)}}.foo.{{Wildcard(1)}}"


class TestStaticValidation:
    def test_max_members_must_be_positive(self) -> None:
        with pytest.raises(ConsumerGroupConfigError):
            validate_static_config(StaticConsumerGroupConfig(max_members=0, members=["m1"]))

    def test_members_and_mappings_are_exclusive(self) -> None:
        with pytest.raises(ConsumerGroupConfigError):
            validate_static_config(
                StaticConsumerGroupConfig(
                    max_members=2,
                    members=["m1"],
                    member_mappings=[MemberMapping(member="m1", partitions=[0, 1])],
                )
            )

    def test_mappings_must_cover_every_partition(self) -> None:
        validate_static_config(
            StaticConsumerGroupConfig(
                max_members=2,
                member_mappings=[MemberMapping(member="m1", partitions=[0, 1])],
            )
        )
        with pytest.raises(ConsumerGroupConfigError):
            validate_static_config(
                StaticConsumerGroupConfig(
                    max_members=3,
                    member_mappings=[MemberMapping(member="m1", partitions=[0, 1])],
                )
            )


class TestElasticValidation:
    """The oracle's TestBaseFunctions validation sequence, case for case."""

    def _mappings(self, *pairs: tuple[str, list[int]]) -> list[MemberMapping]:
        return [MemberMapping(member=member, partitions=partitions) for member, partitions in pairs]

    def test_members_only_is_valid(self) -> None:
        validate_elastic_config(elastic(2, partitioning_filters=[_pf()], members=["m1", "m2"]))

    def test_members_plus_mappings_is_invalid(self) -> None:
        with pytest.raises(ConsumerGroupConfigError):
            validate_elastic_config(
                elastic(2, partitioning_filters=[_pf()], members=["m1"], member_mappings=self._mappings(("m1", [0, 1])))
            )

    @pytest.mark.parametrize(
        ("max_members", "pairs"),
        [
            (2, [("m1", [1, 1])]),  # duplicate partition
            (2, [("m1", [1])]),  # not enough partitions
            (2, [("m1", [0, 1, 2])]),  # too many / out of range
            (2, [("m1", [0, 2])]),  # partition out of range
            (2, [("m1", [0, 1]), ("m1", [0, 1])]),  # duplicate member
            (2, [("m1", [0, 1]), ("m2", [0, 1])]),  # full overlap
            (2, [("m1", [0, 1]), ("m2", [2, 3])]),  # out of range
            (3, [("m1", [0]), ("m2", [1])]),  # partitions left unclaimed
            (3, [("m1", [0, 2]), ("m2", [1, 2])]),  # partial overlap
        ],
    )
    def test_invalid_mappings(self, max_members: int, pairs: list[tuple[str, list[int]]]) -> None:
        with pytest.raises(ConsumerGroupConfigError):
            validate_elastic_config(
                elastic(max_members, partitioning_filters=[_pf()], member_mappings=self._mappings(*pairs))
            )

    @pytest.mark.parametrize(
        ("max_members", "pairs"),
        [
            (2, [("m1", [0]), ("m2", [1])]),
            (3, [("m1", [0, 2]), ("m2", [1])]),
        ],
    )
    def test_valid_mappings(self, max_members: int, pairs: list[tuple[str, list[int]]]) -> None:
        validate_elastic_config(
            elastic(max_members, partitioning_filters=[_pf()], member_mappings=self._mappings(*pairs))
        )

    def test_filter_must_be_partitionable(self) -> None:
        with pytest.raises(ConsumerGroupConfigError):
            validate_elastic_config(elastic(2, partitioning_filters=[PartitioningFilter(filter="foo.bar")]))
        validate_elastic_config(elastic(2, partitioning_filters=[PartitioningFilter(filter="foo.bar.>")]))

    def test_empty_filter_rejected(self) -> None:
        with pytest.raises(ConsumerGroupConfigError):
            validate_elastic_config(elastic(2, partitioning_filters=[PartitioningFilter(filter="")]))

    @pytest.mark.parametrize("wildcards", [[0], [3], [1, 1], [1, 2, 3]])
    def test_bad_partitioning_wildcards(self, wildcards: list[int]) -> None:
        with pytest.raises(ConsumerGroupConfigError):
            validate_elastic_config(
                elastic(
                    2,
                    partitioning_filters=[
                        PartitioningFilter(filter="foo.*.*", partitioning_wildcards=wildcards),
                    ],
                )
            )


def _pf() -> PartitioningFilter:
    return PartitioningFilter(filter="foo.*", partitioning_wildcards=[1])


class TestWireContract:
    """Byte-level pins. Changing anything here is an interop event."""

    def test_bucket_and_group_names(self) -> None:
        assert STATIC_BUCKET == "static-consumer-groups"
        assert ELASTIC_BUCKET == "elastic-consumer-groups"
        assert PRIORITY_GROUP == "PCG"
        assert DEFAULT_ACK_WAIT.total_seconds() == 5

    def test_static_config_json(self) -> None:
        config = StaticConsumerGroupConfig(max_members=2, filters=["foo.*"], members=["m1", "m2"])
        assert config.to_wire() == {"max_members": 2, "filters": ["foo.*"], "members": ["m1", "m2"]}

    def test_static_config_omits_empty_collections(self) -> None:
        # Go tags these omitempty; an emitted "[]" would make an otherwise
        # identical Go CreateStatic fail its reflect.DeepEqual check.
        config = StaticConsumerGroupConfig(max_members=1, filters=[], members=[], member_mappings=[])
        assert config.to_wire() == {"max_members": 1}

    def test_static_mappings_json(self) -> None:
        config = StaticConsumerGroupConfig(
            max_members=2, member_mappings=[MemberMapping(member="m1", partitions=[0, 1])]
        )
        assert config.to_wire() == {
            "max_members": 2,
            "member_mappings": [{"member": "m1", "partitions": [0, 1]}],
        }

    def test_elastic_config_json(self) -> None:
        config = ElasticConsumerGroupConfig(
            max_members=4,
            partitioning_filters=[PartitioningFilter(filter="foo.*", partitioning_wildcards=[1])],
            max_buffered_msg=-1,
            max_buffered_bytes=-1,
            members=["m1"],
        )
        assert config.to_wire() == {
            "max_members": 4,
            # NOTE the singular key: orbit.go tags MaxBufferedMsgs "max_buffered_msg".
            "max_buffered_msg": -1,
            "max_buffered_bytes": -1,
            "partitioning_filters": [{"filter": "foo.*", "partitioning_wildcards": [1]}],
            "members": ["m1"],
        }

    def test_elastic_zero_buffering_limits_are_omitted(self) -> None:
        config = ElasticConsumerGroupConfig(max_members=1, max_buffered_msg=0, max_buffered_bytes=0)
        assert config.to_wire() == {"max_members": 1}

    def test_elastic_partitioning_filters_distinguish_empty_from_absent(self) -> None:
        # Go does NOT tag partitioning_filters omitempty: an empty slice
        # marshals to "[]" and a nil one to null/absent. Both round-trip.
        assert ElasticConsumerGroupConfig(max_members=1, partitioning_filters=[]).to_wire() == {
            "max_members": 1,
            "partitioning_filters": [],
        }
        assert ElasticConsumerGroupConfig(max_members=1).to_wire() == {"max_members": 1}

    def test_configs_round_trip_through_json(self) -> None:
        for config in (
            StaticConsumerGroupConfig(max_members=3, filters=["a.*"], members=["m1"]),
            ElasticConsumerGroupConfig(
                max_members=3,
                partitioning_filters=[PartitioningFilter(filter="a.*", partitioning_wildcards=[1])],
                member_mappings=[MemberMapping(member="m1", partitions=[0, 1, 2])],
            ),
        ):
            decoded = type(config).from_wire(json.loads(json.dumps(config.to_wire())))
            assert decoded == config

    def test_unknown_server_fields_survive(self) -> None:
        raw = {"max_members": 2, "members": ["m1"], "future_field": {"x": 1}}
        config = StaticConsumerGroupConfig.from_wire(raw)
        assert config.to_wire() == raw

    def test_go_written_static_config_decodes(self) -> None:
        # Exactly what orbit.go's CreateStatic writes for
        # CreateStatic(..., 2, ["foo.*","bar.*"], ["m1","m2"], nil).
        raw = b'{"max_members":2,"filters":["foo.*","bar.*"],"members":["m1","m2"]}'
        config = StaticConsumerGroupConfig.from_wire(json.loads(raw))
        assert config.max_members == 2
        assert config.is_in_membership("m2")
        assert not config.is_in_membership("m3")

    def test_go_written_elastic_config_decodes(self) -> None:
        raw = (
            b'{"max_members":2,"partitioning_filters":[{"filter":"foo.*","partitioning_wildcards":[1]}],'
            b'"max_buffered_msg":-1,"max_buffered_bytes":-1,"members":["m1"]}'
        )
        config = ElasticConsumerGroupConfig.from_wire(json.loads(raw))
        assert config.max_buffered_msg == -1
        assert config.partitioning_filters == [PartitioningFilter(filter="foo.*", partitioning_wildcards=[1])]
        assert config.is_in_membership("m1")


class TestMembership:
    def test_is_in_membership_covers_both_shapes(self) -> None:
        assert StaticConsumerGroupConfig(max_members=1, members=["m1"]).is_in_membership("m1")
        assert ElasticConsumerGroupConfig(
            max_members=1, member_mappings=[MemberMapping(member="m1", partitions=[0])]
        ).is_in_membership("m1")
        assert not ElasticConsumerGroupConfig(max_members=1).is_in_membership("m1")


class TestPartitionedMsg:
    def _msg(self, subject: str) -> PartitionedMsg:
        return PartitionedMsg(JsMsg(Msg(subject=subject, payload=b"body"), cast("Any", None)))

    def test_subject_is_stripped(self) -> None:
        msg = self._msg("7.orders.new")
        assert msg.subject == "orders.new"
        assert msg.partitioned_subject == "7.orders.new"
        assert msg.partition == 7
        assert msg.data == b"body"
        assert msg.payload == b"body"

    def test_non_numeric_first_token_has_no_partition(self) -> None:
        assert self._msg("orders.new").partition is None

    def test_repr_mentions_the_stripped_subject(self) -> None:
        assert "orders.new" in repr(self._msg("2.orders.new"))
