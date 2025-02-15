from datetime import datetime, timedelta, timezone

from natsio.client.jetstream.new_entities import (
    StreamConfig,
    Retention,
    Storage,
    Compression,
    Discard,
    ConsumerInfo,
    PushConsumerConfig,
    PullConsumerConfig,
    DeliverPolicy,
    AckPolicy,
    ReplayPolicy,
    SequenceInfo,
    Placement,
    Republish,
    ConsumerLimits,
    Cluster,
    Replica,
)
from natsio.utils.time import to_nanoseconds


def test_stream_config_serialization_format():
    config = StreamConfig(
        name="test-stream",
        description="Test stream",
        subjects=["test.*"],
        retention=Retention.limits,
        max_consumers=10,
        max_msgs=1000,
        max_bytes=1024 * 1024,
        max_age=timedelta(hours=24),
        storage=Storage.file,
        num_replicas=3,
        no_ack=False,
        discard=Discard.old,
        duplicate_window=timedelta(minutes=2),
    )

    serialized = config.to_dict()

    assert serialized["max_age"] == to_nanoseconds(24 * 60 * 60)
    assert serialized["duplicate_window"] == to_nanoseconds(2 * 60)
    assert serialized["retention"] == "limits"
    assert serialized["storage"] == "file"
    assert serialized["discard"] == "old"

    expected_dict = {
        "name": "test-stream",
        "description": "Test stream",
        "subjects": ["test.*"],
        "retention": "limits",
        "max_consumers": 10,
        "max_msgs": 1000,
        "max_bytes": 1024 * 1024,
        "max_age": to_nanoseconds(24 * 60 * 60),
        "storage": "file",
        "num_replicas": 3,
        "no_ack": False,
        "discard": "old",
        "duplicate_window": to_nanoseconds(2 * 60),
        "max_msgs_per_subject": -1,
        "compression": "none",
    }

    for key, expected_value in expected_dict.items():
        assert serialized[key] == expected_value, (
            f"Mismatch in {key}: got {serialized[key]}, expected {expected_value}"
        )


def test_consumer_info_serialization_format():
    created_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    last_active_dt = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)

    push_config = PushConsumerConfig(
        name="push-consumer",
        deliver_subject="delivery.subject",
        deliver_policy=DeliverPolicy.all,
        ack_policy=AckPolicy.explicit,
        replay_policy=ReplayPolicy.instant,
        deliver_group="group1",
        flow_control=True,
    )

    consumer_info = ConsumerInfo(
        stream_name="test-stream",
        name="push-consumer",
        config=push_config,
        created=created_dt,
        delivered=SequenceInfo(
            consumer_seq=1,
            stream_seq=1,
            last_active=last_active_dt,
        ),
        ack_floor=SequenceInfo(
            consumer_seq=1,
            stream_seq=1,
            last_active=last_active_dt,
        ),
        num_ack_pending=0,
        num_redelivered=0,
        num_waiting=0,
        num_pending=0,
    )

    serialized = consumer_info.to_dict()

    assert serialized["created"] == "2024-01-01T00:00:00+00:00"
    assert (
        serialized["delivered"]["last_active"] == "2024-01-01T12:00:00+00:00"
    )

    config_dict = serialized["config"]
    assert config_dict["name"] == "push-consumer"
    assert config_dict["deliver_subject"] == "delivery.subject"
    assert config_dict["deliver_policy"] == "all"
    assert config_dict["ack_policy"] == "explicit"
    assert config_dict["replay_policy"] == "instant"
    assert config_dict["deliver_group"] == "group1"
    assert config_dict["flow_control"] is True


def test_cluster_serialization_format():
    replica = Replica(
        name="node1",
        current=True,
        active=timedelta(seconds=3600),
        lag=0,
    )

    serialized_replica = replica.to_dict()
    expected_replica = {
        "name": "node1",
        "current": True,
        "active": to_nanoseconds(3600),
        "lag": 0,
        "observer": False,
        "offline": False,
    }
    assert serialized_replica == expected_replica, (
        "Replica serialization mismatch"
    )

    cluster = Cluster(
        name="main-cluster",
        leader="node1",
        replicas=[replica],
        raft_group="group1",
    )

    serialized = cluster.to_dict()

    expected_dict = {
        "name": "main-cluster",
        "leader": "node1",
        "raft_group": "group1",
        "replicas": [expected_replica],
    }

    assert serialized == expected_dict, (
        f"Dictionary mismatch:\nGot: {serialized}\nExpected: {expected_dict}"
    )


# def test_cluster_serialization_format():
#     """Test that ensures Cluster serializes timedelta correctly in replicas."""
#     cluster = Cluster(
#         name="main-cluster",
#         leader="node1",
#         replicas=[
#             Replica(
#                 name="node1",
#                 current=True,
#                 active=timedelta(seconds=3600),
#                 lag=0,
#             ),
#         ],
#         raft_group="group1",
#     )
#
#     serialized = cluster.to_dict()
#
#     assert serialized["replicas"][0]["active"] == to_nanoseconds(3600)
#
#     expected_replica = {
#         "name": "node1",
#         "current": True,
#         "active": to_nanoseconds(3600),
#         "lag": 0,
#         "observer": False,
#         "offline": False,
#     }
#
#     assert serialized["replicas"][0] == expected_replica
#     assert serialized["name"] == "main-cluster"
#     assert serialized["leader"] == "node1"
#     assert serialized["raft_group"] == "group1"


def test_basic_stream_config():
    config = StreamConfig(
        name="test-stream",
        description="Test stream",
        subjects=["test.*"],
        retention=Retention.limits,
        max_consumers=10,
        max_msgs=1000,
        max_bytes=1024 * 1024,
        max_age=timedelta(hours=24),
        storage=Storage.file,
        num_replicas=3,
        no_ack=False,
        discard=Discard.old,
        duplicate_window=timedelta(minutes=2),
    )

    serialized = config.to_dict()
    deserialized = StreamConfig.from_response(**serialized)

    assert deserialized == config


def test_stream_config_with_optional_fields():
    config_minimal = StreamConfig(
        retention=Retention.limits,
        max_consumers=5,
        max_msgs=1000,
        max_bytes=1024,
        max_age=timedelta(hours=1),
        storage=Storage.memory,
        num_replicas=1,
    )

    serialized = config_minimal.to_dict()
    deserialized = StreamConfig.from_response(**serialized)

    assert deserialized == config_minimal

    config_full = StreamConfig(
        retention=Retention.limits,
        max_consumers=5,
        max_msgs=1000,
        max_bytes=1024,
        max_age=timedelta(hours=1),
        storage=Storage.memory,
        num_replicas=1,
        name="optional-test",
        description="Test with optional fields",
        max_msg_size=1024,
        compression=Compression.s2,
        metadata={"key": "value"},
    )

    serialized_full = config_full.to_dict()
    deserialized_full = StreamConfig.from_response(**serialized_full)

    assert deserialized_full == config_full


def test_nested_structures():
    config = StreamConfig(
        retention=Retention.limits,
        max_consumers=5,
        max_msgs=1000,
        max_bytes=1024,
        max_age=timedelta(hours=1),
        storage=Storage.memory,
        num_replicas=1,
        placement=Placement(
            cluster="main-cluster",
            tags=["tag1", "tag2"],
        ),
        republish=Republish(
            src="source.>",
            dest="destination.>",
            headers_only=True,
        ),
        consumer_limits=ConsumerLimits(
            inactive_threshold=timedelta(minutes=30),
            max_ack_pending=100,
        ),
    )

    serialized = config.to_dict()
    deserialized = StreamConfig.from_response(**serialized)

    assert deserialized == config


def test_polymorphic_consumer_config():
    push_config = PushConsumerConfig(
        name="push-consumer",
        deliver_subject="delivery.subject",
        deliver_policy=DeliverPolicy.all,
        ack_policy=AckPolicy.explicit,
        replay_policy=ReplayPolicy.instant,
        deliver_group="group1",
        flow_control=True,
    )

    push_info = ConsumerInfo(
        stream_name="test-stream",
        name="push-consumer",
        config=push_config,
        created=datetime(2024, 1, 1),
        delivered=SequenceInfo(
            consumer_seq=1,
            stream_seq=1,
            last_active=datetime(2024, 1, 1, 12, 0),
        ),
        ack_floor=SequenceInfo(
            consumer_seq=1,
            stream_seq=1,
            last_active=datetime(2024, 1, 1, 12, 0),
        ),
        num_ack_pending=0,
        num_redelivered=0,
        num_waiting=0,
        num_pending=0,
    )

    serialized_push = push_info.to_dict()
    deserialized_push = ConsumerInfo.from_response(**serialized_push)

    assert deserialized_push == push_info

    pull_config = PullConsumerConfig(
        name="pull-consumer",
        deliver_policy=DeliverPolicy.all,
        ack_policy=AckPolicy.explicit,
        replay_policy=ReplayPolicy.instant,
        max_batch=100,
        max_expires=30000,
    )

    pull_info = ConsumerInfo(
        stream_name="test-stream",
        name="pull-consumer",
        config=pull_config,
        created=datetime(2024, 1, 1),
        delivered=SequenceInfo(
            consumer_seq=1,
            stream_seq=1,
            last_active=datetime(2024, 1, 1, 12, 0),
        ),
        ack_floor=SequenceInfo(
            consumer_seq=1,
            stream_seq=1,
            last_active=datetime(2024, 1, 1, 12, 0),
        ),
        num_ack_pending=0,
        num_redelivered=0,
        num_waiting=0,
        num_pending=0,
    )

    serialized_pull = pull_info.to_dict()
    deserialized_pull = ConsumerInfo.from_response(**serialized_pull)

    assert deserialized_pull == pull_info


def test_complex_cluster_info():
    """Test serialization of complex cluster structures."""
    cluster = Cluster(
        name="main-cluster",
        leader="node1",
        replicas=[
            Replica(
                name="node1",
                current=True,
                active=timedelta(seconds=3600),
                lag=0,
            ),
            Replica(
                name="node2",
                current=False,
                active=timedelta(seconds=3500),
                lag=100,
            ),
        ],
        raft_group="group1",
    )

    serialized = cluster.to_dict()
    deserialized = Cluster.from_response(**serialized)

    assert deserialized == cluster
