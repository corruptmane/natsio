"""JetStream: the ADR-37 simplified API (streams, pull consumers, KV later).

js = nc.jetstream()
stream = await js.create_stream(StreamConfig(name="ORDERS", subjects=["orders.>"]))
await js.publish("orders.new", b"...", msg_id="o-1")

consumer = await stream.create_consumer(ConsumerConfig(durable_name="worker"))
async with consumer.consume() as messages:
    async for msg in messages:
        await msg.ack()
"""

from natsio.jetstream import headers
from natsio.jetstream.consumer import Consumer, Consumption, OrderedConsumer
from natsio.jetstream.context import JetStreamContext
from natsio.jetstream.entities import (
    AccountInfo,
    AckPolicy,
    ClusterInfo,
    ConsumerConfig,
    ConsumerInfo,
    ConsumerLimits,
    DeliverPolicy,
    DiscardPolicy,
    External,
    Placement,
    PriorityPolicy,
    PubAck,
    ReplayPolicy,
    Republish,
    RetentionPolicy,
    SequenceInfo,
    StorageCompression,
    StorageType,
    StreamConfig,
    StreamInfo,
    StreamSource,
    StreamState,
    SubjectTransform,
)
from natsio.jetstream.errors import (
    APIError,
    ConsumerDeletedError,
    ConsumerNotFoundError,
    JetStreamError,
    JetStreamNotEnabledError,
    MessageNotFoundError,
    NoMessagesError,
    NoStreamResponseError,
    StreamNameInUseError,
    StreamNotFoundError,
    WrongLastSequenceError,
)
from natsio.jetstream.message import AckMetadata, JsMsg, MessageAlreadyAckedError
from natsio.jetstream.stream import StoredMsg, Stream

__all__ = [
    "APIError",
    "AccountInfo",
    "AckMetadata",
    "AckPolicy",
    "ClusterInfo",
    "Consumer",
    "ConsumerConfig",
    "ConsumerDeletedError",
    "ConsumerInfo",
    "ConsumerLimits",
    "ConsumerNotFoundError",
    "Consumption",
    "DeliverPolicy",
    "DiscardPolicy",
    "External",
    "JetStreamContext",
    "JetStreamError",
    "JetStreamNotEnabledError",
    "JsMsg",
    "MessageAlreadyAckedError",
    "MessageNotFoundError",
    "NoMessagesError",
    "NoStreamResponseError",
    "OrderedConsumer",
    "Placement",
    "PriorityPolicy",
    "PubAck",
    "ReplayPolicy",
    "Republish",
    "RetentionPolicy",
    "SequenceInfo",
    "StorageCompression",
    "StorageType",
    "StoredMsg",
    "Stream",
    "StreamConfig",
    "StreamInfo",
    "StreamNameInUseError",
    "StreamNotFoundError",
    "StreamSource",
    "StreamState",
    "SubjectTransform",
    "WrongLastSequenceError",
    "headers",
]
