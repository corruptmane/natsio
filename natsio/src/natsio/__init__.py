"""natsio — zero-dependency asyncio NATS client for modern Python.

import natsio

async with await natsio.connect("nats://localhost:4222") as nc:
    async with nc.subscribe("greet.>") as sub:
        await nc.publish("greet.world", b"hello")
        async for msg in sub:
            await msg.respond(b"hi back")
            break
"""

from importlib.metadata import version as _version

from natsio._internal.lifecycle import (
    Closed,
    Connected,
    ConnectionEvent,
    ConnectionState,
    Disconnected,
    ErrorOccurred,
    LameDuck,
    Reconnected,
    ServersDiscovered,
)
from natsio._internal.protocol import Headers, InlineStatus, StatusCode
from natsio.client import Client, ClientStatistics, connect
from natsio.errors import (
    AuthenticationExpiredError,
    AuthorizationViolationError,
    BadHeadersError,
    ConfigError,
    ConnectionClosedError,
    MaxControlLineExceededError,
    MaxPayloadExceededError,
    MaxSubscriptionsExceededError,
    MissingDependencyError,
    NATSError,
    NoReplySubjectError,
    NoRespondersError,
    NoServersAvailableError,
    ParserError,
    PermissionsViolationError,
    ProtocolError,
    ServerError,
    SlowConsumerError,
    StaleConnectionError,
    SubscriptionClosedError,
    TimeoutError,
)
from natsio.instrumentation import Instrumentation, NoopInstrumentation
from natsio.message import Msg
from natsio.options import ConnectOptions, TLSConfig
from natsio.subscription import PendingLimitPolicy, Subscription

__version__ = _version("natsio")

__all__ = [
    "AuthenticationExpiredError",
    "AuthorizationViolationError",
    "BadHeadersError",
    "Client",
    "ClientStatistics",
    "Closed",
    "ConfigError",
    "ConnectOptions",
    "Connected",
    "ConnectionClosedError",
    "ConnectionEvent",
    "ConnectionState",
    "Disconnected",
    "ErrorOccurred",
    "Headers",
    "InlineStatus",
    "Instrumentation",
    "LameDuck",
    "MaxControlLineExceededError",
    "MaxPayloadExceededError",
    "MaxSubscriptionsExceededError",
    "MissingDependencyError",
    "Msg",
    "NATSError",
    "NoReplySubjectError",
    "NoRespondersError",
    "NoServersAvailableError",
    "NoopInstrumentation",
    "ParserError",
    "PendingLimitPolicy",
    "PermissionsViolationError",
    "ProtocolError",
    "Reconnected",
    "ServerError",
    "ServersDiscovered",
    "SlowConsumerError",
    "StaleConnectionError",
    "StatusCode",
    "Subscription",
    "SubscriptionClosedError",
    "TLSConfig",
    "TimeoutError",
    "__version__",
    "connect",
]
