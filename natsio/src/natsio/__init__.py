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
from pkgutil import extend_path

# natsio is a regular package (this file gives it the flat top-level API), but
# extension distributions — natsio-<name>, imported as natsio.<name> — install
# additional subpackages into the natsio namespace. extend_path lets those
# subpackages be found even when an extension lands on a different sys.path
# root than the core (editable installs, split site-packages).
__path__ = extend_path(__path__, __name__)

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
from natsio._internal.protocol import Headers, HeadersInput, InlineStatus, StatusCode
from natsio.auth import (
    Authenticator,
    AuthResult,
    CallbackAuth,
    CredsAuth,
    CredsFileAuth,
    NKeyAuth,
    TokenAuth,
    UserPasswordAuth,
)
from natsio.client import Client, ClientStatistics, connect
from natsio.errors import (
    AuthenticationExpiredError,
    AuthorizationViolationError,
    BadHeadersError,
    ConfigError,
    ConnectionClosedError,
    DrainTimeoutError,
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
from natsio.options import ConnectKwargs, ConnectOptions, TLSConfig
from natsio.subscription import Callback, PendingLimitPolicy, Subscription

__version__ = _version("natsio")

__all__ = [
    "AuthResult",
    "AuthenticationExpiredError",
    "Authenticator",
    "AuthorizationViolationError",
    "BadHeadersError",
    "Callback",
    "CallbackAuth",
    "Client",
    "ClientStatistics",
    "Closed",
    "ConfigError",
    "ConnectKwargs",
    "ConnectOptions",
    "Connected",
    "ConnectionClosedError",
    "ConnectionEvent",
    "ConnectionState",
    "CredsAuth",
    "CredsFileAuth",
    "Disconnected",
    "DrainTimeoutError",
    "ErrorOccurred",
    "Headers",
    "HeadersInput",
    "InlineStatus",
    "Instrumentation",
    "LameDuck",
    "MaxControlLineExceededError",
    "MaxPayloadExceededError",
    "MaxSubscriptionsExceededError",
    "MissingDependencyError",
    "Msg",
    "NATSError",
    "NKeyAuth",
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
    "TokenAuth",
    "UserPasswordAuth",
    "__version__",
    "connect",
]
