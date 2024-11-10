from enum import Enum
from typing import TypeVar, Protocol

from natsio.messages.core import CoreMsg
from natsio.messages.jetstream import JetStreamMsg


MsgType = TypeVar("MsgType", CoreMsg, JetStreamMsg, contravariant=True)


class Callback(Protocol[MsgType]):
    async def __call__(self, msg: MsgType) -> None:
        raise NotImplementedError


CoreCallback = Callback[CoreMsg]
JetStreamCallback = Callback[JetStreamMsg]


class SubscriptionStatus(Enum):
    INITIALISING = "INITIALISING"
    OPERATING = "OPERATING"
    DRAINING = "DRAINING"
    CLOSED = "CLOSED"


class SubscriptionProto(Protocol):
    sid: str
    subject: str
    queue: str | None

    @property
    def is_ready_to_close(self) -> bool:
        raise NotImplementedError

    async def add_msg(self, msg: CoreMsg) -> None:
        raise NotImplementedError

    async def unsubscribe(self, max_msgs: int = 0) -> None:
        raise NotImplementedError


__all__ = (
    "CoreCallback",
    "JetStreamCallback",
    "SubscriptionStatus",
    "SubscriptionProto",
)
