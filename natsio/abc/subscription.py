from typing import TypeVar, Protocol

from natsio.messages.core import CoreMsg
from natsio.messages.jetstream import JetStreamMsg


MsgType = TypeVar("MsgType", CoreMsg, JetStreamMsg, contravariant=True)


class Callback(Protocol[MsgType]):
    async def __call__(self, msg: MsgType) -> None:
        raise NotImplementedError


CoreCallback = Callback[CoreMsg]
JetStreamCallback = Callback[JetStreamMsg]


__all__ = (
    "CoreCallback",
    "JetStreamCallback",
)
