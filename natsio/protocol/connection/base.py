import abc
import asyncio

from natsio.protocol.operations.base import BaseProtocolServerMessage


class BaseNATSProtocol(asyncio.Protocol, abc.ABC):
    updates_queue: asyncio.Queue[BaseProtocolServerMessage]
    on_con_made: asyncio.Future
