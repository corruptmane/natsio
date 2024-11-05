from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from functools import cached_property
from typing import TYPE_CHECKING, Mapping, Self, Sequence, cast

from natsio.exceptions.client import MessageAlreadyAckedError
from natsio.utils.json import json_dumps
from natsio.utils.time import from_nanoseconds, to_nanoseconds

from .core import CoreMsg

if TYPE_CHECKING:
    from natsio.client.core import NATSCore
    from natsio.client.jetstream import JetStream


class Ack:
    Ack = b"+ACK"
    Nak = b"-NAK"
    Progress = b"+WPI"
    Term = b"+TERM"


class MetadataTokensV1(int, Enum):
    stream_name = 2
    consumer_name = 3
    num_delivered = 4
    stream_seq = 5
    consumer_seq = 6
    timestamp = 7
    num_pending = 8


class MetadataTokensV2(int, Enum):
    domain = 2
    account_hash = 3
    stream_name = 4
    consumer_name = 5
    num_delivered = 6
    stream_seq = 7
    consumer_seq = 8
    timestamp = 9
    num_pending = 10
    random_token = 11


@dataclass(kw_only=True, slots=True)
class Metadata:
    stream_name: str
    consumer_name: str
    num_delivered: int
    stream_seq: int
    consumer_seq: int
    timestamp: datetime
    num_pending: int
    domain: str | None = None
    account_hash: str | None = None
    random_token: str | None = None

    @classmethod
    def _parse_v1_subject(cls, tokens: Sequence[str]) -> Self:
        return cls(
            stream_name=tokens[MetadataTokensV1.stream_name],
            consumer_name=tokens[MetadataTokensV1.consumer_name],
            num_delivered=int(tokens[MetadataTokensV1.num_delivered]),
            stream_seq=int(tokens[MetadataTokensV1.stream_seq]),
            consumer_seq=int(tokens[MetadataTokensV1.consumer_seq]),
            timestamp=datetime.fromtimestamp(from_nanoseconds(int(tokens[MetadataTokensV1.timestamp]))),
            num_pending=int(tokens[MetadataTokensV1.num_pending]),
        )

    @classmethod
    def _parse_v2_subject(cls, tokens: Sequence[str]) -> Self:
        return cls(
            stream_name=tokens[MetadataTokensV2.stream_name],
            consumer_name=tokens[MetadataTokensV2.consumer_name],
            num_delivered=int(tokens[MetadataTokensV2.num_delivered]),
            stream_seq=int(tokens[MetadataTokensV2.stream_seq]),
            consumer_seq=int(tokens[MetadataTokensV2.consumer_seq]),
            timestamp=datetime.fromtimestamp(from_nanoseconds(int(tokens[MetadataTokensV2.timestamp]))),
            num_pending=int(tokens[MetadataTokensV2.num_pending]),
            domain=str(tokens[MetadataTokensV2.domain]),
            account_hash=str(tokens[MetadataTokensV2.account_hash]),
            random_token=str(tokens[MetadataTokensV2.random_token]),
        )

    @classmethod
    def from_reply_subject(cls, reply_to: str) -> Self:
        tokens = reply_to.split(".")
        if len(tokens) == 9:
            return cls._parse_v1_subject(tokens)
        return cls._parse_v2_subject(tokens)


class JetStreamMsg:
    def __init__(
        self,
        nats: "NATSCore",
        jetstream: "JetStream",
        msg: CoreMsg,
    ) -> None:
        self._nc = nats
        self._js = jetstream
        self._msg = msg
        self._is_acked: bool = False
        self._metadata: Metadata | None

    @property
    def subject(self) -> str:
        return self._msg.subject

    @property
    def payload(self) -> bytes:
        return self._msg.payload

    @property
    def reply_to(self) -> str:
        return cast(str, self._msg.reply_to)

    @property
    def headers(self) -> Mapping[str, str] | None:
        return self._msg.headers

    @cached_property
    def metadata(self) -> Metadata | None:
        return Metadata.from_reply_subject(self.reply_to)

    def _raise_if_already_acked(self) -> None:
        if self._is_acked:
            raise MessageAlreadyAckedError()

    async def reply(self, data: bytes, headers: Mapping[str, str] | None = None) -> None:
        await self._msg.reply(data, headers)

    async def ack(self) -> None:
        self._raise_if_already_acked()
        await self.reply(b"")
        self._is_acked = True

    async def ack_sync(self, timeout: float = 1) -> CoreMsg:
        self._raise_if_already_acked()
        resp = await self._nc.request(self.reply_to, b"", timeout=timeout)
        self._is_acked = True
        return resp

    async def nak(self, delay: float | int | None = None) -> None:
        self._raise_if_already_acked()
        payload = Ack.Nak
        if delay is not None:
            payload = payload + b" " + json_dumps({"delay": to_nanoseconds(delay)})

        await self.reply(payload)
        self._is_acked = True

    async def term(self) -> None:
        self._raise_if_already_acked()
        await self.reply(Ack.Term)
        self._is_acked = True

    async def in_progress(self) -> None:
        await self.reply(Ack.Progress)
