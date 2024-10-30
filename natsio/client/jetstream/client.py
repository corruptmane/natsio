import logging
from typing import TYPE_CHECKING, Any, Final, Mapping, MutableMapping, cast

from natsio.exceptions.jetstream import APIError, ServiceUnavailableError
from natsio.exceptions.protocol import NoRespondersError
from natsio.utils.json import json_dumps, json_loads
from .entities import (
    AccountInfo,
    ConsumerInfo,
    ConsumerList,
    GetMsgRequest,
    PullConsumerConfig,
    PushConsumerConfig,
    RawMsg,
    StreamInfo,
    CreateStreamRequest,
    StreamList,
    UpdateStreamRequest,
)

if TYPE_CHECKING:
    from natsio.client.core import NATSCore

log = logging.getLogger(__name__)

JS_API_PREFIX: Final[str] = "$JS{domain}.API"
NAME_INVALID_CHARS: Final[set[str]] = set(".*>/\\")
NAME_INVALID_CHARS_LIST_PRETTY: Final[str] = ", ".join(
    [f'"{char}"' for char in NAME_INVALID_CHARS]
)


def validate_name(name: str | None) -> None:
    if name is None:
        raise ValueError("Name is required")
    if any(char in name for char in NAME_INVALID_CHARS):
        raise ValueError(
            f"Name contains one or more invalid characters ({NAME_INVALID_CHARS_LIST_PRETTY})"
        )
    if any(char.isspace() for char in name):
        raise ValueError("Name contains whitespaces")
    if not name.isprintable():
        raise ValueError("Name contains unprintable characters")


class JetStream:
    def __init__(
        self,
        core: "NATSCore",
        domain: str | None = None,
        timeout: float | int = 5,
    ) -> None:
        self._nc = core
        if domain is None:
            self._prefix = JS_API_PREFIX.format(domain="")
        else:
            self._prefix = JS_API_PREFIX.format(domain=f".{domain}")
        self.timeout = timeout

    async def get_account_info(self) -> AccountInfo:
        resp = await self._api_request(f"{self._prefix}.INFO")
        return AccountInfo.from_response(**resp)

    async def get_stream_list(self, subject: str | None = None, offset: int = 0) -> StreamList:
        data: MutableMapping[str, str | int] = dict(offset=offset)
        if subject is not None:
            data["subject"] = subject
        resp = await self._api_request(f"{self._prefix}.STREAM.LIST", json_dumps(data))
        return StreamList.from_response(**resp)

    async def get_stream_names(
        self, subject: str | None = None, offset: int = 0
    ) -> list[str] | None:
        data: MutableMapping[str, str | int] = dict(offset=offset)
        if subject is not None:
            data["subject"] = subject
        resp = await self._api_request(f"{self._prefix}.STREAM.NAMES", json_dumps(data))
        return cast(list[str] | None, resp["streams"])

    async def create_stream(self, req: CreateStreamRequest) -> StreamInfo:
        validate_name(req.name)
        resp = await self._api_request(
            f"{self._prefix}.STREAM.CREATE.{req.name}", json_dumps(req.to_dict())
        )
        return StreamInfo.from_response(**resp)

    async def update_stream(self, req: UpdateStreamRequest) -> StreamInfo:
        validate_name(req.name)
        resp = await self._api_request(
            f"{self._prefix}.STREAM.UPDATE.{req.name}", json_dumps(req.to_dict())
        )
        return StreamInfo.from_response(**resp)

    async def get_stream_info(self, stream_name: str) -> StreamInfo:
        validate_name(stream_name)
        resp = await self._api_request(f"{self._prefix}.STREAM.INFO.{stream_name}")
        return StreamInfo.from_response(**resp)

    async def delete_stream(self, stream_name: str) -> bool:
        validate_name(stream_name)
        resp = await self._api_request(f"{self._prefix}.STREAM.DELETE.{stream_name}")
        return bool(resp["success"])

    async def purge_stream(
        self,
        stream_name: str,
        filter_subject: str | None = None,
        seq: int | None = None,
        keep: int | None = None,
    ) -> int:
        validate_name(stream_name)
        if seq and keep:
            raise ValueError("`seq` and `keep` arguments can not be combined")

        data: MutableMapping[str, str | int] = {}
        if filter_subject:
            data["filter"] = filter_subject
        if seq:
            data["seq"] = seq
        if keep:
            data["keep"] = keep
        payload = json_dumps(data) if data else b""

        resp = await self._api_request(f"{self._prefix}.STREAM.PURGE.{stream_name}", payload)
        return bool(resp["purged"])

    async def _get_msg(self, stream_name: str, payload: Mapping[str, Any]) -> RawMsg:
        log.info("Request payload: %s", payload)
        resp = await self._api_request(
            f"{self._prefix}.STREAM.MSG.GET.{stream_name}", json_dumps(payload)
        )
        return RawMsg.from_response(**resp["message"])

    async def get_msg(self, stream_name: str, req: GetMsgRequest) -> RawMsg:
        validate_name(stream_name)

        payload: MutableMapping[str, str | int | list[str]] = {}
        if req.seq and req.last_by_subj:
            raise ValueError("`seq` and `last_by_subj` properties can not be combined")
        if req.seq is None and req.last_by_subj is None:
            raise ValueError("One of `seq` and `last_by_subj` must be specified")
        if req.seq is not None:
            payload["seq"] = req.seq
        if req.last_by_subj is not None:
            payload["last_by_subj"] = req.last_by_subj
        if req.next_by_subj is not None:
            payload["next_by_subj"] = req.next_by_subj
        if req.batch is not None:
            payload["batch"] = req.batch
        if req.max_bytes is not None:
            payload["max_bytes"] = req.max_bytes
        if req.start_time is not None:
            payload["start_time"] = req.render_start_time()  # type: ignore[assignment]
        if req.multi_last is not None:
            payload["multi_last"] = req.multi_last
        if req.up_to_seq is not None:
            payload["up_to_seq"] = req.up_to_seq
        if req.up_to_time is not None:
            payload["up_to_time"] = req.render_up_to_time()  # type: ignore[assignment]

        return await self._get_msg(stream_name, payload)

    async def create_or_update_consumer(
        self,
        stream_name: str,
        config: PushConsumerConfig | PullConsumerConfig,
    ) -> ConsumerInfo:
        validate_name(stream_name)
        if config.durable_name is not None:
            validate_name(config.durable_name)
        if not config.filter_subject and not config.filter_subjects:
            raise ValueError("One of `filter_subject` and `filter_subjects` must be specified")

        current_server_version = self._nc.current_server_version
        is_new = current_server_version.major >= 2 and current_server_version.minor >= 9
        if is_new and config.name:
            if config.filter_subject and config.filter_subject != ">":
                subj = f"{self._prefix}.CONSUMER.CREATE.{stream_name}.{config.name}.{config.filter_subject}"
            else:
                subj = f"{self._prefix}.CONSUMER.CREATE.{stream_name}.{config.name}"
        elif config.durable_name:
            subj = f"{self._prefix}.CONSUMER.DURABLE.CREATE.{stream_name}.{config.durable_name}"
        else:
            subj = f"{self._prefix}.CONSUMER.CREATE.{stream_name}"

        payload = {"stream_name": stream_name, "config": config.to_dict()}
        resp = await self._api_request(subj, json_dumps(payload))
        return ConsumerInfo.from_response(**resp)

    async def get_consumer_list(self, stream_name: str, offset: int = 0) -> ConsumerList:
        validate_name(stream_name)
        resp = await self._api_request(
            f"{self._prefix}.CONSUMER.LIST.{stream_name}", json_dumps({"offset": offset})
        )
        return ConsumerList.from_response(**resp)

    async def _api_request(
        self, subject: str, data: bytes = b"", timeout: int | float | None = None
    ) -> Mapping[str, Any]:
        if timeout is None:
            timeout = self.timeout

        try:
            msg = await self._nc.request(subject, data, timeout=timeout)
        except NoRespondersError:
            raise ServiceUnavailableError()

        resp = cast(Mapping[str, Any], json_loads(msg.payload))
        if "error" in resp:
            raise APIError.from_error(**resp["error"])

        return resp
