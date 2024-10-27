from typing import TYPE_CHECKING, Any, Final, Mapping, MutableMapping, cast

from natsio.exceptions.jetstream import APIError, ServiceUnavailableError
from natsio.exceptions.protocol import NoRespondersError
from natsio.utils.json import json_dumps, json_loads
from .entities import AccountInfo, StreamInfo, CreateStreamRequest, UpdateStreamRequest

if TYPE_CHECKING:
    from natsio.client.core import NATSCore

JS_API_PREFIX: Final[str] = "$JS{domain}.API"
STREAM_NAME_INVALID_CHARS: Final[set[str]] = set(".*>/\\")
STREAM_NAME_INVALID_CHARS_LIST_PRETTY: Final[str] = ", ".join([
    f'"{char}"' for char in STREAM_NAME_INVALID_CHARS
])


def validate_stream_name(stream_name: str | None) -> None:
    if stream_name is None:
        raise ValueError("stream_name is required")
    if any(char in stream_name for char in STREAM_NAME_INVALID_CHARS):
        raise ValueError(f"stream_name contains one or more invalid characters ({STREAM_NAME_INVALID_CHARS_LIST_PRETTY})")
    if any(char.isspace() for char in stream_name):
        raise ValueError("stream_name contains whitespaces")
    if not stream_name.isprintable():
        raise ValueError("stream_name contains unprintable characters")


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

    async def get_stream_list(self, subject: str | None = None, offset: int = 0) -> list[StreamInfo]:
        data: MutableMapping[str, str | int] = dict(offset=offset)
        if subject is not None:
            data["subject"] = subject
        resp = await self._api_request(f"{self._prefix}.STREAM.LIST", json_dumps(data))
        return [StreamInfo.from_response(**obj) for obj in resp["streams"]]

    async def get_stream_names(self, subject: str | None = None, offset: int = 0) -> list[str] | None:
        data: MutableMapping[str, str | int] = dict(offset=offset)
        if subject is not None:
            data["subject"] = subject
        resp = await self._api_request(f"{self._prefix}.STREAM.NAMES", json_dumps(data))
        return cast(list[str] | None, resp["streams"])

    async def create_stream(self, req: CreateStreamRequest) -> StreamInfo:
        validate_stream_name(req.name)
        resp = await self._api_request(f"{self._prefix}.STREAM.CREATE.{req.name}", json_dumps(req.to_dict()))
        return StreamInfo.from_response(**resp)

    async def update_stream(self, req: UpdateStreamRequest) -> StreamInfo:
        validate_stream_name(req.name)
        resp = await self._api_request(f"{self._prefix}.STREAM.UPDATE.{req.name}", json_dumps(req.to_dict()))
        return StreamInfo.from_response(**resp)

    async def get_stream_info(self, stream_name: str) -> StreamInfo:
        validate_stream_name(stream_name)
        resp = await self._api_request(f"{self._prefix}.STREAM.INFO.{stream_name}")
        return StreamInfo.from_response(**resp)

    async def delete_stream(self, stream_name: str) -> bool:
        validate_stream_name(stream_name)
        resp = await self._api_request(f"{self._prefix}.STREAM.DELETE.{stream_name}")
        return bool(resp["success"])

    async def purge_stream(self, stream_name: str, filter_subject: str | None = None, seq: int | None = None, keep: int | None = None) -> int:
        validate_stream_name(stream_name)
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

    async def _api_request(self, subject: str, data: bytes = b"", timeout: int | float | None = None) -> Mapping[str, Any]:
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
