from typing import TYPE_CHECKING, Any, Final, Mapping, MutableMapping, cast

from natsio.exceptions.jetstream import APIError, ServiceUnavailableError
from natsio.exceptions.protocol import NoRespondersError
from natsio.utils.json import json_dumps, json_loads
from .entities import AccountInfo, StreamInfo

if TYPE_CHECKING:
    from natsio.client.core import NATSCore

JS_API_PREFIX: Final[str] = "$JS{domain}.API"


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
        resp = await self._api_request("{self._prefix}.STREAM.LIST", json_dumps(data))
        return [StreamInfo.from_response(**obj) for obj in resp["streams"]]

    async def _api_request(self, subject: str, data: bytes = b"", timeout: int | float | None = None) -> Mapping[str, Any]:
        if timeout is None:
            timeout = self.timeout

        try:
            msg = await self._nc.request(subject, data, timeout=timeout)
        except NoRespondersError:
            raise ServiceUnavailableError()

        resp = cast(Mapping[str, Any], json_loads(msg.payload))
        if "error" in resp:
            raise APIError.from_error(**resp)

        return resp
