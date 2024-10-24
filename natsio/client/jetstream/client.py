from typing import TYPE_CHECKING, Any, Final, Mapping

from natsio.exceptions.jetstream import APIError, ServiceUnavailableError
from natsio.exceptions.protocol import NoRespondersError
from natsio.utils.json import json_loads
from .entities import AccountInfo

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
        subj = f"{self._prefix}.INFO"
        resp = await self._api_request(subj)
        return AccountInfo.from_response(**resp)

    async def _api_request(self, subject: str, data: bytes = b"", timeout: int | float | None = None) -> Mapping[str, Any]:
        if timeout is None:
            timeout = self.timeout

        try:
            msg = await self._nc.request(subject, data, timeout=timeout)
        except NoRespondersError:
            raise ServiceUnavailableError()

        resp = json_loads(msg.payload)
        if "error" in resp:
            raise APIError.from_error(**resp)

        return resp
