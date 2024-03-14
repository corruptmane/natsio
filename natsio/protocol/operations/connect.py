from dataclasses import dataclass
from typing import Final, Optional, Union

from natsio.abc.protocol import ClientMessageProto
from natsio.const import CRLF
from natsio.utils.json import json_dumps

CONNECT_OP: Final[bytes] = b"CONNECT"


@dataclass
class Connect(ClientMessageProto):
    verbose: bool
    pedantic: bool
    tls_required: bool
    lang: str
    version: str
    auth_token: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    name: Optional[str] = None
    protocol: Optional[int] = None
    echo: Optional[bool] = None
    sig: Optional[str] = None
    jwt: Optional[str] = None
    no_responders: Optional[bool] = None
    headers: Optional[bool] = None
    nkey: Optional[str] = None

    def _build_payload(self) -> bytes:
        payload: dict[str, Union[str, bool, int]] = dict(
            verbose=self.verbose,
            pedantic=self.pedantic,
            tls_required=self.tls_required,
            lang=self.lang,
            version=self.version,
        )
        if self.auth_token is not None:
            payload["auth_token"] = self.auth_token
        if self.user is not None:
            payload["user"] = self.user
        if self.password is not None:
            payload["pass"] = self.password
        if self.name is not None:
            payload["name"] = self.name
        if self.protocol is not None:
            payload["protocol"] = self.protocol
        if self.echo is not None:
            payload["echo"] = self.echo
        if self.sig is not None:
            payload["sig"] = self.sig
        if self.jwt is not None:
            payload["jwt"] = self.jwt
        if self.no_responders is not None:
            payload["no_responders"] = self.no_responders
        if self.headers is not None:
            payload["headers"] = self.headers
        if self.nkey is not None:
            payload["nkey"] = self.nkey
        return json_dumps(payload)

    def build(self) -> bytes:
        return CONNECT_OP + b" " + self._build_payload() + CRLF


__all__ = (
    "CONNECT_OP",
    "Connect",
)
