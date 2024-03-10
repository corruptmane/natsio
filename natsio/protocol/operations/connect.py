from dataclasses import dataclass
from typing import Final, Optional

from .base import BaseProtocolClientMessage

CONNECT_OP: Final[bytes] = b"CONNECT"


@dataclass
class Connect(BaseProtocolClientMessage):
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

    def build(self) -> bytes:
        # TODO
        msg = b"CONNECT {}\r\n"
        return msg


__all__ = (
    "CONNECT_OP",
    "Connect",
)
