from dataclasses import dataclass
from typing import Optional

from .base import BaseProtocolClientMessage


@dataclass(slots=True)
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
        msg = f"CONNECT {{}}\r\n"
        return msg.encode()


__all__ = (
    "Connect",
)
