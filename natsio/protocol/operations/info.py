from dataclasses import dataclass
from typing import Final, List, Optional

from natsio.abc.protocol import ServerMessageProto

INFO_OP: Final[bytes] = b"INFO"


@dataclass
class Info(ServerMessageProto):
    server_id: str
    server_name: str
    version: str
    go: str
    host: str
    port: int
    headers: bool
    max_payload: int
    proto: int
    client_id: Optional[int] = None
    auth_required: Optional[bool] = None
    tls_required: Optional[bool] = None
    tls_verify: Optional[bool] = None
    tls_available: Optional[bool] = None
    connect_urls: Optional[List[str]] = None
    ws_connect_urls: Optional[List[str]] = None
    ldm: Optional[bool] = None
    git_commit: Optional[str] = None
    jetstream: Optional[bool] = None
    ip: Optional[str] = None
    client_ip: Optional[str] = None
    nonce: Optional[str] = None
    cluster: Optional[str] = None
    domain: Optional[str] = None


__all__ = (
    "INFO_OP",
    "Info",
)
