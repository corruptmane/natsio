from dataclasses import dataclass
from functools import cached_property
from typing import Final, List, Optional

from natsio.abc.protocol import ServerMessageProto
from natsio.client.config import ServerInfo

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

    @cached_property
    def server_info(self) -> ServerInfo:
        return ServerInfo(
            server_id=self.server_id,
            server_name=self.server_name,
            version=self.version,
            go=self.go,
            host=self.host,
            port=self.port,
            headers=self.headers,
            max_payload=self.max_payload,
            proto=self.proto,
            client_id=self.client_id,
            auth_required=self.auth_required,
            tls_required=self.tls_required,
            tls_verify=self.tls_verify,
            tls_available=self.tls_available,
            connect_urls=self.connect_urls,
            ws_connect_urls=self.ws_connect_urls,
            ldm=self.ldm,
            git_commit=self.git_commit,
            jetstream=self.jetstream,
            ip=self.ip,
            client_ip=self.client_ip,
            nonce=self.nonce,
            cluster=self.cluster,
            domain=self.domain,
        )


__all__ = (
    "INFO_OP",
    "Info",
)
