from dataclasses import dataclass
from functools import cached_property
from typing import Final

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
    client_id: int | None = None
    auth_required: bool | None = None
    tls_required: bool | None = None
    tls_verify: bool | None = None
    tls_available: bool | None = None
    connect_urls: list[str] | None = None
    ws_connect_urls: list[str] | None = None
    ldm: bool | None = None
    git_commit: str | None = None
    jetstream: bool | None = None
    ip: str | None = None
    client_ip: str | None = None
    nonce: str | None = None
    cluster: str | None = None
    domain: str | None = None

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
