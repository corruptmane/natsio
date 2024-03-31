from dataclasses import dataclass, field
from functools import cached_property
from random import shuffle
from ssl import SSLContext
from typing import Final, List, Optional, Tuple
from urllib.parse import ParseResult, urlparse

from natsio import __version__ as natsio_version
from natsio.exceptions.client import (
    ConfigError,
    NoServersProvided,
    TLSNotConfigured,
    WebSocketError,
)
from natsio.protocol.operations.connect import Connect

DEFAULT_CONNECT_TIMEOUT: Final[float] = 5
DEFAULT_RECONNECT_TIME_WAIT: Final[float] = 2
DEFAULT_MAX_RECONNECT_ATTEMPTS: Final[int] = 60
DEFAULT_PING_INTERVAL: Final[int] = 120
DEFAULT_MAX_OUTSTANDING_PINGS: Final[int] = 2
DEFAULT_MAX_FLUSHER_QUEUE_SIZE: Final[int] = 1024
DEFAULT_DRAIN_TIMEOUT: Final[float] = 30
DEFAULT_FLUSH_TIMEOUT: Final[int] = 10
DEFAULT_MAX_PENDING_SIZE: Final[int] = 2 * 1024 * 1024


@dataclass
class TLSConfig:
    ssl: SSLContext
    hostname: Optional[str] = None
    handshake_first: bool = False


@dataclass
class ServerInfo:
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


@dataclass
class Server:
    uri: ParseResult
    reconnects: int = 0
    last_attempt: int = 0
    info: Optional[ServerInfo] = None

    @property
    def is_discovered(self) -> bool:
        return bool(self.info)


@dataclass
class ClientConfig:
    servers: List[str] = field(default_factory=lambda: ["nats://localhost:4222"])
    name: Optional[str] = None
    pedantic: bool = True
    verbose: bool = False
    allow_reconnect: bool = True
    reconnect_time_wait: float = DEFAULT_RECONNECT_TIME_WAIT
    connection_timeout: float = DEFAULT_CONNECT_TIMEOUT
    drain_timeout: float = DEFAULT_DRAIN_TIMEOUT
    flush_timeout: int = DEFAULT_FLUSH_TIMEOUT
    flusher_queue_size: int = DEFAULT_MAX_FLUSHER_QUEUE_SIZE
    max_pending_size: int = DEFAULT_MAX_PENDING_SIZE
    max_reconnect_attempts: int = DEFAULT_MAX_RECONNECT_ATTEMPTS
    max_outstanding_pings: int = DEFAULT_MAX_OUTSTANDING_PINGS
    ping_interval: int = DEFAULT_PING_INTERVAL
    randomize_servers: bool = False
    echo: bool = True
    tls: Optional[TLSConfig] = None
    tls_required: bool = False
    user: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    inbox_prefix: str = "_INBOX"

    def _build_single_server(self, server_url: str) -> Server:
        if server_url.startswith("nats://"):
            uri = urlparse(server_url)
        elif server_url.startswith("ws://") or server_url.startswith("wss://"):
            raise WebSocketError()
        elif server_url.startswith("tls://"):
            if not self.tls:
                raise TLSNotConfigured()
            uri = urlparse(server_url)
        elif ":" in server_url:
            uri = urlparse(f"nats://{server_url}")
        else:
            raise ConfigError(f"Invalid server URL: {server_url}")
        if uri.hostname is None or uri.hostname == "none":
            raise ConfigError(f"Invalid server hostname: {server_url}")
        if uri.port is None:
            uri = urlparse(f"nats://{uri.hostname}:4222")
        return Server(uri=uri)

    @cached_property
    def server_pool(self) -> Tuple[Server, ...]:
        if not self.servers:
            raise NoServersProvided()
        parsed_servers: List[Server] = []
        for server in self.servers:
            parsed_servers.append(self._build_single_server(server))
        if self.randomize_servers:
            shuffle(parsed_servers)
        return tuple(parsed_servers)

    def build_connect_operation(self) -> Connect:
        return Connect(
            verbose=self.verbose,
            pedantic=self.pedantic,
            tls_required=self.tls_required,
            lang="python/natsio",
            version=natsio_version,
            auth_token=self.token,
            user=self.user,
            password=self.password,
            name=self.name,
            protocol=1,
            echo=self.echo,
        )
