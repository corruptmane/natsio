from dataclasses import dataclass, field
from functools import cached_property
from random import shuffle
from ssl import SSLContext
from typing import Final
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
DEFAULT_REQUEST_TIMEOUT: Final[int] = 5
DEFAULT_MAX_PENDING_SIZE: Final[int] = 2 * 1024 * 1024


@dataclass
class TLSConfig:
    ssl: SSLContext
    hostname: str | None = None
    handshake_first: bool = False


class ServerVersion:
    def __init__(self, version: str) -> None:
        self._server_version = version
        self._major: int | None = None
        self._minor: int | None = None
        self._patch: int | None = None
        self._dev: int | None = None
        self.parse()

    def parse(self) -> None:
        sv = self._server_version.split("-")
        if len(sv) > 1:
            self._dev = int(sv[1])
        tokens = sv[0].split(".")
        tokens_count = len(tokens)
        if tokens_count >= 1:
            self._major = int(tokens[0])
        if tokens_count >= 2:
            self._minor = int(tokens[1])
        if tokens_count >= 3:
            self._patch = int(tokens[2])

    @property
    def major(self) -> int:
        return self._major or 0

    @property
    def minor(self) -> int:
        return self._minor or 0

    @property
    def patch(self) -> int:
        return self._patch or 0

    @property
    def dev(self) -> int:
        return self._dev or 0


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
    def server_version(self) -> ServerVersion:
        return ServerVersion(self.version)


@dataclass
class Server:
    uri: ParseResult
    reconnects: int = 0
    last_attempt: int = 0
    info: ServerInfo | None = None

    @property
    def is_discovered(self) -> bool:
        return bool(self.info)


@dataclass
class ClientConfig:
    servers: list[str] = field(default_factory=lambda: ["nats://localhost:4222"])
    name: str | None = None
    pedantic: bool = True
    verbose: bool = False
    allow_reconnect: bool = True
    reconnect_time_wait: float = DEFAULT_RECONNECT_TIME_WAIT
    connection_timeout: float = DEFAULT_CONNECT_TIMEOUT
    drain_timeout: float = DEFAULT_DRAIN_TIMEOUT
    flush_timeout: int = DEFAULT_FLUSH_TIMEOUT
    request_timeout: float | int = DEFAULT_REQUEST_TIMEOUT
    flusher_queue_size: int = DEFAULT_MAX_FLUSHER_QUEUE_SIZE
    max_pending_size: int = DEFAULT_MAX_PENDING_SIZE
    max_reconnect_attempts: int = DEFAULT_MAX_RECONNECT_ATTEMPTS
    max_outstanding_pings: int = DEFAULT_MAX_OUTSTANDING_PINGS
    ping_interval: int = DEFAULT_PING_INTERVAL
    randomize_servers: bool = False
    echo: bool = True
    tls: TLSConfig | None = None
    tls_required: bool = False
    user: str | None = None
    password: str | None = None
    token: str | None = None
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
    def server_pool(self) -> tuple[Server, ...]:
        if not self.servers:
            raise NoServersProvided()
        parsed_servers: list[Server] = []
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
