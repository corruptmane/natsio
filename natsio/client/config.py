from dataclasses import dataclass, field
from functools import cached_property
from ssl import OP_ALL, SSLContext
from typing import Final, List, Optional, Tuple
from urllib.parse import ParseResult, urlparse
from random import shuffle

DEFAULT_CONNECT_TIMEOUT: Final[float] = 5
DEFAULT_RECONNECT_TIMEOUT: Final[float] = 2
DEFAULT_MAX_RECONNECT_ATTEMPTS: Final[int] = 60
DEFAULT_PING_INTERVAL: Final[int] = 120
DEFAULT_MAX_OUTSTANDING_PINGS: Final[int] = 2
DEFAULT_MAX_FLUSHER_QUEUE_SIZE: Final[int] = 1024
DEFAULT_DRAIN_TIMEOUT: Final[float] = 30
DEFAULT_PENDING_SIZE: Final[int] = 2 * 1024 * 1024


@dataclass
class TLSConfig:
    ssl: SSLContext
    hostname: Optional[str] = None
    handshake_first: bool = False


@dataclass
class Server:
    uri: ParseResult
    reconnects: int = 0
    last_attempt: int = 0
    did_connect: bool = False
    discovered: bool = False
    version: Optional[str] = None


@dataclass
class ServerPool:
    servers: Tuple[Server, ...]
    tls: Optional[TLSConfig] = None


@dataclass
class ClientConfig:
    servers: List[str] = field(default_factory=lambda: ["nats://localhost:4222"])
    name: Optional[str] = None
    pedantic: bool = True
    verbose: bool = False
    allow_reconnect: bool = True
    connection_timeout: float = DEFAULT_CONNECT_TIMEOUT
    reconnection_timeout: float = DEFAULT_RECONNECT_TIMEOUT
    max_reconnect_attempts: int = DEFAULT_MAX_RECONNECT_ATTEMPTS
    ping_interval: int = DEFAULT_PING_INTERVAL
    max_outstanding_pings: int = DEFAULT_MAX_OUTSTANDING_PINGS
    randomize_servers: bool = False
    flusher_queue_size: int = DEFAULT_MAX_FLUSHER_QUEUE_SIZE
    echo: bool = True
    tls: Optional[TLSConfig] = None
    user: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    drain_timeout: float = DEFAULT_DRAIN_TIMEOUT
    inbox_prefix: str = "_INBOX"
    pending_size: int = DEFAULT_PENDING_SIZE
    flush_timeout: Optional[float] = None

    def _build_single_server(self, server_url: str) -> Server:
        if server_url.startswith("nats://"):
            uri = urlparse(server_url)
        elif server_url.startswith("ws://") or server_url.startswith("wss://"):
            raise ValueError("WebSocket is not supported yet")
        elif server_url.startswith("tls://"):
            if not self.tls:
                raise ValueError("TLS is not configured")
            uri = urlparse(server_url)
        elif ":" in server_url:
            uri = urlparse(f"nats://{server_url}")
        else:
            raise ValueError(f"Invalid server URL: {server_url}")
        if uri.hostname is None or uri.hostname == "none":
            raise ValueError(f"Invalid server URL (hostname): {server_url}")
        if uri.port is None:
            uri = urlparse(f"nats://{uri.hostname}:422")
        return Server(uri=uri)

    @cached_property
    def server_pool(self) -> ServerPool:
        parsed_servers: List[Server] = []
        try:
            for server in self.servers:
                parsed_servers.append(self._build_single_server(server))
        except Exception as e:
            print(e)
        if self.randomize_servers:
            shuffle(parsed_servers)
        return ServerPool(servers=tuple(parsed_servers), tls=self.tls)
