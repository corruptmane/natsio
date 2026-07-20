"""Client configuration: a frozen, typed options object."""

import os
import ssl as ssl_module
from dataclasses import dataclass, field, replace
from typing import Any, Self, TypedDict

from natsio._internal.auth import (
    Authenticator,
    CredsFileAuth,
    NKeyAuth,
    TokenAuth,
    UserPasswordAuth,
)
from natsio._internal.auth.authenticators import StrSource
from natsio.errors import ConfigError
from natsio.instrumentation import Instrumentation

__all__ = ["ConnectKwargs", "ConnectOptions", "TLSConfig"]


@dataclass(frozen=True, slots=True, kw_only=True)
class TLSConfig:
    """TLS settings. ``context=None`` uses :func:`ssl.create_default_context`."""

    context: ssl_module.SSLContext | None = None
    hostname: str | None = None
    # 2.10.4+ tls-first handshake: upgrade before the server sends INFO.
    handshake_first: bool = False

    def resolve_context(self) -> ssl_module.SSLContext:
        return self.context if self.context is not None else ssl_module.create_default_context()


@dataclass(frozen=True, slots=True, kw_only=True)
class ConnectOptions:
    servers: tuple[str, ...] = ("nats://127.0.0.1:4222",)
    name: str | None = None
    connect_timeout: float = 5.0
    verbose: bool = False
    pedantic: bool = False
    echo: bool = True
    tls: TLSConfig | None = None

    # -- authentication (flat convenience fields; `authenticator` overrides) --
    user: StrSource | None = None
    password: StrSource | None = None
    token: StrSource | None = None
    nkey_seed: str | None = None
    credentials: str | os.PathLike[str] | None = None
    authenticator: Authenticator | None = None

    # -- reconnect --
    allow_reconnect: bool = True
    max_reconnect_attempts: int = 60  # consecutive failures per server; -1 = unlimited
    reconnect_time_wait: float = 2.0
    reconnect_time_wait_max: float = 8.0
    reconnect_jitter: float = 0.1
    reconnect_jitter_tls: float = 1.0
    no_randomize: bool = False
    ignore_discovered_servers: bool = False

    # -- liveness --
    ping_interval: float = 120.0
    max_outstanding_pings: int = 2

    # -- write path / shutdown --
    max_pending_size: int = 2 * 1024 * 1024
    flush_timeout: float = 10.0
    drain_timeout: float = 30.0

    # -- requests & subscriptions --
    request_timeout: float = 5.0
    inbox_prefix: str = "_INBOX"
    pending_msgs_limit: int = 65_536
    pending_bytes_limit: int = 64 * 1024 * 1024

    # -- limits --
    max_control_line: int = 4096

    # -- observability --
    instrumentation: "Instrumentation | None" = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if not self.servers:
            raise ConfigError("at least one server URL is required")
        for positive in (
            ("connect_timeout", self.connect_timeout),
            ("reconnect_time_wait", self.reconnect_time_wait),
            ("reconnect_time_wait_max", self.reconnect_time_wait_max),
            ("ping_interval", self.ping_interval),
            ("flush_timeout", self.flush_timeout),
            ("drain_timeout", self.drain_timeout),
            ("request_timeout", self.request_timeout),
        ):
            if positive[1] <= 0:
                raise ConfigError(f"{positive[0]} must be positive")
        if self.max_outstanding_pings < 1:
            raise ConfigError("max_outstanding_pings must be at least 1")
        if self.max_reconnect_attempts == 0 or self.max_reconnect_attempts < -1:
            raise ConfigError(
                "max_reconnect_attempts must be -1 (unlimited) or >= 1; "
                "to connect once without reconnecting, use allow_reconnect=False"
            )
        if (self.user is None) != (self.password is None):
            raise ConfigError("user and password must be provided together")
        explicit = [
            name
            for name, value in (
                ("user/password", self.user),
                ("token", self.token),
                ("nkey_seed", self.nkey_seed),
                ("credentials", self.credentials),
                ("authenticator", self.authenticator),
            )
            if value is not None
        ]
        if len(explicit) > 1:
            raise ConfigError(f"conflicting auth options: {', '.join(explicit)}")
        if self.inbox_prefix.endswith(".") or not self.inbox_prefix:
            raise ConfigError("inbox_prefix must be a non-empty subject prefix without trailing dot")

    def replace(self, **changes: Any) -> Self:
        return replace(self, **changes)

    def resolve_authenticator(self) -> Authenticator | None:
        if self.authenticator is not None:
            return self.authenticator
        if self.user is not None and self.password is not None:
            return UserPasswordAuth(user=self.user, password=self.password)
        if self.token is not None:
            return TokenAuth(token=self.token)
        if self.nkey_seed is not None:
            return NKeyAuth(seed=self.nkey_seed)
        if self.credentials is not None:
            return CredsFileAuth(path=self.credentials)
        return None


class ConnectKwargs(TypedDict, total=False):
    """Keyword arguments accepted by :func:`natsio.connect`.

    A typed mirror of :class:`ConnectOptions` — kept in sync by a unit test —
    so ``connect(..., ping_interval=30)`` type-checks instead of being ``Any``.
    """

    servers: tuple[str, ...]
    name: str | None
    connect_timeout: float
    verbose: bool
    pedantic: bool
    echo: bool
    tls: TLSConfig | None
    user: StrSource | None
    password: StrSource | None
    token: StrSource | None
    nkey_seed: str | None
    credentials: str | os.PathLike[str] | None
    authenticator: Authenticator | None
    allow_reconnect: bool
    max_reconnect_attempts: int
    reconnect_time_wait: float
    reconnect_time_wait_max: float
    reconnect_jitter: float
    reconnect_jitter_tls: float
    no_randomize: bool
    ignore_discovered_servers: bool
    ping_interval: float
    max_outstanding_pings: int
    max_pending_size: int
    flush_timeout: float
    drain_timeout: float
    request_timeout: float
    inbox_prefix: str
    pending_msgs_limit: int
    pending_bytes_limit: int
    max_control_line: int
    instrumentation: Instrumentation | None
