"""Server pool: URL parsing, ordering, discovery merge, backoff bookkeeping."""

import random
from dataclasses import dataclass, field
from urllib.parse import unquote, urlparse

from natsio.errors import ConfigError, ServerError

from .diagnostics import sanitize_url

__all__ = ["ParsedServer", "ServerPool"]

_DEFAULT_PORT = 4222
_TLS_SCHEMES = frozenset({"tls", "nats+tls", "wss"})
_WS_SCHEMES = frozenset({"ws", "wss"})
_KNOWN_SCHEMES = frozenset({"nats", "tls", "nats+tls", "ws", "wss"})
# WebSocket default ports mirror nats.go (defaultWSPortString / defaultWSSPortString).
_DEFAULT_PORTS = {"ws": 80, "wss": 443}


@dataclass(slots=True)
class ParsedServer:
    host: str
    port: int
    tls_required: bool
    url: str = field(repr=False)
    # Original URL scheme ("nats"/"tls"/"ws"/"wss"): drives transport selection
    # and the scheme prepended to gossiped bare host:port connect_urls.
    scheme: str = "nats"
    # True for ws:// and wss://: a WebSocket transport is used and INFO
    # tls_required never triggers an in-band TLS upgrade (wss is TLS-first).
    websocket: bool = False
    # HTTP request path for the WebSocket Upgrade (ignored for non-ws schemes).
    ws_path: str = field(default="/", repr=False)
    # Credentials embedded in the URL (nats://user:pass@host) — these take
    # precedence over option-derived auth (parity with nats.go).
    username: str | None = field(default=None, repr=False)
    password: str | None = field(default=None, repr=False)
    discovered: bool = False
    consecutive_failures: int = field(default=0, repr=False)
    # Monotonic clock reading of the last connection attempt; -inf means never.
    last_attempt: float = field(default=float("-inf"), repr=False)
    # Most recent auth rejection from this server, remembered across reconnect
    # attempts to detect a repeated (revoked) credential; cleared on success.
    last_auth_error: ServerError | None = field(default=None, repr=False)

    @property
    def key(self) -> tuple[str, int]:
        return (self.host, self.port)


def parse_server_url(url: str, *, discovered: bool = False) -> ParsedServer:
    text = url.strip()
    if "://" not in text:
        text = f"nats://{text}"
    try:
        parsed = urlparse(text)
    except ValueError:
        raise ConfigError("invalid server URL") from None
    if parsed.scheme not in _KNOWN_SCHEMES:
        raise ConfigError(f"unsupported server URL scheme: {sanitize_url(url)}")
    if not parsed.hostname:
        raise ConfigError(f"server URL has no host: {sanitize_url(url)}")
    try:
        port = parsed.port or _DEFAULT_PORTS.get(parsed.scheme, _DEFAULT_PORT)
    except ValueError:
        raise ConfigError(f"invalid port in server URL: {sanitize_url(url)}") from None
    websocket = parsed.scheme in _WS_SCHEMES
    ws_path = "/"
    if websocket:
        ws_path = parsed.path or "/"
        if parsed.query:
            ws_path = f"{ws_path}?{parsed.query}"
    return ParsedServer(
        host=parsed.hostname,
        port=port,
        tls_required=parsed.scheme in _TLS_SCHEMES,
        url=text,
        scheme=parsed.scheme,
        websocket=websocket,
        ws_path=ws_path,
        username=unquote(parsed.username) if parsed.username else None,
        password=unquote(parsed.password) if parsed.password else None,
        discovered=discovered,
    )


class ServerPool:
    """Explicitly-configured plus discovered servers, in connection-attempt order."""

    def __init__(
        self,
        urls: tuple[str, ...],
        *,
        randomize: bool = True,
        max_consecutive_failures: int = 60,
        accept_discovered: bool = True,
    ) -> None:
        self._servers = [parse_server_url(u) for u in urls]
        if not self._servers:
            raise ConfigError("empty server pool")
        # WebSocket and non-WebSocket URLs cannot be mixed in one pool (parity
        # with nats.go ErrMixingWebsocketSchemes): the whole connection is either
        # WebSocket or not, and gossiped servers inherit that decision.
        if len({s.websocket for s in self._servers}) > 1:
            raise ConfigError("mixing of websocket and non-websocket server URLs is not allowed")
        self._randomize = randomize
        self._max_failures = max_consecutive_failures
        self._accept_discovered = accept_discovered
        if randomize:
            random.shuffle(self._servers)

    @property
    def servers(self) -> list[ParsedServer]:
        return list(self._servers)

    def candidates(self) -> list[ParsedServer]:
        """Servers eligible for a connection attempt, in order."""
        if self._max_failures < 0:
            return list(self._servers)
        return [s for s in self._servers if s.consecutive_failures < self._max_failures]

    def mark_failure(self, server: ParsedServer) -> None:
        server.consecutive_failures += 1

    def mark_success(self, server: ParsedServer) -> None:
        server.consecutive_failures = 0
        server.last_auth_error = None
        if not self._randomize:
            # no_randomize means "honor my exact order" — keep the configured
            # primary first so failover returns to it.
            return
        # Otherwise rotate the healthy server to the back so the next reconnect
        # tries a different one first (spreads load after a cluster event).
        try:
            self._servers.remove(server)
        except ValueError:
            return
        self._servers.append(server)

    def merge_discovered(
        self,
        connect_urls: list[str],
        *,
        keep_key: tuple[str, int] | None = None,
        source: ParsedServer | None = None,
        tls_required: bool = False,
    ) -> list[ParsedServer]:
        """Reconcile the pool against INFO ``connect_urls`` (host:port entries).

        A non-empty list is the server's full advertisement of the cluster:
        previously-discovered servers no longer present are pruned (parity with
        nats.go processInfo), while explicitly-configured servers and the
        currently-connected one (``keep_key``) are always kept. An empty list
        carries no topology and prunes nothing. Returns newly-added servers.
        """
        if not self._accept_discovered or not connect_urls:
            return []
        parsed: list[ParsedServer] = []
        for url in connect_urls:
            try:
                server = parse_server_url(url, discovered=True)
                parts = urlparse(server.url)
                # Discovery is topology, never a source of credentials, paths
                # or a change of transport family. WS retains its established
                # re-scheming contract for bare/TCP advertisements.
                if parts.username is not None or parts.password is not None or parts.query or parts.fragment:
                    continue
                if parts.port == 0:
                    continue
                if parts.path not in ("", "/") or (source and not source.websocket and server.websocket):
                    continue
                if source is not None:
                    secure = tls_required or server.tls_required
                    scheme = ("wss" if tls_required else "ws") if source.websocket else ("tls" if secure else "nats")
                    host = f"[{server.host}]" if ":" in server.host else server.host
                    server = parse_server_url(f"{scheme}://{host}:{server.port}", discovered=True)
                parsed.append(server)
            except (ConfigError, ValueError):
                continue
        advertised = {s.key for s in parsed}
        self._servers = [s for s in self._servers if not s.discovered or s.key in advertised or s.key == keep_key]
        known = {s.key: s for s in self._servers}
        added: list[ParsedServer] = []
        for server in parsed:
            if server.key in known:
                previous = known[server.key]
                # Never downgrade a discovered endpoint learned through TLS.
                # Explicit mixed pools keep their per-entry configured policy.
                if previous.discovered and server.tls_required and not previous.tls_required:
                    previous.tls_required = True
                    previous.scheme = server.scheme
                    previous.url = server.url
                continue
            known[server.key] = server
            added.append(server)
        if added:
            if self._randomize:
                random.shuffle(added)
            self._servers.extend(added)
        return added
