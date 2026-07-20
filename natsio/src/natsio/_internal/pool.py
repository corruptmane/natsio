"""Server pool: URL parsing, ordering, discovery merge, backoff bookkeeping."""

import random
from dataclasses import dataclass, field
from urllib.parse import unquote, urlparse

from natsio.errors import ConfigError, ServerError

__all__ = ["ParsedServer", "ServerPool"]

_DEFAULT_PORT = 4222
_TLS_SCHEMES = frozenset({"tls", "nats+tls", "wss"})
_KNOWN_SCHEMES = frozenset({"nats", "tls", "nats+tls"})


@dataclass(slots=True)
class ParsedServer:
    host: str
    port: int
    tls_required: bool
    url: str
    # Credentials embedded in the URL (nats://user:pass@host) — these take
    # precedence over option-derived auth (parity with nats.go).
    username: str | None = None
    password: str | None = None
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
    parsed = urlparse(text)
    if parsed.scheme not in _KNOWN_SCHEMES:
        raise ConfigError(f"unsupported server URL scheme: {url!r}")
    if not parsed.hostname:
        raise ConfigError(f"server URL has no host: {url!r}")
    try:
        port = parsed.port or _DEFAULT_PORT
    except ValueError as exc:
        raise ConfigError(f"invalid port in server URL {url!r}: {exc}") from None
    return ParsedServer(
        host=parsed.hostname,
        port=port,
        tls_required=parsed.scheme in _TLS_SCHEMES,
        url=text,
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
        self, connect_urls: list[str], *, keep_key: tuple[str, int] | None = None
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
                parsed.append(parse_server_url(url, discovered=True))
            except ConfigError:
                continue
        advertised = {s.key for s in parsed}
        self._servers = [s for s in self._servers if not s.discovered or s.key in advertised or s.key == keep_key]
        known = {s.key for s in self._servers}
        added: list[ParsedServer] = []
        for server in parsed:
            if server.key in known:
                continue
            known.add(server.key)
            added.append(server)
        if added:
            if self._randomize:
                random.shuffle(added)
            self._servers.extend(added)
        return added
