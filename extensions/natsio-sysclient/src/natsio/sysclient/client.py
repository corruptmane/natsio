"""The `$SYS` monitoring client (mirrors orbit.go `natssysclient/api.go`).

Every endpoint has the two request shapes the oracle defines:

- **by server id** — `$SYS.REQ.SERVER.<id>.<ENDPOINT>`, one request, one typed
  response. A no-responders reply means the id names no reachable server, and
  becomes `InvalidServerIDError`.
- **cluster ping** — `$SYS.REQ.SERVER.PING.<ENDPOINT>`, scatter-gather over
  `Client.request_many`, bounded by the overall timeout, an optional expected
  `server_count`, and an optional `stall` gap between replies. Zero collected
  responses is `NoResponsesError`, never an empty list.
"""

import json
import time
from collections.abc import AsyncGenerator, Generator
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Any, Final

from natsio import ConnectionState
from natsio._internal.jsonmodel import JsonModel
from natsio.client import Client
from natsio.errors import ConnectionClosedError, NoRespondersError

from .connz import ConnzResponse
from .errors import (
    InvalidResponseError,
    InvalidServerIDError,
    NoResponsesError,
    PagerStateError,
    SysAPIError,
    SysValidationError,
)
from .healthz import HealthzResponse
from .jsz import JszResponse
from .options import ConnzOptions, HealthzOptions, JszOptions, StatszOptions, SubszOptions, VarzOptions
from .statsz import StatszResponse
from .subsz import SubszResponse
from .varz import VarzResponse

__all__ = [
    "DEFAULT_REQUEST_TIMEOUT",
    "DEFAULT_STALL",
    "PING_TARGET",
    "SYS_REQ_SERVER_PREFIX",
    "Endpoint",
    "PagedResponses",
    "SysClient",
    "SysClientOptions",
]

# -- pinned wire contract ----------------------------------------------------
# orbit.go api.go: "$SYS.REQ.SERVER.%s.VARZ" and friends, with "PING" as the
# id that fans the request out to every server in the cluster.
SYS_REQ_SERVER_PREFIX: Final = "$SYS.REQ.SERVER"
PING_TARGET: Final = "PING"


class Endpoint(StrEnum):
    """The monitoring endpoints this client speaks."""

    VARZ = "VARZ"
    CONNZ = "CONNZ"
    JSZ = "JSZ"
    HEALTHZ = "HEALTHZ"
    STATSZ = "STATSZ"
    SUBSZ = "SUBSZ"


# orbit.go api.go: DefaultRequestTimeout / DefaultStall.
DEFAULT_REQUEST_TIMEOUT: Final = 10.0
DEFAULT_STALL: Final = 0.3

# Server ids are NUIDs; a dot or wildcard in one would silently retarget the
# request at a different subject, so it is rejected before any I/O.
_FORBIDDEN_IN_ID: Final = frozenset(".*> \t\r\n")


@dataclass(frozen=True, slots=True, kw_only=True)
class SysClientOptions:
    """Client-wide request defaults.

    `stall` and `server_count` shape the cluster-ping gather and are not
    exclusive: setting both stops at whichever fires first. `server_count`
    should be the cluster size as the connected server sees it.

    `stall` bounds only the gap *between* replies — the first reply always gets
    the full `timeout` — so the 300 ms default does not penalise a slow
    cluster, it just stops waiting once the cluster has gone quiet.
    """

    timeout: float = DEFAULT_REQUEST_TIMEOUT
    stall: float | None = DEFAULT_STALL
    server_count: int | None = None

    def __post_init__(self) -> None:
        if self.timeout <= 0:
            raise SysValidationError("timeout has to be greater than 0")
        if self.stall is not None and self.stall <= 0:
            raise SysValidationError("stall has to be greater than 0 (use None to disable it)")
        if self.server_count is not None and self.server_count <= 0:
            raise SysValidationError("server_count has to be greater than 0")


class PagedResponses[ResponseT]:
    """Async iterator over every page of an offset/limit-paged endpoint.

    Each step issues one request, so nothing happens until you iterate.
    `aclose()` releases the underlying generator if you stop early; `async
    with` does it for you.

    Single-pass and single-consumer, enforced with `PagerStateError`: a spent
    pager re-iterated would otherwise yield zero pages and read as "no data",
    and two tasks sharing one pager would surface as a bare `RuntimeError`
    from the generator machinery. Build a fresh pager to page again.
    """

    __slots__ = ("_closed", "_pages", "_running", "_started")

    def __init__(self, pages: AsyncGenerator[ResponseT]) -> None:
        self._pages = pages
        self._started = False
        self._running = False
        self._closed = False

    def __aiter__(self) -> "PagedResponses[ResponseT]":
        if self._started:
            raise PagerStateError("this pager has already been iterated; build a new one to page again")
        self._started = True
        return self

    async def __anext__(self) -> ResponseT:
        if self._closed:
            raise PagerStateError("this pager was closed")
        if self._running:
            raise PagerStateError("this pager is already being advanced by another task; pagers are single-consumer")
        self._running = True
        try:
            return await anext(self._pages)
        finally:
            self._running = False

    def __await__(self) -> Generator[None, None, "PagedResponses[ResponseT]"]:
        """``await`` is optional and completes immediately: building the pager
        does no I/O. Supported so muscle memory — ``pages = await
        sys.all_connz(...)`` — works unchanged; see
        `natsio.subscription.Subscription.__await__()`."""
        return self
        yield  # unreachable: makes this a generator that never suspends

    async def aclose(self) -> None:
        """Stop paging and release the generator. Idempotent."""
        if self._running:
            raise PagerStateError("cannot close this pager while another task is advancing it")
        self._closed = True
        await self._pages.aclose()

    async def __aenter__(self) -> "PagedResponses[ResponseT]":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()


def _encode(options: JsonModel | None) -> bytes:
    return json.dumps({} if options is None else options.to_wire(), separators=(",", ":")).encode()


def _validate_server_id(server_id: str) -> None:
    if not server_id:
        raise SysValidationError("server id cannot be empty")
    if not _FORBIDDEN_IN_ID.isdisjoint(server_id):
        raise SysValidationError(f"server id {server_id!r} contains a subject separator or wildcard")
    if server_id == PING_TARGET:
        # A divergence from the oracle's bare `fmt.Sprintf`: "PING" is not a
        # server id, it is the fan-out target, and a by-id call using it would
        # silently degrade to a cluster ping that keeps only the first reply.
        raise SysValidationError(f"server id {PING_TARGET!r} is the cluster-ping target; use the *_ping() methods")


def _validate_timeout(timeout: float | None, default: float) -> float:
    """Resolve a per-call timeout override, held to the same bound as
    `SysClientOptions.timeout` so a nonsense deadline never reaches the wire."""
    if timeout is None:
        return default
    if timeout <= 0:
        raise SysValidationError("timeout has to be greater than 0")
    return timeout


def _api_int(error: dict[str, Any], key: str, subject: str) -> int | None:
    """Read one integer field out of a hostile `error` block.

    `bool` is excluded on purpose (`True` is an `int` in Python, but a JSON
    `true` here is malformed, not the code 1).
    """
    raw = error.get(key)
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise InvalidResponseError(f"response to {subject!r} carries a non-integer error {key!r}: {raw!r}")
    return raw


def _decode(payload: bytes, subject: str) -> dict[str, Any]:
    """Parse one response envelope, raising on a server-side `error` block.

    This is the *envelope* boundary: a body that is not JSON, not a JSON object,
    or carries a malformed `error` block leaves as a typed `InvalidResponseError`
    rather than a raw `ValueError`/`int()` error. It does **not** cover
    field-level decoding — the `*.from_wire(...)` step the callers run on the
    returned dict surfaces whatever `JsonModel` raises for a malformed `data`
    block (a non-object `data`, a bad RFC3339 timestamp, a string where a number
    belongs). That nested behaviour is core-`JsonModel`-wide, not this module's.
    """
    try:
        envelope = json.loads(payload)
    except ValueError as exc:
        raise InvalidResponseError(f"response to {subject!r} is not JSON: {exc}") from exc
    if not isinstance(envelope, dict):
        raise InvalidResponseError(f"response to {subject!r} is not a JSON object")
    error = envelope.get("error")
    if error is not None:
        # `is not None`, not truthiness: `{"error": []}` and `{"error": {}}`
        # are malformed envelopes, not data, and used to pass through silently.
        if not isinstance(error, dict):
            raise InvalidResponseError(f"response to {subject!r} carries a non-object 'error'")
        description = error.get("description")
        raise SysAPIError(
            code=_api_int(error, "code", subject) or 0,
            description="" if description is None else str(description),
            err_code=_api_int(error, "err_code", subject),
        )
    return envelope


class SysClient:
    """Monitoring client over a `natsio` connection bound to the system account.

        nc = await natsio.connect("nats://localhost:4222", user="sys", password="pw")
        sys = SysClient(nc)

        varz = await sys.varz(server_id)          # one named server
        everyone = await sys.varz_ping()          # the whole cluster

    The connection must be authenticated into `$SYS`; a connection in a regular
    account gets no responders (by id) or no responses (on a ping), both of
    which raise rather than returning something empty.
    """

    __slots__ = ("_nc", "_options")

    def __init__(self, nc: Client, *, options: SysClientOptions | None = None) -> None:
        self._nc = nc
        self._options = options if options is not None else SysClientOptions()

    @property
    def options(self) -> SysClientOptions:
        """The request defaults this client was built with."""
        return self._options

    # -- request shapes ------------------------------------------------------

    async def _by_id(
        self,
        server_id: str,
        endpoint: Endpoint,
        options: JsonModel | None,
        timeout: float | None,  # noqa: ASYNC109
    ) -> dict[str, Any]:
        _validate_server_id(server_id)
        subject = f"{SYS_REQ_SERVER_PREFIX}.{server_id}.{endpoint}"
        deadline = _validate_timeout(timeout, self._options.timeout)
        try:
            msg = await self._nc.request(subject, _encode(options), timeout=deadline)
        except NoRespondersError as exc:
            raise InvalidServerIDError(f"no server answered {subject!r}: unknown server id {server_id!r}") from exc
        return _decode(msg.payload, subject)

    async def _ping(
        self,
        endpoint: Endpoint,
        options: JsonModel | None,
        timeout: float | None,  # noqa: ASYNC109
        server_count: int | None,
    ) -> list[dict[str, Any]]:
        subject = f"{SYS_REQ_SERVER_PREFIX}.{PING_TARGET}.{endpoint}"
        count = server_count if server_count is not None else self._options.server_count
        if count is not None and count <= 0:
            raise SysValidationError("server_count has to be greater than 0")
        deadline = _validate_timeout(timeout, self._options.timeout)
        stall = self._options.stall
        started = time.monotonic()
        # Comprehension, not `async for` + break: it always drains the
        # generator, so `request_many`'s own cleanup runs deterministically
        # even when a later decode raises.
        replies = [
            msg
            async for msg in self._nc.request_many(
                subject, _encode(options), timeout=deadline, max_msgs=count, stall=stall
            )
        ]
        if not replies:
            elapsed = time.monotonic() - started
            state = self._nc.status
            if state is ConnectionState.CLOSED:
                # A genuinely closed connection: core's `request_many` now raises
                # `ConnectionClosedError` at the seam when it drains the closure
                # sentinel, so in practice we rarely reach here — but keep the
                # explicit closed check as a backstop. It is deliberately NOT
                # `is not CONNECTED`: a client that is merely RECONNECTING is
                # healthy and recovering, and reporting that as a closure sends
                # a retrying caller to tear down the client. A reconnect that
                # outlasts the deadline falls through to the timeout branch.
                raise ConnectionClosedError(f"connection is {state.value} while gathering replies to {subject!r}")
            # `stall` cannot be the bound that fired: it only limits the gap
            # BETWEEN replies, so with zero replies it was never armed.
            reason = (
                "the overall timeout elapsed"
                if elapsed >= deadline
                else "the reply stream ended with no responders (is this connection bound to the system account?)"
            )
            raise NoResponsesError(
                f"no server answered {subject!r}: {reason} after {elapsed:.3f}s "
                f"(timeout={deadline}s, stall={stall}, server_count={count})"
            )
        return [_decode(msg.payload, subject) for msg in replies]

    # -- VARZ ----------------------------------------------------------------

    async def varz(
        self,
        server_id: str,
        options: VarzOptions | None = None,
        *,
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> VarzResponse:
        """General information about one server."""
        return VarzResponse.from_wire(await self._by_id(server_id, Endpoint.VARZ, options, timeout))

    async def varz_ping(
        self,
        options: VarzOptions | None = None,
        *,
        timeout: float | None = None,  # noqa: ASYNC109
        server_count: int | None = None,
    ) -> list[VarzResponse]:
        """General information from every server that answers."""
        return [
            VarzResponse.from_wire(envelope)
            for envelope in await self._ping(Endpoint.VARZ, options, timeout, server_count)
        ]

    # -- STATSZ --------------------------------------------------------------

    async def statsz(
        self,
        server_id: str,
        options: StatszOptions | None = None,
        *,
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> StatszResponse:
        """The periodic stats snapshot for one server (payload under `statsz`)."""
        return StatszResponse.from_wire(await self._by_id(server_id, Endpoint.STATSZ, options, timeout))

    async def statsz_ping(
        self,
        options: StatszOptions | None = None,
        *,
        timeout: float | None = None,  # noqa: ASYNC109
        server_count: int | None = None,
    ) -> list[StatszResponse]:
        """The stats snapshot from every server that answers."""
        return [
            StatszResponse.from_wire(envelope)
            for envelope in await self._ping(Endpoint.STATSZ, options, timeout, server_count)
        ]

    # -- HEALTHZ -------------------------------------------------------------

    async def healthz(
        self,
        server_id: str,
        options: HealthzOptions | None = None,
        *,
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> HealthzResponse:
        """Health of one server. An unhealthy server answers with
        `status="error"` — that is data, not an exception."""
        return HealthzResponse.from_wire(await self._by_id(server_id, Endpoint.HEALTHZ, options, timeout))

    async def healthz_ping(
        self,
        options: HealthzOptions | None = None,
        *,
        timeout: float | None = None,  # noqa: ASYNC109
        server_count: int | None = None,
    ) -> list[HealthzResponse]:
        """Health of every server that answers."""
        return [
            HealthzResponse.from_wire(envelope)
            for envelope in await self._ping(Endpoint.HEALTHZ, options, timeout, server_count)
        ]

    # -- CONNZ ---------------------------------------------------------------

    async def connz(
        self,
        server_id: str,
        options: ConnzOptions | None = None,
        *,
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> ConnzResponse:
        """One page of one server's connections."""
        return ConnzResponse.from_wire(await self._by_id(server_id, Endpoint.CONNZ, options, timeout))

    async def connz_ping(
        self,
        options: ConnzOptions | None = None,
        *,
        timeout: float | None = None,  # noqa: ASYNC109
        server_count: int | None = None,
    ) -> list[ConnzResponse]:
        """One page of connections from every server that answers."""
        return [
            ConnzResponse.from_wire(envelope)
            for envelope in await self._ping(Endpoint.CONNZ, options, timeout, server_count)
        ]

    def all_connz(
        self, server_id: str, options: ConnzOptions | None = None, *, timeout: float | None = None
    ) -> PagedResponses[ConnzResponse]:
        """Every page of one server's connections, walking `offset` to `total`.

        Set `options.limit` to choose the page size; the server caps it.

        A complete walk assumes stable page order across requests. The default
        sort (by `cid`) is a total order and is safe; a sort whose key has ties
        can duplicate or skip entries between pages with no error. For a
        tie-prone sort, prefer a single large page.
        """
        return PagedResponses(self._page_connz(server_id, options, timeout))

    async def all_connz_ping(
        self,
        options: ConnzOptions | None = None,
        *,
        timeout: float | None = None,  # noqa: ASYNC109
        server_count: int | None = None,
    ) -> list[PagedResponses[ConnzResponse]]:
        """One pager per responding server, each paging that server independently.

        The initial ping happens here (hence the `await`); the returned pagers
        can then be consumed concurrently.
        """
        first_pages = await self.connz_ping(options, timeout=timeout, server_count=server_count)
        return [PagedResponses(self._page_connz_after(page, options, timeout)) for page in first_pages]

    async def _page_connz(
        self,
        server_id: str,
        options: ConnzOptions | None,
        timeout: float | None,  # noqa: ASYNC109
    ) -> AsyncGenerator[ConnzResponse]:
        base = options if options is not None else ConnzOptions()
        offset = max(0, base.offset or 0)
        while True:
            page = await self.connz(server_id, replace(base, offset=offset), timeout=timeout)
            yield page
            received = offset + len(page.data.connections)
            if received >= page.data.total or not page.data.connections:
                return
            offset = received

    async def _page_connz_after(
        self,
        first: ConnzResponse,
        options: ConnzOptions | None,
        timeout: float | None,  # noqa: ASYNC109
    ) -> AsyncGenerator[ConnzResponse]:
        yield first
        base = options if options is not None else ConnzOptions()
        offset = max(0, base.offset or 0) + len(first.data.connections)
        while offset < first.data.total:
            page = await self.connz(first.server.id, replace(base, offset=offset), timeout=timeout)
            yield page
            if not page.data.connections:
                return
            offset += len(page.data.connections)

    # -- SUBSZ ---------------------------------------------------------------

    async def subsz(
        self,
        server_id: str,
        options: SubszOptions | None = None,
        *,
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> SubszResponse:
        """One page of one server's sublist."""
        return SubszResponse.from_wire(await self._by_id(server_id, Endpoint.SUBSZ, options, timeout))

    async def subsz_ping(
        self,
        options: SubszOptions | None = None,
        *,
        timeout: float | None = None,  # noqa: ASYNC109
        server_count: int | None = None,
    ) -> list[SubszResponse]:
        """One page of the sublist from every server that answers."""
        return [
            SubszResponse.from_wire(envelope)
            for envelope in await self._ping(Endpoint.SUBSZ, options, timeout, server_count)
        ]

    def all_subsz(
        self, server_id: str, options: SubszOptions | None = None, *, timeout: float | None = None
    ) -> PagedResponses[SubszResponse]:
        """Every page of one server's sublist. Needs `options.subscriptions`
        to be set — without it the server returns statistics and no list, and
        paging stops after the first page.

        Best-effort: `SUBSZ` page order is unspecified and not stable across
        requests, so an `offset` walk can duplicate or skip entries between
        pages. Use a single large page (`options.limit`) for a complete list.
        """
        return PagedResponses(self._page_subsz(server_id, options, timeout))

    async def all_subsz_ping(
        self,
        options: SubszOptions | None = None,
        *,
        timeout: float | None = None,  # noqa: ASYNC109
        server_count: int | None = None,
    ) -> list[PagedResponses[SubszResponse]]:
        """One sublist pager per responding server."""
        first_pages = await self.subsz_ping(options, timeout=timeout, server_count=server_count)
        return [PagedResponses(self._page_subsz_after(page, options, timeout)) for page in first_pages]

    async def _page_subsz(
        self,
        server_id: str,
        options: SubszOptions | None,
        timeout: float | None,  # noqa: ASYNC109
    ) -> AsyncGenerator[SubszResponse]:
        base = options if options is not None else SubszOptions()
        offset = max(0, base.offset or 0)
        while True:
            page = await self.subsz(server_id, replace(base, offset=offset), timeout=timeout)
            yield page
            listed = page.data.subscriptions_list or []
            received = offset + len(listed)
            if received >= page.data.total or not listed:
                return
            offset = received

    async def _page_subsz_after(
        self,
        first: SubszResponse,
        options: SubszOptions | None,
        timeout: float | None,  # noqa: ASYNC109
    ) -> AsyncGenerator[SubszResponse]:
        yield first
        base = options if options is not None else SubszOptions()
        offset = max(0, base.offset or 0) + len(first.data.subscriptions_list or [])
        while offset < first.data.total:
            page = await self.subsz(first.server.id, replace(base, offset=offset), timeout=timeout)
            yield page
            listed = page.data.subscriptions_list or []
            if not listed:
                return
            offset += len(listed)

    # -- JSZ -----------------------------------------------------------------

    async def jsz(
        self,
        server_id: str,
        options: JszOptions | None = None,
        *,
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> JszResponse:
        """One server's JetStream state."""
        return JszResponse.from_wire(await self._by_id(server_id, Endpoint.JSZ, options, timeout))

    async def jsz_ping(
        self,
        options: JszOptions | None = None,
        *,
        timeout: float | None = None,  # noqa: ASYNC109
        server_count: int | None = None,
    ) -> list[JszResponse]:
        """JetStream state from every server that answers."""
        return [
            JszResponse.from_wire(envelope)
            for envelope in await self._ping(Endpoint.JSZ, options, timeout, server_count)
        ]

    def all_jsz(
        self, server_id: str, options: JszOptions | None = None, *, timeout: float | None = None
    ) -> PagedResponses[JszResponse]:
        """Every page of one server's JetStream state.

        `JSZ` pages `account_details` against the `accounts` total, so paging
        only does anything with `options.accounts` set; otherwise this yields a
        single page — the same short-circuit the oracle's `AllJsz` takes.

        `options.account` (a single-account filter) also yields exactly one
        page: the server ignores `offset` for it while still reporting the
        unfiltered `accounts` count, so walking that total would re-request the
        same account once per JS-enabled account.
        """
        return PagedResponses(self._page_jsz(server_id, options, timeout))

    async def all_jsz_ping(
        self,
        options: JszOptions | None = None,
        *,
        timeout: float | None = None,  # noqa: ASYNC109
        server_count: int | None = None,
    ) -> list[PagedResponses[JszResponse]]:
        """One JetStream pager per responding server."""
        first_pages = await self.jsz_ping(options, timeout=timeout, server_count=server_count)
        return [PagedResponses(self._page_jsz_after(page, options, timeout)) for page in first_pages]

    async def _page_jsz(
        self,
        server_id: str,
        options: JszOptions | None,
        timeout: float | None,  # noqa: ASYNC109
    ) -> AsyncGenerator[JszResponse]:
        base = options if options is not None else JszOptions()
        if not base.accounts or base.account is not None:
            # `account=` is a single-account filter: nats-server IGNORES
            # `offset` for it but still reports the UNFILTERED `accounts`
            # count, so walking that total re-requests and re-yields the same
            # account once per JS-enabled account on the server. One page is
            # all there is. (The oracle's `AllJsz` has the same loop and the
            # same bug; this is a deliberate divergence.)
            yield await self.jsz(server_id, base, timeout=timeout)
            return
        offset = max(0, base.offset or 0)
        while True:
            page = await self.jsz(server_id, replace(base, offset=offset), timeout=timeout)
            yield page
            listed = page.data.account_details or []
            received = offset + len(listed)
            if received >= page.data.accounts or not listed:
                return
            offset = received

    async def _page_jsz_after(
        self,
        first: JszResponse,
        options: JszOptions | None,
        timeout: float | None,  # noqa: ASYNC109
    ) -> AsyncGenerator[JszResponse]:
        yield first
        base = options if options is not None else JszOptions()
        if not base.accounts or base.account is not None:
            return  # see `_page_jsz`: `account=` makes `accounts` an unusable bound
        offset = max(0, base.offset or 0) + len(first.data.account_details or [])
        while offset < first.data.accounts:
            page = await self.jsz(first.server.id, replace(base, offset=offset), timeout=timeout)
            yield page
            listed = page.data.account_details or []
            if not listed:
                return
            offset += len(listed)
