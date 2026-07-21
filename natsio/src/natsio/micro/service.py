"""The ADR-32 micro service: endpoints, groups, and ``$SRV`` monitoring.

    svc = add_service(nc, name="calc", version="1.0.0")
    svc.add_endpoint("add", handle_add)                # $SRV request on "add"

    math = svc.add_group("math")
    math.add_endpoint("mul", handle_mul)                # subject "math.mul"

    async with svc:                                     # stops on exit
        await svc.stopped                               # or await the surface

Every instance answers ``$SRV.PING``/``INFO``/``STATS`` (bare, ``.<name>`` and
``.<name>.<id>``) with no queue group, so monitoring reaches all instances;
request endpoints share a queue group (default ``"q"``) so load is split.
"""

import asyncio
import contextlib
import json
import time
from collections.abc import Awaitable, Callable, Generator
from datetime import UTC, datetime
from functools import partial
from types import TracebackType
from typing import TYPE_CHECKING, Any, Final, Self

from natsio._internal.nuid import next_nuid
from natsio._internal.validation import validate_queue_group, validate_subject
from natsio.message import Msg

from .entities import (
    API_PREFIX,
    INFO_RESPONSE_TYPE,
    PING_RESPONSE_TYPE,
    STATS_RESPONSE_TYPE,
    EndpointInfo,
    EndpointStats,
    InfoResponse,
    PingResponse,
    ServiceConfig,
    StatsResponse,
    validate_endpoint_name,
)
from .errors import ServiceConfigError, ServiceError
from .request import Request

if TYPE_CHECKING:
    from natsio.client import Client
    from natsio.subscription import Subscription

__all__ = ["Endpoint", "Group", "Service", "add_service", "control_subject"]

# Handler contract: async def handler(req: Request) -> None.
type Handler = Callable[[Request], Awaitable[None]]

_PING: Final = "PING"
_INFO: Final = "INFO"
_STATS: Final = "STATS"
_VERBS: Final = (_PING, _INFO, _STATS)


def control_subject(verb: str, name: str = "", id: str = "") -> str:
    """Build a ``$SRV`` monitoring subject.

    ``verb`` alone monitors all services; ``verb`` + ``name`` all instances of a
    named service; ``verb`` + ``name`` + ``id`` one specific instance. An ``id``
    without a ``name`` is rejected.
    """
    if not name and id:
        raise ServiceConfigError("a service name is required to build an id control subject")
    if not name:
        return f"{API_PREFIX}.{verb}"
    if not id:
        return f"{API_PREFIX}.{verb}.{name}"
    return f"{API_PREFIX}.{verb}.{name}.{id}"


def _resolve_queue_group(custom: str | None, parent: str) -> str:
    """A custom (validated) queue group overrides the inherited one."""
    if custom is not None:
        if custom:
            validate_queue_group(custom)
        return custom
    return parent


class Endpoint:
    """A registered request handler and its live statistics.

    Counters are plain integers mutated on the subscription's own task (single
    asyncio thread, no lock needed); ``processing_time`` is in nanoseconds.
    """

    __slots__ = (
        "_handler",
        "_subscription",
        "last_error",
        "metadata",
        "name",
        "num_errors",
        "num_requests",
        "processing_time",
        "queue_group",
        "subject",
    )

    def __init__(
        self,
        name: str,
        subject: str,
        queue_group: str,
        handler: Handler,
        metadata: dict[str, str],
    ) -> None:
        self.name = name
        self.subject = subject
        self.queue_group = queue_group
        self.metadata = metadata
        self._handler = handler
        self._subscription: Subscription | None = None
        self.num_requests = 0
        self.num_errors = 0
        self.last_error = ""
        self.processing_time = 0

    def __await__(self) -> Generator[None, None, "Endpoint"]:
        """``await`` is optional and completes immediately (no I/O) — the same
        muscle-memory tolerance as `natsio.Client.subscribe()`."""
        return self
        yield  # unreachable: makes this a generator that never suspends

    @property
    def average_processing_time(self) -> int:
        """Mean handler time in nanoseconds (0 before the first request)."""
        return self.processing_time // self.num_requests if self.num_requests else 0

    def _info(self) -> EndpointInfo:
        return EndpointInfo(
            name=self.name,
            subject=self.subject,
            queue_group=self.queue_group,
            metadata=self.metadata,
        )

    def _stats(self, data: object = None) -> EndpointStats:
        return EndpointStats(
            name=self.name,
            subject=self.subject,
            queue_group=self.queue_group,
            num_requests=self.num_requests,
            num_errors=self.num_errors,
            last_error=self.last_error,
            processing_time=self.processing_time,
            average_processing_time=self.average_processing_time,
            data=data,
        )


class Group:
    """A dotted subject prefix under which endpoints are registered.

    Groups nest (``add_group``) and inherit their queue group from the parent
    unless overridden.
    """

    __slots__ = ("_prefix", "_queue_group", "_service")

    def __init__(self, service: "Service", prefix: str, queue_group: str) -> None:
        self._service = service
        self._prefix = prefix
        self._queue_group = queue_group

    def __await__(self) -> Generator[None, None, "Group"]:
        """``await`` is optional and completes immediately (no I/O) — the same
        muscle-memory tolerance as `natsio.Client.subscribe()`."""
        return self
        yield  # unreachable: makes this a generator that never suspends

    def add_group(self, name: str, *, queue_group: str | None = None) -> "Group":
        """Derive a nested group, prefixed by this group's prefix."""
        prefix = f"{self._prefix}.{name}" if name else self._prefix
        return Group(self._service, prefix, _resolve_queue_group(queue_group, self._queue_group))

    def add_endpoint(
        self,
        name: str,
        handler: Handler,
        *,
        subject: str | None = None,
        queue_group: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> Endpoint:
        """Register an endpoint whose subject is prefixed by this group."""
        leaf = subject if subject is not None else name
        full = f"{self._prefix}.{leaf}" if self._prefix else leaf
        return self._service._register(
            name,
            full,
            handler,
            _resolve_queue_group(queue_group, self._queue_group),
            metadata,
        )


class Service:
    """A running micro service instance (create via `add_service()`).

    Answers ``$SRV`` monitoring for its lifetime and routes requests to
    registered endpoints. Use it as an async context manager, or await
    `stopped` to block until it stops.
    """

    __slots__ = (
        "_config",
        "_endpoints",
        "_id",
        "_metadata",
        "_monitoring",
        "_nc",
        "_started",
        "_stopped",
        "_stopped_future",
    )

    def __init__(self, nc: "Client", config: ServiceConfig) -> None:
        self._nc = nc
        self._config = config
        self._id = next_nuid()
        self._metadata = dict(config.metadata) if config.metadata else {}
        self._started = datetime.now(UTC)
        self._endpoints: list[Endpoint] = []
        self._monitoring: list[Subscription] = []
        self._stopped = False
        self._stopped_future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._start_monitoring()

    def __await__(self) -> Generator[None, None, "Service"]:
        """``await`` is optional and completes immediately (no I/O) — the same
        muscle-memory tolerance as `natsio.Client.subscribe()`."""
        return self
        yield  # unreachable: makes this a generator that never suspends

    # -- identity ------------------------------------------------------------

    @property
    def id(self) -> str:
        """This instance's unique id (a fresh NUID)."""
        return self._id

    @property
    def name(self) -> str:
        return self._config.name

    @property
    def version(self) -> str:
        return self._config.version

    @property
    def started(self) -> datetime:
        """When this instance started (UTC)."""
        return self._started

    @property
    def is_stopped(self) -> bool:
        return self._stopped

    @property
    def stopped(self) -> asyncio.Future[None]:
        """A future resolved when the service stops (the 'done' surface)."""
        return self._stopped_future

    # -- topology ------------------------------------------------------------

    def add_group(self, name: str, *, queue_group: str | None = None) -> Group:
        """Create a top-level group with the given subject prefix."""
        return Group(self, name, _resolve_queue_group(queue_group, self._config.queue_group))

    def add_endpoint(
        self,
        name: str,
        handler: Handler,
        *,
        subject: str | None = None,
        queue_group: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> Endpoint:
        """Register a top-level endpoint (subject defaults to ``name``)."""
        return self._register(
            name,
            subject if subject is not None else name,
            handler,
            _resolve_queue_group(queue_group, self._config.queue_group),
            metadata,
        )

    def _register(
        self,
        name: str,
        subject: str,
        handler: Handler,
        queue_group: str,
        metadata: dict[str, str] | None,
    ) -> Endpoint:
        if self._stopped:
            raise ServiceConfigError("cannot add an endpoint to a stopped service")
        validate_endpoint_name(name)
        validate_subject(subject, wildcards=True)
        endpoint = Endpoint(name, subject, queue_group, handler, dict(metadata) if metadata else {})
        endpoint._subscription = self._nc.subscribe(
            subject,
            queue=queue_group or None,
            cb=partial(self._on_request, endpoint),
        )
        self._endpoints.append(endpoint)
        return endpoint

    # -- request handling ----------------------------------------------------

    async def _on_request(self, endpoint: Endpoint, msg: Msg) -> None:
        req = Request(msg)
        start = time.perf_counter_ns()
        try:
            await endpoint._handler(req)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # A handler fault must not kill the subscription. Ensure the failure
            # is counted — but a handler that already sent a deliberate
            # respond_error keeps ITS code/description as last_error (nats.go
            # keeps the handler-set error; the raise is secondary).
            if req._response_error is None:
                req._response_error = f"500:{exc}"
            if req.reply and not req._responded:
                with contextlib.suppress(Exception):  # best-effort error reply
                    await req.respond_error("500", str(exc))
            await self._dispatch_error(
                ServiceError(str(exc), code="500", subject=req.subject, endpoint=endpoint.name),
                cause=exc,
            )
        finally:
            endpoint.num_requests += 1
            endpoint.processing_time += time.perf_counter_ns() - start
            if req._response_error is not None:
                endpoint.num_errors += 1
                endpoint.last_error = req._response_error

    async def _dispatch_error(self, error: ServiceError, *, cause: BaseException | None = None) -> None:
        if cause is not None:
            error.__cause__ = cause
        handler = self._config.error_handler
        if handler is None:
            return
        # The error handler must not re-raise into the subscription task.
        with contextlib.suppress(Exception):
            result = handler(self, error)
            if result is not None:
                await result

    # -- monitoring ----------------------------------------------------------

    def _start_monitoring(self) -> None:
        for verb in _VERBS:
            for subject in (
                control_subject(verb),
                control_subject(verb, self.name),
                control_subject(verb, self.name, self._id),
            ):
                sub = self._nc.subscribe(subject, cb=partial(self._on_monitoring, verb))
                self._monitoring.append(sub)

    async def _on_monitoring(self, verb: str, msg: Msg) -> None:
        req = Request(msg)
        try:
            if verb == _PING:
                payload = self._ping()
            elif verb == _INFO:
                payload = self._info()
            else:
                payload = self._stats()
            await req.respond(json.dumps(payload).encode())
        except Exception as exc:  # never let a monitor fault kill the sub
            if req.reply and not req._responded:
                with contextlib.suppress(Exception):
                    await req.respond_error("500", f"error handling {verb} request: {exc}")
            await self._dispatch_error(
                ServiceError(str(exc), code="500", subject=req.subject, endpoint=verb),
                cause=exc,
            )

    def _ping(self) -> dict[str, object]:
        return PingResponse(
            name=self.name,
            id=self._id,
            version=self.version,
            metadata=self._metadata,
            type=PING_RESPONSE_TYPE,
        ).to_wire()

    def _info(self) -> dict[str, object]:
        return InfoResponse(
            name=self.name,
            id=self._id,
            version=self.version,
            metadata=self._metadata,
            type=INFO_RESPONSE_TYPE,
            description=self._config.description,
            endpoints=[endpoint._info() for endpoint in self._endpoints],
        ).to_wire()

    def _stats(self) -> dict[str, object]:
        stats_handler = self._config.stats_handler
        endpoints = [
            endpoint._stats(stats_handler(endpoint) if stats_handler is not None else None)
            for endpoint in self._endpoints
        ]
        return StatsResponse(
            name=self.name,
            id=self._id,
            version=self.version,
            metadata=self._metadata,
            type=STATS_RESPONSE_TYPE,
            started=self._started,
            endpoints=endpoints,
        ).to_wire()

    # -- local accessors -----------------------------------------------------

    def info(self) -> InfoResponse:
        """The service's INFO as a typed object (same content as ``$SRV.INFO``)."""
        return InfoResponse(
            name=self.name,
            id=self._id,
            version=self.version,
            metadata=dict(self._metadata),
            type=INFO_RESPONSE_TYPE,
            description=self._config.description,
            endpoints=[endpoint._info() for endpoint in self._endpoints],
        )

    def stats(self) -> StatsResponse:
        """The service's STATS as a typed object (same content as ``$SRV.STATS``)."""
        stats_handler = self._config.stats_handler
        return StatsResponse(
            name=self.name,
            id=self._id,
            version=self.version,
            metadata=dict(self._metadata),
            type=STATS_RESPONSE_TYPE,
            started=self._started,
            endpoints=[
                endpoint._stats(stats_handler(endpoint) if stats_handler is not None else None)
                for endpoint in self._endpoints
            ],
        )

    def reset(self) -> None:
        """Reset all endpoint counters and restart the ``started`` clock."""
        for endpoint in self._endpoints:
            endpoint.num_requests = 0
            endpoint.num_errors = 0
            endpoint.last_error = ""
            endpoint.processing_time = 0
        self._started = datetime.now(UTC)

    # -- lifecycle -----------------------------------------------------------

    async def stop(self) -> None:
        """Drain every subscription (endpoints + monitoring) and mark stopped.

        Idempotent, and drain-friendly: in-flight requests finish before their
        subscription closes. Resolves `stopped`.
        """
        if self._stopped:
            return
        self._stopped = True
        subs: list[Subscription] = [e._subscription for e in self._endpoints if e._subscription is not None]
        subs.extend(self._monitoring)
        self._endpoints.clear()
        self._monitoring.clear()
        for sub in subs:
            await sub.drain()
        if not self._stopped_future.done():
            self._stopped_future.set_result(None)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.stop()


def add_service(
    nc: "Client",
    config: ServiceConfig | None = None,
    /,
    **kwargs: Any,
) -> Service:
    """Create and start a micro service.

    Pass a `ServiceConfig`, or the config fields as keyword arguments
    (``add_service(nc, name="calc", version="1.0.0")``). Enables the ``$SRV``
    monitoring responders immediately; register handlers with
    `Service.add_endpoint()` / `Service.add_group()`.
    """
    if config is None:
        config = ServiceConfig(**kwargs)
    elif kwargs:
        raise TypeError("pass either a ServiceConfig or keyword arguments, not both")
    return Service(nc, config)
