"""Micro services framework (ADR-32): request/reply endpoints with monitoring.

    from natsio.micro import add_service

    async def handle_add(req):
        a, b = req.data.split(b"+")
        await req.respond(str(int(a) + int(b)).encode())

    svc = add_service(nc, name="calc", version="1.0.0")
    svc.add_endpoint("add", handle_add)
    async with svc:
        await svc.stopped

Each instance gets a unique id and answers ``$SRV.PING``/``INFO``/``STATS`` on
three subject variants (bare, ``.<name>``, ``.<name>.<id>``) with no queue
group, so every instance replies to discovery; request endpoints share a queue
group (default ``"q"``) so requests are load-balanced across instances.
"""

from natsio.micro.entities import (
    API_PREFIX,
    DEFAULT_QUEUE_GROUP,
    ERROR_CODE_HEADER,
    ERROR_HEADER,
    INFO_RESPONSE_TYPE,
    PING_RESPONSE_TYPE,
    STATS_RESPONSE_TYPE,
    EndpointInfo,
    EndpointStats,
    ErrorHandler,
    InfoResponse,
    PingResponse,
    ServiceConfig,
    StatsHandler,
    StatsResponse,
    validate_endpoint_name,
    validate_service_name,
    validate_version,
)
from natsio.micro.errors import ServiceConfigError, ServiceError
from natsio.micro.request import Request
from natsio.micro.service import Endpoint, Group, Service, add_service, control_subject

__all__ = [
    "API_PREFIX",
    "DEFAULT_QUEUE_GROUP",
    "ERROR_CODE_HEADER",
    "ERROR_HEADER",
    "INFO_RESPONSE_TYPE",
    "PING_RESPONSE_TYPE",
    "STATS_RESPONSE_TYPE",
    "Endpoint",
    "EndpointInfo",
    "EndpointStats",
    "ErrorHandler",
    "Group",
    "InfoResponse",
    "PingResponse",
    "Request",
    "Service",
    "ServiceConfig",
    "ServiceConfigError",
    "ServiceError",
    "StatsHandler",
    "StatsResponse",
    "add_service",
    "control_subject",
    "validate_endpoint_name",
    "validate_service_name",
    "validate_version",
]
