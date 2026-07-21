"""Unit tests for the micro (ADR-32) framework: no server required.

Topology (subject building, queue-group inheritance) is exercised against a
fake client that only records ``subscribe`` calls, so these stay hermetic.
"""

from collections.abc import Callable
from typing import cast

import pytest

import natsio
from natsio.micro import (
    ERROR_CODE_HEADER,
    ERROR_HEADER,
    INFO_RESPONSE_TYPE,
    PING_RESPONSE_TYPE,
    STATS_RESPONSE_TYPE,
    EndpointInfo,
    EndpointStats,
    InfoResponse,
    PingResponse,
    ServiceConfig,
    ServiceConfigError,
    StatsResponse,
    add_service,
    control_subject,
    validate_endpoint_name,
    validate_service_name,
    validate_version,
)


class FakeSubscription:
    def __init__(self, subject: str, queue: str | None) -> None:
        self.subject = subject
        self.queue = queue
        self.drained = False

    async def drain(self) -> None:
        self.drained = True


class FakeClient:
    """Records subscriptions; enough of the Client surface for topology tests."""

    def __init__(self) -> None:
        self.subs: list[FakeSubscription] = []

    def subscribe(
        self,
        subject: str,
        *,
        queue: str | None = None,
        cb: Callable[..., object] | None = None,
    ) -> FakeSubscription:
        sub = FakeSubscription(subject, queue)
        self.subs.append(sub)
        return sub


async def _handler(req: object) -> None:  # pragma: no cover - never invoked in unit tests
    pass


def _add(nc: FakeClient, config: ServiceConfig | None = None, **kwargs: object):
    """Call add_service with the fake client (cast to satisfy the nominal type)."""
    return add_service(cast("natsio.Client", nc), config, **kwargs)


# --------------------------------------------------------------------------- #
# Validation                                                                   #
# --------------------------------------------------------------------------- #


class TestValidation:
    @pytest.mark.parametrize("name", ["calc", "my-service", "A_1", "x", "SVC-2_a"])
    def test_valid_names(self, name: str) -> None:
        validate_service_name(name)
        validate_endpoint_name(name)

    @pytest.mark.parametrize("name", ["", "with.dot", "with space", "star*", "gt>", "üni", "a/b"])
    def test_invalid_names(self, name: str) -> None:
        with pytest.raises(ServiceConfigError):
            validate_service_name(name)
        with pytest.raises(ServiceConfigError):
            validate_endpoint_name(name)

    @pytest.mark.parametrize(
        "version",
        ["0.0.0", "1.0.0", "1.2.3", "10.20.30", "1.0.0-rc.1", "1.0.0-alpha.1+build.5", "2.1.0+meta"],
    )
    def test_valid_versions(self, version: str) -> None:
        validate_version(version)

    @pytest.mark.parametrize("version", ["", "1", "1.0", "1.0.0.0", "v1.0.0", "1.0.x", "01.0.0", "abc"])
    def test_invalid_versions(self, version: str) -> None:
        with pytest.raises(ServiceConfigError):
            validate_version(version)

    def test_config_rejects_bad_name(self) -> None:
        with pytest.raises(ServiceConfigError):
            ServiceConfig(name="bad name", version="1.0.0")

    def test_config_rejects_bad_version(self) -> None:
        with pytest.raises(ServiceConfigError):
            ServiceConfig(name="calc", version="1.0")

    def test_config_defaults(self) -> None:
        cfg = ServiceConfig(name="calc", version="1.2.3")
        assert cfg.queue_group == "q"
        assert cfg.description == ""
        assert cfg.metadata is None
        assert cfg.stats_handler is None
        assert cfg.error_handler is None


# --------------------------------------------------------------------------- #
# Control subjects                                                             #
# --------------------------------------------------------------------------- #


class TestControlSubject:
    def test_variants(self) -> None:
        assert control_subject("PING") == "$SRV.PING"
        assert control_subject("PING", "calc") == "$SRV.PING.calc"
        assert control_subject("PING", "calc", "ID9") == "$SRV.PING.calc.ID9"
        assert control_subject("INFO", "svc") == "$SRV.INFO.svc"
        assert control_subject("STATS", "svc", "X") == "$SRV.STATS.svc.X"

    def test_id_without_name_rejected(self) -> None:
        with pytest.raises(ServiceConfigError):
            control_subject("PING", "", "ID")


# --------------------------------------------------------------------------- #
# Topology: subject building + queue-group inheritance                         #
# --------------------------------------------------------------------------- #


class TestTopology:
    async def test_monitoring_subscriptions(self) -> None:
        nc = FakeClient()
        svc = _add(nc, name="calc", version="1.0.0")
        subjects = {s.subject for s in nc.subs}
        # 3 verbs x 3 variants, all queue-group-less.
        for verb in ("PING", "INFO", "STATS"):
            assert f"$SRV.{verb}" in subjects
            assert f"$SRV.{verb}.calc" in subjects
            assert f"$SRV.{verb}.calc.{svc.id}" in subjects
        assert all(s.queue is None for s in nc.subs)
        assert len(nc.subs) == 9

    async def test_endpoint_default_subject_and_queue(self) -> None:
        nc = FakeClient()
        svc = _add(nc, name="calc", version="1.0.0")
        ep = svc.add_endpoint("add", _handler)
        assert ep.subject == "add"
        assert ep.queue_group == "q"
        sub = nc.subs[-1]
        assert sub.subject == "add"
        assert sub.queue == "q"

    async def test_endpoint_explicit_subject(self) -> None:
        nc = FakeClient()
        svc = _add(nc, name="calc", version="1.0.0")
        ep = svc.add_endpoint("add", _handler, subject="math.add")
        assert ep.subject == "math.add"

    async def test_group_prefix(self) -> None:
        nc = FakeClient()
        svc = _add(nc, name="calc", version="1.0.0")
        group = svc.add_group("math")
        ep = group.add_endpoint("add", _handler)
        assert ep.subject == "math.add"
        assert nc.subs[-1].subject == "math.add"

    async def test_nested_groups_dotted_prefix(self) -> None:
        nc = FakeClient()
        svc = _add(nc, name="calc", version="1.0.0")
        inner = svc.add_group("a").add_group("b").add_group("c")
        ep = inner.add_endpoint("do", _handler)
        assert ep.subject == "a.b.c.do"

    async def test_queue_group_inheritance(self) -> None:
        nc = FakeClient()
        svc = _add(nc, name="calc", version="1.0.0", queue_group="workers")
        # service default flows down to a plain endpoint...
        assert svc.add_endpoint("a", _handler).queue_group == "workers"
        # ...and to group endpoints...
        group = svc.add_group("g")
        assert group.add_endpoint("b", _handler).queue_group == "workers"
        # ...unless the group overrides it (inherited by its endpoints)...
        override = svc.add_group("h", queue_group="special")
        assert override.add_endpoint("c", _handler).queue_group == "special"
        # ...and a nested group inherits the override.
        assert override.add_group("i").add_endpoint("d", _handler).queue_group == "special"
        # An endpoint can override directly.
        assert svc.add_endpoint("e", _handler, queue_group="solo").queue_group == "solo"

    async def test_add_service_rejects_config_and_kwargs(self) -> None:
        nc = FakeClient()
        cfg = ServiceConfig(name="calc", version="1.0.0")
        with pytest.raises(TypeError):
            _add(nc, cfg, name="other")

    async def test_endpoint_name_validated(self) -> None:
        nc = FakeClient()
        svc = _add(nc, name="calc", version="1.0.0")
        with pytest.raises(ServiceConfigError):
            svc.add_endpoint("bad name", _handler)


# --------------------------------------------------------------------------- #
# Entity JSON round-trips: pin exact type strings and field names              #
# --------------------------------------------------------------------------- #


class TestEntities:
    def test_error_header_constants(self) -> None:
        assert ERROR_HEADER == "Nats-Service-Error"
        assert ERROR_CODE_HEADER == "Nats-Service-Error-Code"

    def test_response_type_strings(self) -> None:
        assert PING_RESPONSE_TYPE == "io.nats.micro.v1.ping_response"
        assert INFO_RESPONSE_TYPE == "io.nats.micro.v1.info_response"
        assert STATS_RESPONSE_TYPE == "io.nats.micro.v1.stats_response"

    def test_ping_response_fields(self) -> None:
        wire = PingResponse(name="calc", id="ID", version="1.0.0", metadata={"k": "v"}).to_wire()
        assert wire == {
            "name": "calc",
            "id": "ID",
            "version": "1.0.0",
            "metadata": {"k": "v"},
            "type": "io.nats.micro.v1.ping_response",
        }
        assert PingResponse.from_wire(wire).name == "calc"

    def test_info_response_fields(self) -> None:
        info = InfoResponse(
            name="calc",
            id="ID",
            version="1.0.0",
            metadata={},
            description="does maths",
            endpoints=[EndpointInfo(name="add", subject="add", queue_group="q", metadata={})],
        )
        wire = info.to_wire()
        assert wire["type"] == "io.nats.micro.v1.info_response"
        assert set(wire) == {"name", "id", "version", "metadata", "type", "description", "endpoints"}
        assert wire["endpoints"] == [{"name": "add", "subject": "add", "queue_group": "q", "metadata": {}}]
        assert InfoResponse.from_wire(wire).endpoints[0].name == "add"

    def test_stats_response_fields(self) -> None:
        stats = StatsResponse(
            name="calc",
            id="ID",
            version="1.0.0",
            metadata={},
            endpoints=[
                EndpointStats(
                    name="add",
                    subject="add",
                    queue_group="q",
                    num_requests=3,
                    num_errors=1,
                    last_error="500:boom",
                    processing_time=300,
                    average_processing_time=100,
                )
            ],
        )
        wire = stats.to_wire()
        assert wire["type"] == "io.nats.micro.v1.stats_response"
        ep = wire["endpoints"][0]
        assert set(ep) == {
            "name",
            "subject",
            "queue_group",
            "num_requests",
            "num_errors",
            "last_error",
            "processing_time",
            "average_processing_time",
        }
        assert ep["processing_time"] == 300
        assert ep["average_processing_time"] == 100
        # data is omitted when there is no stats handler payload.
        assert "data" not in ep

    def test_stats_data_present_when_set(self) -> None:
        wire = EndpointStats(name="a", subject="a", queue_group="q", data={"total": 5}).to_wire()
        assert wire["data"] == {"total": 5}

    def test_unknown_fields_round_trip(self) -> None:
        raw = {
            "name": "calc",
            "id": "ID",
            "version": "1.0.0",
            "metadata": {},
            "type": "io.nats.micro.v1.ping_response",
            "future_field": 42,
        }
        assert PingResponse.from_wire(raw).to_wire()["future_field"] == 42


class TestOptionalAwait:
    """Service/Group/Endpoint tolerate `await` as a no-op, like subscribe()."""

    async def test_await_returns_self(self) -> None:
        svc = _add(FakeClient(), name="aw", version="1.0.0")
        assert (await svc) is svc
        group = await svc.add_group("math")
        assert group.add_group("x") is not None

        async def handler(req) -> None:  # pragma: no cover - never called
            pass

        endpoint = await group.add_endpoint("mul", handler)
        assert (await endpoint) is endpoint
