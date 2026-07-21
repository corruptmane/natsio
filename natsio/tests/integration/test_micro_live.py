"""End-to-end micro (ADR-32) tests against a real nats-server.

The service runs on one client; a second plain client issues ordinary
requests, so the wire contract is verified exactly as an external observer
(the nats CLI, another language's client) would see it.
"""

import asyncio
import json

import pytest

import natsio
from natsio.errors import NoRespondersError
from natsio.micro import (
    ERROR_CODE_HEADER,
    ERROR_HEADER,
    Request,
    ServiceConfig,
    add_service,
)
from server import NatsServerProcess, require_server_binary


@pytest.fixture
async def server():
    binary = require_server_binary()
    process = NatsServerProcess(binary)
    await process.start()
    yield process
    await process.stop()


@pytest.fixture
async def nc(server: NatsServerProcess):
    client = await natsio.connect(server.url, connect_timeout=5.0, request_timeout=5.0)
    yield client
    await client.close()


@pytest.fixture
async def client2(server: NatsServerProcess):
    client = await natsio.connect(server.url, connect_timeout=5.0, request_timeout=5.0)
    yield client
    await client.close()


async def _wait_for(predicate, timeout: float = 2.0) -> None:  # noqa: ASYNC109
    """Poll until ``predicate()`` is truthy (stats update after the reply is sent)."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met within timeout")


class TestRequestReply:
    async def test_echo_end_to_end(self, nc: natsio.Client, client2: natsio.Client) -> None:
        async def echo(req: Request) -> None:
            await req.respond(req.data)

        svc = add_service(nc, name="echo", version="1.0.0")
        svc.add_endpoint("echo", echo)
        async with svc:
            reply = await client2.request("echo", b"hello")
            assert reply.data == b"hello"

    async def test_respond_error_surfaces_both_headers(self, nc: natsio.Client, client2: natsio.Client) -> None:
        async def boom(req: Request) -> None:
            await req.respond_error("418", "teapot", b"body")

        svc = add_service(nc, name="err", version="1.0.0")
        svc.add_endpoint("boom", boom)
        async with svc:
            reply = await client2.request("boom", b"")
            assert reply.data == b"body"
            assert reply.headers is not None
            assert reply.headers[ERROR_HEADER] == "teapot"
            assert reply.headers[ERROR_CODE_HEADER] == "418"
            await _wait_for(lambda: svc.stats().endpoints[0].num_errors == 1)
            ep = svc.stats().endpoints[0]
            assert ep.last_error == "418:teapot"

    async def test_handler_exception_becomes_error_response(self, nc: natsio.Client, client2: natsio.Client) -> None:
        async def crash(req: Request) -> None:
            raise ValueError("kaboom")

        svc = add_service(nc, name="crash", version="1.0.0")
        svc.add_endpoint("crash", crash)
        async with svc:
            reply = await client2.request("crash", b"")
            assert reply.headers is not None
            assert reply.headers[ERROR_CODE_HEADER] == "500"
            assert "kaboom" in reply.headers[ERROR_HEADER]
            await _wait_for(lambda: svc.stats().endpoints[0].num_errors == 1)
            ep = svc.stats().endpoints[0]
            assert ep.num_requests == 1
            assert ep.num_errors == 1
            assert ep.last_error.startswith("500:")
            assert "kaboom" in ep.last_error

    async def test_error_handler_callback_invoked(self, nc: natsio.Client, client2: natsio.Client) -> None:
        seen: list[str] = []

        async def on_error(service, error) -> None:
            seen.append(error.description)

        async def crash(req: Request) -> None:
            raise RuntimeError("routed")

        svc = add_service(nc, ServiceConfig(name="cb", version="1.0.0", error_handler=on_error))
        svc.add_endpoint("crash", crash)
        async with svc:
            await client2.request("crash", b"")
            await _wait_for(lambda: bool(seen))
            assert any("routed" in s for s in seen)


class TestMonitoring:
    async def test_ping_all_variants(self, nc: natsio.Client, client2: natsio.Client) -> None:
        svc = add_service(nc, name="mon", version="2.1.0", metadata={"env": "test"})
        async with svc:
            for subject in ("$SRV.PING", "$SRV.PING.mon", f"$SRV.PING.mon.{svc.id}"):
                reply = await client2.request(subject, b"")
                body = json.loads(reply.data)
                assert body["type"] == "io.nats.micro.v1.ping_response"
                assert body["name"] == "mon"
                assert body["id"] == svc.id
                assert body["version"] == "2.1.0"
                assert body["metadata"] == {"env": "test"}

    async def test_info_contract(self, nc: natsio.Client, client2: natsio.Client) -> None:
        async def h(req: Request) -> None:  # pragma: no cover - not invoked here
            await req.respond(b"")

        svc = add_service(nc, name="mon", version="1.0.0", description="a service")
        svc.add_endpoint("a", h, metadata={"role": "worker"})
        async with svc:
            for subject in ("$SRV.INFO", "$SRV.INFO.mon", f"$SRV.INFO.mon.{svc.id}"):
                reply = await client2.request(subject, b"")
                body = json.loads(reply.data)
                assert body["type"] == "io.nats.micro.v1.info_response"
                assert body["description"] == "a service"
                assert body["endpoints"] == [
                    {"name": "a", "subject": "a", "queue_group": "q", "metadata": {"role": "worker"}}
                ]

    async def test_stats_contract_and_accumulation(self, nc: natsio.Client, client2: natsio.Client) -> None:
        async def echo(req: Request) -> None:
            await req.respond(req.data)

        svc = add_service(nc, name="mon", version="1.0.0")
        svc.add_endpoint("echo", echo)
        async with svc:
            for _ in range(3):
                await client2.request("echo", b"x")
            await _wait_for(lambda: svc.stats().endpoints[0].num_requests == 3)
            reply = await client2.request(f"$SRV.STATS.mon.{svc.id}", b"")
            body = json.loads(reply.data)
            assert body["type"] == "io.nats.micro.v1.stats_response"
            assert "started" in body
            ep = body["endpoints"][0]
            assert ep["name"] == "echo"
            assert ep["num_requests"] >= 3
            assert ep["num_errors"] == 0
            # average is a sane derivation of total / count, in nanoseconds.
            assert ep["processing_time"] > 0
            assert ep["average_processing_time"] == ep["processing_time"] // ep["num_requests"]

    async def test_custom_stats_handler_data(self, nc: natsio.Client, client2: natsio.Client) -> None:
        def stats_handler(endpoint) -> dict[str, int]:
            return {"custom": endpoint.num_requests}

        async def echo(req: Request) -> None:
            await req.respond(req.data)

        svc = add_service(nc, ServiceConfig(name="mon", version="1.0.0", stats_handler=stats_handler))
        svc.add_endpoint("echo", echo)
        async with svc:
            await client2.request("echo", b"x")
            await _wait_for(lambda: svc.stats().endpoints[0].num_requests == 1)
            reply = await client2.request(f"$SRV.STATS.mon.{svc.id}", b"")
            body = json.loads(reply.data)
            assert body["endpoints"][0]["data"] == {"custom": 1}


class TestScaling:
    async def test_queue_split_but_all_answer_ping(self, nc: natsio.Client, client2: natsio.Client) -> None:
        services = []
        for _ in range(2):
            svc = add_service(nc, name="scaled", version="1.0.0")
            this_id = svc.id

            async def whoami(req: Request, _id: str = this_id) -> None:
                await req.respond(_id.encode())

            svc.add_endpoint("who", whoami)
            services.append(svc)

        ids = {s.id for s in services}
        try:
            # Endpoint requests are queue-balanced: over many requests, both
            # instances handle some.
            handled: set[str] = set()
            for _ in range(30):
                reply = await client2.request("who", b"")
                handled.add(reply.data.decode())
            assert handled == ids

            # PING has no queue group: every instance answers.
            answered: set[str] = set()
            async for msg in client2.request_many("$SRV.PING.scaled", b"", max_msgs=2, timeout=1.0):
                answered.add(json.loads(msg.data)["id"])
            assert answered == ids
        finally:
            for svc in services:
                await svc.stop()


class TestLifecycle:
    async def test_stop_removes_all_interest(self, nc: natsio.Client, client2: natsio.Client) -> None:
        async def echo(req: Request) -> None:
            await req.respond(req.data)

        svc = add_service(nc, name="life", version="1.0.0")
        svc.add_endpoint("echo", echo)
        # Before stop: endpoint and monitoring both respond.
        assert (await client2.request("echo", b"x")).data == b"x"
        await client2.request("$SRV.PING.life", b"")

        await svc.stop()
        assert svc.is_stopped
        assert svc.stopped.done()

        with pytest.raises(NoRespondersError):
            await client2.request("echo", b"x", timeout=1.0)
        with pytest.raises(NoRespondersError):
            await client2.request("$SRV.PING.life", b"", timeout=1.0)

    async def test_stop_is_idempotent(self, nc: natsio.Client) -> None:
        svc = add_service(nc, name="idem", version="1.0.0")
        await svc.stop()
        await svc.stop()
        assert svc.is_stopped

    async def test_async_context_manager_stops(self, nc: natsio.Client, client2: natsio.Client) -> None:
        async def echo(req: Request) -> None:
            await req.respond(req.data)

        svc = add_service(nc, name="ctx", version="1.0.0")
        svc.add_endpoint("echo", echo)
        async with svc as entered:
            assert entered is svc
            assert (await client2.request("echo", b"x")).data == b"x"
        assert svc.is_stopped
        with pytest.raises(NoRespondersError):
            await client2.request("echo", b"x", timeout=1.0)

    async def test_nested_group_subjects_live(self, nc: natsio.Client, client2: natsio.Client) -> None:
        async def handle(req: Request) -> None:
            await req.respond(b"ok")

        svc = add_service(nc, name="grp", version="1.0.0")
        svc.add_group("a").add_group("b").add_endpoint("c", handle)
        async with svc:
            assert (await client2.request("a.b.c", b"")).data == b"ok"


class TestReviewRegressions:
    async def test_respond_error_then_raise_keeps_handler_error(self, nc) -> None:
        """Review finding: the exception path clobbered a deliberately-set
        respond_error; the handler-set code/description must win last_error."""
        from natsio.micro import add_service

        async def handler(req) -> None:
            await req.respond_error("400", "deliberate")
            raise RuntimeError("post-respond failure")

        svc = add_service(nc, name="regress", version="1.0.0")
        try:
            endpoint = svc.add_endpoint("boom", handler)
            reply = await nc.request("boom", b"", timeout=5.0)
            assert reply.headers is not None
            assert reply.headers["Nats-Service-Error-Code"] == "400"
            await asyncio.sleep(0.05)  # let the finally-block accounting land
            assert endpoint.num_errors == 1
            assert endpoint.last_error == "400:deliberate"
        finally:
            await svc.stop()
