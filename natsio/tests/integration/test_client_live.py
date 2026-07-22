"""End-to-end tests of the public client API against a real nats-server."""

import asyncio

import pytest

import natsio
from natsio import Msg, PendingLimitPolicy
from natsio.errors import NoRespondersError, PermissionsViolationError, TimeoutError
from server import NatsServerProcess, require_server_binary


@pytest.fixture
async def server():
    binary = require_server_binary()
    process = NatsServerProcess(binary)
    await process.start()
    yield process
    await process.stop()


async def client_for(server: NatsServerProcess, **overrides) -> natsio.Client:
    return await natsio.connect(
        server.url,
        connect_timeout=5.0,
        request_timeout=5.0,
        reconnect_time_wait=0.05,
        **overrides,
    )


class TestCoreMessaging:
    async def test_publish_subscribe_iterator(self, server: NatsServerProcess) -> None:
        async with await client_for(server) as nc, nc.subscribe("orders.>") as sub:
            await nc.flush()
            await nc.publish("orders.new", b"order-1")
            msg = await sub.next_msg(timeout=5)
            assert msg.subject == "orders.new"
            assert msg.payload == b"order-1"

    async def test_headers_round_trip(self, server: NatsServerProcess) -> None:
        async with await client_for(server) as nc, nc.subscribe("hdr") as sub:
            await nc.flush()
            await nc.publish("hdr", b"body", headers={"Trace-Id": "abc", "Tag": ["x", "y"]})
            msg = await sub.next_msg(timeout=5)
            assert msg.payload == b"body"
            assert msg.headers is not None
            assert msg.headers["Trace-Id"] == "abc"
            assert msg.headers.get_all("Tag") == ["x", "y"]

    async def test_queue_group_distributes(self, server: NatsServerProcess) -> None:
        async with await client_for(server) as nc:
            received: list[str] = []
            done = asyncio.Event()

            async def handler(name: str, msg: Msg) -> None:
                received.append(name)
                if len(received) == 10:
                    done.set()

            nc.subscribe("work", queue="w", cb=lambda m: handler("a", m))
            nc.subscribe("work", queue="w", cb=lambda m: handler("b", m))
            await nc.flush()
            for _ in range(10):
                await nc.publish("work", b"job")
            await asyncio.wait_for(done.wait(), timeout=5)
            assert len(received) == 10  # each message delivered exactly once

    async def test_unsubscribe_after(self, server: NatsServerProcess) -> None:
        async with await client_for(server) as nc:
            sub = nc.subscribe("limited")
            await sub.unsubscribe_after(2)
            await nc.flush()
            for i in range(5):
                await nc.publish("limited", b"%d" % i)
            await nc.flush()
            assert (await sub.next_msg(timeout=5)).payload == b"0"
            assert (await sub.next_msg(timeout=5)).payload == b"1"
            with pytest.raises((TimeoutError, natsio.SubscriptionClosedError)):
                await sub.next_msg(timeout=0.3)

    async def test_throughput_smoke(self, server: NatsServerProcess) -> None:
        async with await client_for(server) as nc:
            count = 5_000
            received = 0
            done = asyncio.Event()

            def handler(msg: Msg) -> None:
                nonlocal received
                received += 1
                if received == count:
                    done.set()

            nc.subscribe("bulk", cb=handler, pending_msgs_limit=count * 2)
            await nc.flush()
            for i in range(count):
                await nc.publish("bulk", b"%d" % i)
            await asyncio.wait_for(done.wait(), timeout=30)
            assert received == count
            assert nc.stats.in_msgs >= count


class TestRequestReply:
    async def test_request_response(self, server: NatsServerProcess) -> None:
        async with await client_for(server) as nc:

            async def responder(msg: Msg) -> None:
                await msg.respond(b"pong:" + msg.payload)

            nc.subscribe("svc.echo", cb=responder)
            await nc.flush()
            reply = await nc.request("svc.echo", b"ping", timeout=5)
            assert reply.payload == b"pong:ping"

    async def test_concurrent_requests_are_not_crossed(self, server: NatsServerProcess) -> None:
        async with await client_for(server) as nc:

            async def responder(msg: Msg) -> None:
                await msg.respond(msg.payload)

            nc.subscribe("svc.mirror", cb=responder)
            await nc.flush()
            payloads = [b"req-%d" % i for i in range(50)]
            replies = await asyncio.gather(*(nc.request("svc.mirror", p, timeout=10) for p in payloads))
            assert [r.payload for r in replies] == payloads

    async def test_no_responders(self, server: NatsServerProcess) -> None:
        async with await client_for(server) as nc:
            with pytest.raises(NoRespondersError):
                await nc.request("nobody.listening", b"x", timeout=5)

    async def test_request_many(self, server: NatsServerProcess) -> None:
        async with await client_for(server) as nc:

            async def responder(name: bytes, msg: Msg) -> None:
                await msg.respond(name)

            for name in (b"a", b"b", b"c"):
                nc.subscribe("svc.fanout", cb=lambda m, n=name: responder(n, m))
            await nc.flush()
            replies = [msg.payload async for msg in nc.request_many("svc.fanout", b"?", timeout=2, stall=0.5)]
            assert sorted(replies) == [b"a", b"b", b"c"]

    async def test_request_many_max_msgs(self, server: NatsServerProcess) -> None:
        async with await client_for(server) as nc:

            async def responder(msg: Msg) -> None:
                await msg.respond(b"r")

            for _ in range(4):
                nc.subscribe("svc.many", cb=responder)
            await nc.flush()
            replies = [msg async for msg in nc.request_many("svc.many", b"?", timeout=5, max_msgs=2)]
            assert len(replies) == 2


class TestLifecycle:
    async def test_drain_delivers_queued_messages(self, server: NatsServerProcess) -> None:
        nc = await client_for(server)
        handled: list[bytes] = []

        async def handler(msg: Msg) -> None:
            await asyncio.sleep(0.001)
            handled.append(msg.payload)

        nc.subscribe("drain.me", cb=handler)
        await nc.flush()
        for i in range(50):
            await nc.publish("drain.me", b"%d" % i)
        await nc.flush()
        await asyncio.sleep(0.05)
        await nc.drain()
        assert len(handled) == 50
        assert nc.status is natsio.ConnectionState.CLOSED

    async def test_reconnect_replays_subscriptions(self, server: NatsServerProcess) -> None:
        nc = await client_for(server)
        try:
            sub = nc.subscribe("survive.>")
            await nc.flush()

            reconnected = asyncio.Event()

            async def watch() -> None:
                async for event in nc.events():
                    if isinstance(event, natsio.Reconnected):
                        reconnected.set()
                        return

            watcher = asyncio.create_task(watch())
            server.kill()
            restarted = NatsServerProcess(server.binary, port=server.port)
            await restarted.start()
            try:
                await asyncio.wait_for(reconnected.wait(), timeout=15)
                await nc.flush()
                await nc.publish("survive.yes", b"after-restart")
                msg = await sub.next_msg(timeout=5)
                assert msg.payload == b"after-restart"
                assert nc.stats.reconnects == 1
            finally:
                watcher.cancel()
                await restarted.stop()
        finally:
            await nc.close()

    async def test_force_reconnect(self, server: NatsServerProcess) -> None:
        # A long reconnect_time_wait proves force_reconnect bypasses the backoff:
        # without the bypass the Reconnected event would not arrive for 30s.
        nc = await natsio.connect(server.url, connect_timeout=5.0, reconnect_time_wait=30.0)
        try:
            sub = nc.subscribe("foo")
            await nc.flush()

            reconnected = asyncio.Event()

            async def watch() -> None:
                async for event in nc.events():
                    if isinstance(event, natsio.Reconnected):
                        reconnected.set()
                        return

            watcher = asyncio.create_task(watch())
            try:
                await nc.force_reconnect()
                await asyncio.wait_for(reconnected.wait(), timeout=10)
                assert nc.status is natsio.ConnectionState.CONNECTED
                assert nc.stats.reconnects == 1
                # The replayed subscription still delivers after the reconnect.
                await nc.publish("foo", b"after-force")
                msg = await sub.next_msg(timeout=5)
                assert msg.payload == b"after-force"
            finally:
                watcher.cancel()
        finally:
            await nc.close()

    async def test_permission_err_on_subscribe(self) -> None:
        binary = require_server_binary()
        config = """
        authorization {
            users = [
                { user: u, password: p, permissions: { subscribe: { deny: "forbidden" } } }
            ]
        }
        """
        process = NatsServerProcess(binary, config=config)
        await process.start()
        try:
            nc = await natsio.connect(
                process.url,
                user="u",
                password="p",
                permission_err_on_subscribe=True,
                connect_timeout=5.0,
            )
            try:
                forbidden = nc.subscribe("forbidden")
                allowed = nc.subscribe("allowed")
                await nc.flush()
                # The denied subscription is failed and terminated.
                with pytest.raises(PermissionsViolationError):
                    await forbidden.next_msg(timeout=5)
                assert forbidden.is_closed
                # A permitted subscription is unaffected.
                await nc.publish("allowed", b"ok")
                assert (await allowed.next_msg(timeout=5)).payload == b"ok"
            finally:
                await nc.close()
        finally:
            await process.stop()

    async def test_slow_consumer_drops_are_reported(self, server: NatsServerProcess) -> None:
        errors: list[Exception] = []

        nc = await natsio.connect(
            server.url,
            connect_timeout=5.0,
            error_cb=errors.append,
        )
        try:
            sub = nc.subscribe("flood", pending_msgs_limit=5, policy=PendingLimitPolicy.DROP_NEW)
            await nc.flush()
            for i in range(200):
                await nc.publish("flood", b"%d" % i)
            await nc.flush()
            await asyncio.sleep(0.2)
            assert sub.dropped > 0
            assert any(isinstance(e, natsio.SlowConsumerError) for e in errors)
        finally:
            await nc.close()
