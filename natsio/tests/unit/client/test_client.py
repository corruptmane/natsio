import asyncio

import pytest

import natsio
from fake import EventRecorder, FakeEnv, frames_written
from natsio import Client, ConnectOptions, Msg, PendingLimitPolicy
from natsio._internal.lifecycle import ConnectionState, Reconnected
from natsio.errors import (
    ConfigError,
    MaxPayloadExceededError,
    NoReplySubjectError,
    NoRespondersError,
    SlowConsumerError,
    SubscriptionClosedError,
    TimeoutError,
)


def make_options(**overrides) -> ConnectOptions:
    defaults: dict = {
        "servers": ("nats://s1.example:4222",),
        "connect_timeout": 1.0,
        "reconnect_time_wait": 0.01,
        "reconnect_time_wait_max": 0.02,
        "reconnect_jitter": 0.001,
        "reconnect_jitter_tls": 0.001,
        "no_randomize": True,
        "ping_interval": 60.0,
        "flush_timeout": 1.0,
        "drain_timeout": 1.0,
        "request_timeout": 1.0,
    }
    defaults.update(overrides)
    return ConnectOptions(**defaults)


async def connected_client(env: FakeEnv, **overrides) -> Client:
    client = Client(make_options(**overrides), _transport_factory=env.factory)
    await client.connect()
    return client


def deliver_msg(env: FakeEnv, sid: int, subject: str, payload: bytes, reply: str | None = None) -> None:
    reply_part = f" {reply}" if reply else ""
    env.current.deliver(f"MSG {subject} {sid}{reply_part} {len(payload)}\r\n".encode() + payload + b"\r\n")


class TestLifecycle:
    async def test_connect_and_close(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        assert client.is_connected
        assert client.status is ConnectionState.CONNECTED
        assert client.connected_url == "nats://s1.example:4222"
        await client.close()
        assert client.status is ConnectionState.CLOSED

    async def test_async_context_manager(self) -> None:
        env = FakeEnv()
        async with Client(make_options(), _transport_factory=env.factory) as client:
            assert client.is_connected
        assert client.status is ConnectionState.CLOSED

    async def test_server_info_and_max_payload(self) -> None:
        env = FakeEnv()
        env.info["max_payload"] = 4096
        client = await connected_client(env)
        try:
            assert client.max_payload == 4096
            assert client.server_info["version"] == "2.14.3"
        finally:
            await client.close()


class TestPublish:
    async def test_publish_bytes_and_str(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            await client.publish("foo", b"bytes")
            await client.publish("bar", "text")
            await client.flush()
            written = frames_written(env.current)
            assert b"PUB foo 5\r\nbytes\r\n" in written
            assert b"PUB bar 4\r\ntext\r\n" in written
        finally:
            await client.close()

    async def test_publish_with_headers_and_reply(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            await client.publish("foo", b"x", reply="r.1", headers={"A": "1"})
            await client.flush()
            written = frames_written(env.current)
            assert b"HPUB foo r.1 " in written
            assert b"NATS/1.0\r\nA: 1\r\n\r\nx\r\n" in written
        finally:
            await client.close()

    async def test_publish_validates_subject(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            with pytest.raises(ConfigError, match="wildcard"):
                await client.publish("foo.*", b"x")
            with pytest.raises(ConfigError, match="empty"):
                await client.publish("", b"x")
        finally:
            await client.close()

    async def test_publish_rejects_oversized_payload(self) -> None:
        env = FakeEnv()
        env.info["max_payload"] = 16
        client = await connected_client(env)
        try:
            with pytest.raises(MaxPayloadExceededError, match="exceeds"):
                await client.publish("foo", b"x" * 17)
        finally:
            await client.close()

    async def test_stats_track_publishes(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            await client.publish("foo", b"12345")
            assert client.stats.out_msgs == 1
            assert client.stats.out_bytes == 5
        finally:
            await client.close()


class TestSubscribe:
    async def test_iterator_mode(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            sub = client.subscribe("foo.>")
            await client.flush()
            assert f"SUB foo.> {sub.sid}\r\n".encode() in frames_written(env.current)
            deliver_msg(env, sub.sid, "foo.bar", b"hello")
            msg = await sub.next_msg(timeout=1)
            assert isinstance(msg, Msg)
            assert (msg.subject, msg.payload) == ("foo.bar", b"hello")
            assert msg.data == b"hello"
        finally:
            await client.close()

    async def test_async_for_iteration(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            sub = client.subscribe("evt")
            for i in range(3):
                deliver_msg(env, sub.sid, "evt", b"%d" % i)
            got = []
            async for msg in sub:
                got.append(msg.payload)
                if len(got) == 3:
                    break
            assert got == [b"0", b"1", b"2"]
        finally:
            await client.close()

    async def test_callback_mode(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        received: list[Msg] = []
        done = asyncio.Event()

        async def handler(msg: Msg) -> None:
            received.append(msg)
            done.set()

        try:
            sub = client.subscribe("cb", cb=handler)
            deliver_msg(env, sub.sid, "cb", b"payload")
            await asyncio.wait_for(done.wait(), timeout=1)
            assert received[0].payload == b"payload"
        finally:
            await client.close()

    async def test_sync_callback_supported(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        received: list[bytes] = []
        try:
            sub = client.subscribe("cb", cb=lambda msg: received.append(msg.payload))
            deliver_msg(env, sub.sid, "cb", b"sync")
            for _ in range(10):
                await asyncio.sleep(0)
                if received:
                    break
            assert received == [b"sync"]
        finally:
            await client.close()

    async def test_callback_mode_rejects_iteration(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            sub = client.subscribe("cb", cb=lambda msg: None)
            with pytest.raises(SubscriptionClosedError, match="callback mode"):
                sub.__aiter__()
        finally:
            await client.close()

    async def test_queue_group_in_sub_frame(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            sub = client.subscribe("work", queue="workers")
            await client.flush()
            assert f"SUB work workers {sub.sid}\r\n".encode() in frames_written(env.current)
            assert sub.queue_group == "workers"
        finally:
            await client.close()

    async def test_subscribe_validates(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            with pytest.raises(ConfigError):
                client.subscribe("foo..bar")
            with pytest.raises(ConfigError, match="queue group"):
                client.subscribe("foo", queue="bad queue")
        finally:
            await client.close()

    async def test_unsubscribe_stops_delivery(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            sub = client.subscribe("foo")
            await sub.unsubscribe()
            await client.flush()
            assert f"UNSUB {sub.sid}\r\n".encode() in frames_written(env.current)
            assert sub.is_closed
            deliver_msg(env, sub.sid, "foo", b"late")
            assert sub.pending_msgs == 0
        finally:
            await client.close()

    async def test_unsubscribe_after_counts_from_creation(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            sub = client.subscribe("foo")
            deliver_msg(env, sub.sid, "foo", b"1")
            await sub.unsubscribe_after(3)
            await client.flush()
            assert f"UNSUB {sub.sid} 3\r\n".encode() in frames_written(env.current)
            for payload in (b"2", b"3", b"4"):
                deliver_msg(env, sub.sid, "foo", payload)
            # The server-side contract counts total deliveries since SUB.
            assert sub.delivered == 3
        finally:
            await client.close()

    async def test_context_manager_unsubscribes(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            async with client.subscribe("foo") as sub:
                assert not sub.is_closed
            assert sub.is_closed
        finally:
            await client.close()

    async def test_next_msg_timeout(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            sub = client.subscribe("quiet")
            with pytest.raises(TimeoutError):
                await sub.next_msg(timeout=0.02)
        finally:
            await client.close()

    async def test_subscriptions_survive_reconnect(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        client = await connected_client(env)
        client._conn.bus.subscribe(recorder.hook)
        try:
            sub = client.subscribe("resub.>")
            await client.flush()
            env.current.drop()
            await recorder.wait_for(Reconnected)
            await client.flush()
            assert f"SUB resub.> {sub.sid}\r\n".encode() in frames_written(env.current)
            deliver_msg(env, sub.sid, "resub.x", b"after")
            assert (await sub.next_msg(timeout=1)).payload == b"after"
        finally:
            await client.close()


class TestBackpressure:
    async def test_drop_new_is_loud(self) -> None:
        env = FakeEnv()
        errors: list[Exception] = []
        client = await connected_client(env)
        client._conn.bus.subscribe(
            lambda e: errors.append(e.error) if hasattr(e, "error") and e.error is not None else None
        )
        try:
            sub = client.subscribe("flood", pending_msgs_limit=2)
            for i in range(5):
                deliver_msg(env, sub.sid, "flood", b"%d" % i)
            assert sub.pending_msgs == 2
            assert sub.dropped == 3
            assert any(isinstance(e, SlowConsumerError) for e in errors)
            # The oldest messages are the ones retained.
            assert (await sub.next_msg(timeout=1)).payload == b"0"
        finally:
            await client.close()

    async def test_drop_old_keeps_newest(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            sub = client.subscribe("flood", pending_msgs_limit=2, policy=PendingLimitPolicy.DROP_OLD)
            for i in range(5):
                deliver_msg(env, sub.sid, "flood", b"%d" % i)
            assert sub.pending_msgs == 2
            assert sub.dropped == 3
            # The two NEWEST survive, oldest-first within what remains.
            assert (await sub.next_msg(timeout=1)).payload == b"3"
            assert (await sub.next_msg(timeout=1)).payload == b"4"
        finally:
            await client.close()

    async def test_error_policy_raises_to_consumer(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            sub = client.subscribe("flood", pending_msgs_limit=1, policy=PendingLimitPolicy.ERROR)
            deliver_msg(env, sub.sid, "flood", b"a")
            deliver_msg(env, sub.sid, "flood", b"b")
            with pytest.raises(SlowConsumerError):
                async for _ in sub:
                    pass
        finally:
            await client.close()

    async def test_block_policy_pauses_reading(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            sub = client.subscribe("flood", pending_msgs_limit=2, policy=PendingLimitPolicy.BLOCK)
            for i in range(2):
                deliver_msg(env, sub.sid, "flood", b"%d" % i)
            assert env.current.reading_paused is True
            await sub.next_msg(timeout=1)
            assert env.current.reading_paused is False
        finally:
            await client.close()

    async def test_byte_limit_enforced(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            sub = client.subscribe("big", pending_bytes_limit=10)
            deliver_msg(env, sub.sid, "big", b"x" * 8)
            deliver_msg(env, sub.sid, "big", b"y" * 8)
            assert sub.pending_msgs == 1
            assert sub.dropped == 1
            assert sub.pending_bytes == 8
        finally:
            await client.close()


class TestRequestReply:
    async def test_request_returns_reply(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            task = asyncio.create_task(client.request("svc", b"ping", timeout=1))
            await asyncio.sleep(0)
            await client.flush()
            written = frames_written(env.current)
            assert client.inbox_prefix.encode() in written
            reply_subject = _extract_reply(written, client.inbox_prefix)
            mux_sid = client._mux_sid
            assert mux_sid is not None
            deliver_msg(env, mux_sid, reply_subject, b"pong")
            msg = await asyncio.wait_for(task, timeout=1)
            assert msg.payload == b"pong"
        finally:
            await client.close()

    async def test_request_timeout(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            with pytest.raises(TimeoutError, match="no reply"):
                await client.request("svc", b"ping", timeout=0.05)
            # The sink must not leak after the timeout.
            assert client._sinks == {}
        finally:
            await client.close()

    async def test_no_responders_raises(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            task = asyncio.create_task(client.request("svc", b"x", timeout=1))
            await asyncio.sleep(0)
            await client.flush()
            reply_subject = _extract_reply(frames_written(env.current), client.inbox_prefix)
            block = b"NATS/1.0 503\r\n\r\n"
            env.current.deliver(
                f"HMSG {reply_subject} {client._mux_sid} {len(block)} {len(block)}\r\n".encode() + block + b"\r\n"
            )
            with pytest.raises(NoRespondersError):
                await asyncio.wait_for(task, timeout=1)
        finally:
            await client.close()

    async def test_single_mux_inbox_for_many_requests(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            tasks = [asyncio.create_task(client.request(f"svc.{i}", b"x", timeout=1)) for i in range(3)]
            await asyncio.sleep(0)
            await client.flush()
            written = frames_written(env.current)
            # Exactly one wildcard SUB is created regardless of request count.
            assert written.count(f"SUB {client.inbox_prefix}.*".encode()) == 1
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            await client.close()

    async def test_request_many_collects_until_max(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            replies: list[bytes] = []

            async def collect() -> None:
                replies.extend([msg.payload async for msg in client.request_many("svc", b"x", timeout=1, max_msgs=2)])

            task = asyncio.create_task(collect())
            await asyncio.sleep(0)
            await client.flush()
            reply_subject = _extract_reply(frames_written(env.current), client.inbox_prefix)
            mux_sid = client._mux_sid
            assert mux_sid is not None
            for payload in (b"one", b"two", b"three"):
                deliver_msg(env, mux_sid, reply_subject, payload)
            await asyncio.wait_for(task, timeout=2)
            assert replies == [b"one", b"two"]
        finally:
            await client.close()

    async def test_request_many_stops_on_stall(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            replies: list[bytes] = []

            async def collect() -> None:
                replies.extend([msg.payload async for msg in client.request_many("svc", b"x", timeout=2, stall=0.05)])

            task = asyncio.create_task(collect())
            await asyncio.sleep(0)
            await client.flush()
            reply_subject = _extract_reply(frames_written(env.current), client.inbox_prefix)
            mux_sid = client._mux_sid
            assert mux_sid is not None
            deliver_msg(env, mux_sid, reply_subject, b"only")
            await asyncio.wait_for(task, timeout=2)
            assert replies == [b"only"]
        finally:
            await client.close()


class TestRespond:
    async def test_respond_publishes_to_reply(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            sub = client.subscribe("svc")
            deliver_msg(env, sub.sid, "svc", b"req", reply="_INBOX.x.1")
            msg = await sub.next_msg(timeout=1)
            await msg.respond(b"resp")
            await client.flush()
            assert b"PUB _INBOX.x.1 4\r\nresp\r\n" in frames_written(env.current)
        finally:
            await client.close()

    async def test_respond_without_reply_raises(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            sub = client.subscribe("svc")
            deliver_msg(env, sub.sid, "svc", b"req")
            msg = await sub.next_msg(timeout=1)
            with pytest.raises(NoReplySubjectError):
                await msg.respond(b"resp")
        finally:
            await client.close()


class TestEvents:
    async def test_event_stream_receives_reconnect(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        seen: list[object] = []

        async def watch() -> None:
            # Open-ended stream: it is cancelled, never exhausted, so this
            # cannot be a comprehension.
            async for event in client.events():
                seen.append(event)  # noqa: PERF401

        task = asyncio.create_task(watch())
        try:
            await asyncio.sleep(0)
            env.current.drop()
            for _ in range(200):
                await asyncio.sleep(0.005)
                if any(isinstance(e, Reconnected) for e in seen):
                    break
            assert any(isinstance(e, Reconnected) for e in seen)
            assert client.stats.reconnects == 1
        finally:
            await client.close()
            await asyncio.wait_for(task, timeout=1)


class TestConnectFactory:
    async def test_connect_helper_merges_kwargs(self) -> None:
        env = FakeEnv()
        client = await natsio.connect(
            "nats://s1.example:4222",
            options=make_options(),
            name="svc-a",
            ping_interval=45.0,
            _transport_factory=env.factory,
        )
        try:
            assert client.is_connected
            assert client._options.name == "svc-a"
            assert client._options.ping_interval == 45.0
            assert client._options.servers == ("nats://s1.example:4222",)
        finally:
            await client.close()


def _extract_reply(written: bytes, inbox_prefix: str) -> str:
    """Pull the reply subject out of the PUB frame the client emitted."""
    marker = inbox_prefix.encode()
    index = written.rindex(marker)
    end = written.index(b" ", index)
    return written[index:end].decode()
