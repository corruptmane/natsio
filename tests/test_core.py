import asyncio
from functools import partial
from os import urandom
from typing import Final
import logging

import pytest
from natsio.client.core import NATSCore
from natsio.config.client import ClientConfig
from natsio.exceptions.client import BadSubjectError
from natsio.exceptions.protocol import SlowConsumer
from natsio.messages.core import CoreMsg
from natsio.subscriptions.core import Subscription

log = logging.getLogger(__name__)


@pytest.mark.asyncio()
async def test_core_subscription_async(nats_uri: list[str]) -> None:
    async with NATSCore(ClientConfig(servers=nats_uri)) as nc:
        subject: Final[str] = "test.core.async"
        messages_count: Final[int] = 5
        received_messages: list[CoreMsg] = []
        all_messages_received: asyncio.Future[None] = asyncio.Future()

        async def callback(msg: CoreMsg) -> None:
            received_messages.append(msg)
            if len(received_messages) == messages_count and not all_messages_received.done():
                all_messages_received.set_result(None)

        await nc.subscribe(subject, callback=callback)

        for i in range(messages_count):
            await nc.publish(subject, f"Hello, async: {i}".encode())

        await asyncio.wait_for(all_messages_received, timeout=1)

        assert len(received_messages) == messages_count
        for index, i in enumerate(range(messages_count)):
            assert received_messages[index].payload == f"Hello, async: {i}".encode()


@pytest.mark.asyncio()
async def test_core_subscription_next(nats_uri: list[str]) -> None:
    async with NATSCore(ClientConfig(servers=nats_uri)) as nc:
        subject: Final[str] = "test.core.next"
        data: Final[bytes] = b"Hello, next"

        sub = await nc.subscribe(subject)

        await nc.publish(subject, data)

        msg = await sub.next_msg()

        assert msg.payload == data


@pytest.mark.asyncio()
async def test_core_subscription_iterator(nats_uri: list[str]) -> None:
    async with NATSCore(ClientConfig(servers=nats_uri)) as nc:
        subject: Final[str] = "test.core.iterator"
        messages_count: Final[int] = 5
        futures: Final[list[asyncio.Future[None]]] = [asyncio.Future() for _ in range(messages_count)]
        received_messages: list[CoreMsg] = []

        sub = await nc.subscribe(subject)

        for i in range(messages_count):
            await nc.publish(subject, f"Hello, iterator {i}".encode())

        async def iterator_func(sub: Subscription) -> None:
            counter = 0
            async for msg in sub.messages:
                received_messages.append(msg)
                fut = futures[counter]
                fut.set_result(None)
                counter += 1

        iterator_task = asyncio.create_task(iterator_func(sub))
        await asyncio.wait_for(asyncio.gather(*futures), timeout=1)
        if not iterator_task.done():
            iterator_task.cancel()

        assert len(received_messages) == messages_count
        for index, i in enumerate(range(messages_count)):
            assert received_messages[index].payload == f"Hello, iterator {i}".encode()


@pytest.mark.asyncio()
async def test_core_multiple_subscriptions_async_queue(nats_uri: list[str]) -> None:
    async with NATSCore(ClientConfig(servers=nats_uri)) as nc:
        subject: Final[str] = "test.core.async"
        queue: Final[str] = "test.core.async.queue"
        messages_count: Final[int] = 5
        received_messages_1: list[CoreMsg] = []
        received_messages_2: list[CoreMsg] = []
        all_messages_received: asyncio.Future[None] = asyncio.Future()

        async def callback(msg: CoreMsg, messages_collection: list[CoreMsg]) -> None:
            messages_collection.append(msg)
            if (len(received_messages_1) + len(received_messages_2)) == messages_count and not all_messages_received.done():
                all_messages_received.set_result(None)

        await nc.subscribe(subject, queue, callback=partial(callback, messages_collection=received_messages_1))
        await nc.subscribe(subject, queue, callback=partial(callback, messages_collection=received_messages_2))

        for i in range(messages_count):
            await nc.publish(subject, f"Hello, async queue: {i}".encode())

        await asyncio.wait_for(all_messages_received, timeout=1)

        assert len(received_messages_1) != len(received_messages_2)
        assert len(received_messages_1 + received_messages_2) == messages_count


@pytest.mark.asyncio()
async def test_core_multiple_subscriptions_async_noqueue(nats_uri: list[str]) -> None:
    async with NATSCore(ClientConfig(servers=nats_uri)) as nc:
        subject: Final[str] = "test.core.async"
        messages_count: Final[int] = 5
        received_messages_1: list[CoreMsg] = []
        received_messages_2: list[CoreMsg] = []
        all_messages_received: asyncio.Future[None] = asyncio.Future()

        async def callback(msg: CoreMsg, messages_collection: list[CoreMsg]) -> None:
            messages_collection.append(msg)
            if (len(received_messages_1) + len(received_messages_2)) == messages_count * 2 and not all_messages_received.done():
                all_messages_received.set_result(None)

        await nc.subscribe(subject, callback=partial(callback, messages_collection=received_messages_1))
        await nc.subscribe(subject, callback=partial(callback, messages_collection=received_messages_2))

        for i in range(messages_count):
            await nc.publish(subject, f"Hello, async noqueue: {i}".encode())

        await asyncio.wait_for(all_messages_received, timeout=1)

        assert len(received_messages_1) == len(received_messages_2)
        assert len(received_messages_1 + received_messages_2) == messages_count * 2


@pytest.mark.asyncio()
async def test_core_subscription_pending_msgs_limit(nats_uri: list[str]) -> None:
    exc_future: asyncio.Future[Exception] = asyncio.Future()

    async def error_callback(e: Exception) -> None:
        exc_future.set_result(e)

    async with NATSCore(ClientConfig(servers=nats_uri), error_callback=error_callback) as nc:
        subject: Final[str] = "test.core.error.pending.msgs"
        fail_at: Final[int] = 10
        messages_count: Final[int] = fail_at * 3

        await nc.subscribe(subject, pending_msgs_limit=fail_at)
        for _ in range(messages_count):
            await nc.publish(subject, b"Hello, error pending msgs")

        await exc_future
        with pytest.raises(SlowConsumer):
            raise exc_future.result()


@pytest.mark.asyncio()
async def test_core_subscription_pending_bytes_limit(nats_uri: list[str]) -> None:
    exc_future: asyncio.Future[Exception] = asyncio.Future()

    async def error_callback(e: Exception) -> None:
        exc_future.set_result(e)

    async with NATSCore(ClientConfig(servers=nats_uri), error_callback=error_callback) as nc:
        subject: Final[str] = "test.core.error.pending.msgs"
        fail_at: Final[int] = 1024
        generate_bytes: Final[int] = fail_at * 3

        await nc.subscribe(subject, pending_bytes_limit=fail_at)

        msg = urandom(generate_bytes)
        await nc.publish(subject, msg)

        await exc_future
        with pytest.raises(SlowConsumer):
            raise exc_future.result()


@pytest.mark.asyncio()
async def test_core_subscription_unsub(nats_uri: list[str]) -> None:
    async with NATSCore(ClientConfig(servers=nats_uri)) as nc:
        subject: Final[str] = "test.core.unsub"
        message_received = asyncio.Event()

        async def callback(msg: CoreMsg) -> None:
            message_received.set()

        sub = await nc.subscribe(subject, callback=callback)

        await sub.unsubscribe()

        for _ in range(5):
            await nc.publish(subject, b"unsub")

        with pytest.raises(TimeoutError):
            await asyncio.wait_for(message_received.wait(), timeout=1)
            pytest.fail("Received a message after unsubscribing")


@pytest.mark.asyncio()
async def test_core_subscription_auto_unsub(nats_uri: list[str]) -> None:
    async with NATSCore(ClientConfig(servers=nats_uri)) as nc:
        subject: Final[str] = "test.core.auto_unsub"
        unsub_after: Final[int] = 5
        received_messages: list[CoreMsg] = []
        all_messages_received: asyncio.Future[None] = asyncio.Future()

        async def callback(msg: CoreMsg) -> None:
            received_messages.append(msg)
            if len(received_messages) == unsub_after and not all_messages_received.done():
                all_messages_received.set_result(None)

        sub = await nc.subscribe(subject, callback=callback)
        await sub.unsubscribe(max_msgs=unsub_after)

        for _ in range(10):
            await nc.publish(subject, b"unsub")

        await asyncio.wait_for(all_messages_received, timeout=1)

        # Ensure no more messages are received after unsubscribing 
        await asyncio.sleep(0.1)

        assert len(received_messages) == unsub_after


@pytest.mark.asyncio()
async def test_core_subscription_bad_subject(nats_uri: list[str]) -> None:
    async with NATSCore(ClientConfig(servers=nats_uri)) as nc:
        subject: Final[str] = "test<core/bad subject"
        with pytest.raises(BadSubjectError):
            await nc.subscribe(subject)


@pytest.mark.asyncio()
async def test_core_publish_bad_subject(nats_uri: list[str]) -> None:
    async with NATSCore(ClientConfig(servers=nats_uri)) as nc:
        subject: Final[str] = "test<core/bad subject"
        with pytest.raises(BadSubjectError):
            await nc.publish(subject, b"something")


@pytest.mark.asyncio()
async def test_core_subscription_wildcard(nats_uri: list[str]) -> None:
    async with NATSCore(ClientConfig(servers=nats_uri)) as nc:
        subscription_subject: Final[str] = "test.core.wildcard.*"
        publish_subject_template: Final[str] = "test.core.wildcard.{part}"
        messages_count: Final[int] = 5
        received_messages: list[CoreMsg] = []
        all_messages_received: asyncio.Future[None] = asyncio.Future()

        async def callback(msg: CoreMsg) -> None:
            received_messages.append(msg)
            if len(received_messages) == messages_count and not all_messages_received.done():
                all_messages_received.set_result(None)

        await nc.subscribe(subscription_subject, callback=callback)

        for i in range(messages_count):
            await nc.publish(publish_subject_template.format(part=i), b"something")

        await asyncio.wait_for(all_messages_received, timeout=1)

        assert len(received_messages) == messages_count
        for index, i in enumerate(range(messages_count)):
            assert received_messages[index].subject.endswith(str(i))
