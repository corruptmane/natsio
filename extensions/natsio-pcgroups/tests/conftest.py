"""Shared fixtures for the natsio-pcgroups test suite.

The nats-server process manager lives in the ``natsio-testing`` extension; this
conftest adds only the repo-local binary lookup (``tools/.bin/nats-server``,
our pinned 2.14) and the connect fixtures.
"""

import asyncio
import os
from datetime import timedelta
from pathlib import Path

import pytest
from natsio.pcgroups import PartitionedMsg  # ty: ignore[unresolved-import]

# Editable installs: the pkgutil ``__path__`` merge is invisible to the type
# checker (real wheel installs resolve statically). Runtime coverage: the whole
# live suite imports through this seam.
from natsio.testing import NatsServerProcess, find_server_binary, free_port  # ty: ignore[unresolved-import]

import natsio
from natsio.jetstream import AckPolicy, ConsumerConfig

__all__ = [
    "GROUP_TEMPLATE",
    "NatsServerProcess",
    "Recorder",
    "free_port",
    "require_server_binary",
    "wait_for_any",
    "wait_for_total",
]

# The oracle's own test template: strictly ordered processing (one unacked
# message at a time) with a 1s ack wait, which is also what drives the failover
# timers, so the live tests react in about a second instead of five.
GROUP_TEMPLATE = ConsumerConfig(
    max_ack_pending=1,
    ack_wait=timedelta(seconds=1),
    ack_policy=AckPolicy.EXPLICIT,
)


class Recorder:
    """A message handler that records deliveries and wakes waiters on arrival.

    Purely event driven: `wait_for_count` resolves from the handler itself, so
    no test ever sleeps waiting for a message.
    """

    def __init__(self, name: str = "") -> None:
        self.name = name
        self.subjects: list[str] = []
        self.partitions: list[int | None] = []
        self._arrived = asyncio.Event()

    @property
    def count(self) -> int:
        return len(self.subjects)

    async def __call__(self, msg: PartitionedMsg) -> None:
        self.subjects.append(msg.subject)
        self.partitions.append(msg.partition)
        await msg.ack()
        self._arrived.set()

    async def wait_for_count(self, count: int, timeout: float = 20.0) -> None:  # noqa: ASYNC109
        async with asyncio.timeout(timeout):
            while self.count < count:
                self._arrived.clear()
                if self.count >= count:
                    return
                await self._arrived.wait()


async def wait_for_total(recorders: "list[Recorder]", count: int, timeout: float = 20.0) -> None:  # noqa: ASYNC109
    """Wait until the recorders have received ``count`` messages between them.

    For splits whose exact shape is the server's choice (a hash decides which
    partition each subject lands in), only the total is predictable.
    """

    def total() -> int:
        return sum(recorder.count for recorder in recorders)

    async with asyncio.timeout(timeout):
        while total() < count:
            for recorder in recorders:
                recorder._arrived.clear()
            if total() >= count:
                return
            waiters = [asyncio.ensure_future(recorder._arrived.wait()) for recorder in recorders]
            try:
                await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for waiter in waiters:
                    waiter.cancel()
                await asyncio.wait(waiters)


async def wait_for_any(*awaitables, timeout: float = 20.0) -> None:  # noqa: ASYNC109
    """Wait until the first awaitable completes; cancel the rest.

    Used where only one of several member instances is expected to be served
    and the test must not care which one.
    """
    tasks = [asyncio.ensure_future(item) for item in awaitables]
    try:
        done, _ = await asyncio.wait(tasks, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        if not done:
            raise TimeoutError(f"none of {len(tasks)} awaitables completed within {timeout}s")
        for task in done:
            task.result()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.wait(tasks)


# extensions/natsio-pcgroups/tests/conftest.py -> repo root is parents[3].
_REPO_ROOT = Path(__file__).resolve().parents[3]


def require_server_binary() -> str:
    candidate = os.environ.get("NATS_SERVER_BIN")
    if candidate and Path(candidate).is_file():
        return candidate
    local = _REPO_ROOT / "tools" / ".bin" / "nats-server"
    if local.is_file():
        return str(local)
    found = find_server_binary()
    if found is None:
        pytest.skip("nats-server binary not found (set NATS_SERVER_BIN or drop it in tools/.bin)")
    return found


@pytest.fixture
def server_binary() -> str:
    return require_server_binary()


@pytest.fixture
async def server():
    process = NatsServerProcess(require_server_binary(), jetstream=True)
    await process.start()
    yield process
    await process.stop()


@pytest.fixture
async def nc(server: NatsServerProcess):
    client = await natsio.connect(server.url, connect_timeout=5.0, request_timeout=5.0)
    yield client
    await client.close()
