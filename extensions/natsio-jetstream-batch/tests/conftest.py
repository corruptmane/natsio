"""Shared fixtures for the natsio-jetstream-batch test suite.

The nats-server process manager lives in the ``natsio-testing`` extension; this
conftest adds the repo-local binary lookup (``tools/.bin/nats-server``, our
pinned 2.14), the connect/JetStream fixtures, and the fast-ingest capability
probe that skips the live suite on a server without ``allow_batched``.
"""

import os
from pathlib import Path

import pytest

# Editable installs: the pkgutil ``__path__`` merge is invisible to the type
# checker (real wheel installs resolve statically). Runtime coverage: the whole
# live suite imports through this seam.
from natsio.jetstream_batch import ALLOW_BATCHED  # ty: ignore[unresolved-import]
from natsio.testing import NatsServerProcess, find_server_binary, free_port  # ty: ignore[unresolved-import]

import natsio
from natsio.jetstream.context import JetStreamContext
from natsio.jetstream.entities import StreamConfig
from natsio.jetstream.stream import Stream

__all__ = ["NatsServerProcess", "batch_stream", "free_port", "require_server_binary"]

# extensions/natsio-jetstream-batch/tests/conftest.py -> repo root is parents[3].
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


@pytest.fixture
async def js(nc: natsio.Client) -> JetStreamContext:
    return nc.jetstream()


async def batch_stream(
    js: JetStreamContext,
    name: str = "BATCH",
    subjects: list[str] | None = None,
    *,
    allow_direct: bool = True,
) -> Stream:
    """Create a fast-ingest-capable stream, or skip if the server has no such thing.

    ``allow_batched`` is 2.14+ and core's `StreamConfig` does not model it yet,
    so it rides in ``extra`` — which also makes the capability probe exact: an
    older server silently drops the field instead of echoing it back.
    """
    stream = await js.create_stream(
        StreamConfig(
            name=name,
            subjects=subjects if subjects is not None else [f"{name.lower()}.>"],
            allow_direct=allow_direct,
            extra={ALLOW_BATCHED: True},
        )
    )
    if not stream.cached_info.config.extra.get(ALLOW_BATCHED):
        pytest.skip("server does not support fast-ingest batch publish (allow_batched) — needs nats-server 2.14+")
    return stream
