"""Shared fixtures for kvcodec live tests.

Uses the natsio-testing extension to run a real ``nats-server -js`` (skipped
when no binary is found), dogfooding the extension namespace exactly as the
core integration suite does.
"""

import os
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from natsio.testing import NatsServerProcess  # ty: ignore[unresolved-import]
from natsio.testing import find_server_binary as _find_on_path  # ty: ignore[unresolved-import]

import natsio


def _require_binary() -> str:
    candidate = os.environ.get("NATS_SERVER_BIN")
    if candidate and Path(candidate).is_file():
        return candidate
    # Walk up looking for the repo's tools/.bin/nats-server (dev checkout).
    here = Path(__file__).resolve()
    for parent in here.parents:
        local = parent / "tools" / ".bin" / "nats-server"
        if local.is_file():
            return str(local)
    on_path = _find_on_path()
    if on_path is None:
        pytest.skip("nats-server binary not found (set NATS_SERVER_BIN)")
    return on_path


@pytest.fixture
async def server() -> AsyncIterator[NatsServerProcess]:
    process = NatsServerProcess(_require_binary(), jetstream=True)
    await process.start()
    yield process
    await process.stop()


@pytest.fixture
async def nc(server: NatsServerProcess) -> AsyncIterator[natsio.Client]:
    client = await natsio.connect(server.url, connect_timeout=5.0, request_timeout=5.0)
    yield client
    await client.close()
