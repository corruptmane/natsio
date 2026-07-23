"""Shared fixtures for the natsio-schedules test suite.

The nats-server process manager lives in the ``natsio-testing`` extension; this
conftest adds only the repo-local binary lookup (``tools/.bin/nats-server``,
our pinned 2.14) and the connect fixtures.
"""

import os
from pathlib import Path

import pytest

# Editable installs: the pkgutil ``__path__`` merge is invisible to the type
# checker (real wheel installs resolve statically). Runtime coverage: the whole
# live suite imports through this seam.
from natsio.testing import NatsServerProcess, find_server_binary, free_port  # ty: ignore[unresolved-import]

import natsio

__all__ = ["NatsServerProcess", "free_port", "require_server_binary"]

# extensions/natsio-schedules/tests/conftest.py -> repo root is parents[3].
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
