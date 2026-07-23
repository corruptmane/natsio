"""Shared fixtures for the natsio-sysclient test suite.

Every live test needs a server with a **system account**, so the fixtures here
start nats-server from a config that defines `$SYS` (user `sys`) alongside a
regular `APP` account (user `app`) with JetStream enabled — the `APP` account
is what gives `JSZ` something to report and what the "wrong account" tests
connect as.
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

# extensions/natsio-sysclient/tests/conftest.py -> repo root is parents[3].
_REPO_ROOT = Path(__file__).resolve().parents[3]

SYS_USER = "sys"
SYS_PASSWORD = "pw"
APP_USER = "app"
APP_PASSWORD = "pw"

SYSTEM_ACCOUNT_CONFIG = f"""
accounts {{
  $SYS {{ users: [{{user: {SYS_USER}, password: {SYS_PASSWORD}}}] }}
  APP  {{ users: [{{user: {APP_USER}, password: {APP_PASSWORD}}}], jetstream: enabled }}
}}
"""


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
    """A single nats-server with `$SYS` and JetStream enabled."""
    process = NatsServerProcess(require_server_binary(), config=SYSTEM_ACCOUNT_CONFIG, jetstream=True)
    await process.start()
    yield process
    await process.stop()


@pytest.fixture
async def nc(server: NatsServerProcess):
    """A connection authenticated into the system account."""
    client = await natsio.connect(
        server.url, user=SYS_USER, password=SYS_PASSWORD, connect_timeout=5.0, request_timeout=5.0
    )
    yield client
    await client.close()


@pytest.fixture
async def app_nc(server: NatsServerProcess):
    """A connection in the regular `APP` account — no system privileges."""
    client = await natsio.connect(
        server.url, user=APP_USER, password=APP_PASSWORD, connect_timeout=5.0, request_timeout=5.0
    )
    yield client
    await client.close()
