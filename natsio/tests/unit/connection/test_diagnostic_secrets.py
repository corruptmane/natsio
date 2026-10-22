"""Credentials belong on the authenticated wire, never in diagnostics."""

import logging

import pytest

from fake import EventRecorder, FakeEnv, connect_payload
from natsio._internal.connection import Connection
from natsio._internal.lifecycle import Connected
from natsio._internal.pool import parse_server_url
from natsio.errors import ConfigError, NoServersAvailableError
from natsio.options import ConnectOptions

SECRET = "DIAGNOSTIC_SECRET_84d6"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"user": "user", "password": SECRET},
        {"token": SECRET},
        {"nkey_seed": SECRET},
        {"servers": (f"nats://{SECRET}@localhost:4222",)},
    ],
)
def test_options_repr_hides_credentials(kwargs) -> None:
    assert SECRET not in repr(ConnectOptions(**kwargs))


def test_parsed_server_repr_hides_url_credentials() -> None:
    assert SECRET not in repr(parse_server_url(f"nats://user:{SECRET}@localhost:4222"))


@pytest.mark.parametrize("url", [f"http://{SECRET}@localhost", f"nats://{SECRET}@localhost:{SECRET}"])
def test_invalid_url_errors_hide_credentials(url) -> None:
    with pytest.raises(ConfigError) as caught:
        parse_server_url(url)
    assert SECRET not in str(caught.value)


async def test_failed_connection_log_hides_url_credentials(caplog) -> None:
    env = FakeEnv()
    env.refuse_next()
    conn = Connection(ConnectOptions(servers=(f"nats://{SECRET}@localhost:4222",)), transport_factory=env.factory)
    with caplog.at_level(logging.INFO, logger="natsio.connection"), pytest.raises(NoServersAvailableError):
        await conn.connect()
    assert SECRET not in caplog.text


async def test_lifecycle_diagnostics_hide_credentials_but_url_auth_still_wins() -> None:
    env = FakeEnv()
    recorder = EventRecorder()
    url = f"nats://{SECRET}@localhost:4222"
    conn = Connection(ConnectOptions(servers=(url,), token="option-token"), transport_factory=env.factory)
    conn.bus.subscribe(recorder.hook)
    try:
        await conn.connect()
        assert SECRET not in repr(await recorder.wait_for(Connected))
        assert connect_payload(env.current)["auth_token"] == SECRET
        # This is configuration access, retained for public API compatibility.
        assert conn.connected_url == url
    finally:
        await conn.close(flush=False)
