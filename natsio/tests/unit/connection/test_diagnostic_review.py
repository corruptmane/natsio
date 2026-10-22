"""Adversarial diagnostic refutation: malformed URLs and nested auth objects."""

import logging

import pytest

from fake import EventRecorder, FakeEnv, connect_payload
from natsio._internal.auth import signer
from natsio._internal.auth.nkeys import KeyPair, Role, decode_seed
from natsio._internal.connection import Connection
from natsio._internal.diagnostics import sanitize_url
from natsio._internal.lifecycle import Connected, LameDuck, Reconnected
from natsio._internal.pool import parse_server_url
from natsio.auth import AuthResult, CallbackAuth, CredsAuth, NKeyAuth, TokenAuth, UserPasswordAuth
from natsio.errors import ConfigError
from natsio.instrumentation import NoopInstrumentation
from natsio.options import ConnectOptions

_SECRET = "REFUTATION_SECRET_e8f53"


class SecretCallable:
    def __repr__(self) -> str:
        return f"SecretCallable({_SECRET})"

    def __call__(self, *args):
        return _SECRET if not args else _SECRET.encode()


def test_auth_callback_representations_do_not_inspect_secret_bearing_providers(monkeypatch) -> None:
    monkeypatch.setattr(signer, "require_backend", lambda: None)
    callback = SecretCallable()
    identities = (
        TokenAuth(callback),
        UserPasswordAuth("user", callback),
        NKeyAuth(callback),
        CredsAuth(callback, callback),
        CallbackAuth(callback, callback),
        ConnectOptions(authenticator=CallbackAuth(callback, callback)),
    )
    for identity in identities:
        assert _SECRET not in repr(identity)


def test_auth_result_and_raw_seed_reprs_hide_secrets_without_erasing_them() -> None:
    result = AuthResult(password=_SECRET, auth_token=_SECRET, jwt=_SECRET, signature=_SECRET)
    pair = KeyPair(Role.USER, "PUBLIC", _SECRET.encode())
    assert _SECRET not in repr(result)
    assert _SECRET not in repr(pair)
    assert result.password == result.auth_token == result.jwt == result.signature == _SECRET
    assert pair._seed == _SECRET.encode()


@pytest.mark.parametrize("seed", [_SECRET, _SECRET + "\u2603", "!" + _SECRET])
def test_malformed_seed_exceptions_never_echo_seed(seed: str) -> None:
    with pytest.raises(ConfigError) as caught:
        decode_seed(seed)
    assert _SECRET not in str(caught.value)


@pytest.mark.parametrize(
    "url",
    [
        f"nats://user:{_SECRET}@[invalid",
        f"nats://{_SECRET}@:4222",
        f"nats://user:{_SECRET}@host:not-a-port",
        f"nats://user:{_SECRET}@host:65536",
        f"unknown://user:{_SECRET}@host",
        f"nats://user:{_SECRET}@host:{_SECRET}",
    ],
)
def test_malformed_url_exception_and_sanitizer_do_not_echo_credentials(url: str) -> None:
    with pytest.raises(ConfigError) as caught:
        parse_server_url(url)
    assert _SECRET not in str(caught.value)
    assert _SECRET not in sanitize_url(url)


@pytest.mark.parametrize(
    "url", [f"nats://{_SECRET}@host:4222", f"wss://u:{_SECRET}@[::1]:443/{_SECRET}?token={_SECRET}#{_SECRET}"]
)
def test_sanitizer_hides_all_non_endpoint_url_components(url: str) -> None:
    assert _SECRET not in sanitize_url(url)


async def test_reconnect_lame_duck_and_failed_hooks_use_safe_endpoint_labels(caplog) -> None:
    class Recorder(NoopInstrumentation):
        def __init__(self) -> None:
            self.labels: list[str] = []

        def on_connect(self, server_url: str) -> None:
            self.labels.append(server_url)

        def on_reconnect(self, server_url: str, attempts: int) -> None:
            self.labels.append(server_url)

    env = FakeEnv()
    recorder = EventRecorder()
    instrumentation = Recorder()
    url = f"nats://user:{_SECRET}@seed:4222"
    conn = Connection(
        ConnectOptions(servers=(url,), reconnect_time_wait=0.01, reconnect_jitter=0, instrumentation=instrumentation),
        transport_factory=env.factory,
    )
    conn.bus.subscribe(recorder.hook)

    def faulty_hook(event) -> None:
        if isinstance(event, Connected | Reconnected | LameDuck):
            raise RuntimeError("diagnostic hook failed")

    conn.bus.subscribe(faulty_hook)
    with caplog.at_level(logging.ERROR, logger="natsio.lifecycle"):
        try:
            await conn.connect()
            env.current.deliver_info({"ldm": True})
            await recorder.wait_for(Reconnected)
            assert recorder.count(LameDuck) == 1
            assert _SECRET not in repr(recorder.events)
            assert _SECRET not in repr(instrumentation.labels)
            assert _SECRET not in caplog.text
            assert connect_payload(env.current)["pass"] == _SECRET
            assert conn.connected_url == url
        finally:
            await conn.close(flush=False)


def test_username_callback_repr_cannot_expose_its_password_or_other_secrets() -> None:
    provider = SecretCallable()
    assert _SECRET not in repr(UserPasswordAuth(provider, "password"))
    assert _SECRET not in repr(ConnectOptions(user=provider, password="password"))
