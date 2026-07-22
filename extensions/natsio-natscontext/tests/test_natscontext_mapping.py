"""The field-mapping matrix: context JSON -> natsio connect kwargs."""

import os
from collections.abc import Callable
from pathlib import Path

import natsio.natscontext as natscontext  # ty: ignore[unresolved-import]
import pytest

from conftest import generate_self_signed_cert, openssl_available  # ty: ignore[unresolved-import]
from natsio._internal.auth import nkeys
from natsio.options import TLSConfig


def _make_seed() -> str:
    return nkeys.encode_seed(nkeys.Role.USER, os.urandom(32))


def test_url_maps_to_servers(write_context: Callable[..., Path]) -> None:
    write_context("c", url="nats://a:4222")
    assert natscontext.load("c").connect_kwargs()["servers"] == ("nats://a:4222",)


def test_comma_separated_urls(write_context: Callable[..., Path]) -> None:
    write_context("c", url="nats://a:4222, nats://b:4222 ,nats://c:4222")
    servers = natscontext.load("c").connect_kwargs()["servers"]
    assert servers == ("nats://a:4222", "nats://b:4222", "nats://c:4222")


def test_empty_url_omits_servers(write_context: Callable[..., Path]) -> None:
    write_context("c", token="t")
    assert "servers" not in natscontext.load("c").connect_kwargs()


def test_user_password(write_context: Callable[..., Path]) -> None:
    write_context("c", url="nats://a:4222", user="alice", password="s3cret")
    kw = natscontext.load("c").connect_kwargs()
    assert kw["user"] == "alice"
    assert kw["password"] == "s3cret"
    assert "token" not in kw


def test_token(write_context: Callable[..., Path]) -> None:
    write_context("c", url="nats://a:4222", token="t0k3n")
    assert natscontext.load("c").connect_kwargs()["token"] == "t0k3n"


def test_creds_expands_path(write_context: Callable[..., Path], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CREDS_HOME", "/secrets")
    write_context("c", url="nats://a:4222", creds="$CREDS_HOME/user.creds")
    assert natscontext.load("c").connect_kwargs()["credentials"] == "/secrets/user.creds"


def test_creds_expands_homedir(write_context: Callable[..., Path], xdg: Path) -> None:
    write_context("c", url="nats://a:4222", creds="~/user.creds")
    creds = natscontext.load("c").connect_kwargs()["credentials"]
    assert creds == str(xdg / "home" / "user.creds")


def test_nkey_reads_seed_from_file(write_context: Callable[..., Path], tmp_path: Path) -> None:
    seed = _make_seed()
    nkfile = tmp_path / "user.nk"
    nkfile.write_text(seed + "\n", encoding="utf-8")
    write_context("c", url="nats://a:4222", nkey=str(nkfile))
    assert natscontext.load("c").connect_kwargs()["nkey_seed"] == seed


def test_nkey_reads_decorated_seed_file(write_context: Callable[..., Path], tmp_path: Path) -> None:
    seed = _make_seed()
    nkfile = tmp_path / "user.nk"
    nkfile.write_text(
        f"-----BEGIN USER NKEY SEED-----\n{seed}\n------END USER NKEY SEED------\n",
        encoding="utf-8",
    )
    write_context("c", url="nats://a:4222", nkey=str(nkfile))
    assert natscontext.load("c").connect_kwargs()["nkey_seed"] == seed


def test_auth_precedence_user_over_token(write_context: Callable[..., Path]) -> None:
    # nats.go would send both; natsio cannot, so the primary mechanism wins.
    write_context("c", url="nats://a:4222", user="alice", password="pw", token="t")
    kw = natscontext.load("c").connect_kwargs()
    assert kw["user"] == "alice"
    assert "token" not in kw


def test_inbox_prefix(write_context: Callable[..., Path]) -> None:
    write_context("c", url="nats://a:4222", inbox_prefix="_MYINBOX")
    assert natscontext.load("c").connect_kwargs()["inbox_prefix"] == "_MYINBOX"


def test_tls_first_builds_tls(write_context: Callable[..., Path]) -> None:
    write_context("c", url="nats://a:4222", tls_first=True)
    tls = natscontext.load("c").connect_kwargs()["tls"]
    assert isinstance(tls, TLSConfig)
    assert tls.handshake_first is True


def test_no_tls_when_unset(write_context: Callable[..., Path]) -> None:
    write_context("c", url="nats://a:4222", user="a", password="b")
    assert "tls" not in natscontext.load("c").connect_kwargs()


@pytest.mark.skipif(not openssl_available(), reason="openssl CLI not available")
def test_tls_ca_and_client_cert(write_context: Callable[..., Path], tmp_path: Path) -> None:
    cert, key = generate_self_signed_cert(tmp_path)
    ca, _ = generate_self_signed_cert(tmp_path, name="ca")
    write_context(
        "c",
        url="tls://a:4222",
        cert=str(cert),
        key=str(key),
        ca=str(ca),
        tls_first=True,
    )
    tls = natscontext.load("c").connect_kwargs()["tls"]
    assert isinstance(tls, TLSConfig)
    assert tls.context is not None
    assert tls.handshake_first is True


@pytest.mark.skipif(not openssl_available(), reason="openssl CLI not available")
def test_tls_ca_only(write_context: Callable[..., Path], tmp_path: Path) -> None:
    ca, _ = generate_self_signed_cert(tmp_path, name="ca")
    write_context("c", url="tls://a:4222", ca=str(ca))
    tls = natscontext.load("c").connect_kwargs()["tls"]
    assert isinstance(tls, TLSConfig)
    assert tls.handshake_first is False


def test_non_applied_fields_preserved(write_context: Callable[..., Path]) -> None:
    write_context(
        "c",
        url="nats://a:4222",
        description="my prod cluster",
        socks_proxy="socks5://localhost:1080",
        nsc="nsc://OP/AC/US",
        jetstream_domain="hub",
        jetstream_api_prefix="$JS.hub.API",
        jetstream_event_prefix="$JS.hub.EVENT",
        user_jwt="eyJ0eXAiOi",
        color_scheme="dark",
    )
    ctx = natscontext.load("c")
    # Preserved on the object.
    assert ctx.description == "my prod cluster"
    assert ctx.socks_proxy == "socks5://localhost:1080"
    assert ctx.nsc == "nsc://OP/AC/US"
    assert ctx.jetstream_domain == "hub"
    assert ctx.jetstream_api_prefix == "$JS.hub.API"
    assert ctx.jetstream_event_prefix == "$JS.hub.EVENT"
    assert ctx.user_jwt == "eyJ0eXAiOi"
    assert ctx.color_scheme == "dark"
    # But NOT projected onto the connection.
    kw = ctx.connect_kwargs()
    for absent in ("socks_proxy", "proxy", "jetstream_domain", "user_jwt", "color_scheme", "nsc"):
        assert absent not in kw


def test_unknown_keys_surfaced(write_context: Callable[..., Path]) -> None:
    write_context("c", url="nats://a:4222", future_option="x", another="y")
    ctx = natscontext.load("c")
    assert ctx.unknown_keys == frozenset({"future_option", "another"})


def test_known_keys_not_flagged(write_context: Callable[..., Path]) -> None:
    write_context("c", url="nats://a:4222", user="a", password="b", jetstream_domain="hub")
    assert natscontext.load("c").unknown_keys == frozenset()


def test_raw_preserved_verbatim(write_context: Callable[..., Path]) -> None:
    write_context("c", url="nats://a:4222", custom=42)
    assert natscontext.load("c").raw["custom"] == 42
