"""Typed error surface for missing / malformed contexts."""

from collections.abc import Callable
from pathlib import Path

import natsio.natscontext as natscontext  # ty: ignore[unresolved-import]
import pytest
from natsio.natscontext import (  # ty: ignore[unresolved-import]
    ContextError,
    ContextMalformedError,
    ContextNotFoundError,
)

from natsio.errors import NATSError


def test_error_hierarchy() -> None:
    assert issubclass(ContextError, NATSError)
    assert issubclass(ContextNotFoundError, ContextError)
    assert issubclass(ContextNotFoundError, FileNotFoundError)
    assert issubclass(ContextMalformedError, ContextError)


def test_malformed_json_raises(xdg: Path) -> None:
    path = xdg / "nats" / "context" / "broken.json"
    path.write_text("{not valid json", encoding="utf-8")
    with pytest.raises(ContextMalformedError, match="not valid JSON"):
        natscontext.load("broken")


def test_non_object_json_raises(xdg: Path) -> None:
    path = xdg / "nats" / "context" / "arr.json"
    path.write_text("[1, 2, 3]", encoding="utf-8")
    with pytest.raises(ContextMalformedError, match="JSON object"):
        natscontext.load("arr")


def test_missing_absolute_path_raises(tmp_path: Path) -> None:
    with pytest.raises(ContextNotFoundError):
        natscontext.load(str(tmp_path / "nope.json"))


def test_windows_cert_store_rejected(write_context: Callable[..., Path]) -> None:
    write_context("win", url="nats://x:4222", windows_cert_store="user")
    ctx = natscontext.load("win")
    # Parsed and preserved on the object...
    assert ctx.windows_cert_store == "user"
    # ...but cannot be applied to a connection.
    with pytest.raises(ContextError, match="windows certificate"):
        ctx.connect_kwargs()


def test_half_specified_client_cert_rejected(write_context: Callable[..., Path]) -> None:
    write_context("halfcert", url="tls://x:4222", cert="/tmp/cert.pem")
    ctx = natscontext.load("halfcert")
    with pytest.raises(ContextError, match="both are required"):
        ctx.connect_kwargs()


async def test_nkey_file_without_seed_rejected(write_context: Callable[..., Path], tmp_path: Path) -> None:
    # The mapper forwards the path to natsio's NKeyFileAuth, which reads+parses
    # (and re-reads on reconnect); a seedless file is a ConfigError at auth time.
    from natsio._internal.auth import NKeyFileAuth
    from natsio.errors import ConfigError
    from natsio.options import ConnectOptions

    bad = tmp_path / "empty.nk"
    bad.write_text("no seed in here\n", encoding="utf-8")
    write_context("nk", url="nats://x:4222", nkey=str(bad))
    kwargs = natscontext.load("nk").connect_kwargs()
    assert kwargs["nkey_file"] == str(bad)

    auth = ConnectOptions(**kwargs).resolve_authenticator()
    assert isinstance(auth, NKeyFileAuth)
    with pytest.raises(ConfigError, match="no NKey seed"):
        await auth.authenticate(b"nonce")
