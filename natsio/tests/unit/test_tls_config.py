"""TLSConfig: lazy context building from PEM files, and its validation errors."""

import ssl

import pytest

from natsio.errors import ConfigError
from natsio.options import TLSConfig
from server import generate_self_signed_cert, openssl_available


def test_bare_config_uses_default_context() -> None:
    # A bare TLSConfig() still works — the from-files fields are purely additive.
    ctx = TLSConfig().resolve_context()
    assert isinstance(ctx, ssl.SSLContext)


def test_explicit_context_passed_through_unchanged() -> None:
    provided = ssl.create_default_context()
    assert TLSConfig(context=provided).resolve_context() is provided


@pytest.mark.skipif(not openssl_available(), reason="openssl CLI not available")
def test_cafile_builds_verifying_context(tmp_path) -> None:
    ca, _ = generate_self_signed_cert(tmp_path)
    ctx = TLSConfig(cafile=str(ca)).resolve_context()
    assert isinstance(ctx, ssl.SSLContext)
    # The CA we loaded is present among the context's trusted certificates.
    assert any(cert for cert in ctx.get_ca_certs())


@pytest.mark.skipif(not openssl_available(), reason="openssl CLI not available")
def test_certfile_and_keyfile_build_client_cert(tmp_path) -> None:
    cert, key = generate_self_signed_cert(tmp_path)
    ca, _ = generate_self_signed_cert(tmp_path)
    # Rebind under a distinct name so the CA and client cert are separate files.
    ctx = TLSConfig(certfile=str(cert), keyfile=str(key), cafile=str(ca)).resolve_context()
    assert isinstance(ctx, ssl.SSLContext)


@pytest.mark.skipif(not openssl_available(), reason="openssl CLI not available")
def test_certfile_without_keyfile_allowed_when_bundled(tmp_path) -> None:
    # openssl -nodes writes cert and key to separate files; concatenate them to
    # exercise the "keyfile bundled into certfile" path (ssl semantics).
    cert, key = generate_self_signed_cert(tmp_path)
    bundle = tmp_path / "bundle.pem"
    bundle.write_bytes(cert.read_bytes() + key.read_bytes())
    ctx = TLSConfig(certfile=str(bundle)).resolve_context()
    assert isinstance(ctx, ssl.SSLContext)


def test_lazy_context_is_rebuilt_each_call(tmp_path) -> None:
    # resolve_context does not memoise — each call returns a fresh context so a
    # rotated file on disk is picked up on the next (re)connect.
    if not openssl_available():
        pytest.skip("openssl CLI not available")
    ca, _ = generate_self_signed_cert(tmp_path)
    cfg = TLSConfig(cafile=str(ca))
    assert cfg.resolve_context() is not cfg.resolve_context()


def test_keyfile_without_certfile_is_config_error() -> None:
    with pytest.raises(ConfigError, match="keyfile requires certfile"):
        TLSConfig(keyfile="/tmp/only-key.pem")


def test_context_with_certfile_is_config_error() -> None:
    with pytest.raises(ConfigError, match="cannot be combined"):
        TLSConfig(context=ssl.create_default_context(), certfile="/tmp/c.pem")


def test_context_with_cafile_is_config_error() -> None:
    with pytest.raises(ConfigError, match="cannot be combined"):
        TLSConfig(context=ssl.create_default_context(), cafile="/tmp/ca.pem")
