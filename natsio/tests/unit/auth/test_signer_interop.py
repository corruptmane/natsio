"""Interop tests for the parts natsio still owns.

We delegate Ed25519 entirely, but we keep our own NKey codec — so we earn it by
differential-testing it against the reference ``nkeys`` package, and by
asserting the two supported Ed25519 backends are interchangeable.

These tests need the dev dependencies (pynacl, cryptography, nkeys) and skip
cleanly without them.
"""

import base64
import os

import pytest

from natsio._internal.auth import nkeys, signer

reference = pytest.importorskip("nkeys", reason="reference nkeys package not installed")

ROLES = [
    (nkeys.Role.USER, "U", "SU"),
    (nkeys.Role.ACCOUNT, "A", "SA"),
    (nkeys.Role.OPERATOR, "O", "SO"),
    (nkeys.Role.SERVER, "N", "SN"),
    (nkeys.Role.CLUSTER, "C", "SC"),
]

SEEDS = [bytes(32), bytes(range(32)), b"\xff" * 32, *(os.urandom(32) for _ in range(16))]


@pytest.mark.parametrize(("role", "public_prefix", "seed_prefix"), ROLES)
def test_role_prefixes_match_convention(role: nkeys.Role, public_prefix: str, seed_prefix: str) -> None:
    seed = nkeys.encode_seed(role, bytes(range(32)))
    assert seed.startswith(seed_prefix)
    assert nkeys.from_seed(seed).public_key.startswith(public_prefix)


@pytest.mark.parametrize("raw", SEEDS)
def test_seed_encoding_matches_reference(raw: bytes) -> None:
    ours = nkeys.encode_seed(nkeys.Role.USER, raw)
    theirs = reference.encode_seed(bytearray(raw), prefix=reference.PREFIX_BYTE_USER).decode()
    assert ours == theirs


@pytest.mark.parametrize("raw", SEEDS)
def test_seed_decoding_matches_reference(raw: bytes) -> None:
    encoded = reference.encode_seed(bytearray(raw), prefix=reference.PREFIX_BYTE_USER).decode()
    role, decoded = nkeys.decode_seed(encoded)
    assert role is nkeys.Role.USER
    assert decoded == raw


@pytest.mark.parametrize("raw", SEEDS)
def test_public_key_matches_reference(raw: bytes) -> None:
    seed = nkeys.encode_seed(nkeys.Role.USER, raw)
    ours = nkeys.from_seed(seed).public_key
    theirs = reference.from_seed(seed.encode()).public_key.decode()
    assert ours == theirs


@pytest.mark.parametrize("raw", SEEDS[:8])
def test_signature_matches_reference(raw: bytes) -> None:
    nonce = b"nonce-under-test"
    seed = nkeys.encode_seed(nkeys.Role.USER, raw)
    ours = nkeys.from_seed(seed).sign(nonce)
    theirs = reference.from_seed(seed.encode()).sign(nonce)
    assert ours == theirs


def test_connect_signature_encoding_matches_reference() -> None:
    """The CONNECT `sig` field is raw-base64url, unpadded (nats.go RawURLEncoding)."""
    raw = os.urandom(32)
    nonce = os.urandom(16)
    seed = nkeys.encode_seed(nkeys.Role.USER, raw)
    ours = nkeys.from_seed(seed).sign_nonce_b64(nonce)
    theirs = base64.urlsafe_b64encode(reference.from_seed(seed.encode()).sign(nonce)).decode().rstrip("=")
    assert ours == theirs


class TestBackendsAreInterchangeable:
    """PyNaCl and cryptography must produce identical keys and signatures."""

    @staticmethod
    def _pynacl(seed: bytes, message: bytes) -> tuple[bytes, bytes]:
        signing = pytest.importorskip("nacl.signing")
        key = signing.SigningKey(seed)
        return bytes(key.verify_key), key.sign(message).signature

    @staticmethod
    def _cryptography(seed: bytes, message: bytes) -> tuple[bytes, bytes]:
        pytest.importorskip("cryptography")
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

        key = Ed25519PrivateKey.from_private_bytes(seed)
        public = key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
        return public, key.sign(message)

    @pytest.mark.parametrize("raw", SEEDS[:8])
    def test_identical_output(self, raw: bytes) -> None:
        message = b"the quick brown fox"
        assert self._pynacl(raw, message) == self._cryptography(raw, message)

    @pytest.mark.parametrize("raw", SEEDS[:8])
    def test_seam_agrees_with_backends(self, raw: bytes) -> None:
        message = b"seam check"
        assert (signer.public_key(raw), signer.sign(raw, message)) == self._pynacl(raw, message)


def test_missing_backend_message_is_actionable() -> None:
    """The error must name both extras and the dependency-free CallbackAuth route."""
    from natsio._internal.auth.signer import _MISSING

    assert "natsio[nkeys]" in _MISSING
    assert "natsio[cryptography]" in _MISSING
    assert "CallbackAuth" in _MISSING


def test_missing_backend_raises_actionable_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """With neither backend importable, the failure names the fix."""
    import builtins as _builtins

    from natsio._internal.auth import signer as signer_module
    from natsio.errors import MissingDependencyError

    real_import = _builtins.__import__

    def blocked(name, *args, **kwargs):
        if name.startswith(("nacl", "cryptography")):
            raise ImportError(f"blocked: {name}")
        return real_import(name, *args, **kwargs)

    signer_module._backend.cache_clear()
    monkeypatch.setattr(_builtins, "__import__", blocked)
    try:
        with pytest.raises(MissingDependencyError, match=r"natsio\[nkeys\]"):
            signer_module.require_backend()
        # It is an ImportError too, so `except ImportError` written against
        # nats-py still catches it.
        signer_module._backend.cache_clear()
        with pytest.raises(ImportError):
            signer_module.sign(bytes(32), b"m")
    finally:
        monkeypatch.undo()
        signer_module._backend.cache_clear()


def test_nkey_auth_fails_fast_without_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    """The error surfaces at construction, not mid-handshake."""
    from natsio._internal.auth import NKeyAuth
    from natsio._internal.auth import signer as signer_module
    from natsio.errors import MissingDependencyError

    def boom() -> None:
        raise MissingDependencyError("no backend")

    monkeypatch.setattr(signer_module, "require_backend", boom)
    with pytest.raises(MissingDependencyError):
        NKeyAuth(seed=nkeys.encode_seed(nkeys.Role.USER, bytes(32)))
