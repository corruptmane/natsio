"""Pluggable authentication.

An :class:`Authenticator` turns the server's INFO (most importantly its
``nonce``) into the auth fields of CONNECT. It is re-invoked on **every**
(re)connect: nonces change each time, credentials files may have been rotated
on disk, and callables may return fresh tokens — nothing here is cached.
"""

import asyncio
import inspect
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from natsio.errors import ConfigError

from . import nkeys, signer
from .creds import parse_creds

__all__ = [
    "AuthResult",
    "Authenticator",
    "CallbackAuth",
    "CredsAuth",
    "CredsFileAuth",
    "NKeyAuth",
    "TokenAuth",
    "UserPasswordAuth",
    "resolve_str",
]

type StrSource = str | Callable[[], str | Awaitable[str]]


async def resolve_str(source: StrSource) -> str:
    """Resolve a static string or a (sync/async) zero-arg callable producing one."""
    if isinstance(source, str):
        return source
    value = source()
    if inspect.isawaitable(value):
        value = await value
    if not isinstance(value, str):
        raise ConfigError(f"credential callable returned {type(value).__name__}, expected str")
    return value


@dataclass(frozen=True, slots=True)
class AuthResult:
    """CONNECT auth fields produced for one handshake. Secrets stay out of repr."""

    user: str | None = None
    password: str | None = field(default=None, repr=False)
    auth_token: str | None = field(default=None, repr=False)
    jwt: str | None = field(default=None, repr=False)
    nkey: str | None = None
    signature: str | None = field(default=None, repr=False)


class Authenticator(Protocol):
    async def authenticate(self, nonce: bytes | None) -> AuthResult: ...


@dataclass(frozen=True, slots=True)
class UserPasswordAuth:
    user: StrSource
    password: StrSource = field(repr=False)

    async def authenticate(self, nonce: bytes | None) -> AuthResult:
        return AuthResult(user=await resolve_str(self.user), password=await resolve_str(self.password))


@dataclass(frozen=True, slots=True)
class TokenAuth:
    token: StrSource = field(repr=False)

    async def authenticate(self, nonce: bytes | None) -> AuthResult:
        return AuthResult(auth_token=await resolve_str(self.token))


@dataclass(frozen=True, slots=True)
class NKeyAuth:
    seed: StrSource = field(repr=False)

    def __post_init__(self) -> None:
        signer.require_backend()

    async def authenticate(self, nonce: bytes | None) -> AuthResult:
        if nonce is None:
            raise ConfigError("server did not send a nonce; NKey auth is not supported here")
        pair = nkeys.from_seed(await resolve_str(self.seed))
        return AuthResult(nkey=pair.public_key, signature=pair.sign_nonce_b64(nonce))


@dataclass(frozen=True, slots=True)
class CredsAuth:
    """JWT + seed held in memory."""

    jwt: StrSource = field(repr=False)
    seed: StrSource = field(repr=False)

    def __post_init__(self) -> None:
        signer.require_backend()

    async def authenticate(self, nonce: bytes | None) -> AuthResult:
        if nonce is None:
            raise ConfigError("server did not send a nonce; JWT auth is not supported here")
        pair = nkeys.from_seed(await resolve_str(self.seed))
        return AuthResult(jwt=await resolve_str(self.jwt), signature=pair.sign_nonce_b64(nonce))


@dataclass(frozen=True, slots=True)
class CredsFileAuth:
    """Decorated .creds file, re-read on every (re)connect to pick up rotation."""

    path: str | os.PathLike[str]

    def __post_init__(self) -> None:
        signer.require_backend()

    async def authenticate(self, nonce: bytes | None) -> AuthResult:
        content = await asyncio.to_thread(Path(self.path).read_text, encoding="utf-8")
        jwt, seed = parse_creds(content)
        return await CredsAuth(jwt=jwt, seed=seed).authenticate(nonce)


@dataclass(frozen=True, slots=True)
class CallbackAuth:
    """Dynamic JWT + external signer (KMS/HSM-style and auth-callout-ready flows)."""

    jwt_callback: Callable[[], str | Awaitable[str]]
    signature_callback: Callable[[bytes], bytes | Awaitable[bytes]]

    async def authenticate(self, nonce: bytes | None) -> AuthResult:
        import base64

        if nonce is None:
            raise ConfigError("server did not send a nonce; signed auth is not supported here")
        jwt = await resolve_str(self.jwt_callback)
        result = self.signature_callback(nonce)
        signature = result if isinstance(result, bytes) else await result
        encoded = base64.urlsafe_b64encode(signature).decode("ascii").rstrip("=")
        return AuthResult(jwt=jwt, signature=encoded)
