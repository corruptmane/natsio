"""Tiny shared helpers for adapters."""

import secrets
from collections.abc import Awaitable

__all__ = ["maybe_await", "unique"]


def unique(prefix: str) -> str:
    """A short, collision-free name — durable consumers, streams, buckets per repeat."""
    return f"{prefix}_{secrets.token_hex(4)}"


async def maybe_await(result: Awaitable[None] | None) -> None:
    """Await ``result`` when a callback returned a coroutine; no-op otherwise."""
    if result is not None:
        await result
