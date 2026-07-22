"""End-to-end: a context file drives a real connection to a throwaway server."""

import json
from collections.abc import Callable
from pathlib import Path

import natsio.natscontext as natscontext  # ty: ignore[unresolved-import]

# The `server` fixture (conftest) starts nats-server with user/password auth:
#   user: ctxuser / password: s3cret


async def _roundtrip(nc) -> None:  # type: ignore[no-untyped-def]
    async with nc.subscribe("ctx.ping") as sub:
        await nc.publish("ctx.ping", b"hello")
        msg = await sub.next_msg(timeout=2.0)
        assert msg.payload == b"hello"


async def test_connect_via_named_context(
    server,  # type: ignore[no-untyped-def]
    write_context: Callable[..., Path],
    select_context: Callable[[str], None],
) -> None:
    write_context("live", url=server.url, user="ctxuser", password="s3cret")
    select_context("live")
    nc = await natscontext.connect("live")
    try:
        await _roundtrip(nc)
    finally:
        await nc.close()


async def test_connect_uses_selected_context(
    server,  # type: ignore[no-untyped-def]
    write_context: Callable[..., Path],
    select_context: Callable[[str], None],
) -> None:
    write_context("selected", url=server.url, user="ctxuser", password="s3cret")
    select_context("selected")
    nc = await natscontext.connect()  # no name -> selection
    try:
        await _roundtrip(nc)
    finally:
        await nc.close()


async def test_connect_via_absolute_path(
    server,  # type: ignore[no-untyped-def]
    tmp_path: Path,
) -> None:
    path = tmp_path / "abs.json"
    path.write_text(
        json.dumps({"url": server.url, "user": "ctxuser", "password": "s3cret"}),
        encoding="utf-8",
    )
    nc = await natscontext.connect(str(path))
    try:
        await _roundtrip(nc)
    finally:
        await nc.close()


async def test_overrides_win_over_context(
    server,  # type: ignore[no-untyped-def]
    write_context: Callable[..., Path],
) -> None:
    write_context("named", url=server.url, user="ctxuser", password="s3cret")
    nc = await natscontext.connect("named", name="my-app")
    try:
        assert nc._options.name == "my-app"
        await _roundtrip(nc)
    finally:
        await nc.close()
