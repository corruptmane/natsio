"""Test fixtures for natsio-natscontext.

Self-contained: the process manager comes from the ``natsio.testing``
extension, but binary discovery (including this repo's ``tools/.bin``) and
certificate generation are kept local so the suite does not depend on the
core repo's ``natsio/tests`` harness.
"""

import json
import os
import shutil
import subprocess
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

import pytest

# Editable installs hide the runtime pkgutil __path__ merge from the type
# checker; the live test below exercises the import at runtime.
from natsio.testing import NatsServerProcess, free_port  # ty: ignore[unresolved-import]
from natsio.testing import find_server_binary as _find_on_path  # ty: ignore[unresolved-import]

_REPO_ROOT = Path(__file__).resolve().parents[3]

__all__ = ["NatsServerProcess", "free_port"]


def find_server_binary() -> str | None:
    candidate = os.environ.get("NATS_SERVER_BIN")
    if candidate and Path(candidate).is_file():
        return candidate
    local = _REPO_ROOT / "tools" / ".bin" / "nats-server"
    if local.is_file():
        return str(local)
    return _find_on_path()


def require_server_binary() -> str:
    binary = find_server_binary()
    if binary is None:
        pytest.skip("nats-server binary not found (set NATS_SERVER_BIN)")
    return binary


def openssl_available() -> bool:
    return shutil.which("openssl") is not None


def generate_self_signed_cert(directory: Path, *, name: str = "cert") -> tuple[Path, Path]:
    """(cert, key) for CN=localhost with a 127.0.0.1 SAN, via the openssl CLI."""
    cert = directory / f"{name}.pem"
    key = directory / f"{name}-key.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "1",
            "-nodes",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost,IP:127.0.0.1",
        ],
        check=True,
        capture_output=True,
    )
    return cert, key


@pytest.fixture
def xdg(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point ``$XDG_CONFIG_HOME`` at a tmp dir with an empty nats/context tree.

    Returns the config root; ``<root>/nats/context`` is created and
    ``$HOME`` is redirected too so ``~`` expansion cannot escape the sandbox.
    """
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "home").mkdir()
    (tmp_path / "nats" / "context").mkdir(parents=True)
    return tmp_path


@pytest.fixture
def write_context(xdg: Path) -> Callable[..., Path]:
    """Write ``<name>.json`` into the sandbox context dir; return its path."""

    def _write(ctx_name: str, /, **settings: Any) -> Path:
        path = xdg / "nats" / "context" / f"{ctx_name}.json"
        path.write_text(json.dumps(settings), encoding="utf-8")
        return path

    return _write


@pytest.fixture
def select_context(xdg: Path) -> Callable[[str], None]:
    """Write ``context.txt`` to mark ``name`` as the active selection."""

    def _select(name: str) -> None:
        (xdg / "nats" / "context.txt").write_text(name, encoding="utf-8")

    return _select


@pytest.fixture
async def server() -> AsyncIterator[NatsServerProcess]:
    binary = require_server_binary()
    config = 'authorization {\n  user: "ctxuser"\n  password: "s3cret"\n}\n'
    process = NatsServerProcess(binary, config=config)
    await process.start()
    yield process
    await process.stop()
