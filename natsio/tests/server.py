"""Repo-side test helpers layered over the natsio-testing extension.

The process manager itself lives in ``natsio.testing`` (the first natsio-*
extension — importing it here is deliberate dogfooding of the extension
namespace). This module adds only repo-specific concerns: the tools/.bin
binary location, pytest skips, and openssl-based certificate generation.
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

# In this workspace both the core and the extension are *editable* installs, so
# the type checker cannot see the runtime pkgutil __path__ merge (regular wheel
# installs merge physically and resolve statically). Runtime coverage: the
# whole integration suite imports through this seam.
from natsio.testing import NatsServerProcess, free_port  # ty: ignore[unresolved-import]
from natsio.testing import find_server_binary as _find_on_path  # ty: ignore[unresolved-import]

__all__ = [
    "NatsServerProcess",
    "find_server_binary",
    "free_port",
    "generate_self_signed_cert",
    "openssl_available",
    "require_server_binary",
]

REPO_ROOT = Path(__file__).resolve().parents[2]


def find_server_binary() -> str | None:
    candidate = os.environ.get("NATS_SERVER_BIN")
    if candidate and Path(candidate).is_file():
        return candidate
    local = REPO_ROOT / "tools" / ".bin" / "nats-server"
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


def generate_self_signed_cert(directory: Path) -> tuple[Path, Path]:
    """(cert, key) for CN=localhost with 127.0.0.1 SAN, via the openssl CLI."""
    cert = directory / "cert.pem"
    key = directory / "key.pem"
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
