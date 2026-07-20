"""Real nats-server process management for integration tests.

Binary discovery order: ``NATS_SERVER_BIN`` env var → ``tools/.bin/nats-server``
in the repo → ``nats-server`` on PATH. Tests are skipped when none is found.
"""

import asyncio
import os
import shutil
import socket
import tempfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]


def find_server_binary() -> str | None:
    candidate = os.environ.get("NATS_SERVER_BIN")
    if candidate and Path(candidate).is_file():
        return candidate
    local = REPO_ROOT / "tools" / ".bin" / "nats-server"
    if local.is_file():
        return str(local)
    return shutil.which("nats-server")


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class NatsServerProcess:
    def __init__(self, binary: str, *, port: int | None = None, config: str | None = None) -> None:
        self.binary = binary
        self.port = port if port is not None else free_port()
        self.config = config
        self.process: asyncio.subprocess.Process | None = None
        self._tmpdir: tempfile.TemporaryDirectory[str] | None = None

    @property
    def url(self) -> str:
        return f"nats://127.0.0.1:{self.port}"

    async def start(self, *, ready_timeout: float = 10.0) -> "NatsServerProcess":
        args = [self.binary, "-a", "127.0.0.1", "-p", str(self.port)]
        if self.config is not None:
            self._tmpdir = tempfile.TemporaryDirectory(prefix="natsio-test-server-")
            config_path = Path(self._tmpdir.name) / "server.conf"
            config_path.write_text(self.config)
            args += ["-c", str(config_path)]
        self.process = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await self._wait_ready(deadline=ready_timeout)
        return self

    async def _wait_ready(self, *, deadline: float) -> None:
        async with asyncio.timeout(deadline):
            while True:
                assert self.process is not None
                if self.process.returncode is not None:
                    raise RuntimeError(f"nats-server exited with code {self.process.returncode} during startup")
                try:
                    reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
                except OSError:
                    await asyncio.sleep(0.05)
                    continue
                try:
                    # A TLS-handshake-first server accepts but stays silent until
                    # the ClientHello — an accepted, open connection is "ready".
                    banner = await asyncio.wait_for(reader.readline(), 0.25)
                except TimeoutError:
                    writer.close()
                    return
                finally:
                    writer.close()
                if banner.startswith(b"INFO "):
                    return
                await asyncio.sleep(0.05)

    async def stop(self) -> None:
        if self.process is not None and self.process.returncode is None:
            self.process.terminate()
            try:
                async with asyncio.timeout(5):
                    await self.process.wait()
            except TimeoutError:
                self.process.kill()
                await self.process.wait()
        if self._tmpdir is not None:
            self._tmpdir.cleanup()
            self._tmpdir = None

    def kill(self) -> None:
        """SIGKILL — abrupt loss, no FIN niceties from the server's side."""
        if self.process is not None and self.process.returncode is None:
            self.process.kill()


def require_server_binary() -> str:
    binary = find_server_binary()
    if binary is None:
        pytest.skip("nats-server binary not found (set NATS_SERVER_BIN)")
    return binary


def openssl_available() -> bool:
    return shutil.which("openssl") is not None


def generate_self_signed_cert(directory: Path) -> tuple[Path, Path]:
    """(cert, key) for CN=localhost with 127.0.0.1 SAN, via the openssl CLI."""
    import subprocess

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
