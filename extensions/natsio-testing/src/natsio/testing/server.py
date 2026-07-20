import asyncio
import os
import shutil
import socket
import tempfile
from pathlib import Path
from typing import Self

__all__ = ["NatsServerProcess", "find_server_binary", "free_port"]


def find_server_binary() -> str | None:
    """The ``NATS_SERVER_BIN`` env var if set, else ``nats-server`` on PATH."""
    candidate = os.environ.get("NATS_SERVER_BIN")
    if candidate and Path(candidate).is_file():
        return candidate
    return shutil.which("nats-server")


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class NatsServerProcess:
    """One managed ``nats-server`` subprocess.

    ``config`` is written to a temporary file and passed via ``-c``.
    ``jetstream=True`` enables JetStream with an isolated, auto-removed store
    directory. ``kill()`` is SIGKILL — an abrupt loss with no FIN niceties —
    for exercising reconnect and leadership-change paths.
    """

    def __init__(
        self,
        binary: str,
        *,
        port: int | None = None,
        config: str | None = None,
        jetstream: bool = False,
        store_dir: str | os.PathLike[str] | None = None,
    ) -> None:
        self.binary = binary
        self.port = port if port is not None else free_port()
        self.config = config
        self.jetstream = jetstream
        # An explicit store_dir survives this instance — restart a "new" server
        # on the same directory to simulate a server bounce that keeps its data.
        self.store_dir = Path(store_dir) if store_dir is not None else None
        self.process: asyncio.subprocess.Process | None = None
        self._tmpdir: tempfile.TemporaryDirectory[str] | None = None

    @property
    def url(self) -> str:
        return f"nats://127.0.0.1:{self.port}"

    def _workdir(self) -> Path:
        if self._tmpdir is None:
            self._tmpdir = tempfile.TemporaryDirectory(prefix="natsio-test-server-")
        return Path(self._tmpdir.name)

    async def start(self, *, ready_timeout: float = 10.0) -> Self:
        args = [self.binary, "-a", "127.0.0.1", "-p", str(self.port)]
        if self.jetstream:
            store = self.store_dir if self.store_dir is not None else self._workdir() / "jetstream"
            args += ["-js", "-sd", str(store)]
        if self.config is not None:
            config_path = self._workdir() / "server.conf"
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
        """SIGKILL the server (abrupt loss; the store directory is kept)."""
        if self.process is not None and self.process.returncode is None:
            self.process.kill()
