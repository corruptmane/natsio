"""Download an official nats-server archive and verify its SHA-256 before use."""

import hashlib
import io
import platform
import re
import sys
import tarfile
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
# Reviewed digests for the merge/release gates. The moving compatibility radar
# verifies the release's published SHA256SUMS instead.
PINNED = {
    ("v2.14.0", "linux", "amd64"): "3d8b74dfad39af184c765a6dd120441ed1c648d6672eddf6b304f222661251b8",
    ("v2.14.0", "darwin", "arm64"): "36f28cf166e5ae5dd88d700a609c810b97ffad641e0c51b49cf8fae25fb3fac7",
    ("v2.15.1", "linux", "amd64"): "bf5edc7b1ce98e885c368cb43a886eb148dc5d0551d99f3cf6141fb5e1954c70",
    ("v2.15.1", "darwin", "arm64"): "ed0fe39222ec2e317c4d0e1a9cdc16c63ec523d94f9355d24a902a4db2655ffd",
}


def download(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=60) as response:
        return response.read()


def main() -> None:
    tag = sys.argv[1]
    if not re.fullmatch(r"v\d+\.\d+\.\d+", tag):
        raise SystemExit("expected a stable server tag vX.Y.Z")
    system = platform.system().lower()
    arch = {"x86_64": "amd64", "aarch64": "arm64", "arm64": "arm64"}.get(platform.machine())
    if system not in {"linux", "darwin"} or arch is None:
        raise SystemExit("automatic server installation supports Linux/macOS amd64/arm64")
    filename = f"nats-server-{tag}-{system}-{arch}.tar.gz"
    base = f"https://github.com/nats-io/nats-server/releases/download/{tag}"
    checksums = download(f"{base}/SHA256SUMS").decode()
    matches = [line.split()[0] for line in checksums.splitlines() if line.split()[-1].lstrip("*") == filename]
    if len(matches) != 1 or not re.fullmatch(r"[0-9a-f]{64}", matches[0]):
        raise SystemExit("missing or ambiguous checksum for server archive")
    expected = PINNED.get((tag, system, arch), matches[0])
    if expected != matches[0]:
        raise SystemExit("published checksum differs from reviewed server digest")
    archive = download(f"{base}/{filename}")
    if hashlib.sha256(archive).hexdigest() != expected:
        raise SystemExit("server archive SHA-256 mismatch")
    # Extract just the known binary, never arbitrary tar paths or symlinks.
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        member = tar.getmember(f"nats-server-{tag}-{system}-{arch}/nats-server")
        if not member.isfile():
            raise SystemExit("server archive binary is not a regular file")
        stream = tar.extractfile(member)
        assert stream is not None
        binary = ROOT / "tools" / ".bin" / "nats-server"
        binary.parent.mkdir(parents=True, exist_ok=True)
        binary.write_bytes(stream.read())
        binary.chmod(0o755)
    print(f"verified {filename}: sha256:{expected}")


if __name__ == "__main__":
    main()
