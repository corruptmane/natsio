"""Local/CI validation helpers, invoked only through just recipes."""

import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import zipfile
from email.parser import BytesParser
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PACKAGES = [ROOT / "natsio", *sorted((ROOT / "extensions").glob("natsio-*"))]


def project(path: Path) -> dict:
    return tomllib.loads((path / "pyproject.toml").read_text())["project"]


def run(*args: str, cwd: Path = ROOT, env: dict[str, str] | None = None) -> None:
    subprocess.run(args, check=True, cwd=cwd, env=env)


def server_binary() -> str:
    explicit = os.environ.get("NATS_SERVER_BIN")
    local = ROOT / "tools" / ".bin" / "nats-server"
    candidate = explicit or (str(local) if local.is_file() else shutil.which("nats-server"))
    if not candidate or not os.access(candidate, os.X_OK):
        raise SystemExit("required nats-server is missing or not executable (set NATS_SERVER_BIN)")
    binary = str(Path(candidate).resolve())
    run(binary, "--version")
    return binary


def live(args: list[str]) -> None:
    env = dict(os.environ, NATS_SERVER_BIN=server_binary())
    run("uv", "run", "pytest", "natsio/tests/integration", "extensions", *args, env=env)


def build_all() -> None:
    for path in PACKAGES:
        run("uv", "build", "--package", project(path)["name"])


def wheel_paths() -> list[Path]:
    result = []
    for path in PACKAGES:
        metadata = project(path)
        wheel = ROOT / "dist" / f"{metadata['name'].replace('-', '_')}-{metadata['version']}-py3-none-any.whl"
        if not wheel.is_file():
            raise SystemExit(f"missing built wheel: {wheel.name}")
        result.append(wheel)
    return result


def wheels() -> None:
    artifacts = wheel_paths()
    # Each scenario starts outside the checkout with no editable installs and
    # no inherited Python path; exercise both signing backends independently.
    for extra in ("", "nkeys", "cryptography"):
        with tempfile.TemporaryDirectory(prefix="natsio-wheel-") as directory:
            temp = Path(directory)
            venv = temp / "venv"
            run("uv", "venv", "--python", sys.executable, str(venv), cwd=temp)
            python = venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
            core = str(artifacts[0]) + (f"[{extra}]" if extra else "")
            run("uv", "pip", "install", "--python", str(python), core, cwd=temp)
            run(str(python), "-I", str(ROOT / "tools" / "wheel_probe.py"), extra or "core", cwd=temp)
            if not extra:
                run("uv", "pip", "install", "--python", str(python), *(str(p) for p in artifacts[1:]), cwd=temp)
                run(str(python), "-I", str(ROOT / "tools" / "wheel_probe.py"), "namespace", cwd=temp)
            run("uv", "pip", "check", "--python", str(python), cwd=temp)


def release_check(tag: str) -> Path:
    match = re.fullmatch(r"(?:(?P<extension>[a-z][a-z-]*)/)?v(?P<version>\d+\.\d+\.\d+)", tag)
    if match is None:
        raise SystemExit("release tag must be vX.Y.Z or <extension>/vX.Y.Z")
    extension = match["extension"]
    path = ROOT / "extensions" / f"natsio-{extension}" if extension else ROOT / "natsio"
    if path not in PACKAGES:
        raise SystemExit("tag does not name a publishable package")
    version = match["version"]
    metadata = project(path)
    if metadata["name"] != (f"natsio-{extension}" if extension else "natsio"):
        raise SystemExit("tag and package name disagree")
    if metadata["version"] != version:
        raise SystemExit("tag and package version disagree")
    changelog = (path if extension else ROOT) / "CHANGELOG.md"
    released = re.search(
        r"^## (\d+\.\d+\.\d+) — \d{4}-\d{2}-\d{2}\n(.+?)(?=^## |\Z)", changelog.read_text(), re.MULTILINE | re.DOTALL
    )
    if released is None or released[1] != version or not released[2].strip():
        raise SystemExit("tag must match the latest non-empty dated changelog release")
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    tagged = subprocess.check_output(["git", "rev-parse", f"refs/tags/{tag}^{{commit}}"], cwd=ROOT, text=True).strip()
    if head != tagged:
        raise SystemExit("release validation must run on the tagged commit")
    print(f"validated {metadata['name']} {version} at {head}")
    return path


def release_artifacts(tag: str) -> None:
    path = release_check(tag)
    metadata = project(path)
    stem = f"{metadata['name'].replace('-', '_')}-{metadata['version']}"
    destination = ROOT / "release-dist"
    destination.mkdir(exist_ok=True)
    if any(destination.iterdir()):
        raise SystemExit("release-dist must be empty; refusing stale release artifacts")
    wheel = ROOT / "dist" / f"{stem}-py3-none-any.whl"
    sdist = ROOT / "dist" / f"{stem}.tar.gz"
    with zipfile.ZipFile(wheel) as archive:
        names = [name for name in archive.namelist() if name.endswith(".dist-info/METADATA")]
        if len(names) != 1:
            raise SystemExit("wheel must contain exactly one package metadata record")
        wheel_metadata = archive.read(names[0])
    with tarfile.open(sdist, "r:gz") as archive:
        stream = archive.extractfile(f"{stem}/PKG-INFO")
        if stream is None:
            raise SystemExit("sdist is missing package metadata")
        sdist_metadata = stream.read()
    for raw in (wheel_metadata, sdist_metadata):
        embedded = BytesParser().parsebytes(raw)
        if embedded["Name"] != metadata["name"] or embedded["Version"] != metadata["version"]:
            raise SystemExit("artifact metadata name/version disagrees with validated package")
    for suffix in ("-py3-none-any.whl", ".tar.gz"):
        shutil.copy2(ROOT / "dist" / f"{stem}{suffix}", destination)


def main() -> None:
    command, *args = sys.argv[1:]
    match command:
        case "live":
            live(args)
        case "require-server":
            server_binary()
        case "reliability":
            env = dict(os.environ, NATS_SERVER_BIN=server_binary())
            run("uv", "run", "pytest", "natsio/tests/integration/test_reliability_live.py", *args, env=env)
        case "pages-artifact":
            with tarfile.open(ROOT / "artifact.tar", "w", dereference=True) as archive:
                archive.add(ROOT / "site", arcname=".")
        case "build-all":
            build_all()
        case "wheels":
            wheels()
        case "release-check":
            release_check(args[0])
        case "release-artifacts":
            release_artifacts(args[0])
        case _:
            raise SystemExit(f"unknown CI helper: {command}")


if __name__ == "__main__":
    main()
