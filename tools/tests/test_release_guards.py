"""Release tooling rejects inconsistent inputs before selecting publish artifacts."""

import hashlib
import importlib.util
import io
import subprocess
import tarfile
import zipfile
from pathlib import Path
from types import ModuleType

import pytest


def load_tool(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, Path(__file__).parents[1] / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ci = load_tool("ci")
installer = load_tool("install_server")


def git(root: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-c", "user.name=Release Test", "-c", "user.email=release@example.invalid", *args],
        cwd=root,
        check=True,
        capture_output=True,
    )


@pytest.fixture
def release_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    core = tmp_path / "natsio"
    core.mkdir()
    (core / "pyproject.toml").write_text('[project]\nname = "natsio"\nversion = "1.2.3"\n')
    (tmp_path / "CHANGELOG.md").write_text("# Changelog\n\n## 1.2.3 — 2026-10-22\n\n- Fixed a bug.\n")
    extension = tmp_path / "extensions" / "natsio-testing"
    extension.mkdir(parents=True)
    (extension / "pyproject.toml").write_text('[project]\nname = "natsio-testing"\nversion = "0.4.5"\n')
    (extension / "CHANGELOG.md").write_text("# Changelog\n\n## 0.4.5 — 2026-10-22\n\n- Fixed metadata.\n")
    git(tmp_path, "init", "-q")
    git(
        tmp_path,
        "add",
        "natsio/pyproject.toml",
        "CHANGELOG.md",
        "extensions/natsio-testing/pyproject.toml",
        "extensions/natsio-testing/CHANGELOG.md",
    )
    git(tmp_path, "-c", "commit.gpgsign=false", "commit", "-qm", "release fixture")
    git(tmp_path, "tag", "v1.2.3")
    git(tmp_path, "tag", "testing/v0.4.5")
    monkeypatch.setattr(ci, "ROOT", tmp_path)
    monkeypatch.setattr(ci, "PACKAGES", [core, extension])
    return tmp_path


def test_release_accepts_exact_core_and_extension_tags(release_root: Path) -> None:
    assert ci.release_check("v1.2.3") == release_root / "natsio"
    assert ci.release_check("testing/v0.4.5") == release_root / "extensions" / "natsio-testing"


@pytest.mark.parametrize("tag", ["v1.2.4", "testing/v0.4.6"])
def test_release_rejects_tag_package_version_disagreement(release_root: Path, tag: str) -> None:
    with pytest.raises(SystemExit, match="version disagree"):
        ci.release_check(tag)


@pytest.mark.parametrize("tag,package", [("v1.2.3", "natsio"), ("testing/v0.4.5", "extensions/natsio-testing")])
def test_release_rejects_project_name_that_disagrees_with_tag(release_root: Path, tag: str, package: str) -> None:
    pyproject = release_root / package / "pyproject.toml"
    source = pyproject.read_text()
    pyproject.write_text(source.replace('name = "natsio', 'name = "unexpected'))
    with pytest.raises(SystemExit, match=r"(?i)(name|package|tag)"):
        ci.release_check(tag)


@pytest.mark.parametrize("tag", ["v1.2", "v1.2.3rc1", "../v1.2.3", "testing/extra/v0.4.5", "unknown/v1.2.3"])
def test_release_rejects_malformed_or_unknown_package_tags(release_root: Path, tag: str) -> None:
    with pytest.raises(SystemExit):
        ci.release_check(tag)


@pytest.mark.parametrize(
    "changelog",
    [
        "## Unreleased\n\n- A fix.\n",
        "## 1.2.2 — 2026-10-22\n\n- A fix.\n",
        "## 1.2.3 — 2026-10-22\n\n",
        "## 1.2.4 — 2026-10-22\n\n- Newer.\n\n## 1.2.3 — 2026-10-21\n\n- Older.\n",
    ],
)
def test_release_rejects_missing_empty_or_stale_changelog(release_root: Path, changelog: str) -> None:
    (release_root / "CHANGELOG.md").write_text(changelog)
    with pytest.raises(SystemExit, match="changelog"):
        ci.release_check("v1.2.3")


def test_release_rejects_head_after_the_tagged_commit(release_root: Path) -> None:
    git(release_root, "-c", "commit.gpgsign=false", "commit", "--allow-empty", "-qm", "unvalidated change")
    with pytest.raises(SystemExit, match="tagged commit"):
        ci.release_check("v1.2.3")


def distributions(
    root: Path, *, wheel_name: str = "natsio", wheel_version: str = "1.2.3", sdist_version: str = "1.2.3"
):
    dist = root / "dist"
    dist.mkdir()
    with zipfile.ZipFile(dist / "natsio-1.2.3-py3-none-any.whl", "w") as wheel:
        wheel.writestr(
            "natsio-1.2.3.dist-info/METADATA",
            f"Metadata-Version: 2.4\nName: {wheel_name}\nVersion: {wheel_version}\n",
        )
    with tarfile.open(dist / "natsio-1.2.3.tar.gz", "w:gz") as archive:
        metadata = f"Metadata-Version: 2.4\nName: natsio\nVersion: {sdist_version}\n".encode()
        info = tarfile.TarInfo("natsio-1.2.3/PKG-INFO")
        info.size = len(metadata)
        archive.addfile(info, io.BytesIO(metadata))


def test_release_selects_only_validated_artifacts(release_root: Path) -> None:
    distributions(release_root)
    ci.release_artifacts("v1.2.3")
    selected = release_root / "release-dist"
    assert sorted(path.name for path in selected.iterdir()) == ["natsio-1.2.3-py3-none-any.whl", "natsio-1.2.3.tar.gz"]
    for path in selected.iterdir():
        assert path.read_bytes() == (release_root / "dist" / path.name).read_bytes()


@pytest.mark.parametrize(
    "metadata",
    [{"wheel_name": "other-project"}, {"wheel_version": "1.2.2"}, {"sdist_version": "1.2.2"}],
)
def test_release_rejects_embedded_metadata_even_when_filenames_match(
    release_root: Path, metadata: dict[str, str]
) -> None:
    distributions(release_root, **metadata)
    with pytest.raises(SystemExit, match=r"(?i)(metadata|artifact|wheel|sdist|version|name)"):
        ci.release_artifacts("v1.2.3")


def test_release_refuses_stale_publication_directory(release_root: Path) -> None:
    distributions(release_root)
    destination = release_root / "release-dist"
    destination.mkdir()
    (destination / "unrelated.whl").touch()
    with pytest.raises(SystemExit, match="stale"):
        ci.release_artifacts("v1.2.3")


def test_required_live_suite_fails_before_pytest_if_server_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ci, "ROOT", tmp_path)
    monkeypatch.setenv("NATS_SERVER_BIN", str(tmp_path / "missing-server"))
    called = []
    monkeypatch.setattr(ci, "run", lambda *args, **kwargs: called.append(args))
    with pytest.raises(SystemExit, match="required nats-server"):
        ci.live(["-q"])
    assert called == []


def test_required_live_suite_rejects_non_executable_server(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    binary = tmp_path / "nats-server"
    binary.write_text("not an executable")
    binary.chmod(0o644)
    monkeypatch.setenv("NATS_SERVER_BIN", str(binary))
    with pytest.raises(SystemExit, match="not executable"):
        ci.server_binary()


def test_server_installer_rejects_tampering_before_writing_binary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    filename = "nats-server-v9.8.7-linux-amd64.tar.gz"
    checksum = hashlib.sha256(b"untampered archive").hexdigest()
    monkeypatch.setattr(installer, "ROOT", tmp_path)
    monkeypatch.setattr(installer.sys, "argv", ["install_server.py", "v9.8.7"])
    monkeypatch.setattr(installer.platform, "system", lambda: "Linux")
    monkeypatch.setattr(installer.platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(
        installer,
        "download",
        lambda url: f"{checksum}  {filename}\n".encode() if url.endswith("SHA256SUMS") else b"tampered",
    )
    with pytest.raises(SystemExit, match="SHA-256 mismatch"):
        installer.main()
    assert not (tmp_path / "tools" / ".bin" / "nats-server").exists()
