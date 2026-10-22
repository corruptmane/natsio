"""Prepare an extension patch release; never commit, tag or publish."""

import re
import sys
import tomllib
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    name, version = sys.argv[1:]
    if not re.fullmatch(r"[a-z][a-z-]*", name) or not re.fullmatch(r"\d+\.\d+\.\d+", version):
        raise SystemExit("usage: just release-extension <name> X.Y.Z")
    path = ROOT / "extensions" / f"natsio-{name}"
    metadata = path / "pyproject.toml"
    text = metadata.read_text()
    old = tomllib.loads(text)["project"]["version"]
    if tuple(map(int, version.split("."))) <= tuple(map(int, old.split("."))):
        raise SystemExit("extension release must increase the version")
    changelog = path / "CHANGELOG.md"
    changes = changelog.read_text()
    section = re.search(r"^## Unreleased\n(.+?)(?=^## |\Z)", changes, re.MULTILINE | re.DOTALL)
    if section is None or not section[1].strip():
        raise SystemExit("extension release requires non-empty ## Unreleased notes")
    metadata.write_text(text.replace(f'version = "{old}"', f'version = "{version}"', 1))
    today = datetime.now(UTC).date().isoformat()
    changelog.write_text(changes.replace("## Unreleased\n", f"## {version} — {today}\n", 1))
    module = path / "src" / "natsio" / name.replace("-", "_") / "__init__.py"
    content = module.read_text()
    if f'__version__ = "{old}"' in content:
        module.write_text(content.replace(f'__version__ = "{old}"', f'__version__ = "{version}"', 1))
    print(f"prepared natsio-{name} {version}; owner signs, tags {name}/v{version} and publishes after validation")


if __name__ == "__main__":
    main()
