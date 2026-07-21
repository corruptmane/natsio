"""Version-bump automation for `just release <version>`.

Mutates every file that carries the version, with guards for the two easy
mistakes: releasing without a changelog section, and bumping backwards.
Prints the exact follow-up commands; it never commits, tags, or pushes.
"""

import json
import re
import sys
from datetime import date
from pathlib import Path
from typing import NoReturn

ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = ROOT / "natsio" / "pyproject.toml"
CHANGELOG = ROOT / "CHANGELOG.md"
VERSION_TEST = ROOT / "natsio" / "tests" / "test_package.py"
CONTEXT7 = ROOT / "context7.json"

_VERSION_RE = re.compile(r"\A\d+\.\d+\.\d+\Z")


def fail(message: str) -> NoReturn:
    print(f"error: {message}", file=sys.stderr)
    raise SystemExit(1)


def main() -> None:
    args = [a for a in sys.argv[1:] if a != "--dry-run"]
    dry_run = "--dry-run" in sys.argv
    if len(args) != 1:
        fail("usage: release.py <version> [--dry-run]   (e.g. release.py 0.12.0)")
    new = args[0]
    if not _VERSION_RE.match(new):
        fail(f"version {new!r} is not X.Y.Z")

    pyproject = PYPROJECT.read_text()
    match = re.search(r'^version = "([^"]+)"$', pyproject, re.M)
    if match is None:
        fail(f"no version line found in {PYPROJECT}")
    old = match.group(1)
    if tuple(map(int, new.split("."))) <= tuple(int(p) for p in re.findall(r"\d+", old)[:3]):
        fail(f"new version {new} must be greater than current {old}")

    changelog = CHANGELOG.read_text()
    if "## Unreleased" not in changelog:
        fail("CHANGELOG.md has no '## Unreleased' section — write the changelog before releasing")

    version_test = VERSION_TEST.read_text()
    if f'"{old}"' not in version_test:
        fail(f"version test does not pin {old!r} — {VERSION_TEST} out of sync?")

    context7 = json.loads(CONTEXT7.read_text())

    print(f"bump: {old} -> {new}")
    if dry_run:
        print("dry run: no files written")
        return

    PYPROJECT.write_text(pyproject.replace(f'version = "{old}"', f'version = "{new}"', 1))
    CHANGELOG.write_text(changelog.replace("## Unreleased\n", f"## {new} — {date.today().isoformat()}\n", 1))
    VERSION_TEST.write_text(version_test.replace(f'"{old}"', f'"{new}"', 1))
    context7["previousVersions"] = [{"tag": f"v{new}"}, *context7.get("previousVersions", [])]
    CONTEXT7.write_text(json.dumps(context7, indent=2) + "\n")

    print("updated: natsio/pyproject.toml, CHANGELOG.md, tests/test_package.py, context7.json")
    print()
    print("next steps (after gates pass):")
    print("  git add natsio/pyproject.toml CHANGELOG.md natsio/tests/test_package.py context7.json uv.lock")
    print(f'  git commit -m "release: natsio {new}"')
    print("  git push origin main")
    print(f"  git tag v{new} && git push origin v{new}   # then approve the pypi deployment")


if __name__ == "__main__":
    main()
