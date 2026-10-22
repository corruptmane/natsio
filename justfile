# natsio development commands — `just` or `just <recipe>`

# Show available recipes
default:
    @just --list

# Install/refresh the workspace and dev tools
sync *args:
    uv sync {{args}}

# Verified official server binary for local runs and CI.
install-server version="v2.15.1":
    uv run python tools/install_server.py '{{version}}'

# Format everything
fmt:
    uv run ruff format natsio extensions tools examples

# Lint (with autofix)
lint:
    uv run ruff check --fix natsio extensions tools examples

# Type-check with ty
typecheck:
    uv run ty check

# Fast, hermetic tests only (no server needed)
unit *ARGS:
    uv run pytest natsio/tests/unit {{ARGS}}

# Complete core + extension live suite; missing infrastructure is an error.
integration *ARGS:
    uv run python tools/ci.py live {{ARGS}}

# Full test suite
test *ARGS:
    uv run pytest {{ARGS}}

# Non-mutating verification: format check, lint, types (CI's lint job)
check:
    uv run ruff format --check natsio extensions tools examples
    uv run ruff check natsio extensions tools examples
    uv run ty check

# Static GitHub Actions validation, pinned tool (requires Go for development).
workflow-check:
    go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 -color

# Everything CI runs: check + the full test suite
gates: check
    uv run pytest -q

# Like `gates` but formats instead of just checking
fix: fmt lint typecheck test

# Build the core wheel + sdist
build:
    uv build --package natsio

# Build every publishable workspace member
build-all:
    uv run python tools/ci.py build-all

# Install built wheels outside the editable workspace, with isolated extras.
wheel-check:
    uv run python tools/ci.py wheels

# Bounded recovery/resource validation (set NATSIO_RELIABILITY_CYCLES for soak).
reliability *ARGS:
    uv run python tools/ci.py reliability {{ARGS}}

pages-artifact:
    uv run python tools/ci.py pages-artifact

# Validation of the exact tag, before any release artifacts can be published.
release-validate tag:
    uv run python tools/ci.py release-check '{{tag}}'
    uv run python tools/ci.py require-server
    just workflow-check
    just gates
    just docs
    just build-all
    just wheel-check
    uv run python tools/ci.py release-artifacts '{{tag}}'

# Run a throwaway JetStream-enabled server in the foreground (Ctrl-C to stop)
server port="4222":
    tools/.bin/nats-server -a 127.0.0.1 -p {{port}} -js -sd /tmp/natsio-dev-js

# Run the benchmark harness (natsio vs nats-py vs nats-core); e.g. `just bench --quick`
bench *ARGS:
    uv run natsio-bench {{ARGS}}

# Bump the version everywhere + refresh the lock + run gates: just release 0.12.0
# (Requires a '## Unreleased' section in CHANGELOG.md; prints the commit/tag ritual.)
release version:
    uv run python tools/release.py {{version}}
    uv sync
    just gates

release-extension name version:
    uv run python tools/release_extension.py '{{name}}' '{{version}}'
    uv sync

# Refresh only benchmark competitors, keeping runtime dependencies unchanged.
bench-refresh:
    uv lock --upgrade-package nats-py --upgrade-package nats-core
    uv sync

# Build the documentation site (strict: broken links fail)
docs:
    uv run --group docs mkdocs build --strict

# Serve the documentation site locally with live reload
docs-serve:
    uv run --group docs mkdocs serve

# Remove caches and build artifacts
clean:
    rm -rf dist site .pytest_cache .ruff_cache .hypothesis
    find . -type d -name __pycache__ -prune -exec rm -rf {} \;
