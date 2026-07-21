# natsio development commands — `just` or `just <recipe>`

# Show available recipes
default:
    @just --list

# Install/refresh the workspace and dev tools
sync:
    uv sync

# Format everything
fmt:
    uv run ruff format natsio extensions

# Lint (with autofix)
lint:
    uv run ruff check --fix natsio extensions

# Type-check with ty
typecheck:
    uv run ty check

# Fast, hermetic tests only (no server needed)
unit *ARGS:
    uv run pytest natsio/tests/unit {{ARGS}}

# Tests against a real nats-server (needs tools/.bin/nats-server or NATS_SERVER_BIN)
integration *ARGS:
    uv run pytest natsio/tests/integration {{ARGS}}

# Full test suite
test *ARGS:
    uv run pytest {{ARGS}}

# Non-mutating verification: format check, lint, types (CI's lint job)
check:
    uv run ruff format --check natsio extensions
    uv run ruff check natsio extensions
    uv run ty check

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
    uv build --package natsio
    uv build --package natsio-testing

# Run a throwaway JetStream-enabled server in the foreground (Ctrl-C to stop)
server port="4222":
    tools/.bin/nats-server -a 127.0.0.1 -p {{port}} -js -sd /tmp/natsio-dev-js

# Remove caches and build artifacts
clean:
    rm -rf dist .pytest_cache .ruff_cache .hypothesis
    find . -type d -name __pycache__ -prune -exec rm -rf {} \;
