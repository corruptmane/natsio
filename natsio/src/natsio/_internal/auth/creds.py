"""Parsing of NATS ``.creds`` files (decorated JWT + NKey seed blocks)."""

import re

from natsio.errors import ConfigError

__all__ = ["parse_creds", "parse_nkey_seed"]

_JWT_RE = re.compile(
    r"-{3,}BEGIN NATS USER JWT-{3,}\s*(?P<jwt>[A-Za-z0-9._\-=]+)\s*-{3,}END NATS USER JWT-{3,}",
)
_SEED_RE = re.compile(
    r"-{3,}BEGIN USER NKEY SEED-{3,}\s*(?P<seed>[A-Z2-7]+)\s*-{3,}END USER NKEY SEED-{3,}",
)
# A decorated ``-----BEGIN USER NKEY SEED-----`` block (any role) or a bare seed
# line — how the ``nk``/``nats`` CLI store nkey files (nats.go
# ``nkeys.ParseDecoratedNKey`` accepts both forms).
_DECORATED_SEED_RE = re.compile(
    r"-{3,}\s*BEGIN [A-Z]+ NKEY SEED\s*-{3,}\s*(?P<seed>[A-Z2-7]+)\s*-{3,}\s*END [A-Z]+ NKEY SEED\s*-{3,}",
)
_BARE_SEED_RE = re.compile(r"(?<![A-Z2-7])(?P<seed>S[A-Z2-7]{2}[A-Z2-7]{50,})(?![A-Z2-7])")


def parse_creds(content: str) -> tuple[str, str]:
    """Extract ``(jwt, seed)`` from decorated .creds file content."""
    jwt_match = _JWT_RE.search(content)
    if jwt_match is None:
        raise ConfigError("credentials file contains no NATS USER JWT block")
    seed_match = _SEED_RE.search(content)
    if seed_match is None:
        raise ConfigError("credentials file contains no USER NKEY SEED block")
    return jwt_match.group("jwt"), seed_match.group("seed")


def parse_nkey_seed(content: str) -> str:
    """Extract an NKey seed from an nkey file (decorated block or bare seed)."""
    match = _DECORATED_SEED_RE.search(content) or _BARE_SEED_RE.search(content.strip())
    if match is None:
        raise ConfigError("nkey file contains no NKey seed")
    return match.group("seed")
