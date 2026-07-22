"""natsio.natscontext — load ``nats`` CLI context files (ADR-21) into natsio.

The `nats` command-line tool stores named connection profiles as JSON under
``$XDG_CONFIG_HOME/nats/context/<name>.json`` (falling back to
``~/.config/nats``), with the active profile named in a sibling
``context.txt``. This extension discovers those files and maps their fields
onto `natsio.connect` options.

    import natsio.natscontext as natscontext

    ctx = natscontext.load("staging")          # or load() for the selected one
    async with await natscontext.connect("staging") as nc:
        ...

This mirrors the semantics of the official Go helper
``github.com/synadia-io/orbit.go/natscontext``. Fields the `nats` CLI
understands but that a natsio connection cannot express (SOCKS proxy,
JetStream domain/prefixes, ``nsc`` lookups, CLI-only cosmetics) are still
parsed and preserved on the returned `Context` — see each field's docstring
and the ``README`` for what is and isn't applied.
"""

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import natsio
from natsio.client import Client
from natsio.errors import NATSError
from natsio.options import ConnectOptions, TLSConfig

__all__ = [
    "Context",
    "ContextError",
    "ContextMalformedError",
    "ContextNotFoundError",
    "config_dir",
    "connect",
    "context_dir",
    "list_contexts",
    "load",
    "selected_context",
]


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class ContextError(NATSError):
    """A NATS CLI context could not be resolved, read, or applied."""


class ContextNotFoundError(ContextError, FileNotFoundError):
    """The named context (or the active selection) does not exist."""


class ContextMalformedError(ContextError):
    """A context file exists but is not valid JSON / not a JSON object."""


# --------------------------------------------------------------------------- #
# Discovery (XDG layout, mirroring orbit.go's parentDir/ctxDir/selectedContext)
# --------------------------------------------------------------------------- #
def config_dir() -> Path:
    """The ``nats`` config directory: ``$XDG_CONFIG_HOME/nats`` or ``~/.config/nats``."""
    xdg = os.environ.get("XDG_CONFIG_HOME")
    parent = Path(xdg) if xdg else Path.home() / ".config"
    return parent / "nats"


def context_dir() -> Path:
    """The directory holding ``<name>.json`` context files."""
    return config_dir() / "context"


def _selected_file() -> Path:
    return config_dir() / "context.txt"


def selected_context() -> str | None:
    """The name of the active context (from ``context.txt``), or ``None``."""
    try:
        name = _selected_file().read_text(encoding="utf-8").strip()
    except (FileNotFoundError, NotADirectoryError, OSError):
        return None
    return name or None


def list_contexts() -> list[str]:
    """Sorted names of all known contexts (``<name>.json`` files); ``[]`` if none."""
    try:
        entries = os.listdir(context_dir())
    except (FileNotFoundError, NotADirectoryError, OSError):
        return []
    return sorted(name[: -len(".json")] for name in entries if name.endswith(".json"))


def _valid_name(name: str) -> bool:
    # orbit.go validName: non-empty, no traversal, no path separators.
    return bool(name) and ".." not in name and os.sep not in name and (os.altsep or os.sep) not in name


# --------------------------------------------------------------------------- #
# Path expansion (orbit.go applies ~ and, for creds, $ENV expansion)
# --------------------------------------------------------------------------- #
def _expand(path: str) -> str:
    """Expand ``~`` and ``$VAR`` in a filesystem path, as the CLI does."""
    return os.path.expanduser(os.path.expandvars(path)) if path else path


# --------------------------------------------------------------------------- #
# Context
# --------------------------------------------------------------------------- #
# Every JSON key orbit.go's Settings struct understands, so an unknown key can
# be flagged instead of silently dropped.
_KNOWN_KEYS = frozenset(
    {
        "name",
        "description",
        "url",
        "socks_proxy",
        "token",
        "user",
        "password",
        "creds",
        "nkey",
        "cert",
        "key",
        "ca",
        "nsc",
        "jetstream_domain",
        "jetstream_api_prefix",
        "jetstream_event_prefix",
        "inbox_prefix",
        "user_jwt",
        "color_scheme",
        "tls_first",
        "windows_cert_store",
        "windows_cert_match_by",
        "windows_cert_match",
        "windows_ca_certs_match",
    }
)


@dataclass(frozen=True, slots=True)
class Context:
    """A parsed NATS CLI context.

    Holds every field the ``nats`` CLI understands. `connect_kwargs` projects
    the *applicable* subset onto `natsio.connect` keyword arguments; the rest
    are preserved here for callers that need them (notably JetStream
    domain/prefixes, which natsio's connection layer does not carry).
    """

    #: Friendly context name (file stem, or the ``name`` JSON field).
    name: str
    #: Path the context was loaded from, if any.
    path: str | None = None

    # -- applied to the natsio connection --
    url: str = ""
    user: str = ""
    password: str = field(default="", repr=False)
    token: str = field(default="", repr=False)
    creds: str = ""
    nkey: str = ""
    cert: str = ""
    key: str = ""
    ca: str = ""
    tls_first: bool = False
    inbox_prefix: str = ""

    # -- parsed & preserved, but NOT applied to the connection (see README) --
    description: str = ""
    socks_proxy: str = ""
    nsc: str = ""
    user_jwt: str = field(default="", repr=False)
    jetstream_domain: str = ""
    jetstream_api_prefix: str = ""
    jetstream_event_prefix: str = ""
    color_scheme: str = ""
    windows_cert_store: str = ""

    #: The raw decoded JSON, verbatim (including any unknown keys).
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_dict(cls, data: dict[str, Any], *, name: str, path: str | None = None) -> "Context":
        def s(key: str) -> str:
            value = data.get(key, "")
            return value if isinstance(value, str) else ""

        return cls(
            name=data.get("name") or name,
            path=path,
            url=s("url"),
            user=s("user"),
            password=s("password"),
            token=s("token"),
            creds=s("creds"),
            nkey=s("nkey"),
            cert=s("cert"),
            key=s("key"),
            ca=s("ca"),
            tls_first=bool(data.get("tls_first", False)),
            inbox_prefix=s("inbox_prefix"),
            description=s("description"),
            socks_proxy=s("socks_proxy"),
            nsc=s("nsc"),
            user_jwt=s("user_jwt"),
            jetstream_domain=s("jetstream_domain"),
            jetstream_api_prefix=s("jetstream_api_prefix"),
            jetstream_event_prefix=s("jetstream_event_prefix"),
            color_scheme=s("color_scheme"),
            windows_cert_store=s("windows_cert_store"),
            raw=data,
        )

    @property
    def unknown_keys(self) -> frozenset[str]:
        """JSON keys present in the file that this mapper does not recognise."""
        return frozenset(self.raw) - _KNOWN_KEYS

    def _build_tls(self) -> TLSConfig | None:
        if not (self.cert or self.key or self.ca or self.tls_first):
            return None
        # orbit.go applies a client cert only when *both* cert and key are set;
        # a half-configured pair is a config error rather than a silent drop.
        if bool(self.cert) != bool(self.key):
            raise ContextError("context sets only one of 'cert'/'key'; both are required for a client certificate")
        # natsio's TLSConfig loads the PEM files itself (lazily, per reconnect).
        return TLSConfig(
            certfile=_expand(self.cert) or None,
            keyfile=_expand(self.key) or None,
            cafile=_expand(self.ca) or None,
            handshake_first=self.tls_first,
        )

    def connect_kwargs(self) -> dict[str, Any]:
        """Project this context onto `natsio.connect` keyword arguments.

        Raises `ContextError` for settings natsio cannot honour (a Windows
        certificate store, or a half-specified client certificate).
        """
        if self.windows_cert_store:
            raise ContextError("windows certificate stores are not supported")

        kwargs: dict[str, Any] = {}

        if self.url:
            # A context may hold a comma-separated server list (nats.Connect
            # splits on ',' too).
            kwargs["servers"] = tuple(part.strip() for part in self.url.split(",") if part.strip())

        # Auth precedence mirrors orbit.go's switch (user > creds > nkey), with
        # token as the final fallback. natsio forbids combining mechanisms, so —
        # unlike nats.go, which would additionally send a token alongside these —
        # a token set together with another mechanism is ignored (see README).
        if self.user:
            kwargs["user"] = self.user
            kwargs["password"] = self.password
        elif self.creds:
            kwargs["credentials"] = _expand(self.creds)
        elif self.nkey:
            # natsio's NKeyFileAuth reads and parses the seed file itself,
            # re-reading on every reconnect (rotation) instead of once at load.
            kwargs["nkey_file"] = _expand(self.nkey)
        elif self.token:
            kwargs["token"] = self.token

        tls = self._build_tls()
        if tls is not None:
            kwargs["tls"] = tls

        if self.inbox_prefix:
            kwargs["inbox_prefix"] = self.inbox_prefix

        return kwargs


# --------------------------------------------------------------------------- #
# Loading
# --------------------------------------------------------------------------- #
def _load_file(path: Path, *, name: str) -> Context:
    try:
        content = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ContextNotFoundError(f"context file not found: {path}") from exc
    except OSError as exc:
        raise ContextError(f"cannot read context file {path}: {exc}") from exc
    try:
        data = json.loads(content)
    except json.JSONDecodeError as exc:
        raise ContextMalformedError(f"context file {path} is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ContextMalformedError(f"context file {path} must contain a JSON object")
    return Context.from_dict(data, name=name, path=str(path))


def load(name: str | None = None) -> Context:
    """Load a context by ``name``, or the selected context when ``name`` is omitted.

    If ``name`` is an absolute path to a JSON file, that file is loaded directly
    (its stem becomes the context name). Otherwise ``name`` is resolved under
    the XDG context directory; when omitted it falls back to the active
    selection from ``context.txt``.

    Raises `ContextNotFoundError` when no name/selection is available or the
    named context does not exist, and `ContextMalformedError` for invalid JSON.
    """
    if name and os.path.isabs(name):
        path = Path(name)
        return _load_file(path, name=path.stem)

    resolved = name or selected_context()
    if not resolved:
        raise ContextNotFoundError(
            "no context name given and no context is selected "
            f"(set one with `nats context select`, or create {_selected_file()})"
        )
    if not _valid_name(resolved):
        raise ContextNotFoundError(f"invalid context name: {resolved!r}")

    path = context_dir() / f"{resolved}.json"
    if not path.exists():
        raise ContextNotFoundError(f"unknown context {resolved!r} (looked in {context_dir()})")
    return _load_file(path, name=resolved)


# --------------------------------------------------------------------------- #
# Convenience connect
# --------------------------------------------------------------------------- #
async def connect(
    name: str | None = None,
    /,
    *,
    options: ConnectOptions | None = None,
    error_cb: Any = None,
    **overrides: Any,
) -> Client:
    """Load a context and connect with it.

    The context selector is **positional-only** so it does not collide with
    natsio's ``name`` connect kwarg (the client name). ``overrides`` (and an
    ``options=`` base) are forwarded to `natsio.connect` and take precedence
    over the context-derived settings — e.g. ``await connect("staging",
    name="my-app")`` connects with context ``staging`` and client name
    ``my-app``.
    """
    ctx = load(name)
    kwargs = ctx.connect_kwargs()
    kwargs.update(overrides)
    return await natsio.connect(options=options, error_cb=error_cb, **kwargs)
