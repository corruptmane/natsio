"""Connection lifecycle events and their in-process fan-out bus.

Bus subscribers are *synchronous and non-blocking* — they run on the read/
dispatch path. Anything slow (user callbacks, queue-fed ``events()`` streams)
must schedule itself onto a task; the client layer owns that adaptation.
"""

import logging
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from enum import Enum

__all__ = ["ConnectionEvent", "ConnectionState", "EventBus"]

log = logging.getLogger("natsio.lifecycle")


class ConnectionState(Enum):
    """The lifecycle state of a `Client`, readable via `Client.status`.

    - `DISCONNECTED` — created but not yet connected, or a connection was lost
      and no reconnect is in progress.
    - `CONNECTING` — the initial connect is in flight.
    - `CONNECTED` — the live, usable state.
    - `RECONNECTING` — the link dropped and the client is re-establishing it
      (a healthy, recoverable state, not a closure).
    - `DRAINING` — `drain()` is in progress: subscriptions are being flushed
      before close.
    - `CLOSED` — terminal; the client cannot be reused.
    """

    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    RECONNECTING = "RECONNECTING"
    DRAINING = "DRAINING"
    CLOSED = "CLOSED"


@dataclass(frozen=True, slots=True)
class Connected:
    server_url: str


@dataclass(frozen=True, slots=True)
class Disconnected:
    error: Exception | None


@dataclass(frozen=True, slots=True)
class Reconnected:
    server_url: str


@dataclass(frozen=True, slots=True)
class LameDuck:
    server_url: str


@dataclass(frozen=True, slots=True)
class ServersDiscovered:
    urls: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ErrorOccurred:
    """A background error not tied to any caller (benign -ERR, callback crash...)."""

    error: Exception


@dataclass(frozen=True, slots=True)
class Closed:
    pass


type ConnectionEvent = Connected | Disconnected | Reconnected | LameDuck | ServersDiscovered | ErrorOccurred | Closed

type EventHook = Callable[[ConnectionEvent], None]


class EventBus:
    __slots__ = ("_hooks",)

    def __init__(self) -> None:
        self._hooks: list[EventHook] = []

    def subscribe(self, hook: EventHook) -> Callable[[], None]:
        """Register a sync hook; returns an unsubscribe function."""
        self._hooks.append(hook)

        def unsubscribe() -> None:
            with suppress(ValueError):
                self._hooks.remove(hook)

        return unsubscribe

    def emit(self, event: ConnectionEvent) -> None:
        for hook in list(self._hooks):
            try:
                hook(event)
            except Exception:
                log.exception("event hook failed for %r", event)
