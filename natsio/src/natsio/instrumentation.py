"""Zero-dependency observability seam.

natsio never imports a metrics or tracing library. Instead, the client calls
these hooks at its load-bearing moments; the default implementation is a
no-op. Exporters (OpenTelemetry, Prometheus, StatsD, ...) live outside the
core — e.g. the ``natsio-otel`` extension — by implementing this protocol and
passing an instance via ``ConnectOptions(instrumentation=...)``.

Hooks are invoked synchronously on hot paths: implementations must be fast
and must never raise (exceptions are swallowed and logged).

The two message hooks carry the message ``headers`` so a tracing exporter can
extract an inbound trace context (``on_message_delivered``) and record
outbound attributes (``on_message_published``). Producer-side context
*injection* is a caller action — add ``traceparent`` to the headers you pass
to ``publish`` (the ``natsio-otel`` extension ships an ``inject`` helper) —
rather than a core hook, so the publish path stays allocation-light. Timing a
span around user *processing* is likewise a userland concern (natsio has three
consumption modes — callback, iterator, ``next_msg`` — and only one is
bracketable by the core), so the extension wraps the handler instead.

This protocol's signatures are frozen: they are the seam exporters compile
against.
"""

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from natsio._internal.protocol import Headers, HeadersInput

__all__ = ["Instrumentation", "NoopInstrumentation"]


class Instrumentation(Protocol):
    def on_connect(self, server_url: str) -> None: ...

    def on_disconnect(self, error: Exception | None) -> None: ...

    def on_reconnect(self, server_url: str, attempts: int) -> None: ...

    def on_close(self) -> None: ...

    def on_bytes_sent(self, count: int) -> None: ...

    def on_bytes_received(self, count: int) -> None: ...

    def on_message_published(self, subject: str, headers: "HeadersInput | None", payload_size: int) -> None: ...

    def on_message_delivered(self, subject: str, headers: "Headers | None", payload_size: int) -> None: ...

    def on_slow_consumer(self, subject: str, sid: int) -> None: ...

    def on_error(self, error: Exception) -> None: ...


class NoopInstrumentation:
    """Default: every hook does nothing."""

    def on_connect(self, server_url: str) -> None:
        return

    def on_disconnect(self, error: Exception | None) -> None:
        return

    def on_reconnect(self, server_url: str, attempts: int) -> None:
        return

    def on_close(self) -> None:
        return

    def on_bytes_sent(self, count: int) -> None:
        return

    def on_bytes_received(self, count: int) -> None:
        return

    def on_message_published(self, subject: str, headers: "HeadersInput | None", payload_size: int) -> None:
        return

    def on_message_delivered(self, subject: str, headers: "Headers | None", payload_size: int) -> None:
        return

    def on_slow_consumer(self, subject: str, sid: int) -> None:
        return

    def on_error(self, error: Exception) -> None:
        return
