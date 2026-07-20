"""Zero-dependency observability seam.

natsio never imports a metrics or tracing library. Instead, the client calls
these hooks at its load-bearing moments; the default implementation is a
no-op. Exporters (OpenTelemetry, Prometheus, StatsD, ...) live outside the
core — e.g. a future ``natsio-otel`` extension — by implementing this
protocol and passing an instance via ``ConnectOptions(instrumentation=...)``.

Hooks are invoked synchronously on hot paths: implementations must be fast
and must never raise (exceptions are swallowed and logged).
"""

from typing import Protocol

__all__ = ["Instrumentation", "NoopInstrumentation"]


class Instrumentation(Protocol):
    def on_connect(self, server_url: str) -> None: ...

    def on_disconnect(self, error: Exception | None) -> None: ...

    def on_reconnect(self, server_url: str, attempts: int) -> None: ...

    def on_close(self) -> None: ...

    def on_bytes_sent(self, count: int) -> None: ...

    def on_bytes_received(self, count: int) -> None: ...

    def on_message_published(self, subject: str, payload_size: int) -> None: ...

    def on_message_delivered(self, subject: str, payload_size: int) -> None: ...

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

    def on_message_published(self, subject: str, payload_size: int) -> None:
        return

    def on_message_delivered(self, subject: str, payload_size: int) -> None:
        return

    def on_slow_consumer(self, subject: str, sid: int) -> None:
        return

    def on_error(self, error: Exception) -> None:
        return
