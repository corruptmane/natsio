"""OpenTelemetry adapter for natsio's zero-dependency instrumentation seam.

``OtelInstrumentation`` implements the core ``Instrumentation`` protocol and
exports **metrics** through ``opentelemetry-api``. Wire it in with::

    import natsio
    from natsio.otel import OtelInstrumentation

    nc = await natsio.connect("nats://localhost", instrumentation=OtelInstrumentation())

For **tracing**, propagation is caller-driven — which the seam supports:

- Producers add ``traceparent`` to the headers they publish with ``inject``.
- Consumers recover the parent context with ``extract`` and run their handler
  inside a span. ``traced_handler`` wraps a subscription callback to do exactly
  that (extract, start a CONSUMER span, time and status it) — the natural home
  for a "process" span, since natsio has three consumption modes (callback,
  iterator, ``next_msg``) and only the callback is bracketable.

The metric hooks additionally receive the message ``headers`` (seam ≥ the 1.0
protocol), so an exporter may derive header-based attributes; this adapter
records subject-cardinality-safe metrics and leaves span creation to
``traced_handler`` / ``inject`` / ``extract``.

Instrument names follow OpenTelemetry messaging semantic conventions where the
spec covers the concept (``messaging.client.sent.messages``,
``messaging.client.consumed.messages``); everything the spec does not cover is
namespaced ``nats.client.*`` and documented as custom in the README.
"""

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.metrics import Counter, Histogram, Meter, MeterProvider, get_meter_provider
from opentelemetry.propagate import extract as _otel_extract
from opentelemetry.propagate import inject as _otel_inject
from opentelemetry.propagators.textmap import Getter, Setter
from opentelemetry.trace import SpanKind, Tracer, TracerProvider

from natsio import Headers, Msg

if TYPE_CHECKING:
    from natsio import HeadersInput

__version__ = "0.1.0"

__all__ = ["OtelInstrumentation", "extract", "inject", "traced_handler"]

# --- semantic-convention attribute keys ------------------------------------
# Hardcoded rather than imported from ``opentelemetry-semantic-conventions``:
# that package is not a runtime dependency, and these keys are stable strings.
_ATTR_SYSTEM = "messaging.system"
_ATTR_DESTINATION = "messaging.destination.name"
_ATTR_OPERATION_TYPE = "messaging.operation.type"
_ATTR_ERROR_TYPE = "error.type"

# ``messaging.system`` value for NATS (open enum; "nats" is the conventional token).
_SYSTEM_NATS = "nats"

_UNIT_MESSAGE = "{message}"
_UNIT_BYTE = "By"
_UNIT_ERROR = "{error}"
_UNIT_EVENT = "{event}"


class OtelInstrumentation:
    """natsio ``Instrumentation`` that records OpenTelemetry metrics.

    ``meter_provider`` defaults to the global provider. All instruments carry
    ``messaging.system=nats``. By default the message ``subject`` is **not**
    recorded as an attribute: NATS subjects (especially ``_INBOX.*`` reply
    subjects) are effectively unbounded and would explode metric cardinality.
    Pass ``record_subject=True`` to opt in when your subject space is known to
    be small; this adds one dict allocation per message on the hot path.

    Every hook is allocation-light and cannot raise: the arithmetic-only
    OpenTelemetry SDK path does not throw, and pre-built attribute dicts are
    reused across calls. (The core also guards the hooks, but this class does
    not rely on that.)
    """

    __slots__ = (
        "_base_attrs",
        "_closes",
        "_connects",
        "_consumed_bytes",
        "_consumed_messages",
        "_consumed_size",
        "_disconnects",
        "_errors",
        "_net_received_bytes",
        "_net_sent_bytes",
        "_reconnects",
        "_record_subject",
        "_recv_attrs",
        "_send_attrs",
        "_sent_bytes",
        "_sent_messages",
        "_sent_size",
        "_slow_consumer_drops",
    )

    def __init__(
        self,
        meter_provider: MeterProvider | None = None,
        *,
        record_subject: bool = False,
    ) -> None:
        meter: Meter = (meter_provider or get_meter_provider()).get_meter("natsio.otel", __version__)
        self._record_subject = record_subject

        # Pre-built, reused attribute dicts: no per-call allocation when
        # record_subject is False (the default).
        self._base_attrs: dict[str, str] = {_ATTR_SYSTEM: _SYSTEM_NATS}
        self._send_attrs: dict[str, str] = {_ATTR_SYSTEM: _SYSTEM_NATS, _ATTR_OPERATION_TYPE: "send"}
        self._recv_attrs: dict[str, str] = {_ATTR_SYSTEM: _SYSTEM_NATS, _ATTR_OPERATION_TYPE: "receive"}

        # -- messages (semconv: messaging.client.{sent,consumed}.messages) ----
        self._sent_messages: Counter = meter.create_counter(
            "messaging.client.sent.messages",
            unit=_UNIT_MESSAGE,
            description="Number of messages published to NATS.",
        )
        self._consumed_messages: Counter = meter.create_counter(
            "messaging.client.consumed.messages",
            unit=_UNIT_MESSAGE,
            description="Number of messages delivered from NATS.",
        )

        # -- per-message payload bytes (custom: nats.client.*) ----------------
        self._sent_bytes: Counter = meter.create_counter(
            "nats.client.sent.bytes",
            unit=_UNIT_BYTE,
            description="Application payload bytes published (excludes protocol framing).",
        )
        self._consumed_bytes: Counter = meter.create_counter(
            "nats.client.consumed.bytes",
            unit=_UNIT_BYTE,
            description="Application payload bytes delivered (excludes protocol framing).",
        )

        # -- per-message payload size distribution (custom histograms) --------
        self._sent_size: Histogram = meter.create_histogram(
            "nats.client.sent.message.size",
            unit=_UNIT_BYTE,
            description="Distribution of published payload sizes.",
        )
        self._consumed_size: Histogram = meter.create_histogram(
            "nats.client.consumed.message.size",
            unit=_UNIT_BYTE,
            description="Distribution of delivered payload sizes.",
        )

        # -- network throughput (custom: whole-flush wire bytes) --------------
        self._net_sent_bytes: Counter = meter.create_counter(
            "nats.client.network.sent.bytes",
            unit=_UNIT_BYTE,
            description="Total bytes written to the transport, including protocol framing.",
        )
        self._net_received_bytes: Counter = meter.create_counter(
            "nats.client.network.received.bytes",
            unit=_UNIT_BYTE,
            description="Total bytes read from the transport, including protocol framing.",
        )

        # -- connection lifecycle (custom event counters) ---------------------
        self._connects: Counter = meter.create_counter(
            "nats.client.connects", unit=_UNIT_EVENT, description="Successful initial connections."
        )
        self._reconnects: Counter = meter.create_counter(
            "nats.client.reconnects", unit=_UNIT_EVENT, description="Successful reconnections."
        )
        self._disconnects: Counter = meter.create_counter(
            "nats.client.disconnects", unit=_UNIT_EVENT, description="Transport disconnections."
        )
        self._closes: Counter = meter.create_counter(
            "nats.client.closes", unit=_UNIT_EVENT, description="Client closes (terminal)."
        )

        # -- errors and drops -------------------------------------------------
        self._errors: Counter = meter.create_counter(
            "nats.client.errors", unit=_UNIT_ERROR, description="Asynchronous/background errors, keyed by error.type."
        )
        self._slow_consumer_drops: Counter = meter.create_counter(
            "nats.client.slow_consumer.drops",
            unit=_UNIT_MESSAGE,
            description="Messages dropped because a subscription's pending limit was exceeded.",
        )

    # -- Instrumentation protocol ------------------------------------------
    # Hooks return early where nothing is recorded, and touch only prebuilt
    # attribute dicts (or a single fresh dict when subject/error attribution is
    # required). None of this raises.

    def on_connect(self, server_url: str) -> None:
        self._connects.add(1, self._base_attrs)

    def on_disconnect(self, error: Exception | None) -> None:
        if error is None:
            self._disconnects.add(1, self._base_attrs)
        else:
            self._disconnects.add(1, {_ATTR_SYSTEM: _SYSTEM_NATS, _ATTR_ERROR_TYPE: type(error).__qualname__})

    def on_reconnect(self, server_url: str, attempts: int) -> None:
        self._reconnects.add(1, self._base_attrs)

    def on_close(self) -> None:
        self._closes.add(1, self._base_attrs)

    def on_bytes_sent(self, count: int) -> None:
        self._net_sent_bytes.add(count, self._send_attrs)

    def on_bytes_received(self, count: int) -> None:
        self._net_received_bytes.add(count, self._recv_attrs)

    def on_message_published(self, subject: str, headers: "HeadersInput | None", payload_size: int) -> None:
        # `headers` (seam >= 1.0) is available for header-derived attributes;
        # this metrics adapter keeps to cardinality-safe counters.
        attrs = {**self._send_attrs, _ATTR_DESTINATION: subject} if self._record_subject else self._send_attrs
        self._sent_messages.add(1, attrs)
        self._sent_bytes.add(payload_size, attrs)
        self._sent_size.record(payload_size, attrs)

    def on_message_delivered(self, subject: str, headers: "Headers | None", payload_size: int) -> None:
        attrs = {**self._recv_attrs, _ATTR_DESTINATION: subject} if self._record_subject else self._recv_attrs
        self._consumed_messages.add(1, attrs)
        self._consumed_bytes.add(payload_size, attrs)
        self._consumed_size.record(payload_size, attrs)

    def on_slow_consumer(self, subject: str, sid: int) -> None:
        # `sid` is intentionally not recorded: it is an ephemeral, unbounded
        # per-subscription integer and would explode cardinality.
        attrs = {**self._recv_attrs, _ATTR_DESTINATION: subject} if self._record_subject else self._recv_attrs
        self._slow_consumer_drops.add(1, attrs)

    def on_error(self, error: Exception) -> None:
        self._errors.add(1, {_ATTR_SYSTEM: _SYSTEM_NATS, _ATTR_ERROR_TYPE: type(error).__qualname__})


# --- context propagation helpers -------------------------------------------
# The seam cannot inject a traceparent into an outgoing publish nor activate an
# extracted context around the subscriber callback, so these are driven by the
# caller explicitly. They bridge OpenTelemetry's TextMap propagation to natsio
# ``Headers`` (multi-value, case-preserving).


class _HeadersSetter(Setter[Headers]):
    def set(self, carrier: Headers, key: str, value: str) -> None:
        carrier.set(key, value)


class _HeadersGetter(Getter[Headers]):
    def get(self, carrier: Headers, key: str) -> list[str] | None:
        values = carrier.get_all(key)
        return values or None

    def keys(self, carrier: Headers) -> list[str]:
        return list(carrier.keys())


_HEADERS_SETTER: Setter[Headers] = _HeadersSetter()
_HEADERS_GETTER: Getter[Headers] = _HeadersGetter()


def inject(headers: "HeadersInput | None" = None, *, context: Context | None = None) -> Headers:
    """Return a fresh ``Headers`` with the current (or given) trace context injected.

    Copies ``headers`` (never mutates the caller's), writes ``traceparent`` /
    ``tracestate`` (and any other configured propagators) into the copy, and
    returns it — ready to pass straight to ``client.publish(..., headers=...)``.

    ::

        with tracer.start_as_current_span("send order", kind=SpanKind.PRODUCER):
            await nc.publish("orders", body, headers=inject())
    """
    carrier = Headers(headers)
    _otel_inject(carrier, context=context, setter=_HEADERS_SETTER)
    return carrier


def extract(source: "Msg | HeadersInput | None", *, context: Context | None = None) -> Context:
    """Extract a trace ``Context`` from a received message (or its headers).

    Accepts a ``Msg``, a ``Headers``/mapping, or ``None`` (empty). Use the
    returned context to parent a consumer span around your handler::

        async def handler(msg):
            ctx = extract(msg)
            with tracer.start_as_current_span(
                "process orders", context=ctx, kind=SpanKind.CONSUMER
            ):
                ...
    """
    raw: HeadersInput | None
    raw = source.headers if isinstance(source, Msg) else source
    carrier = Headers(raw) if raw is not None else Headers()
    return _otel_extract(carrier, context=context, getter=_HEADERS_GETTER)


def traced_handler(
    handler: Callable[[Msg], Awaitable[None]],
    *,
    tracer: Tracer | TracerProvider | None = None,
    span_name: str | None = None,
) -> Callable[[Msg], Awaitable[None]]:
    """Wrap a subscription callback so each message runs inside a CONSUMER span.

    The wrapper extracts the parent context from the message headers, opens a
    span parented to it (so producer and consumer spans link across the wire),
    runs the handler inside that span, and records exceptions + error status —
    the "process" span natsio's seam can't create itself (only the callback is
    bracketable; iterator / ``next_msg`` consumers span their own code)::

        sub = nc.subscribe("orders", cb=traced_handler(handle_order))

    ``span_name`` defaults to ``"process <subject>"``. Pass a ``Tracer`` (or a
    provider) to override the default global tracer.
    """
    resolved: Tracer = (
        tracer.get_tracer("natsio.otel")
        if isinstance(tracer, TracerProvider)
        else (tracer if tracer is not None else trace.get_tracer("natsio.otel"))
    )

    async def wrapped(msg: Msg) -> None:
        name = span_name if span_name is not None else f"process {msg.subject}"
        parent = extract(msg)
        with resolved.start_as_current_span(
            name,
            context=parent,
            kind=SpanKind.CONSUMER,
            attributes={_ATTR_SYSTEM: _SYSTEM_NATS, _ATTR_DESTINATION: msg.subject},
        ) as span:
            try:
                await handler(msg)
            except BaseException as exc:
                span.record_exception(exc)
                span.set_status(trace.Status(trace.StatusCode.ERROR, type(exc).__qualname__))
                raise

    return wrapped
