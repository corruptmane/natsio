"""Tests for natsio.otel: metric instruments per hook, and propagation helpers.

Unit tests drive the ``Instrumentation`` hooks directly against an
``InMemoryMetricReader`` and assert the exported points. One live test wires
``OtelInstrumentation`` into a real ``natsio.connect`` and asserts metric
deltas from actual pub/sub traffic. The inject/extract round-trip goes through
real natsio ``Headers``.
"""

import os
from pathlib import Path

import pytest

# Editable/namespace install: the runtime pkgutil __path__ merge is invisible to
# the type checker (see natsio/tests/server.py for the same workaround).
from natsio.otel import OtelInstrumentation, extract, inject  # ty: ignore[unresolved-import]
from opentelemetry import trace
from opentelemetry.metrics import Counter, Histogram
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider

import natsio
from natsio import Headers, Msg

# --- metric plumbing --------------------------------------------------------


def _reader_and_instr(**kwargs) -> tuple[InMemoryMetricReader, OtelInstrumentation]:
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    return reader, OtelInstrumentation(provider, **kwargs)


def _points(reader: InMemoryMetricReader) -> dict[str, list]:
    """{instrument_name: [data_point, ...]} across all scopes."""
    out: dict[str, list] = {}
    data = reader.get_metrics_data()
    if data is None:
        return out
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                out.setdefault(metric.name, []).extend(metric.data.data_points)
    return out


def _sum(points: list) -> float:
    return sum(p.value for p in points)


# --- unit: instruments fire per hook ---------------------------------------


def test_publish_hook_records_messages_bytes_and_size():
    reader, instr = _reader_and_instr()
    instr.on_message_published("orders", None, 100)
    instr.on_message_published("orders", None, 50)
    pts = _points(reader)

    assert _sum(pts["messaging.client.sent.messages"]) == 2
    assert _sum(pts["nats.client.sent.bytes"]) == 150
    # histogram of payload sizes
    hist = pts["nats.client.sent.message.size"][0]
    assert hist.count == 2
    assert hist.sum == 150


def test_delivered_hook_records_consumed_metrics():
    reader, instr = _reader_and_instr()
    instr.on_message_delivered("events", None, 42)
    pts = _points(reader)
    assert _sum(pts["messaging.client.consumed.messages"]) == 1
    assert _sum(pts["nats.client.consumed.bytes"]) == 42
    assert pts["nats.client.consumed.message.size"][0].count == 1


def test_base_attributes_carry_messaging_system_and_operation_type():
    reader, instr = _reader_and_instr()
    instr.on_message_published("orders", None, 10)
    instr.on_message_delivered("orders", None, 10)
    pts = _points(reader)
    send = pts["messaging.client.sent.messages"][0]
    recv = pts["messaging.client.consumed.messages"][0]
    assert send.attributes["messaging.system"] == "nats"
    assert send.attributes["messaging.operation.type"] == "send"
    assert recv.attributes["messaging.operation.type"] == "receive"


def test_subject_not_recorded_by_default():
    reader, instr = _reader_and_instr()
    instr.on_message_published("secret.inbox.abc", None, 10)
    pt = _points(reader)["messaging.client.sent.messages"][0]
    assert "messaging.destination.name" not in pt.attributes


def test_subject_recorded_when_opted_in():
    reader, instr = _reader_and_instr(record_subject=True)
    instr.on_message_published("orders.eu", None, 10)
    pt = _points(reader)["messaging.client.sent.messages"][0]
    assert pt.attributes["messaging.destination.name"] == "orders.eu"


def test_network_byte_counters():
    reader, instr = _reader_and_instr()
    instr.on_bytes_sent(200)
    instr.on_bytes_received(300)
    pts = _points(reader)
    assert _sum(pts["nats.client.network.sent.bytes"]) == 200
    assert _sum(pts["nats.client.network.received.bytes"]) == 300


def test_connection_lifecycle_counters():
    reader, instr = _reader_and_instr()
    instr.on_connect("nats://a")
    instr.on_reconnect("nats://b", 1)
    instr.on_reconnect("nats://b", 2)
    instr.on_disconnect(None)
    instr.on_close()
    pts = _points(reader)
    assert _sum(pts["nats.client.connects"]) == 1
    assert _sum(pts["nats.client.reconnects"]) == 2
    assert _sum(pts["nats.client.disconnects"]) == 1
    assert _sum(pts["nats.client.closes"]) == 1


def test_disconnect_with_error_records_error_type():
    reader, instr = _reader_and_instr()
    instr.on_disconnect(TimeoutError("boom"))
    pt = _points(reader)["nats.client.disconnects"][0]
    assert pt.attributes["error.type"] == "TimeoutError"


def test_error_hook_keys_by_error_type():
    reader, instr = _reader_and_instr()
    instr.on_error(ValueError("x"))
    instr.on_error(ValueError("y"))
    instr.on_error(KeyError("z"))
    pts = _points(reader)["nats.client.errors"]
    by_type = {p.attributes["error.type"]: p.value for p in pts}
    assert by_type == {"ValueError": 2, "KeyError": 1}


def test_slow_consumer_drops_counter():
    reader, instr = _reader_and_instr()
    instr.on_slow_consumer("firehose", 7)
    instr.on_slow_consumer("firehose", 7)
    assert _sum(_points(reader)["nats.client.slow_consumer.drops"]) == 2


def test_instrument_types_and_units():
    _, instr = _reader_and_instr()
    assert isinstance(instr._sent_messages, Counter)
    assert isinstance(instr._sent_size, Histogram)


def test_hooks_do_not_raise_without_sdk_provider():
    # With only the API (no configured SDK), instruments are no-ops but the
    # calls must still be safe.
    instr = OtelInstrumentation()
    instr.on_message_published("x", None, 1)
    instr.on_message_delivered("x", None, 1)
    instr.on_error(RuntimeError())
    instr.on_slow_consumer("x", 1)


def test_implements_full_instrumentation_protocol():
    from natsio.instrumentation import Instrumentation

    instr: Instrumentation = OtelInstrumentation()
    assert instr is not None  # structural check: assignment type-checks


# --- unit: inject / extract round-trip -------------------------------------


def test_inject_returns_fresh_headers_with_traceparent():
    trace.set_tracer_provider(TracerProvider())
    tracer = trace.get_tracer("test")
    original: dict[str, str] = {"X-Custom": "keep"}
    with tracer.start_as_current_span("send"):
        headers = inject(original)
    assert isinstance(headers, Headers)
    assert "traceparent" in headers
    assert headers["X-Custom"] == "keep"
    # caller's mapping is untouched
    assert "traceparent" not in original


def test_inject_extract_round_trip_preserves_trace_id():
    trace.set_tracer_provider(TracerProvider())
    tracer = trace.get_tracer("test")
    with tracer.start_as_current_span("producer") as span:
        expected = span.get_span_context().trace_id
        headers = inject()
    ctx = extract(headers)
    extracted = trace.get_current_span(ctx).get_span_context()
    assert extracted.trace_id == expected


def test_extract_from_msg():
    trace.set_tracer_provider(TracerProvider())
    tracer = trace.get_tracer("test")
    with tracer.start_as_current_span("producer") as span:
        expected = span.get_span_context().trace_id
        headers = inject()
    msg = Msg(subject="orders", payload=b"", headers=headers)
    ctx = extract(msg)
    assert trace.get_current_span(ctx).get_span_context().trace_id == expected


def test_extract_from_none_and_empty_is_safe():
    assert extract(None) is not None
    assert extract(Msg(subject="x", payload=b"", headers=None)) is not None


# --- live: real server, real traffic ---------------------------------------


def _server_binary() -> str | None:
    env = os.environ.get("NATS_SERVER_BIN")
    if env and Path(env).is_file():
        return env
    repo_root = Path(__file__).resolve().parents[3]
    local = repo_root / "tools" / ".bin" / "nats-server"
    if local.is_file():
        return str(local)
    from natsio.testing import find_server_binary  # ty: ignore[unresolved-import]

    return find_server_binary()


@pytest.fixture
async def server():
    from natsio.testing import NatsServerProcess  # ty: ignore[unresolved-import]

    binary = _server_binary()
    if binary is None:
        pytest.skip("nats-server binary not found (set NATS_SERVER_BIN)")
    proc = NatsServerProcess(binary=binary)
    await proc.start()
    yield proc
    await proc.stop()


async def test_live_pub_sub_produces_metric_deltas(server):
    import asyncio

    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    instr = OtelInstrumentation(provider, record_subject=True)

    received: list[Msg] = []
    nc = await natsio.connect(server.url, instrumentation=instr, connect_timeout=5.0)
    try:
        await nc.subscribe("metrics.test", cb=received.append)
        await nc.publish("metrics.test", b"hello")
        await nc.publish("metrics.test", b"world!!")
        await nc.flush()
        # let the delivery hooks run on the read path
        for _ in range(50):
            if len(received) >= 2:
                break
            await asyncio.sleep(0.02)
        assert len(received) == 2
    finally:
        await nc.close()

    pts = _points(reader)
    # published: 2 messages, 5 + 7 = 12 payload bytes
    sent = [
        p
        for p in pts["messaging.client.sent.messages"]
        if p.attributes.get("messaging.destination.name") == "metrics.test"
    ]
    assert _sum(sent) == 2
    sent_bytes = [
        p for p in pts["nats.client.sent.bytes"] if p.attributes.get("messaging.destination.name") == "metrics.test"
    ]
    assert _sum(sent_bytes) == 12
    # delivered: our subscription got both back
    consumed = [
        p
        for p in pts["messaging.client.consumed.messages"]
        if p.attributes.get("messaging.destination.name") == "metrics.test"
    ]
    assert _sum(consumed) == 2
    # connection came up and network counters moved
    assert _sum(pts["nats.client.connects"]) == 1
    assert _sum(pts["nats.client.network.sent.bytes"]) > 0
    assert _sum(pts["nats.client.network.received.bytes"]) > 0


async def test_live_header_propagation_round_trip(server):
    import asyncio

    trace.set_tracer_provider(TracerProvider())
    tracer = trace.get_tracer("live")

    got: list[Msg] = []
    nc = await natsio.connect(server.url, connect_timeout=5.0)
    try:
        await nc.subscribe("trace.test", cb=got.append)
        await nc.flush()
        with tracer.start_as_current_span("producer") as span:
            expected = span.get_span_context().trace_id
            await nc.publish("trace.test", b"payload", headers=inject())
        await nc.flush()
        for _ in range(50):
            if got:
                break
            await asyncio.sleep(0.02)
        assert got
        ctx = extract(got[0])
        assert trace.get_current_span(ctx).get_span_context().trace_id == expected
    finally:
        await nc.close()


async def test_traced_handler_creates_child_span_linked_to_producer(server):
    import asyncio

    from natsio.otel import traced_handler  # ty: ignore[unresolved-import]
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    tracer = trace.get_tracer("producer-side")

    done = asyncio.Event()

    async def handler(msg: Msg) -> None:
        done.set()

    nc = await natsio.connect(server.url, connect_timeout=5.0)
    try:
        await nc.subscribe("traced.work", cb=traced_handler(handler, tracer=provider))
        await nc.flush()
        with tracer.start_as_current_span("producer", kind=trace.SpanKind.PRODUCER) as span:
            producer_trace = span.get_span_context().trace_id
            await nc.publish("traced.work", b"job", headers=inject())
        await asyncio.wait_for(done.wait(), timeout=5.0)
    finally:
        await nc.close()

    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert "process traced.work" in spans
    consumer_span = spans["process traced.work"]
    # The consumer span is parented to the producer's trace — cross-wire link.
    assert consumer_span.context.trace_id == producer_trace
    assert consumer_span.kind is trace.SpanKind.CONSUMER
    assert consumer_span.attributes is not None
    assert consumer_span.attributes["messaging.destination.name"] == "traced.work"


async def test_traced_handler_records_exceptions(server):
    import asyncio

    from natsio.otel import traced_handler  # ty: ignore[unresolved-import]
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    seen = asyncio.Event()

    async def boom(msg: Msg) -> None:
        seen.set()
        raise ValueError("handler failed")

    nc = await natsio.connect(server.url, connect_timeout=5.0)
    try:
        await nc.subscribe("traced.fail", cb=traced_handler(boom, tracer=provider))
        await nc.flush()
        await nc.publish("traced.fail", b"x")
        await asyncio.wait_for(seen.wait(), timeout=5.0)
        await asyncio.sleep(0.1)  # let the span finish recording
    finally:
        await nc.close()

    span = exporter.get_finished_spans()[0]
    assert span.status.status_code is trace.StatusCode.ERROR
    assert any(e.name == "exception" for e in span.events)
