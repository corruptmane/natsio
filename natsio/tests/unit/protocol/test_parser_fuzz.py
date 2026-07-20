"""Property-based tests: chunking invariance over generated streams, and
crash-safety over arbitrary garbage."""

from helpers import header_block, hmsg_frame, msg_frame, parse_chunked, parse_whole
from hypothesis import given, settings
from hypothesis import strategies as st

from natsio._internal.protocol import NEED_DATA, Parser
from natsio.errors import NATSError

SUBJECTS = st.from_regex(r"[a-zA-Z0-9_]{1,12}(\.[a-zA-Z0-9_*>]{1,8}){0,3}", fullmatch=True)
PAYLOADS = st.binary(max_size=2048)
SIDS = st.integers(min_value=0, max_value=2**31)
HEADER_KEYS = st.from_regex(r"[A-Za-z0-9!#$%&'*+\-.^_`|~]{1,16}", fullmatch=True)
HEADER_VALUES = st.text(
    alphabet=st.characters(codec="utf-8", exclude_characters="\r\n"),
    max_size=32,
).map(str.strip)


@st.composite
def frames(draw: st.DrawFn) -> bytes:
    kind = draw(st.integers(min_value=0, max_value=5))
    if kind == 0:
        return draw(st.sampled_from([b"PING\r\n", b"PONG\r\n", b"+OK\r\n"]))
    if kind == 1:
        return b"INFO " + draw(st.binary(max_size=64).filter(lambda b: b"\r" not in b and b"\n" not in b)) + b"\r\n"
    if kind == 2:
        return b"-ERR '" + draw(st.text(alphabet="abcdefgh ", max_size=24)).encode() + b"'\r\n"
    subject = draw(SUBJECTS)
    sid = draw(SIDS)
    reply = draw(st.none() | SUBJECTS)
    payload = draw(PAYLOADS)
    if kind in (3, 4):
        return msg_frame(subject, sid, payload, reply=reply)
    lines = draw(
        st.lists(
            st.tuples(HEADER_KEYS, HEADER_VALUES).map(lambda kv: f"{kv[0]}: {kv[1]}"),
            max_size=4,
        )
    )
    status = draw(st.none() | st.sampled_from(["503", "100 Idle Heartbeat", "409 Consumer Deleted"]))
    return hmsg_frame(subject, sid, header_block(*lines, status=status), payload, reply=reply)


@st.composite
def stream_and_boundaries(draw: st.DrawFn) -> tuple[bytes, list[int]]:
    stream = b"".join(draw(st.lists(frames(), min_size=1, max_size=8)))
    if len(stream) < 2:
        return stream, []
    boundaries = draw(
        st.lists(st.integers(min_value=1, max_value=len(stream) - 1), max_size=16).map(
            lambda splits: sorted(set(splits))
        )
    )
    return stream, boundaries


@settings(max_examples=300, deadline=None)
@given(stream_and_boundaries())
def test_chunking_invariance(case: tuple[bytes, list[int]]) -> None:
    stream, boundaries = case
    assert parse_chunked(stream, boundaries) == parse_whole(stream)


@settings(max_examples=300, deadline=None)
@given(st.binary(max_size=4096), st.integers(min_value=1, max_value=64))
def test_garbage_never_hangs_or_leaks_exceptions(data: bytes, chunk: int) -> None:
    """Arbitrary bytes must produce events and/or a NATSError — nothing else."""
    parser = Parser(max_control_line=256, max_payload=1 << 16)
    try:
        for offset in range(0, len(data), chunk):
            parser.receive_data(data[offset : offset + chunk])
            while parser.next_event() is not NEED_DATA:
                pass
    except NATSError:
        pass
