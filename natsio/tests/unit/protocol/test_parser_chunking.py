"""The assertion whose absence hid the old client's worst bug: parsing must be
invariant under every possible chunking of the byte stream."""

import random

from helpers import concat, err_frame, header_block, hmsg_frame, info_frame, msg_frame, parse_chunked, parse_whole

REFERENCE_STREAM = concat(
    [
        info_frame(b'{"server_id":"s","max_payload":1048576}'),
        b"PING\r\n",
        msg_frame("foo.bar", 1, b"hello world"),
        hmsg_frame("foo.baz", 2, header_block("A: 1", "A: 2", "B: x"), b"payload", reply="_INBOX.r.1"),
        msg_frame("empty", 3, b""),
        hmsg_frame("status.only", 4, header_block(status="503"), b""),
        msg_frame("tricky", 5, b"contains\r\nCRLF\r\n\r\nMSG lookalike 9 3\r\n"),
        b"PONG\r\n",
        err_frame("Stale Connection"),
        b"+OK\r\n",
        msg_frame("last", 6, b"bye", reply="r.2"),
    ]
)

EXPECTED = parse_whole(REFERENCE_STREAM)


def test_reference_stream_parses() -> None:
    assert len(EXPECTED) == 11


def test_every_single_split_point() -> None:
    for split in range(1, len(REFERENCE_STREAM)):
        events = parse_chunked(REFERENCE_STREAM, [split])
        assert events == EXPECTED, f"split at byte {split} diverged"


def test_one_byte_at_a_time() -> None:
    events = parse_chunked(REFERENCE_STREAM, list(range(1, len(REFERENCE_STREAM))))
    assert events == EXPECTED


def test_random_multiway_splits() -> None:
    rng = random.Random(0xA75)
    for _ in range(200):
        count = rng.randint(2, 12)
        boundaries = sorted(rng.sample(range(1, len(REFERENCE_STREAM)), count))
        events = parse_chunked(REFERENCE_STREAM, boundaries)
        assert events == EXPECTED, f"boundaries {boundaries} diverged"


def test_large_payload_across_many_chunks() -> None:
    payload = bytes(range(256)) * 4096  # 1 MiB
    stream = concat([msg_frame("big", 7, payload), b"PING\r\n"])
    whole = parse_whole(stream)
    chunked = parse_chunked(stream, list(range(8192, len(stream), 8192)))
    assert chunked == whole
    assert len(chunked) == 2


def test_buffer_compaction_across_many_messages() -> None:
    # Total volume far beyond the compaction threshold, fed in odd-sized chunks.
    frames = [msg_frame(f"s.{i}", i, bytes([i % 256]) * 977) for i in range(300)]
    stream = concat(frames)
    events = parse_chunked(stream, list(range(1017, len(stream), 1017)))
    assert events == parse_whole(stream)
    assert len(events) == 300
