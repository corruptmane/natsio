from natsio._internal.nuid import NUID, next_nuid
from natsio._internal.nuid import NUID_LEN as LENGTH

_ALPHABET = set("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")


def test_length_and_alphabet() -> None:
    value = next_nuid()
    assert len(value) == LENGTH == 22
    assert set(value) <= _ALPHABET


def test_values_are_unique() -> None:
    nuid = NUID()
    values = {nuid.next_str() for _ in range(10_000)}
    assert len(values) == 10_000


def test_prefix_is_stable_while_sequence_advances() -> None:
    nuid = NUID()
    first, second = nuid.next_str(), nuid.next_str()
    assert first[:12] == second[:12]
    assert first[12:] != second[12:]


def test_separate_generators_use_different_prefixes() -> None:
    prefixes = {NUID().next_str()[:12] for _ in range(50)}
    assert len(prefixes) > 40  # 62^12 space; collisions should be vanishingly rare


def test_sequence_rollover_rerandomizes_prefix() -> None:
    nuid = NUID()
    before = nuid.next_str()[:12]
    nuid._seq = 62**10 - 1  # force the wrap on the next call
    after = nuid.next_str()[:12]
    assert before != after
