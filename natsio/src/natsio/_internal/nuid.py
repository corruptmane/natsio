"""NUID: fast, collision-resistant identifiers for inboxes and reply tokens.

Port of the reference NATS NUID (nats.go / nats-py, Apache-2.0): a 22-character
base62 string built from a 12-character random prefix plus a 10-character
sequence that advances by a random increment. The prefix is re-randomized when
the sequence wraps.

This is an identifier generator, not a security primitive: the prefix comes from
``os.urandom`` so inbox subjects are unguessable in practice, but nothing here
is relied upon for authentication.
"""

import os
import random
from typing import Final

__all__ = ["NUID", "next_nuid"]

_DIGITS: Final = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
_BASE: Final = 62
_PREFIX_LEN: Final = 12
_SEQ_LEN: Final = 10
_MAX_SEQ: Final = 62**10
_MIN_INC: Final = 33
_MAX_INC: Final = 333

NUID_LEN: Final = _PREFIX_LEN + _SEQ_LEN


class NUID:
    __slots__ = ("_inc", "_prefix", "_rng", "_seq")

    def __init__(self) -> None:
        self._rng = random.Random()
        self._prefix = bytearray(_PREFIX_LEN)
        self._seq = 0
        self._inc = 0
        self._randomize_prefix()
        self._reset_sequence()

    def _randomize_prefix(self) -> None:
        self._prefix = bytearray(_DIGITS[byte % _BASE] for byte in os.urandom(_PREFIX_LEN))

    def _reset_sequence(self) -> None:
        self._seq = self._rng.randint(0, _MAX_SEQ - 1)
        self._inc = _MIN_INC + self._rng.randint(0, _MAX_INC - _MIN_INC)

    def next(self) -> bytes:
        self._seq += self._inc
        if self._seq >= _MAX_SEQ:
            self._randomize_prefix()
            self._reset_sequence()
        seq = self._seq
        suffix = bytearray(_SEQ_LEN)
        for index in range(_SEQ_LEN - 1, -1, -1):
            suffix[index] = _DIGITS[seq % _BASE]
            seq //= _BASE
        return bytes(self._prefix) + bytes(suffix)

    def next_str(self) -> str:
        return self.next().decode("ascii")


_GLOBAL = NUID()


def next_nuid() -> str:
    """A fresh NUID from the process-wide generator."""
    return _GLOBAL.next_str()
