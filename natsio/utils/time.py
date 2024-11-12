from datetime import datetime
import time
from typing import Final, TypeVar

NANOSECOND_POWER: Final[int] = 10**9

Time = TypeVar("Time", int, float)


def to_nanoseconds(time: Time) -> int:
    return int(time * NANOSECOND_POWER)


def from_nanoseconds(time: Time) -> float:
    return time / NANOSECOND_POWER


def fromisoformat(time: str) -> datetime:
    return datetime.fromisoformat(time)


def get_now_ns() -> int:
    return time.monotonic_ns()
