from typing import Final, TypeVar

NANOSECOND_POWER: Final[int] = 10**9

Time = TypeVar("Time", int, float)

def to_nanoseconds(time: Time) -> Time:
    return time * NANOSECOND_POWER


def from_nanoseconds(time: Time) -> float:
    return time / NANOSECOND_POWER
