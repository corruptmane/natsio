from datetime import datetime, timedelta
from typing import Final

from natsio.utils.time import fromisoformat

from .types import Converter

NANOSECOND_POWER: Final[int] = 10**9


class TimedeltaNanosecondsConverter(Converter[timedelta, int]):
    def to_wire(self, value: timedelta) -> int:
        return int(value.total_seconds() * NANOSECOND_POWER)

    def from_wire(self, value: int) -> timedelta:
        return timedelta(microseconds=value / 1000)


class DatetimeIsoConverter(Converter[datetime, str]):
    def to_wire(self, value: datetime) -> str:
        return value.isoformat()

    def from_wire(self, value: str) -> datetime:
        return fromisoformat(value)


TIMEDELTA_NANO = TimedeltaNanosecondsConverter()
DATETIME_ISO = DatetimeIsoConverter()
