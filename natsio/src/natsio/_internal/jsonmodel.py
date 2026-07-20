"""Typed dataclass ↔ JSON mapping for the JetStream API, zero dependencies.

Field conversion is declared inline with ``Annotated``::

    @dataclass(slots=True, kw_only=True)
    class StreamConfig(JsonModel):
        name: str = ""
        max_age: Annotated[timedelta | None, NS_DURATION] = None

Rules (per ADR-1's JSON conventions):
- durations travel as integer nanoseconds, timestamps as RFC 3339 strings;
- ``None`` fields are omitted on emission (never ``null``); zeros and ``False``
  are always emitted;
- unknown wire keys are captured into ``extra`` on decode and merged back on
  encode, so round-tripping never destroys fields added by newer servers.
"""

import types
from dataclasses import dataclass, field, fields
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Annotated, Any, Protocol, Self, Union, get_args, get_origin, get_type_hints

__all__ = ["NS_DURATION", "RFC3339", "Converter", "JsonModel"]


class Converter[P, W](Protocol):
    """Two-way mapping between a Python value and its wire representation."""

    def to_wire(self, value: P) -> W: ...

    def from_wire(self, raw: W) -> P: ...


class _NsDuration:
    """``timedelta`` ↔ integer nanoseconds."""

    def to_wire(self, value: timedelta) -> int:
        return int(value.total_seconds() * 1_000_000_000)

    def from_wire(self, raw: int) -> timedelta:
        return timedelta(microseconds=raw / 1_000)


class _Rfc3339:
    """Timezone-aware ``datetime`` ↔ RFC 3339 string.

    The server emits UTC with a ``Z`` suffix and nanosecond precision; Python's
    ``fromisoformat`` (3.11+) accepts ``Z`` but only microsecond precision, so
    fractional seconds are truncated to 6 digits before parsing.
    """

    def to_wire(self, value: datetime) -> str:
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")

    def from_wire(self, raw: str) -> datetime:
        text = raw.strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        if "." in text:
            head, _, rest = text.partition(".")
            split = next((i for i, char in enumerate(rest) if not char.isdigit()), len(rest))
            digits, suffix = rest[:split], rest[split:]
            text = f"{head}.{digits[:6].ljust(6, '0')}{suffix}"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed


NS_DURATION: Converter[timedelta, int] = _NsDuration()
RFC3339: Converter[datetime, str] = _Rfc3339()

type _FieldPlan = tuple[str, Any, Any]  # (name, converter | None, base_type)

_PLANS: dict[type, list[_FieldPlan]] = {}

if False:  # pragma: no cover - forward declaration for type checkers
    JsonModel = None


def _unwrap(annotation: Any) -> tuple[Any, Any]:
    """(converter | None, base type with Annotated/Optional peeled off)."""
    converter = None
    if get_origin(annotation) is Annotated:
        base, *meta = get_args(annotation)
        for item in meta:
            if hasattr(item, "to_wire") and hasattr(item, "from_wire"):
                converter = item
        annotation = base
    if get_origin(annotation) in (Union, types.UnionType):
        non_none = [a for a in get_args(annotation) if a is not type(None)]
        if len(non_none) == 1:
            inner_conv, annotation = _unwrap(non_none[0])
            converter = converter or inner_conv
    return converter, annotation


def _plan(cls: "type[JsonModel]") -> list[_FieldPlan]:
    plan = _PLANS.get(cls)
    if plan is None:
        hints = get_type_hints(cls, include_extras=True)
        plan = []
        for spec in fields(cls):
            if spec.name == "extra":
                continue
            converter, base = _unwrap(hints[spec.name])
            plan.append((spec.name, converter, base))
        _PLANS[cls] = plan
    return plan


def _encode(value: Any, converter: Any, base: Any = Any) -> Any:
    if converter is not None:
        return converter.to_wire(value)
    if isinstance(value, JsonModel):
        return value.to_wire()
    if isinstance(value, Enum):
        return value.value
    origin = get_origin(base)
    if isinstance(value, list):
        item_conv, item_base = (None, Any)
        if origin in (list, tuple):
            args = get_args(base)
            if args:
                item_conv, item_base = _unwrap(args[0])
        return [_encode(item, item_conv, item_base) for item in value]
    if isinstance(value, dict):
        value_conv, value_base = (None, Any)
        if origin is dict:
            args = get_args(base)
            if len(args) == 2:
                value_conv, value_base = _unwrap(args[1])
        return {key: _encode(item, value_conv, value_base) for key, item in value.items()}
    return value


def _decode(raw: Any, converter: Any, base: Any) -> Any:
    if raw is None:
        return None
    if converter is not None:
        return converter.from_wire(raw)
    if isinstance(base, type) and issubclass(base, JsonModel):
        return base.from_wire(raw)
    if isinstance(base, type) and issubclass(base, Enum):
        return base(raw)
    origin = get_origin(base)
    if origin in (list, tuple):
        (item_type,) = get_args(base) or (Any,)
        item_conv, item_base = _unwrap(item_type)
        return [_decode(item, item_conv, item_base) for item in raw]
    if origin is dict:
        args = get_args(base)
        value_conv, value_base = _unwrap(args[1]) if len(args) == 2 else (None, Any)
        return {key: _decode(item, value_conv, value_base) for key, item in raw.items()}
    return raw


@dataclass(slots=True, kw_only=True)
class JsonModel:
    """Base for JetStream API entities. Subclasses are slotted dataclasses."""

    extra: dict[str, Any] = field(default_factory=dict, repr=False, compare=False)

    def to_wire(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for name, converter, base in _plan(type(self)):
            value = getattr(self, name)
            if value is None:
                continue
            out[name] = _encode(value, converter, base)
        out.update(self.extra)
        return out

    @classmethod
    def from_wire(cls, data: dict[str, Any]) -> Self:
        known: dict[str, Any] = {}
        consumed: set[str] = set()
        for name, converter, base in _plan(cls):
            if name in data:
                known[name] = _decode(data[name], converter, base)
                consumed.add(name)
        extra = {key: value for key, value in data.items() if key not in consumed}
        return cls(**known, extra=extra)
