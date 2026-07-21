"""Typed dataclass ↔ JSON mapping for the JetStream API, zero dependencies.

Field conversion is declared inline with ``Annotated``:

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

# A field's decode/encode strategy, classified ONCE when the per-class plan is
# built so from_wire/to_wire never call typing.get_origin/get_args again. Each
# strategy is ``(kind, payload)``:
#   _PLAIN -> passthrough           (payload: None)
#   _CONV  -> two-way Converter      (payload: the converter)
#   _MODEL -> nested JsonModel       (payload: the model class)
#   _ENUM  -> Enum                   (payload: the enum class)
#   _LIST  -> list/tuple of items    (payload: the item strategy, recursively)
#   _DICT  -> dict of values         (payload: the value strategy, recursively)
_PLAIN, _CONV, _MODEL, _ENUM, _LIST, _DICT = range(6)

type _Strategy = tuple[int, Any]
type _FieldPlan = tuple[str, _Strategy]  # (name, strategy)

_PLAIN_STRATEGY: _Strategy = (_PLAIN, None)

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


def _build_strategy(converter: Any, base: Any) -> _Strategy:
    """Classify one (already Annotated/Optional-unwrapped) field type ONCE.

    The branch order mirrors the old per-call ``_decode`` — converter, nested
    model, enum, list/tuple, dict, then passthrough — so the wire contract is
    unchanged; only the reflection moves from per-call to per-class.
    """
    if converter is not None:
        return (_CONV, converter)
    if isinstance(base, type):
        if issubclass(base, JsonModel):
            return (_MODEL, base)
        if issubclass(base, Enum):
            return (_ENUM, base)
    origin = get_origin(base)
    if origin in (list, tuple):
        args = get_args(base)
        return (_LIST, _build_strategy(*_unwrap(args[0])) if args else _PLAIN_STRATEGY)
    if origin is dict:
        args = get_args(base)
        return (_DICT, _build_strategy(*_unwrap(args[1])) if len(args) == 2 else _PLAIN_STRATEGY)
    return _PLAIN_STRATEGY


def _plan(cls: "type[JsonModel]") -> list[_FieldPlan]:
    plan = _PLANS.get(cls)
    if plan is None:
        hints = get_type_hints(cls, include_extras=True)
        plan = []
        for spec in fields(cls):
            if spec.name == "extra":
                continue
            plan.append((spec.name, _build_strategy(*_unwrap(hints[spec.name]))))
        _PLANS[cls] = plan
    return plan


def _encode(value: Any, strategy: _Strategy) -> Any:
    """Encode one value against its precomputed strategy.

    Deliberately value-driven with the exact branch order of the pre-plan
    version (converter, model, enum, list, dict, passthrough), descending
    through the precomputed nested strategies rather than reflecting again.
    """
    kind, payload = strategy
    if kind == _CONV:
        return payload.to_wire(value)
    if isinstance(value, JsonModel):
        return value.to_wire()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, list):
        item = payload if kind == _LIST else _PLAIN_STRATEGY
        return [_encode(element, item) for element in value]
    if isinstance(value, dict):
        item = payload if kind == _DICT else _PLAIN_STRATEGY
        return {key: _encode(element, item) for key, element in value.items()}
    return value


def _decode(raw: Any, strategy: _Strategy) -> Any:
    if raw is None:
        return None
    kind, payload = strategy
    if kind == _PLAIN:
        return raw
    if kind == _CONV:
        return payload.from_wire(raw)
    if kind == _MODEL:
        return payload.from_wire(raw)
    if kind == _ENUM:
        return payload(raw)
    if kind == _LIST:
        return [_decode(element, payload) for element in raw]
    return {key: _decode(element, payload) for key, element in raw.items()}


@dataclass(slots=True, kw_only=True)
class JsonModel:
    """Base for JetStream API entities. Subclasses are slotted dataclasses."""

    extra: dict[str, Any] = field(default_factory=dict, repr=False, compare=False)

    def to_wire(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for name, strategy in _plan(type(self)):
            value = getattr(self, name)
            if value is None:
                continue
            out[name] = _encode(value, strategy)
        out.update(self.extra)
        return out

    @classmethod
    def from_wire(cls, data: dict[str, Any]) -> Self:
        known: dict[str, Any] = {}
        consumed: set[str] = set()
        for name, strategy in _plan(cls):
            if name in data:
                known[name] = _decode(data[name], strategy)
                consumed.add(name)
        extra = {key: value for key, value in data.items() if key not in consumed}
        return cls(**known, extra=extra)
