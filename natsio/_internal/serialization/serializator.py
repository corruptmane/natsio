from collections.abc import Mapping as MappingABC
from dataclasses import fields, is_dataclass
from enum import Enum
from types import UnionType
from typing import (
    Annotated,
    Any,
    Mapping,
    Union,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

from .types import DT, Converter, DataclassInstance


def get_converter_from_annotation(
    field_type: type,
) -> Converter[Any, Any] | None:
    if get_origin(field_type) is Annotated:
        args = get_args(field_type)
        for arg in args[1:]:
            if isinstance(arg, Converter):
                return arg
    return None


def get_base_type(field_type: type) -> type:
    origin = get_origin(field_type)
    args = get_args(field_type)
    if origin is Annotated:
        return cast(type, args[0])

    if origin is not None:
        if origin is UnionType or origin is Union:
            for arg in args:
                if arg is not type(None):
                    return get_base_type(arg)
        return cast(type, origin)
    return field_type


def handle_enum(value: Any, enum_type: type) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, str) and issubclass(enum_type, Enum):
        try:
            return enum_type(value)
        except ValueError:
            raise ValueError(f"Invalid value '{value}' for enum {enum_type.__name__}")
    return value


def serialize_value(value: Any, field_type: type) -> Any:
    if value is None:
        return None

    converter = get_converter_from_annotation(field_type)
    if converter is not None:
        return converter.to_wire(value)

    base_type = get_base_type(field_type)
    origin = get_origin(field_type)
    args = get_args(field_type)

    if isinstance(value, Enum):
        return handle_enum(value, base_type)

    if origin is list and args:
        item_type = args[0] if args else type(Any)
        return [serialize_value(val, item_type) for val in value]

    if origin is MappingABC and len(args) == 2:
        item_type = args[1] if args else type(Any)
        return {k: serialize_value(v, item_type) for k, v in value.items()}

    if is_dataclass(value.__class__):
        return serialize_dataclass(value)

    return value


def deserialize_value(value: Any, field_type: type) -> Any:
    if value is None:
        return None

    converter = get_converter_from_annotation(field_type)
    if converter is not None:
        return converter.from_wire(value)

    base_type = get_base_type(field_type)
    origin = get_origin(field_type)
    args = get_args(field_type)

    if is_dataclass(base_type):
        return deserialize_dataclass(value, base_type)

    if origin is list and args:
        item_type = args[0] if args else type(Any)
        return [deserialize_value(val, item_type) for val in value]

    if origin is MappingABC and len(args) == 2:
        item_type = args[1] if args else type(Any)
        return {k: deserialize_value(v, item_type) for k, v in value.items()}

    if issubclass(base_type, Enum):
        return handle_enum(value, base_type)

    if isinstance(value, (str, int, float, bool)) and not isinstance(value, base_type):
        try:
            return base_type(value)  # type: ignore[call-arg]
        except (ValueError, TypeError):
            pass

    return value


def serialize_dataclass(obj: DataclassInstance) -> Mapping[str, Any]:
    result = {}
    type_hints = get_type_hints(obj.__class__, include_extras=True)

    for field in fields(obj):
        value = getattr(obj, field.name)
        if value is not None or field.default is None or field.default_factory is None:
            result[field.name] = serialize_value(value, type_hints[field.name])

    return result


def deserialize_dataclass(data: Mapping[str, Any], cls: type[DT]) -> DT:
    type_hints = get_type_hints(cls, include_extras=True)
    kwargs = {}

    for field in fields(cls):
        if field.name in data:
            kwargs[field.name] = deserialize_value(
                data[field.name],
                type_hints[field.name],
            )

    return cls(**kwargs)
