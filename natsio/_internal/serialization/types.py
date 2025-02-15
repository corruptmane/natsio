from dataclasses import Field
from typing import Any, ClassVar, Protocol, TypeVar, runtime_checkable


class DataclassInstance(Protocol):
    __dataclass_fields__: ClassVar[dict[str, Field[Any]]]


DT = TypeVar("DT", bound=DataclassInstance)

T = TypeVar("T")
V = TypeVar("V")


@runtime_checkable
class Converter(Protocol[T, V]):
    def to_wire(self, value: T) -> V: ...
    def from_wire(self, value: V) -> T: ...
