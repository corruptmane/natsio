from typing import Any, Protocol


class JSONSerializerProto(Protocol):
    def load(self, obj: str | bytes) -> Any:
        raise NotImplementedError

    def dump(self, obj: Any) -> bytes:
        raise NotImplementedError
