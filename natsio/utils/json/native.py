import json
from typing import Any
from .base import JSONSerializerProto


class JSONSerializer(JSONSerializerProto):
    def load(self, obj: str | bytes) -> Any:
        return json.loads(obj)

    def dump(self, obj: Any) -> bytes:
        return json.dumps(obj, sort_keys=True).encode()
