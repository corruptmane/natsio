from typing import Any
from natsio.abc.json import JSONSerializerProto
import orjson


class ORJSONSerializer(JSONSerializerProto):
    def load(self, obj: str | bytes) -> Any:
        return orjson.loads(obj)

    def dump(self, obj: Any) -> bytes:
        return orjson.dumps(obj, option=orjson.OPT_SORT_KEYS)
