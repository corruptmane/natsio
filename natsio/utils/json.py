from typing import Any, Union

try:
    import orjson
except ImportError:
    import json

    def json_loads(obj: Union[str, bytes]) -> Any:
        return json.loads(obj)

    def json_dumps(obj: Any) -> bytes:
        return json.dumps(obj, sort_keys=True).encode()

else:

    def json_loads(obj: Union[str, bytes]) -> Any:
        return orjson.loads(obj)

    def json_dumps(obj: Any) -> bytes:
        return orjson.dumps(obj, option=orjson.OPT_SORT_KEYS)
