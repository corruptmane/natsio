from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from .client import JetStream


class KeyValue:
    def __init__(
        self,
        bucket_name: str,
        stream_name: str,
        pre: str,
        jetstream: "JetStream",
        is_direct: bool,
    ) -> None:
        self._bucket_name = bucket_name
        self._stream_name = stream_name
        self._pre = pre
        self._js = jetstream
        self._is_direct = is_direct
