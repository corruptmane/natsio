from typing import Protocol

from .entities import GetMsgRequest, RawMsg


class RawMsgGetter(Protocol):
    async def __call__(self, stream_name: str, req: GetMsgRequest, timeout: int | float | None = None) -> RawMsg:
        raise NotImplementedError
