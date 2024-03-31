from typing import Awaitable, Callable

ErrorCallback = Callable[[Exception], Awaitable[None]]
