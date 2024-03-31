from typing import Awaitable, Callable, TypeAlias


ErrorCallback: TypeAlias = Callable[[Exception], Awaitable[None]]
