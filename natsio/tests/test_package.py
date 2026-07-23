import asyncio

import natsio


def test_version_is_exposed() -> None:
    assert natsio.__version__ == "1.0.0"


async def test_pytest_asyncio_auto_mode() -> None:
    await asyncio.sleep(0)
