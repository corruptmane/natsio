"""natsio — zero-dependency asyncio NATS client for modern Python."""

from importlib.metadata import version as _version

__version__ = _version("natsio")

__all__ = ["__version__"]
