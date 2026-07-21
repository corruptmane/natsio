"""Client adapters and the registry that names them.

Each adapter wraps one NATS client behind the :class:`Adapter` contract, driven
idiomatically. ``ADAPTERS`` maps the CLI ``--clients`` name to its class.
"""

from natsio_bench.adapters.base import Adapter, BenchSub, Capability, MsgCallback
from natsio_bench.adapters.natscore_adapter import NatsCoreAdapter
from natsio_bench.adapters.natsio_adapter import NatsioAdapter
from natsio_bench.adapters.natspy_adapter import NatsPyAdapter

__all__ = [
    "ADAPTERS",
    "Adapter",
    "BenchSub",
    "Capability",
    "MsgCallback",
    "NatsCoreAdapter",
    "NatsPyAdapter",
    "NatsioAdapter",
]

# Insertion order is display order: the client under test first, then the two
# references it is measured against.
ADAPTERS: dict[str, type[Adapter]] = {
    NatsioAdapter.name: NatsioAdapter,
    NatsPyAdapter.name: NatsPyAdapter,
    NatsCoreAdapter.name: NatsCoreAdapter,
}
