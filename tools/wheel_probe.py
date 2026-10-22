"""Run under isolated Python from a clean wheel environment, never editable."""

import importlib
import importlib.metadata
import importlib.util
import sys
from pathlib import Path

import natsio
from natsio._internal.auth import nkeys


def main() -> None:
    mode = sys.argv[1]
    assert "site-packages" in str(natsio.__file__)
    assert natsio.__version__ == importlib.metadata.version("natsio")
    assert not importlib.metadata.requires("natsio") or all(
        "extra ==" in entry for entry in importlib.metadata.requires("natsio") or []
    )
    for name in ("jetstream", "kv", "objectstore", "micro"):
        importlib.import_module(f"natsio.{name}")
    if mode == "namespace":
        for name in (
            "testing",
            "otel",
            "kvcodec",
            "counters",
            "natscontext",
            "schedules",
            "jetstream_batch",
            "sysclient",
            "pcgroups",
        ):
            module = importlib.import_module(f"natsio.{name}")
            assert module.__file__ is not None and "site-packages" in module.__file__
            assert Path(module.__file__).with_name("py.typed").is_file()
    elif mode != "core":
        pair = nkeys.from_seed(nkeys.encode_seed(nkeys.Role.USER, bytes(range(32))))
        assert len(pair.sign(b"wheel-install-probe")) == 64
        absent = "cryptography" if mode == "nkeys" else "nacl"
        assert importlib.util.find_spec(absent) is None
    print(f"wheel probe passed: {mode}")


if __name__ == "__main__":
    main()
