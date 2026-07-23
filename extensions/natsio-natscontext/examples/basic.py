"""natsio-natscontext: connect using `nats` CLI context files (ADR-21).

The `nats` CLI stores named connection contexts as
`$XDG_CONFIG_HOME/nats/context/<name>.json`. This extension reads them and
connects with their settings, so your app shares one source of connection
truth with the CLI. To stay self-contained, this example writes a throwaway
context into a temp XDG dir instead of touching your real `~/.config/nats`.

Run it (needs a server: `just server`):

    python extensions/natsio-natscontext/examples/basic.py
"""

import asyncio
import json
import os
import tempfile
from pathlib import Path


async def main() -> None:
    url = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")

    with tempfile.TemporaryDirectory() as tmp:
        # Lay out a fake `nats` config dir and point XDG_CONFIG_HOME at it,
        # BEFORE importing natscontext (it reads the env when it looks things up).
        ctx_dir = Path(tmp) / "nats" / "context"
        ctx_dir.mkdir(parents=True)
        (ctx_dir / "staging.json").write_text(json.dumps({"url": url, "description": "demo"}))
        os.environ["XDG_CONFIG_HOME"] = tmp

        import natsio.natscontext as natscontext  # ty: ignore[unresolved-import]  # imported after XDG is set

        print("known contexts:", natscontext.list_contexts())

        # Connect using the named context; overrides win over the context file.
        async with await natscontext.connect("staging", name="demo-app") as nc:
            sub = await nc.subscribe("greet")
            await nc.publish("greet", b"hello from a context")
            async for msg in sub:
                print("received:", msg.data.decode())
                break


if __name__ == "__main__":
    asyncio.run(main())
