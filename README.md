# natsio – Modern Python client for NATS

---

## Basic usage

```python
import asyncio

from natsio.client import NATSCore, ClientConfig
from natsio.messages import CoreMsg


async def callback(msg: CoreMsg) -> None:
    text = f"Received message from {msg.subject}: {msg.payload}"
    if msg.headers:
        text += f" with headers: {msg.headers}"
    print(text)
    if msg.reply_to:
        await msg.reply(b"OK")


async def main() -> None:
    config = ClientConfig(servers=["nats://localhost:4222"])

    async with NATSCore(config) as client:
        sub = await client.subscribe("foo.>", callback=callback)
        await sub.unsubscribe(max_msgs=3) # Automatically unsubscribe after 3 messages

        await client.publish("foo.bar", b"Hello, World!", reply_to="foo.bar.reply")
        await client.publish("foo.baz", b"Hello, World!", headers={"x": "y"})

        await asyncio.Future()


def run() -> None:
    asyncio.run(main())


if __name__ == "__main__":
    run()
```

---

## Installation

```bash
pip install natsio
```
