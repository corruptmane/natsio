# natsio-testing

Run real `nats-server` processes from your test suite: start/stop, temporary
config files, free-port allocation, readiness probing (plaintext and
TLS-handshake-first), and fault injection (`kill()`).

```python
from natsio.testing import NatsServerProcess

async def test_against_a_real_server():
    server = await NatsServerProcess(binary="nats-server").start()
    try:
        nc = await natsio.connect(server.url)
        ...
    finally:
        await server.stop()
```

Binary discovery: pass `binary=` explicitly, or use `find_server_binary()`
(checks `NATS_SERVER_BIN`, then `nats-server` on `PATH`).

Part of the natsio extension tier: distribution `natsio-testing`, imported as
`natsio.testing`. Pre-1.0, no API-stability promises.
