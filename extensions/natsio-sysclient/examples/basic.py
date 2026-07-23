"""natsio-sysclient: the `$SYS` monitoring API client.

`SysClient` speaks the server's `$SYS.REQ.SERVER.*` monitoring endpoints
(VARZ, CONNZ, JSZ, HEALTHZ, STATSZ, SUBSZ) over a normal connection bound to
the system account — by server id, or as a cluster ping that gathers every
server that answers. This example self-starts a server with a `$SYS` account
so it runs with no external setup.

Run it (needs `natsio-testing` for the self-start; a nats-server binary in
`tools/.bin/` or on PATH):

    python extensions/natsio-sysclient/examples/basic.py
"""

import asyncio

from natsio.sysclient import ConnzOptions, SysClient  # ty: ignore[unresolved-import]
from natsio.testing import (  # ty: ignore[unresolved-import]  # dev-only, for the self-start
    NatsServerProcess,
    find_server_binary,
)

import natsio

SYS_CONFIG = """
accounts {
  $SYS { users: [{user: sys, password: pw}] }
  APP  { users: [{user: app, password: pw}], jetstream: enabled }
}
"""


async def main() -> None:
    binary = find_server_binary()
    if binary is None:
        raise SystemExit("nats-server not found (put one in tools/.bin/ or on PATH)")

    server = await NatsServerProcess(binary, config=SYS_CONFIG, jetstream=True).start()
    try:
        async with await natsio.connect(server.url, user="sys", password="pw") as nc:
            sysc = SysClient(nc)

            # Cluster ping: every server that answers.
            for varz in await sysc.varz_ping():
                data = varz.data
                print(f"server {varz.server.id}: nats {data.version}, {data.connections} connections")

            server_id = (await sysc.varz_ping())[0].server.id

            # By server id: a health check and a stats snapshot.
            health = await sysc.healthz(server_id)
            print("healthz:", health.data.status)
            stats = await sysc.statsz(server_id)  # payload lives under .statsz
            print(f"statsz: sent={stats.statsz.sent.msgs} received={stats.statsz.received.msgs}")

            # Paged endpoint: walk connections (there's just our own here).
            print("connections:")
            async for page in sysc.all_connz(server_id, ConnzOptions(limit=64)):
                for conn in page.data.connections:
                    print(f"  cid={conn.cid} {conn.name or '(unnamed)'} rtt={conn.rtt}")
    finally:
        await server.stop()


if __name__ == "__main__":
    asyncio.run(main())
