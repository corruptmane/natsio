"""Hostile INFO input fails through the typed connection teardown path."""

import pytest

from fake import FakeEnv
from natsio._internal.connection import Connection
from natsio.errors import NoServersAvailableError, ParserError
from natsio.options import ConnectOptions


@pytest.mark.parametrize(
    "raw",
    [
        b"[]",
        b"null",
        b"1",
        b"{",
        b'{"max_payload":{}}',
        b'{"max_payload":-1}',
        b'{"max_payload":true}',
        b'{"tls_required":"no"}',
        b'{"connect_urls":"host:4222"}',
    ],
)
@pytest.mark.parametrize("established", [False, True])
async def test_malformed_info_is_typed_connection_failure(raw, established, monkeypatch) -> None:
    env = FakeEnv()
    conn = Connection(ConnectOptions(allow_reconnect=False), transport_factory=env.factory)
    try:
        if established:
            await conn.connect()
            session = conn._session
            assert session is not None
            env.current.deliver(b"INFO " + raw + b"\r\n")
            assert session.lost_future.done()
            assert isinstance(session.lost_future.result(), ParserError)
        else:
            original = env.factory

            def factory(**kwargs):
                transport = original(**kwargs)

                def deliver_info(info):
                    transport.deliver(b"INFO " + raw + b"\r\n")

                monkeypatch.setattr(transport, "deliver_info", deliver_info)
                return transport

            conn._transport_factory = factory
            with pytest.raises(NoServersAvailableError) as error:
                await conn.connect()
            assert isinstance(error.value.__cause__, ParserError)
    finally:
        await conn.close(flush=False)
