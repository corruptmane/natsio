"""End-to-end tests against a real nats-server (pinned 2.14.3) with `$SYS` + `-js`.

Single server, so a cluster ping must return exactly one response and its
`server.id` must equal the id the by-id shape was asked about — that pair of
assertions is what proves both request shapes hit the same server.
"""

import asyncio
import time

import pytest
from natsio.sysclient import (  # ty: ignore[unresolved-import]
    ConnzOptions,
    HealthzErrorType,
    HealthzOptions,
    InvalidServerIDError,
    JszOptions,
    NoResponsesError,
    SortOpt,
    SubszOptions,
    SysAPIError,
    SysClient,
    SysClientOptions,
    SysValidationError,
    VarzOptions,
)

import natsio
from conftest import APP_PASSWORD, APP_USER, NatsServerProcess  # ty: ignore[unresolved-import]
from natsio.errors import ConnectionClosedError
from natsio.jetstream import StreamConfig
from natsio.message import Msg


@pytest.fixture
def sys_client(nc: natsio.Client) -> SysClient:
    return SysClient(nc, options=SysClientOptions(timeout=5.0, stall=1.0))


@pytest.fixture
async def server_id(sys_client: SysClient) -> str:
    responses = await sys_client.varz_ping()
    return responses[0].server.id


@pytest.fixture
async def app_streams(app_nc: natsio.Client) -> None:
    """Two streams in the `APP` account, so JSZ has assets to report."""
    js = app_nc.jetstream()
    await js.create_stream(StreamConfig(name="ORDERS", subjects=["orders.>"]))
    await js.create_stream(StreamConfig(name="EVENTS", subjects=["events.>"]))


async def open_connections(server: NatsServerProcess, count: int) -> list[natsio.Client]:
    """Extra `APP` connections, each with a live subscription so CONNZ/SUBSZ
    have something non-trivial to report."""
    clients = []
    for index in range(count):
        client = await natsio.connect(
            server.url, user=APP_USER, password=APP_PASSWORD, name=f"probe-{index}", connect_timeout=5.0
        )
        client.subscribe(f"probe.{index}")
        await client.flush()
        clients.append(client)
    return clients


class TestVarz:
    async def test_ping_returns_exactly_one_server(self, sys_client: SysClient) -> None:
        responses = await sys_client.varz_ping()
        assert len(responses) == 1
        assert responses[0].server.id
        assert responses[0].data.version.startswith("2.")

    async def test_by_id_matches_the_pinged_server(self, sys_client: SysClient, server_id: str) -> None:
        response = await sys_client.varz(server_id)
        assert response.server.id == server_id
        assert response.data.server_id == server_id
        assert response.error is None

    async def test_durations_and_timestamps_decode(self, sys_client: SysClient, server_id: str) -> None:
        varz = (await sys_client.varz(server_id)).data
        assert varz.ping_interval is not None and varz.ping_interval.total_seconds() > 0
        assert varz.write_deadline is not None and varz.write_deadline.total_seconds() > 0
        assert varz.start is not None and varz.start.tzinfo is not None
        assert varz.now is not None and varz.now >= varz.start
        # The server formats uptime itself; it is a string, not a duration.
        assert isinstance(varz.uptime, str)

    async def test_jetstream_block_populated(self, sys_client: SysClient, server_id: str) -> None:
        jetstream = (await sys_client.varz(server_id)).data.jetstream
        assert jetstream.config is not None
        assert jetstream.config.max_storage > 0
        assert jetstream.stats is not None

    async def test_unknown_server_fields_survive(self, sys_client: SysClient, server_id: str) -> None:
        # 2.14 sends `feature_flags` on the server envelope; the oracle's
        # ServerInfo predates it, so it has to land in `extra`.
        response = await sys_client.varz(server_id)
        assert "feature_flags" in response.server.extra

    async def test_server_count_short_circuits(self, sys_client: SysClient) -> None:
        responses = await sys_client.varz_ping(server_count=1, timeout=5.0)
        assert len(responses) == 1

    async def test_filter_by_name_matches(self, sys_client: SysClient, server_id: str) -> None:
        name = (await sys_client.varz(server_id)).data.server_name
        responses = await sys_client.varz_ping(VarzOptions(server_name=name))
        assert [r.server.id for r in responses] == [server_id]

    async def test_filter_by_wrong_name_yields_no_responses(self, sys_client: SysClient) -> None:
        with pytest.raises(NoResponsesError):
            await sys_client.varz_ping(VarzOptions(server_name="not-this-server"), timeout=1.0)


class TestStatsz:
    async def test_by_id_and_ping(self, sys_client: SysClient, server_id: str) -> None:
        response = await sys_client.statsz(server_id)
        assert response.server.id == server_id
        # STATSZ is the one envelope whose payload is under `statsz`, not `data`.
        assert response.statsz.connections >= 1
        assert response.statsz.start is not None
        assert response.statsz.received.msgs >= 0

        pinged = await sys_client.statsz_ping()
        assert len(pinged) == 1
        assert pinged[0].statsz.active_accounts >= 1

    async def test_jetstream_stats_present(self, sys_client: SysClient, server_id: str) -> None:
        statsz = (await sys_client.statsz(server_id)).statsz
        assert statsz.jetstream is not None
        assert statsz.jetstream.stats is not None


class TestHealthz:
    async def test_healthy_server(self, sys_client: SysClient, server_id: str) -> None:
        response = await sys_client.healthz(server_id)
        assert response.data.status == "ok"
        assert response.data.status_code in (None, 200)

    async def test_ping(self, sys_client: SysClient) -> None:
        responses = await sys_client.healthz_ping(HealthzOptions(js_enabled_only=True))
        assert len(responses) == 1
        assert responses[0].data.status == "ok"

    async def test_failed_check_is_data_not_an_exception(self, sys_client: SysClient, server_id: str) -> None:
        # A stream health check without an account is a 400 *inside* the data
        # block — the request itself succeeded, so nothing raises.
        response = await sys_client.healthz(server_id, HealthzOptions(stream="nope", details=True))
        assert response.data.status == "error"
        assert response.data.status_code == 400
        errors = response.data.errors or []
        assert [error.type for error in errors] == [HealthzErrorType.BAD_REQUEST]
        assert errors[0].error

    async def test_detailed_errors_name_the_missing_asset(
        self, sys_client: SysClient, server_id: str, app_streams: None
    ) -> None:
        response = await sys_client.healthz(server_id, HealthzOptions(account="APP", stream="NOPE", details=True))
        assert response.data.status_code == 404
        errors = response.data.errors or []
        assert errors[0].type == HealthzErrorType.STREAM
        assert errors[0].account == "APP"
        assert errors[0].stream == "NOPE"


class TestConnz:
    async def test_counts_real_connections(
        self, sys_client: SysClient, server_id: str, server: NatsServerProcess
    ) -> None:
        clients = await open_connections(server, 3)
        try:
            response = await sys_client.connz(server_id, ConnzOptions(auth=True, subscriptions=True))
            assert response.data.total >= 4  # 3 probes + the system connection
            names = {conn.name for conn in response.data.connections}
            assert {"probe-0", "probe-1", "probe-2"} <= names
            probe = next(conn for conn in response.data.connections if conn.name == "probe-0")
            assert probe.account == "APP"
            assert probe.authorized_user == APP_USER
            assert probe.subscriptions_list is not None
            assert "probe.0" in probe.subscriptions_list
        finally:
            await asyncio.gather(*(client.close() for client in clients))

    async def test_subscription_detail(self, sys_client: SysClient, server_id: str, server: NatsServerProcess) -> None:
        clients = await open_connections(server, 1)
        try:
            response = await sys_client.connz(server_id, ConnzOptions(subscriptions_detail=True, sort=SortOpt.SUBS))
            probe = next(conn for conn in response.data.connections if conn.name == "probe-0")
            assert probe.subscriptions_list_detail is not None
            assert [detail.subject for detail in probe.subscriptions_list_detail] == ["probe.0"]
        finally:
            await asyncio.gather(*(client.close() for client in clients))

    async def test_ping(self, sys_client: SysClient, server_id: str) -> None:
        responses = await sys_client.connz_ping()
        assert len(responses) == 1
        assert responses[0].server.id == server_id

    async def test_paged_iteration_covers_every_connection(
        self, sys_client: SysClient, server_id: str, server: NatsServerProcess
    ) -> None:
        clients = await open_connections(server, 4)
        try:
            first = await sys_client.connz(server_id, ConnzOptions(limit=1))
            total = first.data.total
            assert total >= 5

            seen: list[int] = []
            pages = 0
            async for page in sys_client.all_connz(server_id, ConnzOptions(limit=2)):
                pages += 1
                seen.extend(conn.cid for conn in page.data.connections)
            assert pages >= 3
            assert len(seen) >= total
            assert len(set(seen)) == len(seen)  # no page overlap
        finally:
            await asyncio.gather(*(client.close() for client in clients))

    async def test_paged_ping_returns_one_pager_per_server(
        self, sys_client: SysClient, server_id: str, server: NatsServerProcess
    ) -> None:
        clients = await open_connections(server, 2)
        try:
            pagers = await sys_client.all_connz_ping(ConnzOptions(limit=1))
            assert len(pagers) == 1
            seen = [conn.cid for pager in pagers async for page in pager for conn in page.data.connections]
            assert len(seen) == len(set(seen))
            assert len(seen) >= 3
        finally:
            await asyncio.gather(*(client.close() for client in clients))


class TestSubsz:
    async def test_stats_without_the_list(self, sys_client: SysClient, server_id: str) -> None:
        response = await sys_client.subsz(server_id)
        assert response.data.num_subscriptions > 0
        assert response.data.subscriptions_list is None

    async def test_list_and_paging(self, sys_client: SysClient, server_id: str) -> None:
        first = await sys_client.subsz(server_id, SubszOptions(subscriptions=True, limit=1))
        assert first.data.total > 2
        assert first.data.subscriptions_list is not None
        assert len(first.data.subscriptions_list) == 1

        seen: list[tuple[int, str]] = []
        async for page in sys_client.all_subsz(server_id, SubszOptions(subscriptions=True, limit=5)):
            listed = page.data.subscriptions_list or []
            seen.extend((detail.cid, detail.sid) for detail in listed)
        assert len(seen) >= first.data.total

    async def test_test_subject_filter(self, sys_client: SysClient, server_id: str) -> None:
        response = await sys_client.subsz(server_id, SubszOptions(subscriptions=True, test="$SYS.REQ.SERVER.PING.VARZ"))
        listed = response.data.subscriptions_list or []
        assert any(detail.subject.startswith("$SYS.REQ.SERVER") for detail in listed)

    async def test_ping(self, sys_client: SysClient, server_id: str) -> None:
        responses = await sys_client.subsz_ping(SubszOptions(subscriptions=True, limit=2))
        assert len(responses) == 1
        assert responses[0].server.id == server_id

    async def test_paged_ping(self, sys_client: SysClient) -> None:
        pagers = await sys_client.all_subsz_ping(SubszOptions(subscriptions=True, limit=10))
        assert len(pagers) == 1
        total = 0
        async for page in pagers[0]:
            total += len(page.data.subscriptions_list or [])
        assert total > 10


class TestJsz:
    async def test_server_totals(self, sys_client: SysClient, server_id: str, app_streams: None) -> None:
        response = await sys_client.jsz(server_id)
        assert response.server.id == server_id
        assert response.data.disabled is None
        assert response.data.streams == 2
        assert response.data.config.max_storage > 0

    async def test_account_and_stream_detail(self, sys_client: SysClient, server_id: str, app_streams: None) -> None:
        response = await sys_client.jsz(server_id, JszOptions(accounts=True, streams=True, config=True))
        assert response.data.accounts == 1
        details = response.data.account_details or []
        assert [account.name for account in details] == ["APP"]
        streams = details[0].stream_detail or []
        assert sorted(stream.name for stream in streams) == ["EVENTS", "ORDERS"]
        orders = next(stream for stream in streams if stream.name == "ORDERS")
        assert orders.config is not None and orders.config.subjects == ["orders.>"]
        assert orders.state is not None and orders.state.messages == 0
        assert orders.cluster is not None and orders.cluster.leader == server_id

    async def test_ping(self, sys_client: SysClient, server_id: str, app_streams: None) -> None:
        responses = await sys_client.jsz_ping(JszOptions(accounts=True))
        assert len(responses) == 1
        assert responses[0].server.id == server_id
        assert responses[0].data.accounts == 1

    async def test_paging_is_a_single_page_without_accounts(
        self, sys_client: SysClient, server_id: str, app_streams: None
    ) -> None:
        pages = [page async for page in sys_client.all_jsz(server_id)]
        assert len(pages) == 1

    async def test_paging_walks_accounts(self, sys_client: SysClient, server_id: str, app_streams: None) -> None:
        pages = [page async for page in sys_client.all_jsz(server_id, JszOptions(accounts=True, limit=1))]
        seen = [account.name for page in pages for account in (page.data.account_details or [])]
        assert seen == ["APP"]

    async def test_account_filter_is_a_single_page(
        self, sys_client: SysClient, server_id: str, app_streams: None
    ) -> None:
        # The server ignores `offset` under `account=` while still reporting
        # the unfiltered `accounts` count, so walking that total would re-yield
        # the same account once per JS-enabled account.
        pages = [page async for page in sys_client.all_jsz(server_id, JszOptions(accounts=True, account="APP"))]
        assert len(pages) == 1
        assert [account.name for account in (pages[0].data.account_details or [])] == ["APP"]

    async def test_paged_ping(self, sys_client: SysClient, app_streams: None) -> None:
        pagers = await sys_client.all_jsz_ping(JszOptions(accounts=True, limit=1))
        assert len(pagers) == 1
        seen = [account.name async for page in pagers[0] for account in (page.data.account_details or [])]
        assert seen == ["APP"]


class TestPingBounds:
    """The gather knobs, exercised against a *fake* server.

    `$SYS.REQ.SERVER.PING.VARZ` is only special inside the system account; in
    the regular `APP` account it is an ordinary subject, so a plain subscriber
    there is a stand-in for a slow (or mute) cluster member.
    """

    async def test_slow_first_reply_is_not_cut_off_by_the_stall(self, server: NatsServerProcess) -> None:
        # Regression: `stall` used to bound the wait for the FIRST reply too,
        # so the 300 ms default turned any cluster slower than that into
        # NoResponsesError — zero results, not degraded results. `stall` now
        # bounds only the gap BETWEEN replies (nats.go natsext parity).
        responder = await natsio.connect(server.url, user=APP_USER, password=APP_PASSWORD, connect_timeout=5.0)
        caller = await natsio.connect(server.url, user=APP_USER, password=APP_PASSWORD, connect_timeout=5.0)
        try:
            reply_after = 0.8
            default_stall = SysClientOptions().stall
            assert default_stall is not None and reply_after > default_stall

            async def slow(msg: Msg) -> None:
                await asyncio.sleep(reply_after)
                if msg.reply:
                    await responder.publish(msg.reply, b'{"server":{"id":"SLOWSRV"},"data":{"server_id":"SLOWSRV"}}')

            responder.subscribe("$SYS.REQ.SERVER.PING.VARZ", cb=slow)
            await responder.flush()

            started = time.monotonic()
            # Shipped defaults on purpose: this is the default that used to fail.
            responses = await SysClient(caller).varz_ping()
            elapsed = time.monotonic() - started
            assert [response.server.id for response in responses] == ["SLOWSRV"]
            assert elapsed >= reply_after
        finally:
            await asyncio.gather(responder.close(), caller.close())

    async def test_close_mid_ping_terminates_promptly(self, server: NatsServerProcess) -> None:
        # Termination discipline: a ping parked on a mute responder must be
        # woken by the close, not hang until the timeout.
        #
        # The error *type* is the honest weak spot: `Client.close()` closes the
        # request sinks before it advances the connection state, so by the time
        # `_ping` looks the connection still reads CONNECTED and the closure is
        # indistinguishable from "no responders". Asserted as either error, and
        # reported as core friction rather than papered over with a sleep.
        responder = await natsio.connect(server.url, user=APP_USER, password=APP_PASSWORD, connect_timeout=5.0)
        caller = await natsio.connect(server.url, user=APP_USER, password=APP_PASSWORD, connect_timeout=5.0)
        try:
            arrived = asyncio.Event()

            async def never_reply(msg: Msg) -> None:
                arrived.set()

            responder.subscribe("$SYS.REQ.SERVER.PING.VARZ", cb=never_reply)
            await responder.flush()

            client = SysClient(caller, options=SysClientOptions(timeout=30.0))
            task = asyncio.create_task(client.varz_ping())
            await arrived.wait()
            started = time.monotonic()
            await caller.close()
            with pytest.raises((ConnectionClosedError, NoResponsesError)):
                await task
            assert time.monotonic() - started < 5.0  # not the 30 s deadline
        finally:
            await responder.close()

    async def test_non_positive_timeout_override_never_reaches_the_wire(self, sys_client: SysClient) -> None:
        # Regression: `SysClientOptions(timeout=0)` was rejected but the
        # per-call override was not, so a negative deadline was published.
        with pytest.raises(SysValidationError, match="timeout has to be greater than 0"):
            await sys_client.varz_ping(timeout=-1.0)

    async def test_no_responses_names_the_bound_that_fired(self, sys_client: SysClient) -> None:
        with pytest.raises(NoResponsesError, match="the overall timeout elapsed"):
            await sys_client.varz_ping(VarzOptions(server_name="not-this-server"), timeout=0.5)


class TestErrors:
    async def test_unknown_server_id(self, sys_client: SysClient) -> None:
        with pytest.raises(InvalidServerIDError, match="unknown server id"):
            await sys_client.varz("NBOGUSSERVERIDTHATDOESNOTEXIST", timeout=2.0)

    async def test_empty_server_id_never_reaches_the_wire(self, sys_client: SysClient) -> None:
        with pytest.raises(SysValidationError):
            await sys_client.varz("")

    async def test_ping_is_not_a_server_id(self, sys_client: SysClient) -> None:
        # Regression: `varz("PING")` composed the cluster-ping subject and
        # returned whichever server happened to answer first.
        with pytest.raises(SysValidationError, match="cluster-ping target"):
            await sys_client.varz("PING")

    async def test_by_id_timeout_override_is_validated(self, sys_client: SysClient, server_id: str) -> None:
        with pytest.raises(SysValidationError, match="timeout has to be greater than 0"):
            await sys_client.varz(server_id, timeout=-1.0)

    async def test_server_side_error_envelope(self, sys_client: SysClient, server_id: str) -> None:
        # A wildcard `test` subject is rejected by the server with an `error`
        # envelope (code 500) instead of a data block.
        with pytest.raises(SysAPIError) as excinfo:
            await sys_client.subsz(server_id, SubszOptions(test="foo.*"))
        assert excinfo.value.code == 500
        assert "test subject" in excinfo.value.api_description

    async def test_ping_error_envelope_also_raises(self, sys_client: SysClient) -> None:
        with pytest.raises(SysAPIError):
            await sys_client.subsz_ping(SubszOptions(test="foo.*"))

    async def test_no_responses_outside_the_system_account(self, app_nc: natsio.Client) -> None:
        # The APP account cannot see `$SYS`: the ping collects nothing, and an
        # empty list would be indistinguishable from a zero-server cluster.
        outsider = SysClient(app_nc, options=SysClientOptions(timeout=1.0))
        with pytest.raises(NoResponsesError, match="no server answered"):
            await outsider.varz_ping()

    async def test_by_id_outside_the_system_account(self, app_nc: natsio.Client, server_id: str) -> None:
        outsider = SysClient(app_nc, options=SysClientOptions(timeout=2.0))
        with pytest.raises(InvalidServerIDError):
            await outsider.varz(server_id)
