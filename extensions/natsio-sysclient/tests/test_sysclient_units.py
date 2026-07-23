"""Local-logic tests: subject composition, request encoding, envelope decoding,
and the paging walk (driven by a stub connection, no server involved)."""

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

import pytest
from natsio.sysclient import (  # ty: ignore[unresolved-import]
    PING_TARGET,
    SYS_REQ_SERVER_PREFIX,
    ConnState,
    ConnzOptions,
    ConnzResponse,
    Endpoint,
    HealthzOptions,
    InvalidResponseError,
    JszOptions,
    NoResponsesError,
    PagerStateError,
    SortOpt,
    SubszOptions,
    SysAPIError,
    SysClient,
    SysClientOptions,
    SysValidationError,
    VarzOptions,
    VarzResponse,
)
from natsio.sysclient.client import _decode, _encode, _validate_server_id  # ty: ignore[unresolved-import]

from natsio import ConnectionState
from natsio.errors import ConnectionClosedError
from natsio.message import Msg

# One real 2.14.3 VARZ envelope, trimmed to the fields the assertions touch.
# `feature_flags` and `config_digest` are 2.14 additions the oracle's structs do
# not name — they must survive in `extra`.
VARZ_ENVELOPE: dict[str, Any] = {
    "server": {
        "name": "NBYG",
        "host": "127.0.0.1",
        "id": "NBYG",
        "ver": "2.14.3",
        "feature_flags": {"js_ack_fc_v2": False},
        "jetstream": True,
        "seq": 13,
        "time": "2026-07-22T20:59:35.690975Z",
    },
    "data": {
        "server_id": "NBYG",
        "server_name": "NBYG",
        "version": "2.14.3",
        "go": "go1.26.4",
        "host": "127.0.0.1",
        "port": 4222,
        "ping_interval": 120000000000,
        "max_payload": 1048576,
        "jetstream": {"config": {"max_memory": 1, "max_storage": 2, "sync_interval": 120000000000}},
        "write_deadline": 10000000000,
        "start": "2026-07-22T20:59:35.642852Z",
        "uptime": "0s",
        "connections": 1,
        "subscriptions": 69,
        "config_digest": "sha256:24e6",
        "slow_consumer_stats": {"clients": 0, "routes": 0, "gateways": 0, "leafs": 0},
    },
}


class TestSubjects:
    def test_by_id_matches_oracle_template(self) -> None:
        # orbit.go api.go: "$SYS.REQ.SERVER.%s.VARZ" etc.
        for endpoint in Endpoint:
            assert f"{SYS_REQ_SERVER_PREFIX}.SRVID.{endpoint}" == f"$SYS.REQ.SERVER.SRVID.{endpoint.value}"
        assert f"{SYS_REQ_SERVER_PREFIX}.SRVID.{Endpoint.VARZ}" == "$SYS.REQ.SERVER.SRVID.VARZ"
        assert f"{SYS_REQ_SERVER_PREFIX}.SRVID.{Endpoint.STATSZ}" == "$SYS.REQ.SERVER.SRVID.STATSZ"
        assert f"{SYS_REQ_SERVER_PREFIX}.SRVID.{Endpoint.CONNZ}" == "$SYS.REQ.SERVER.SRVID.CONNZ"
        assert f"{SYS_REQ_SERVER_PREFIX}.SRVID.{Endpoint.SUBSZ}" == "$SYS.REQ.SERVER.SRVID.SUBSZ"
        assert f"{SYS_REQ_SERVER_PREFIX}.SRVID.{Endpoint.HEALTHZ}" == "$SYS.REQ.SERVER.SRVID.HEALTHZ"
        assert f"{SYS_REQ_SERVER_PREFIX}.SRVID.{Endpoint.JSZ}" == "$SYS.REQ.SERVER.SRVID.JSZ"

    def test_ping_target(self) -> None:
        assert PING_TARGET == "PING"
        assert f"{SYS_REQ_SERVER_PREFIX}.{PING_TARGET}.{Endpoint.JSZ}" == "$SYS.REQ.SERVER.PING.JSZ"


class TestServerIDValidation:
    def test_empty_rejected(self) -> None:
        with pytest.raises(SysValidationError, match="cannot be empty"):
            _validate_server_id("")

    @pytest.mark.parametrize("bad", ["a.b", "*", "id>", "id with space", "id\n"])
    def test_subject_injection_rejected(self, bad: str) -> None:
        with pytest.raises(SysValidationError, match="separator or wildcard"):
            _validate_server_id(bad)

    def test_nuid_shaped_id_accepted(self) -> None:
        _validate_server_id("NBYGMRHKIZ5OUATFV4YRWRVYR2MCYPRHR57ZDNHUIHS7UOBFQJTUUCSG")

    def test_ping_target_rejected(self) -> None:
        # Regression: `varz("PING")` used to compose the cluster-ping subject
        # and quietly return whichever server replied first.
        with pytest.raises(SysValidationError, match="cluster-ping target"):
            _validate_server_id(PING_TARGET)


class TestRequestEncoding:
    def test_no_options_is_empty_object(self) -> None:
        assert _encode(None) == b"{}"

    def test_unset_filters_are_omitted(self) -> None:
        assert _encode(VarzOptions()) == b"{}"

    def test_event_filters(self) -> None:
        payload = json.loads(_encode(VarzOptions(server_name="n1", cluster="c1", tags=["a", "b"], domain="hub")))
        assert payload == {"server_name": "n1", "cluster": "c1", "tags": ["a", "b"], "domain": "hub"}

    def test_connz_wire_names(self) -> None:
        payload = json.loads(
            _encode(
                ConnzOptions(
                    sort=SortOpt.SUBS,
                    auth=True,
                    subscriptions=True,
                    subscriptions_detail=True,
                    offset=10,
                    limit=5,
                    cid=7,
                    state=ConnState.ALL,
                    user="alice",
                    acc="APP",
                    filter_subject="orders.>",
                    cluster="c1",
                )
            )
        )
        assert payload == {
            "sort": "subs",
            "auth": True,
            "subscriptions": True,
            "subscriptions_detail": True,
            "offset": 10,
            "limit": 5,
            "cid": 7,
            "state": 2,
            "user": "alice",
            "acc": "APP",
            "filter_subject": "orders.>",
            "cluster": "c1",
        }

    def test_healthz_hyphenated_keys(self) -> None:
        payload = json.loads(_encode(HealthzOptions(js_enabled_only=True, js_server_only=False, details=True)))
        assert payload == {"js-enabled-only": True, "js-server-only": False, "details": True}
        assert "js_enabled_only" not in payload

    def test_healthz_extra_only_hyphenated_key_survives(self) -> None:
        options = HealthzOptions(details=True)
        options.extra["js-enabled-only"] = True
        assert json.loads(_encode(options)) == {"details": True, "js-enabled-only": True}

    def test_healthz_double_spelling_is_rejected(self) -> None:
        # Regression: the rename ran *after* `extra` had been merged, so the
        # typed field silently overwrote a hand-set hyphenated key.
        options = HealthzOptions(js_server_only=True)
        options.extra["js-server-only"] = False
        with pytest.raises(SysValidationError, match="same wire key"):
            _encode(options)

    def test_jsz_and_subsz_wire_names(self) -> None:
        assert json.loads(_encode(JszOptions(accounts=True, consumer=True, raft=True, stream_leader_only=True))) == {
            "accounts": True,
            "consumer": True,
            "raft": True,
            "stream_leader_only": True,
        }
        assert json.loads(_encode(SubszOptions(subscriptions=True, test="orders.new", limit=2))) == {
            "subscriptions": True,
            "test": "orders.new",
            "limit": 2,
        }


class TestEnvelopeDecoding:
    def test_error_envelope_raises(self) -> None:
        raw = json.dumps({"server": {"id": "X"}, "error": {"code": 400, "description": "bad json"}}).encode()
        with pytest.raises(SysAPIError) as excinfo:
            _decode(raw, "$SYS.REQ.SERVER.PING.VARZ")
        assert excinfo.value.code == 400
        assert excinfo.value.api_description == "bad json"
        assert "400" in str(excinfo.value)

    def test_error_envelope_with_err_code(self) -> None:
        raw = json.dumps({"error": {"code": 500, "err_code": 10071, "description": "boom"}}).encode()
        with pytest.raises(SysAPIError) as excinfo:
            _decode(raw, "s")
        assert excinfo.value.err_code == 10071

    def test_non_json_payload(self) -> None:
        with pytest.raises(InvalidResponseError, match="not JSON"):
            _decode(b"not json at all", "s")

    def test_non_object_payload(self) -> None:
        with pytest.raises(InvalidResponseError, match="not a JSON object"):
            _decode(b"[1, 2, 3]", "s")

    def test_non_object_error_block(self) -> None:
        with pytest.raises(InvalidResponseError, match="non-object"):
            _decode(b'{"error": "oops"}', "s")

    @pytest.mark.parametrize("raw", [b'{"error": []}', b'{"error": 0}', b'{"error": ""}'])
    def test_falsy_non_object_error_block_is_not_data(self, raw: bytes) -> None:
        # Regression: `if error:` let every falsy `error` value through as if
        # it were a successful envelope.
        with pytest.raises(InvalidResponseError, match="non-object"):
            _decode(raw, "s")

    def test_empty_error_object_still_raises(self) -> None:
        with pytest.raises(SysAPIError) as excinfo:
            _decode(b'{"error": {}}', "s")
        assert excinfo.value.code == 0

    def test_non_integer_code_is_typed(self) -> None:
        # Regression: `int(error["code"])` let a raw ValueError out of the
        # module's hostile-input boundary.
        with pytest.raises(InvalidResponseError, match="non-integer error 'code'"):
            _decode(b'{"error": {"code": "boom"}}', "s")

    def test_non_integer_err_code_is_typed(self) -> None:
        # Regression: `err_code` is declared `int | None` but was never checked.
        with pytest.raises(InvalidResponseError, match="non-integer error 'err_code'"):
            _decode(b'{"error": {"code": 400, "err_code": "x"}}', "s")

    def test_boolean_code_is_not_an_integer(self) -> None:
        with pytest.raises(InvalidResponseError, match="non-integer error 'code'"):
            _decode(b'{"error": {"code": true}}', "s")

    def test_null_error_is_data(self) -> None:
        assert _decode(b'{"error": null, "data": {"ok": true}}', "s")["data"] == {"ok": True}

    def test_success_envelope_passes_through(self) -> None:
        assert _decode(json.dumps(VARZ_ENVELOPE).encode(), "s")["data"]["server_id"] == "NBYG"


class TestResponseModels:
    def test_varz_decodes(self) -> None:
        resp = VarzResponse.from_wire(VARZ_ENVELOPE)
        assert resp.server.id == "NBYG"
        assert resp.server.time is not None and resp.server.time.year == 2026
        assert resp.data.version == "2.14.3"
        assert resp.data.ping_interval is not None
        assert resp.data.ping_interval.total_seconds() == 120.0
        assert resp.data.jetstream.config is not None
        assert resp.data.jetstream.config.max_storage == 2
        assert resp.data.slow_consumer_stats is not None
        assert resp.data.slow_consumer_stats.clients == 0

    def test_unknown_server_fields_survive(self) -> None:
        resp = VarzResponse.from_wire(VARZ_ENVELOPE)
        # Neither key is in the oracle's structs; both must round-trip.
        assert resp.server.extra["feature_flags"] == {"js_ack_fc_v2": False}
        assert resp.data.extra["config_digest"] == "sha256:24e6"
        assert resp.to_wire()["data"]["config_digest"] == "sha256:24e6"
        assert resp.to_wire()["server"]["feature_flags"] == {"js_ack_fc_v2": False}


class TestClientOptions:
    @pytest.mark.parametrize(
        "kwargs",
        [{"timeout": 0}, {"timeout": -1.0}, {"stall": 0}, {"stall": -0.5}, {"server_count": 0}, {"server_count": -3}],
    )
    def test_rejects_non_positive(self, kwargs: dict[str, Any]) -> None:
        with pytest.raises(SysValidationError):
            SysClientOptions(**kwargs)

    def test_stall_may_be_disabled(self) -> None:
        assert SysClientOptions(stall=None).stall is None

    def test_defaults_match_oracle(self) -> None:
        options = SysClientOptions()
        assert options.timeout == 10.0
        assert options.stall == 0.3
        assert options.server_count is None


class _StubConnection:
    """Answers CONNZ requests from a canned connection list, honouring offset/limit."""

    status = ConnectionState.CONNECTED

    def __init__(self, total: int) -> None:
        self.total = total
        self.requests: list[dict[str, Any]] = []

    async def request(self, subject: str, payload: bytes, *, timeout: float | None = None) -> Msg:  # noqa: ASYNC109
        options = json.loads(payload)
        self.requests.append(options)
        offset = options.get("offset", 0)
        limit = options.get("limit", self.total)
        conns = [{"cid": cid} for cid in range(offset, min(offset + limit, self.total))]
        envelope = {
            "server": {"id": "STUB"},
            "data": {
                "server_id": "STUB",
                "num_connections": len(conns),
                "total": self.total,
                "offset": offset,
                "limit": limit,
                "connections": conns,
            },
        }
        return Msg(subject=subject, payload=json.dumps(envelope).encode())


class _JszStubConnection:
    """Answers JSZ the way nats-server 2.14.3 actually does.

    Two behaviours the pager has to respect, both measured live: `offset` is
    **ignored** when `account` is set, and `accounts` always reports the
    *unfiltered* number of JS-enabled accounts.
    """

    status = ConnectionState.CONNECTED

    def __init__(self, accounts: list[str]) -> None:
        self.accounts = accounts
        self.requests: list[dict[str, Any]] = []

    async def request(self, subject: str, payload: bytes, *, timeout: float | None = None) -> Msg:  # noqa: ASYNC109
        options = json.loads(payload)
        self.requests.append(options)
        named = options.get("account")
        if named is not None:
            selected = [name for name in self.accounts if name == named]
        else:
            offset = options.get("offset", 0)
            limit = options.get("limit", len(self.accounts))
            selected = self.accounts[offset : offset + limit]
        data: dict[str, Any] = {"server_id": "STUB", "accounts": len(self.accounts)}
        if options.get("accounts"):
            data["account_details"] = [{"name": name} for name in selected]
        envelope = {"server": {"id": "STUB"}, "data": data}
        return Msg(subject=subject, payload=json.dumps(envelope).encode())

    async def request_many(
        self,
        subject: str,
        payload: bytes,
        *,
        timeout: float | None = None,  # noqa: ASYNC109
        max_msgs: int | None = None,
        stall: float | None = None,
    ) -> AsyncIterator[Msg]:
        yield await self.request(subject, payload, timeout=timeout)


class _PingStubConnection:
    """Drives `_ping` without a server: yields canned replies, then stops."""

    def __init__(self, replies: list[bytes], *, status: ConnectionState = ConnectionState.CONNECTED) -> None:
        self.replies = replies
        self.status = status
        self.requests: list[tuple[str, dict[str, Any]]] = []

    async def request_many(
        self,
        subject: str,
        payload: bytes,
        *,
        timeout: float | None = None,  # noqa: ASYNC109
        max_msgs: int | None = None,
        stall: float | None = None,
    ) -> AsyncIterator[Msg]:
        self.requests.append((subject, json.loads(payload)))
        for reply in self.replies:
            yield Msg(subject=subject, payload=reply)


class TestPaging:
    async def test_walks_every_page(self) -> None:
        stub = _StubConnection(total=5)
        sys = SysClient(stub, options=SysClientOptions())
        pages = [page async for page in sys.all_connz("STUB", ConnzOptions(limit=2))]
        assert [[c.cid for c in page.data.connections] for page in pages] == [[0, 1], [2, 3], [4]]
        assert [r["offset"] for r in stub.requests] == [0, 2, 4]

    async def test_single_page_when_everything_fits(self) -> None:
        stub = _StubConnection(total=2)
        sys = SysClient(stub)
        pages = [page async for page in sys.all_connz("STUB", ConnzOptions(limit=10))]
        assert len(pages) == 1
        assert len(stub.requests) == 1

    async def test_empty_result_terminates(self) -> None:
        stub = _StubConnection(total=0)
        sys = SysClient(stub)
        pages = [page async for page in sys.all_connz("STUB")]
        assert len(pages) == 1
        assert pages[0].data.connections == []

    async def test_caller_options_are_not_mutated(self) -> None:
        stub = _StubConnection(total=5)
        sys = SysClient(stub)
        options = ConnzOptions(limit=2)
        _ = [page async for page in sys.all_connz("STUB", options)]
        assert options.offset is None

    async def test_starting_offset_is_honoured(self) -> None:
        stub = _StubConnection(total=5)
        sys = SysClient(stub)
        pages = [page async for page in sys.all_connz("STUB", ConnzOptions(limit=2, offset=3))]
        assert [[c.cid for c in page.data.connections] for page in pages] == [[3, 4]]

    async def test_await_is_a_no_op(self) -> None:
        stub = _StubConnection(total=1)
        sys = SysClient(stub)
        pager = await sys.all_connz("STUB")
        pages = [page async for page in pager]
        assert len(pages) == 1

    async def test_aclose_stops_early(self) -> None:
        stub = _StubConnection(total=100)
        sys = SysClient(stub)
        async with sys.all_connz("STUB", ConnzOptions(limit=1)) as pager:
            async for page in pager:
                assert isinstance(page, ConnzResponse)
                break
        assert len(stub.requests) == 1

    async def test_validation_happens_before_any_request(self) -> None:
        stub = _StubConnection(total=1)
        sys = SysClient(stub)
        with pytest.raises(SysValidationError):
            _ = [page async for page in sys.all_connz("bad.id")]
        assert stub.requests == []

    async def test_negative_offset_is_clamped(self) -> None:
        # Regression: a negative starting offset was counted up from as-is
        # while the server clamped it to 0, so the walk re-requested (and
        # re-yielded) the first rows.
        stub = _StubConnection(total=3)
        sys = SysClient(stub)
        pages = [page async for page in sys.all_connz("STUB", ConnzOptions(offset=-5, limit=2))]
        assert [r["offset"] for r in stub.requests] == [0, 2]
        seen = [conn.cid for page in pages for conn in page.data.connections]
        assert seen == [0, 1, 2]


class TestJszPaging:
    async def test_walks_accounts(self) -> None:
        stub = _JszStubConnection(["A", "B", "C"])
        sys = SysClient(stub)
        pages = [page async for page in sys.all_jsz("STUB", JszOptions(accounts=True, limit=2))]
        assert [[a.name for a in (page.data.account_details or [])] for page in pages] == [["A", "B"], ["C"]]

    async def test_account_filter_is_a_single_request(self) -> None:
        # Regression: nats-server ignores `offset` under `account=` but keeps
        # reporting the unfiltered `accounts` total, so the pager walked that
        # total and re-requested/re-yielded the same account once per
        # JS-enabled account (5 accounts -> 5 JSZ round trips, 5 identical
        # pages) on one of the most expensive monitoring endpoints.
        stub = _JszStubConnection(["A", "B", "C", "D", "E"])
        sys = SysClient(stub)
        pages = [page async for page in sys.all_jsz("STUB", JszOptions(accounts=True, account="A"))]
        assert len(stub.requests) == 1
        assert [[a.name for a in (page.data.account_details or [])] for page in pages] == [["A"]]

    async def test_account_filter_is_a_single_page_on_the_ping_path(self) -> None:
        stub = _JszStubConnection(["A", "B", "C", "D", "E"])
        sys = SysClient(stub)
        pagers = await sys.all_jsz_ping(JszOptions(accounts=True, account="A"))
        assert len(pagers) == 1
        seen = [account.name async for page in pagers[0] for account in (page.data.account_details or [])]
        assert seen == ["A"]
        assert len(stub.requests) == 1  # the ping itself, and no follow-up pages


class TestPagerState:
    async def test_second_iteration_is_loud(self) -> None:
        # Regression: `__aiter__` handed back the spent generator, so a second
        # pass silently yielded zero pages and read as "no data".
        stub = _StubConnection(total=2)
        sys = SysClient(stub)
        pager = sys.all_connz("STUB", ConnzOptions(limit=1))
        assert len([page async for page in pager]) == 2
        with pytest.raises(PagerStateError, match="already been iterated"):
            _ = [page async for page in pager]

    async def test_concurrent_consumers_are_loud(self) -> None:
        stub = _StubConnection(total=10)
        sys = SysClient(stub)
        pager = sys.all_connz("STUB", ConnzOptions(limit=1))

        async def drain() -> int:
            return len([page async for page in pager])

        results = await asyncio.gather(drain(), drain(), return_exceptions=True)
        assert any(isinstance(result, PagerStateError) for result in results)

    async def test_close_is_idempotent(self) -> None:
        stub = _StubConnection(total=2)
        sys = SysClient(stub)
        pager = sys.all_connz("STUB")
        await pager.aclose()
        await pager.aclose()
        assert stub.requests == []

    async def test_iterating_a_closed_pager_is_loud(self) -> None:
        stub = _StubConnection(total=2)
        sys = SysClient(stub)
        pager = sys.all_connz("STUB")
        await pager.aclose()
        with pytest.raises(PagerStateError, match="was closed"):
            _ = [page async for page in pager]


class TestPingBounds:
    async def test_timeout_override_is_validated_before_any_io(self) -> None:
        # Regression: `SysClientOptions(timeout=0)` was rejected but the
        # per-call override went straight to the wire as a nonsense deadline.
        stub = _StubConnection(total=1)
        sys = SysClient(stub)
        with pytest.raises(SysValidationError, match="timeout has to be greater than 0"):
            await sys.varz("STUB", timeout=-1.0)
        assert stub.requests == []

        ping_stub = _PingStubConnection([])
        ping_sys = SysClient(ping_stub)
        with pytest.raises(SysValidationError, match="timeout has to be greater than 0"):
            await ping_sys.varz_ping(timeout=0)
        assert ping_stub.requests == []

    async def test_no_responses_reports_the_elapsed_window(self) -> None:
        # Regression: the message interpolated the configured timeout even
        # when a far shorter bound had ended the gather, pointing operators at
        # the wrong knob.
        stub = _PingStubConnection([])
        sys = SysClient(stub, options=SysClientOptions(timeout=30.0))
        with pytest.raises(NoResponsesError) as excinfo:
            await sys.varz_ping()
        message = str(excinfo.value)
        assert "no responders" in message
        assert "within 30.0s" not in message

    async def test_closed_connection_is_not_an_empty_cluster(self) -> None:
        # Regression: `request_many` ends its stream identically for "closed"
        # and "nobody answered", so a connection that died mid-ping surfaced
        # as NoResponsesError ("is this bound to $SYS?") instead of the truth.
        stub = _PingStubConnection([], status=ConnectionState.CLOSED)
        sys = SysClient(stub)
        with pytest.raises(ConnectionClosedError, match="CLOSED"):
            await sys.varz_ping()

    async def test_reconnecting_is_not_reported_as_a_closure(self) -> None:
        # A client merely RECONNECTING is healthy and recovering; the empty
        # gather is the overall timeout, not a dead connection. Reporting it as
        # ConnectionClosedError would send a retrying caller to tear the client
        # down, and it escapes SysClientError entirely. The guard is restricted
        # to CLOSED for exactly this reason.
        stub = _PingStubConnection([], status=ConnectionState.RECONNECTING)
        sys = SysClient(stub)
        with pytest.raises(NoResponsesError):
            await sys.varz_ping()
