"""ADR-42 priority groups: client-side validation and pin-id bookkeeping.

The wire behaviour (overflow gating, pinning, unpin handover) is covered live
in the integration suite; these are the local, network-free contracts:
group validation before any request is issued, and the per-group pin-id store
the client replays on subsequent pulls.
"""

import pytest

from natsio._internal.protocol import Headers
from natsio.errors import ConfigError
from natsio.jetstream import ConsumerConfig, ConsumerInfo
from natsio.jetstream import headers as js_headers
from natsio.jetstream.consumer import Consumer
from natsio.message import Msg


def _consumer(groups: list[str] | None = None) -> Consumer:
    """A Consumer handle with priority_groups set but no live connection.

    Group validation and pin bookkeeping run entirely off ``cached_info`` and
    the local pin store, so an unstarted handle exercises them faithfully.
    """
    handle = Consumer.__new__(Consumer)
    handle.cached_info = ConsumerInfo(config=ConsumerConfig(priority_groups=groups))
    handle._pin_ids = {}
    return handle


class TestGroupValidation:
    def test_consume_requires_group_when_configured(self) -> None:
        with pytest.raises(ConfigError, match="required"):
            _consumer(["A"]).consume()

    def test_consume_rejects_unknown_group(self) -> None:
        with pytest.raises(ConfigError, match="invalid priority group"):
            _consumer(["A", "B"]).consume(group="C")

    def test_consume_rejects_group_when_not_configured(self) -> None:
        with pytest.raises(ConfigError, match="not supported"):
            _consumer(None).consume(group="A")

    def test_consume_accepts_configured_group(self) -> None:
        session = _consumer(["A"]).consume(group="A")
        assert session._group == "A"

    def test_consume_min_pending_must_be_positive(self) -> None:
        with pytest.raises(ConfigError, match="min_pending"):
            _consumer(["A"]).consume(group="A", min_pending=0)

    def test_consume_min_ack_pending_must_be_positive(self) -> None:
        with pytest.raises(ConfigError, match="min_ack_pending"):
            _consumer(["A"]).consume(group="A", min_ack_pending=-1)

    async def test_fetch_requires_group_when_configured(self) -> None:
        with pytest.raises(ConfigError, match="required"):
            await _consumer(["A"]).fetch(5)

    async def test_fetch_rejects_unknown_group(self) -> None:
        with pytest.raises(ConfigError, match="invalid priority group"):
            await _consumer(["A"]).fetch(5, group="Z")

    async def test_fetch_rejects_group_when_not_configured(self) -> None:
        with pytest.raises(ConfigError, match="not supported"):
            await _consumer(None).fetch(5, group="A")

    async def test_fetch_min_pending_must_be_positive(self) -> None:
        with pytest.raises(ConfigError, match="min_pending"):
            await _consumer(["A"]).fetch(5, group="A", min_pending=0)

    async def test_next_validates_group(self) -> None:
        with pytest.raises(ConfigError, match="invalid priority group"):
            await _consumer(["A"]).next(group="B")


class TestPinBookkeeping:
    def _msg(self, pin: str | None) -> Msg:
        headers = Headers({js_headers.PIN_ID: pin}) if pin is not None else None
        return Msg(subject="x", payload=b"data", headers=headers)

    def test_remember_and_replay(self) -> None:
        handle = _consumer(["A"])
        handle._remember_pin("A", self._msg("PIN-1"))
        assert handle._pin_id("A") == "PIN-1"

    def test_latest_pin_wins(self) -> None:
        handle = _consumer(["A"])
        handle._remember_pin("A", self._msg("PIN-1"))
        handle._remember_pin("A", self._msg("PIN-2"))
        assert handle._pin_id("A") == "PIN-2"

    def test_forget_clears(self) -> None:
        handle = _consumer(["A"])
        handle._remember_pin("A", self._msg("PIN-1"))
        handle._forget_pin("A")
        assert handle._pin_id("A") is None

    def test_pins_are_isolated_per_group(self) -> None:
        handle = _consumer(["A", "B"])
        handle._remember_pin("A", self._msg("PIN-A"))
        assert handle._pin_id("B") is None
        handle._forget_pin("A")
        assert handle._pin_id("A") is None

    def test_no_group_is_never_pinned(self) -> None:
        handle = _consumer(None)
        handle._remember_pin(None, self._msg("PIN-1"))
        assert handle._pin_id(None) is None

    def test_message_without_pin_header_is_ignored(self) -> None:
        handle = _consumer(["A"])
        handle._remember_pin("A", self._msg(None))
        assert handle._pin_id("A") is None
