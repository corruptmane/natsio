"""Admission settings reject impossible/non-finite configurations."""

import pytest

from natsio.errors import ConfigError
from natsio.options import ConnectOptions


@pytest.mark.parametrize(
    "name",
    [
        "connect_timeout",
        "flush_timeout",
        "drain_timeout",
        "request_timeout",
        "ping_interval",
        "reconnect_time_wait",
        "reconnect_time_wait_max",
    ],
)
@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf")])
def test_deadlines_are_finite_and_positive(name, value) -> None:
    with pytest.raises(ConfigError, match=name):
        ConnectOptions(**{name: value})


@pytest.mark.parametrize("name", ["max_pending_size", "max_control_line"])
@pytest.mark.parametrize("value", [0, -1, 1.5, float("inf")])
def test_connected_buffer_and_parser_limits_are_positive_integers(name, value) -> None:
    with pytest.raises(ConfigError, match=name):
        ConnectOptions(**{name: value})


@pytest.mark.parametrize("name", ["reconnect_jitter", "reconnect_jitter_tls"])
@pytest.mark.parametrize("value", [-1, float("nan"), float("inf")])
def test_jitter_is_finite_and_nonnegative(name, value) -> None:
    with pytest.raises(ConfigError, match=name):
        ConnectOptions(**{name: value})


@pytest.mark.parametrize("value", [-1, 0])
def test_reconnect_and_subscription_special_values_are_preserved(value) -> None:
    options = ConnectOptions(
        reconnect_buf_size=value,
        pending_msgs_limit=value,
        pending_bytes_limit=value,
        reconnect_jitter=0,
        reconnect_jitter_tls=0,
    )
    assert options.reconnect_buf_size == value
    assert options.pending_bytes_limit == value


@pytest.mark.parametrize("value", [-2, 1.5, float("inf")])
def test_reconnect_buffer_rejects_undocumented_values(value) -> None:
    with pytest.raises(ConfigError, match="reconnect_buf_size"):
        ConnectOptions(reconnect_buf_size=value)
