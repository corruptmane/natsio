"""Guards on the public surface itself."""

import dataclasses
import typing

import natsio
from natsio.options import ConnectKwargs, ConnectOptions


def test_every_export_resolves() -> None:
    missing = [name for name in natsio.__all__ if not hasattr(natsio, name)]
    assert missing == []


def test_all_is_sorted_and_unique() -> None:
    assert list(natsio.__all__) == sorted(set(natsio.__all__))


def test_connect_kwargs_mirrors_connect_options() -> None:
    """ConnectKwargs must stay in sync with ConnectOptions field-for-field."""
    option_fields = {f.name for f in dataclasses.fields(ConnectOptions)}
    kwarg_keys = set(typing.get_type_hints(ConnectKwargs))
    assert kwarg_keys == option_fields


def test_error_message_references_are_exported() -> None:
    # The MissingDependencyError text tells users to reach for CallbackAuth.
    assert hasattr(natsio, "CallbackAuth")
    assert hasattr(natsio, "MissingDependencyError")


def test_msg_identity_semantics() -> None:
    a = natsio.Msg(subject="s", payload=b"x", headers=natsio.Headers({"A": "1"}))
    b = natsio.Msg(subject="s", payload=b"x", headers=natsio.Headers({"A": "1"}))
    assert a != b  # two deliveries are distinct events
    assert len({a, b}) == 2  # hashable even with (unhashable) Headers attached
