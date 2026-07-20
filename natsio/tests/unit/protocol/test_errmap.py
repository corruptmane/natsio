import pytest

from natsio._internal.protocol import classify_server_error
from natsio.errors import (
    AuthenticationExpiredError,
    AuthorizationViolationError,
    MaxSubscriptionsExceededError,
    PermissionsViolationError,
    ServerError,
    StaleConnectionError,
)


@pytest.mark.parametrize(
    ("message", "expected_type", "fatal"),
    [
        ("Stale Connection", StaleConnectionError, True),
        ("Authorization Violation", AuthorizationViolationError, True),
        ("Authentication Expired", AuthenticationExpiredError, True),
        ("User Authentication Expired", AuthenticationExpiredError, True),
        ("User Authentication Revoked", AuthenticationExpiredError, True),
        ("Account Authentication Expired", AuthenticationExpiredError, True),
        ('Permissions Violation for Publish to "foo.bar"', PermissionsViolationError, False),
        ('Permissions Violation for Subscription to "foo.>"', PermissionsViolationError, False),
        ("maximum subscriptions exceeded", MaxSubscriptionsExceededError, False),
        ("Maximum Subscriptions Exceeded", MaxSubscriptionsExceededError, False),
        ("Invalid Subject", ServerError, False),
        ("Maximum Payload Violation", ServerError, True),
        ("Slow Consumer Detected", ServerError, True),
        ("Maximum Connections Exceeded", ServerError, True),
        ("Parser Error", ServerError, True),
        ("Unknown Protocol Operation", ServerError, True),
        ("Secure Connection - TLS Required", ServerError, True),
        ("some future error nobody predicted", ServerError, True),
    ],
)
def test_classification(message: str, expected_type: type[ServerError], fatal: bool) -> None:
    error = classify_server_error(message)
    assert type(error) is expected_type
    assert error.fatal is fatal
    assert error.description == message


def test_all_results_are_server_errors() -> None:
    assert isinstance(classify_server_error("anything"), ServerError)
