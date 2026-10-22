"""Contexts remain usable without exposing credentials through nested reprs."""

from natsio.natscontext import Context  # ty: ignore[unresolved-import]  # namespace extension

from natsio.options import ConnectOptions


def test_context_repr_hides_url_proxy_auth_and_unknown_credentials() -> None:
    secret = "CONTEXT_DIAGNOSTIC_SECRET_320af"
    context = Context.from_dict(
        {
            "url": f"nats://user:{secret}@localhost:4222",
            "socks_proxy": f"socks5://proxy:{secret}@localhost:1080",
            "password": secret,
            "token": secret,
            "user_jwt": secret,
            "future_secret": secret,
        },
        name="test-context",
    )
    assert secret not in repr(context)
    assert secret not in repr(ConnectOptions(**context.connect_kwargs()))
    assert secret in context.url
    assert secret in context.socks_proxy
    assert context.raw["future_secret"] == secret
