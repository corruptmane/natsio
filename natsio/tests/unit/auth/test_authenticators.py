import pytest
from test_nkeys import make_seed

from natsio._internal.auth import (
    CredsAuth,
    CredsFileAuth,
    NKeyAuth,
    NKeyFileAuth,
    TokenAuth,
    UserPasswordAuth,
    nkeys,
)
from natsio._internal.auth.creds import parse_creds, parse_nkey_seed
from natsio.errors import ConfigError

CREDS_TEMPLATE = """-----BEGIN NATS USER JWT-----
{jwt}
------END NATS USER JWT------

************************* IMPORTANT *************************
NKEY Seed printed below can be used to sign and prove identity.
NKEYs are sensitive and should be treated as secrets.

-----BEGIN USER NKEY SEED-----
{seed}
------END USER NKEY SEED------

*************************************************************
"""


async def test_user_password_static() -> None:
    result = await UserPasswordAuth(user="u", password="p").authenticate(None)
    assert (result.user, result.password) == ("u", "p")


async def test_token_from_async_callable() -> None:
    async def fetch() -> str:
        return "tok-123"

    result = await TokenAuth(token=fetch).authenticate(None)
    assert result.auth_token == "tok-123"


async def test_callable_reinvoked_each_time() -> None:
    calls = []

    def fetch() -> str:
        calls.append(1)
        return f"tok-{len(calls)}"

    auth = TokenAuth(token=fetch)
    assert (await auth.authenticate(None)).auth_token == "tok-1"
    assert (await auth.authenticate(None)).auth_token == "tok-2"


async def test_nkey_auth_signs_nonce() -> None:
    seed = make_seed(nkeys.Role.USER, bytes(range(32)))
    result = await NKeyAuth(seed=seed).authenticate(b"nonce-abc")
    pair = nkeys.from_seed(seed)
    assert result.nkey == pair.public_key
    assert result.signature == pair.sign_nonce_b64(b"nonce-abc")
    assert result.jwt is None


async def test_nkey_auth_requires_nonce() -> None:
    seed = make_seed(nkeys.Role.USER, bytes(32))
    with pytest.raises(ConfigError, match="nonce"):
        await NKeyAuth(seed=seed).authenticate(None)


def test_parse_nkey_seed_bare() -> None:
    seed = make_seed(nkeys.Role.USER, bytes(range(32)))
    assert parse_nkey_seed(f"{seed}\n") == seed


def test_parse_nkey_seed_decorated() -> None:
    seed = make_seed(nkeys.Role.USER, bytes(range(32)))
    decorated = f"-----BEGIN USER NKEY SEED-----\n{seed}\n------END USER NKEY SEED------\n"
    assert parse_nkey_seed(decorated) == seed


def test_parse_nkey_seed_missing() -> None:
    with pytest.raises(ConfigError, match="no NKey seed"):
        parse_nkey_seed("nothing here")


async def test_nkey_file_signs_nonce(tmp_path) -> None:
    seed = make_seed(nkeys.Role.USER, bytes(range(32)))
    nkfile = tmp_path / "user.nk"
    nkfile.write_text(seed + "\n")
    result = await NKeyFileAuth(path=nkfile).authenticate(b"nonce-abc")
    pair = nkeys.from_seed(seed)
    assert result.nkey == pair.public_key
    assert result.signature == pair.sign_nonce_b64(b"nonce-abc")
    assert result.jwt is None


async def test_nkey_file_reread_picks_up_rotation(tmp_path) -> None:
    seed1 = make_seed(nkeys.Role.USER, bytes(range(32)))
    seed2 = make_seed(nkeys.Role.USER, bytes(range(1, 33)))
    nkfile = tmp_path / "user.nk"
    nkfile.write_text(seed1)
    auth = NKeyFileAuth(path=nkfile)

    first = await auth.authenticate(b"n1")
    assert first.nkey == nkeys.from_seed(seed1).public_key

    nkfile.write_text(seed2)
    second = await auth.authenticate(b"n2")
    assert second.nkey == nkeys.from_seed(seed2).public_key
    assert second.signature == nkeys.from_seed(seed2).sign_nonce_b64(b"n2")


async def test_nkey_file_requires_nonce(tmp_path) -> None:
    seed = make_seed(nkeys.Role.USER, bytes(32))
    nkfile = tmp_path / "user.nk"
    nkfile.write_text(seed)
    with pytest.raises(ConfigError, match="nonce"):
        await NKeyFileAuth(path=nkfile).authenticate(None)


async def test_creds_auth_sets_jwt_and_signature() -> None:
    seed = make_seed(nkeys.Role.USER, bytes(range(32)))
    result = await CredsAuth(jwt="eyJhbGciOiJlZDI1NTE5In0.payload.sig", seed=seed).authenticate(b"n")
    assert result.jwt == "eyJhbGciOiJlZDI1NTE5In0.payload.sig"
    assert result.signature == nkeys.from_seed(seed).sign_nonce_b64(b"n")
    assert result.nkey is None  # JWT auth sends jwt+sig, not the bare nkey


def test_parse_creds() -> None:
    seed = make_seed(nkeys.Role.USER, bytes(32))
    jwt, parsed_seed = parse_creds(CREDS_TEMPLATE.format(jwt="a.b.c", seed=seed))
    assert jwt == "a.b.c"
    assert parsed_seed == seed


def test_parse_creds_missing_blocks() -> None:
    with pytest.raises(ConfigError, match="JWT"):
        parse_creds("not a creds file")
    with pytest.raises(ConfigError, match="SEED"):
        parse_creds("-----BEGIN NATS USER JWT-----\na.b.c\n------END NATS USER JWT------")


async def test_creds_file_reread_picks_up_rotation(tmp_path) -> None:
    seed1 = make_seed(nkeys.Role.USER, bytes(range(32)))
    seed2 = make_seed(nkeys.Role.USER, bytes(range(1, 33)))
    creds = tmp_path / "user.creds"
    creds.write_text(CREDS_TEMPLATE.format(jwt="one.jwt.x", seed=seed1))
    auth = CredsFileAuth(path=creds)

    first = await auth.authenticate(b"n1")
    assert first.jwt == "one.jwt.x"

    creds.write_text(CREDS_TEMPLATE.format(jwt="two.jwt.y", seed=seed2))
    second = await auth.authenticate(b"n2")
    assert second.jwt == "two.jwt.y"
    assert second.signature == nkeys.from_seed(seed2).sign_nonce_b64(b"n2")
