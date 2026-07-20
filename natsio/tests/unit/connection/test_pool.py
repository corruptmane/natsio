import pytest

from natsio._internal.pool import ServerPool, parse_server_url
from natsio.errors import ConfigError


class TestParseServerUrl:
    def test_bare_host_defaults(self) -> None:
        server = parse_server_url("example.com")
        assert (server.host, server.port, server.tls_required) == ("example.com", 4222, False)

    def test_tls_scheme(self) -> None:
        assert parse_server_url("tls://example.com:4443").tls_required is True

    def test_userinfo_is_percent_decoded(self) -> None:
        # '@' and ':' MUST be percent-encoded inside userinfo; the decoded value
        # is what goes on the wire.
        server = parse_server_url("nats://us%40er:p%40ss%3Aword@host:4222")
        assert server.username == "us@er"
        assert server.password == "p@ss:word"

    def test_token_only_userinfo(self) -> None:
        server = parse_server_url("nats://s3cr3t@host")
        assert server.username == "s3cr3t"
        assert server.password is None

    def test_bad_scheme_and_missing_host(self) -> None:
        with pytest.raises(ConfigError, match="scheme"):
            parse_server_url("http://example.com")
        with pytest.raises(ConfigError, match="host"):
            parse_server_url("nats://:4222")


class TestServerPool:
    def test_no_randomize_preserves_order_across_success(self) -> None:
        pool = ServerPool(("nats://a:4222", "nats://b:4222"), randomize=False)
        primary = pool.candidates()[0]
        assert primary.host == "a"
        pool.mark_success(primary)
        # The configured primary must still be tried first.
        assert pool.candidates()[0].host == "a"

    def test_randomized_pool_rotates_after_success(self) -> None:
        pool = ServerPool(("nats://a:4222",), randomize=True)
        server = pool.candidates()[0]
        pool.mark_success(server)
        assert pool.candidates()[0] is server  # single-server pool still works

    def test_failure_budget_excludes_server(self) -> None:
        pool = ServerPool(("nats://a:4222", "nats://b:4222"), randomize=False, max_consecutive_failures=2)
        first = pool.candidates()[0]
        pool.mark_failure(first)
        assert len(pool.candidates()) == 2
        pool.mark_failure(first)
        assert [s.host for s in pool.candidates()] == ["b"]
        pool.mark_success(first)
        assert len(pool.candidates()) == 2

    def test_unlimited_attempts(self) -> None:
        pool = ServerPool(("nats://a:4222",), randomize=False, max_consecutive_failures=-1)
        for _ in range(100):
            pool.mark_failure(pool.candidates()[0])
        assert len(pool.candidates()) == 1

    def test_merge_discovered_dedups_and_reports(self) -> None:
        pool = ServerPool(("nats://a:4222",), randomize=False)
        added = pool.merge_discovered(["a:4222", "10.0.0.5:4222", "10.0.0.5:4222", "garbage://x"])
        assert [s.host for s in added] == ["10.0.0.5"]
        assert added[0].discovered is True
        assert pool.merge_discovered(["10.0.0.5:4222"]) == []

    def test_discovery_can_be_disabled(self) -> None:
        pool = ServerPool(("nats://a:4222",), randomize=False, accept_discovered=False)
        assert pool.merge_discovered(["10.0.0.5:4222"]) == []
