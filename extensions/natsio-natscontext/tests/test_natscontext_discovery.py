"""Discovery, selection, and the XDG directory layout."""

from collections.abc import Callable
from pathlib import Path

import natsio.natscontext as natscontext  # ty: ignore[unresolved-import]
import pytest
from natsio.natscontext import ContextNotFoundError  # ty: ignore[unresolved-import]


def test_config_dir_uses_xdg(xdg: Path) -> None:
    assert natscontext.config_dir() == xdg / "nats"
    assert natscontext.context_dir() == xdg / "nats" / "context"


def test_config_dir_falls_back_to_home(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
    monkeypatch.setenv("HOME", str(tmp_path))
    assert natscontext.config_dir() == tmp_path / ".config" / "nats"


def test_list_contexts_empty(xdg: Path) -> None:
    assert natscontext.list_contexts() == []


def test_list_contexts_missing_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    # XDG points somewhere with no nats/context tree at all.
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "nonexistent"))
    assert natscontext.list_contexts() == []


def test_list_contexts_sorted(write_context: Callable[..., Path]) -> None:
    write_context("prod", url="nats://prod:4222")
    write_context("alpha", url="nats://alpha:4222")
    write_context("staging", url="nats://staging:4222")
    assert natscontext.list_contexts() == ["alpha", "prod", "staging"]


def test_selected_context_none(xdg: Path) -> None:
    assert natscontext.selected_context() is None


def test_selected_context_reads_and_trims(
    write_context: Callable[..., Path],
    select_context: Callable[[str], None],
    xdg: Path,
) -> None:
    write_context("staging", url="nats://staging:4222")
    (xdg / "nats" / "context.txt").write_text("  staging \n", encoding="utf-8")
    assert natscontext.selected_context() == "staging"


def test_load_by_name(write_context: Callable[..., Path]) -> None:
    write_context("prod", url="nats://prod:4222", user="bob", password="pw")
    ctx = natscontext.load("prod")
    assert ctx.name == "prod"
    assert ctx.url == "nats://prod:4222"
    assert ctx.user == "bob"


def test_load_uses_selection_when_name_omitted(
    write_context: Callable[..., Path],
    select_context: Callable[[str], None],
) -> None:
    write_context("staging", url="nats://staging:4222")
    write_context("prod", url="nats://prod:4222")
    select_context("staging")
    ctx = natscontext.load()
    assert ctx.name == "staging"
    assert ctx.url == "nats://staging:4222"


def test_load_no_name_no_selection_raises(xdg: Path) -> None:
    with pytest.raises(ContextNotFoundError):
        natscontext.load()


def test_load_unknown_context_raises(xdg: Path) -> None:
    with pytest.raises(ContextNotFoundError, match="unknown context"):
        natscontext.load("ghost")


@pytest.mark.parametrize("bad", ["../evil", "a/b", "..", "sub/../../etc"])
def test_load_rejects_traversal_names(xdg: Path, bad: str) -> None:
    with pytest.raises(ContextNotFoundError):
        natscontext.load(bad)


def test_load_absolute_path(tmp_path: Path) -> None:
    path = tmp_path / "somewhere" / "myctx.json"
    path.parent.mkdir()
    path.write_text('{"url": "nats://abs:4222", "token": "t0k"}', encoding="utf-8")
    ctx = natscontext.load(str(path))
    assert ctx.name == "myctx"
    assert ctx.url == "nats://abs:4222"
    assert ctx.token == "t0k"
    assert ctx.path == str(path)


def test_name_field_overrides_stem(write_context: Callable[..., Path]) -> None:
    write_context("filestem", name="FriendlyName", url="nats://x:4222")
    ctx = natscontext.load("filestem")
    assert ctx.name == "FriendlyName"
