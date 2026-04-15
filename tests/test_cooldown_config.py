"""Tests for pipewatch.cooldown_config."""

from pathlib import Path

import pytest

from pipewatch.cooldown_config import CooldownConfigError, load_cooldown_configs


def _write(tmp_path: Path, content: str) -> str:
    p = tmp_path / "cooldowns.yml"
    p.write_text(content)
    return str(p)


def test_missing_file_returns_empty_list(tmp_path: Path) -> None:
    result = load_cooldown_configs(str(tmp_path / "nonexistent.yml"))
    assert result == []


def test_empty_file_returns_empty_list(tmp_path: Path) -> None:
    path = _write(tmp_path, "")
    assert load_cooldown_configs(path) == []


def test_no_cooldowns_key_returns_empty_list(tmp_path: Path) -> None:
    path = _write(tmp_path, "other_key: []\n")
    assert load_cooldown_configs(path) == []


def test_load_basic_cooldown(tmp_path: Path) -> None:
    path = _write(tmp_path, "cooldowns:\n  - pipeline: pipe_a\n    seconds: 300\n")
    result = load_cooldown_configs(path)
    assert len(result) == 1
    assert result[0].pipeline == "pipe_a"
    assert result[0].seconds == 300


def test_load_multiple_cooldowns(tmp_path: Path) -> None:
    content = "cooldowns:\n  - pipeline: pipe_a\n    seconds: 60\n  - pipeline: pipe_b\n    seconds: 120\n"
    path = _write(tmp_path, content)
    result = load_cooldown_configs(path)
    assert len(result) == 2
    assert result[1].pipeline == "pipe_b"


def test_missing_pipeline_raises(tmp_path: Path) -> None:
    path = _write(tmp_path, "cooldowns:\n  - seconds: 60\n")
    with pytest.raises(CooldownConfigError, match="pipeline"):
        load_cooldown_configs(path)


def test_missing_seconds_raises(tmp_path: Path) -> None:
    path = _write(tmp_path, "cooldowns:\n  - pipeline: pipe_a\n")
    with pytest.raises(CooldownConfigError, match="seconds"):
        load_cooldown_configs(path)


def test_nonpositive_seconds_raises(tmp_path: Path) -> None:
    path = _write(tmp_path, "cooldowns:\n  - pipeline: pipe_a\n    seconds: 0\n")
    with pytest.raises(CooldownConfigError, match="positive"):
        load_cooldown_configs(path)


def test_wildcard_pipeline_is_accepted(tmp_path: Path) -> None:
    path = _write(tmp_path, "cooldowns:\n  - pipeline: '*'\n    seconds: 180\n")
    result = load_cooldown_configs(path)
    assert result[0].pipeline == "*"
