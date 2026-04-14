"""Tests for pipewatch.silencing_config."""
from pathlib import Path

import pytest
import yaml

from pipewatch.silencing_config import SilencingConfigError, load_silence_windows


def _write(tmp_path: Path, data: dict) -> Path:
    p = tmp_path / "silences.yml"
    p.write_text(yaml.dump(data))
    return p


def test_missing_file_returns_empty_list(tmp_path):
    result = load_silence_windows(tmp_path / "nonexistent.yml")
    assert result == []


def test_empty_file_returns_empty_list(tmp_path):
    p = tmp_path / "silences.yml"
    p.write_text("")
    assert load_silence_windows(p) == []


def test_load_basic_window(tmp_path):
    data = {
        "silences": [
            {
                "name": "maintenance",
                "pipeline": "orders",
                "start": "2024-06-01T00:00:00",
                "end": "2024-06-01T06:00:00",
            }
        ]
    }
    p = _write(tmp_path, data)
    windows = load_silence_windows(p)
    assert len(windows) == 1
    assert windows[0].name == "maintenance"
    assert windows[0].pipeline == "orders"


def test_load_window_with_reason(tmp_path):
    data = {
        "silences": [
            {
                "name": "deploy",
                "pipeline": "*",
                "start": "2024-06-01T10:00:00",
                "end": "2024-06-01T11:00:00",
                "reason": "scheduled deployment",
            }
        ]
    }
    p = _write(tmp_path, data)
    windows = load_silence_windows(p)
    assert windows[0].reason == "scheduled deployment"
    assert windows[0].pipeline == "*"


def test_missing_required_field_raises(tmp_path):
    data = {
        "silences": [
            {"name": "bad", "pipeline": "orders", "start": "2024-06-01T00:00:00"}
        ]
    }
    p = _write(tmp_path, data)
    with pytest.raises(SilencingConfigError, match="end"):
        load_silence_windows(p)


def test_invalid_datetime_raises(tmp_path):
    data = {
        "silences": [
            {
                "name": "bad",
                "pipeline": "orders",
                "start": "not-a-date",
                "end": "2024-06-01T06:00:00",
            }
        ]
    }
    p = _write(tmp_path, data)
    with pytest.raises(SilencingConfigError):
        load_silence_windows(p)
