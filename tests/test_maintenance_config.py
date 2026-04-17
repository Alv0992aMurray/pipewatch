"""Tests for pipewatch.maintenance_config."""
from datetime import datetime, timedelta
from pathlib import Path

import pytest
import yaml

from pipewatch.maintenance_config import (
    MaintenanceConfigError,
    load_maintenance_windows,
)


def _write(tmp_path: Path, data: dict) -> str:
    p = tmp_path / "maintenance.yml"
    p.write_text(yaml.dump(data))
    return str(p)


def _iso(offset_hours: int = 0) -> str:
    return (datetime.utcnow() + timedelta(hours=offset_hours)).isoformat()


def test_missing_file_returns_empty_list(tmp_path):
    result = load_maintenance_windows(str(tmp_path / "missing.yml"))
    assert result == []


def test_empty_file_returns_empty_list(tmp_path):
    p = tmp_path / "m.yml"
    p.write_text("")
    assert load_maintenance_windows(str(p)) == []


def test_no_maintenance_key_returns_empty_list(tmp_path):
    path = _write(tmp_path, {"other": []})
    assert load_maintenance_windows(path) == []


def test_load_basic_window(tmp_path):
    path = _write(tmp_path, {
        "maintenance": [
            {"pipeline": "pipe_a", "start": _iso(-1), "end": _iso(1)}
        ]
    })
    windows = load_maintenance_windows(path)
    assert len(windows) == 1
    assert windows[0].pipeline == "pipe_a"


def test_load_window_with_reason(tmp_path):
    path = _write(tmp_path, {
        "maintenance": [
            {"pipeline": "*", "start": _iso(-1), "end": _iso(1), "reason": "upgrade"}
        ]
    })
    windows = load_maintenance_windows(path)
    assert windows[0].reason == "upgrade"


def test_default_pipeline_is_wildcard(tmp_path):
    path = _write(tmp_path, {
        "maintenance": [
            {"start": _iso(-1), "end": _iso(1)}
        ]
    })
    windows = load_maintenance_windows(path)
    assert windows[0].pipeline == "*"


def test_missing_start_raises_error(tmp_path):
    path = _write(tmp_path, {
        "maintenance": [{"pipeline": "x", "end": _iso(1)}]
    })
    with pytest.raises(MaintenanceConfigError):
        load_maintenance_windows(path)
