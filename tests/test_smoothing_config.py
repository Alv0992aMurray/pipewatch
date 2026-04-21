"""Tests for pipewatch.smoothing_config."""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from pipewatch.smoothing_config import (
    load_smoothing_configs,
    SmoothingConfig,
    SmoothingConfigError,
    _parse_config,
)


def _write(tmp_path: Path, data: dict) -> str:
    p = tmp_path / "smoothing.yml"
    p.write_text(yaml.dump(data))
    return str(p)


def test_missing_file_returns_empty_list(tmp_path):
    result = load_smoothing_configs(str(tmp_path / "nonexistent.yml"))
    assert result == []


def test_empty_file_returns_empty_list(tmp_path):
    p = tmp_path / "s.yml"
    p.write_text("")
    assert load_smoothing_configs(str(p)) == []


def test_no_smoothing_key_returns_empty_list(tmp_path):
    path = _write(tmp_path, {"other": []})
    assert load_smoothing_configs(path) == []


def test_load_basic_config(tmp_path):
    path = _write(tmp_path, {"smoothing": [{"pipeline": "pipe_a"}]})
    result = load_smoothing_configs(path)
    assert len(result) == 1
    assert result[0].pipeline == "pipe_a"


def test_load_default_alpha_and_metric(tmp_path):
    path = _write(tmp_path, {"smoothing": [{"pipeline": "p"}]})
    cfg = load_smoothing_configs(path)[0]
    assert cfg.alpha == pytest.approx(0.3)
    assert cfg.metric == "success_rate"
    assert cfg.min_points == 2


def test_load_custom_alpha(tmp_path):
    path = _write(tmp_path, {"smoothing": [{"pipeline": "p", "alpha": 0.7}]})
    cfg = load_smoothing_configs(path)[0]
    assert cfg.alpha == pytest.approx(0.7)


def test_load_custom_metric(tmp_path):
    path = _write(tmp_path, {"smoothing": [{"pipeline": "p", "metric": "throughput"}]})
    cfg = load_smoothing_configs(path)[0]
    assert cfg.metric == "throughput"


def test_invalid_alpha_raises(tmp_path):
    path = _write(tmp_path, {"smoothing": [{"pipeline": "p", "alpha": 1.5}]})
    with pytest.raises(SmoothingConfigError, match="alpha"):
        load_smoothing_configs(path)


def test_missing_pipeline_raises():
    with pytest.raises(SmoothingConfigError, match="pipeline"):
        _parse_config({"alpha": 0.3})


def test_load_multiple_configs(tmp_path):
    path = _write(
        tmp_path,
        {
            "smoothing": [
                {"pipeline": "a", "alpha": 0.1},
                {"pipeline": "b", "alpha": 0.9, "metric": "throughput"},
            ]
        },
    )
    result = load_smoothing_configs(path)
    assert len(result) == 2
    assert result[1].metric == "throughput"
