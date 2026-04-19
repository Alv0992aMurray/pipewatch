"""Tests for pipewatch.breach."""
from datetime import datetime, timedelta
import pytest
from pipewatch.history import PipelineHistory
from pipewatch.breach import BreachConfig, detect_breaches


def _ts(offset: int = 0) -> datetime:
    return datetime(2024, 1, 1, 12, 0, 0) + timedelta(minutes=offset)


def _snap(history: PipelineHistory, success_rate: float, offset: int = 0):
    from pipewatch.history import MetricSnapshot
    snap = MetricSnapshot(
        pipeline=history.pipeline,
        timestamp=_ts(offset),
        success_rate=success_rate,
        error_rate=1.0 - success_rate,
        throughput=100.0,
        is_healthy=success_rate >= 0.95,
    )
    history.add(snap)
    return snap


def _history(rates, pipeline="pipe"):
    h = PipelineHistory(pipeline=pipeline)
    for i, r in enumerate(rates):
        _snap(h, r, offset=i)
    return h


def test_empty_history_returns_none():
    h = PipelineHistory(pipeline="p")
    cfg = BreachConfig(metric="success_rate", threshold=0.95, direction="below")
    assert detect_breaches(h, cfg) is None


def test_no_breaches_when_all_healthy():
    h = _history([1.0, 0.99, 0.98])
    cfg = BreachConfig(metric="success_rate", threshold=0.95, direction="below")
    result = detect_breaches(h, cfg)
    assert result is not None
    assert result.total_breaches == 0
    assert result.latest is None


def test_detects_single_breach():
    h = _history([1.0, 0.90, 1.0])
    cfg = BreachConfig(metric="success_rate", threshold=0.95, direction="below")
    result = detect_breaches(h, cfg)
    assert result.total_breaches == 1
    assert abs(result.latest.value - 0.90) < 1e-6


def test_detects_multiple_breaches():
    h = _history([0.80, 0.85, 0.70, 0.99])
    cfg = BreachConfig(metric="success_rate", threshold=0.95, direction="below")
    result = detect_breaches(h, cfg)
    assert result.total_breaches == 3


def test_direction_above_detected():
    h = _history([0.10, 0.20, 0.60])
    cfg = BreachConfig(metric="error_rate", threshold=0.50, direction="above")
    result = detect_breaches(h, cfg)
    assert result.total_breaches == 1
    assert abs(result.latest.value - 0.40) < 1e-6


def test_to_dict_keys_present():
    h = _history([0.80])
    cfg = BreachConfig(metric="success_rate", threshold=0.95, direction="below")
    result = detect_breaches(h, cfg)
    d = result.to_dict()
    assert "pipeline" in d
    assert "total_breaches" in d
    assert "latest" in d


def test_latest_is_last_breach():
    h = _history([0.80, 0.70, 0.60])
    cfg = BreachConfig(metric="success_rate", threshold=0.95, direction="below")
    result = detect_breaches(h, cfg)
    assert abs(result.latest.value - 0.60) < 1e-6
