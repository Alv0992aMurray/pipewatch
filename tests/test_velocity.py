"""Tests for pipewatch.velocity."""
from datetime import datetime, timedelta

import pytest

from pipewatch.history import MetricSnapshot, PipelineHistory
from pipewatch.velocity import compute_velocity


def _ts(offset: int) -> datetime:
    return datetime(2024, 1, 1, 12, 0, 0) + timedelta(minutes=offset)


def _snap(offset: int, rate: float) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline="pipe",
        timestamp=_ts(offset),
        success_rate=rate,
        throughput=100.0,
        error_count=0,
        is_healthy=rate >= 0.95,
    )


def _history(rates: list) -> PipelineHistory:
    h = PipelineHistory(pipeline="pipe")
    for i, r in enumerate(rates):
        h.add(_snap(i, r))
    return h


def test_empty_history_returns_none():
    h = PipelineHistory(pipeline="pipe")
    assert compute_velocity(h) is None


def test_single_snapshot_returns_none():
    h = _history([0.9])
    assert compute_velocity(h) is None


def test_stable_series_labelled_stable():
    h = _history([0.95, 0.95, 0.95, 0.95, 0.95])
    result = compute_velocity(h)
    assert result is not None
    assert result.label == "stable"
    assert result.delta == pytest.approx(0.0)


def test_improving_series_labelled_improving():
    h = _history([0.70, 0.80, 0.90, 0.95, 0.99])
    result = compute_velocity(h)
    assert result.label == "improving"
    assert result.delta > 0


def test_declining_series_labelled_declining():
    h = _history([0.99, 0.95, 0.90, 0.80, 0.70])
    result = compute_velocity(h)
    assert result.label == "declining"
    assert result.delta < 0


def test_per_step_calculated_correctly():
    h = _history([0.80, 0.85, 0.90, 0.95, 1.00])
    result = compute_velocity(h)
    assert result.per_step == pytest.approx(0.05, abs=1e-6)


def test_window_limits_snapshots_used():
    h = _history([0.50, 0.60, 0.70, 0.99, 0.99])
    result = compute_velocity(h, window=2)
    assert result.window_size == 2
    assert result.first_rate == pytest.approx(0.99)
    assert result.last_rate == pytest.approx(0.99)


def test_to_dict_contains_expected_keys():
    h = _history([0.80, 0.90])
    result = compute_velocity(h)
    d = result.to_dict()
    for key in ("pipeline", "window_size", "first_rate", "last_rate", "delta", "per_step", "label"):
        assert key in d
