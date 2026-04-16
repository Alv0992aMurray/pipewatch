"""Tests for pipewatch.capacity."""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.capacity import CapacityConfig, estimate_capacity


def _ts(offset: int) -> datetime:
    return datetime(2024, 1, 1, 12, 0, 0) + timedelta(minutes=offset)


def _snap(offset: int, success_rate: float, throughput: float = 100.0) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline="pipe",
        timestamp=_ts(offset),
        success_rate=success_rate,
        throughput=throughput,
        error_count=0,
        is_healthy=success_rate >= 0.95,
    )


def _history(rates: list[float]) -> PipelineHistory:
    h = PipelineHistory(pipeline="pipe")
    for i, r in enumerate(rates):
        h.add(_snap(i, r))
    return h


@pytest.fixture
def falling_config() -> CapacityConfig:
    return CapacityConfig(pipeline="pipe", metric="success_rate", threshold=0.80, direction="falling")


def test_empty_history_returns_no_breach(falling_config):
    h = PipelineHistory(pipeline="pipe")
    result = estimate_capacity(falling_config, h)
    assert result.will_breach is False
    assert result.current_value is None
    assert result.runs_until_breach is None


def test_stable_series_no_breach(falling_config):
    h = _history([0.99, 0.99, 0.99, 0.99, 0.99])
    result = estimate_capacity(falling_config, h)
    assert result.will_breach is False
    assert result.slope_per_run is not None


def test_rising_series_no_falling_breach(falling_config):
    h = _history([0.90, 0.91, 0.92, 0.93, 0.94])
    result = estimate_capacity(falling_config, h)
    assert result.will_breach is False


def test_falling_series_detects_breach(falling_config):
    # Drops ~0.02 per step; current ~0.90, threshold 0.80 → ~5 runs away
    rates = [0.99, 0.97, 0.95, 0.93, 0.91]
    h = _history(rates)
    result = estimate_capacity(falling_config, h)
    assert result.will_breach is True
    assert result.runs_until_breach is not None
    assert result.runs_until_breach >= 1


def test_runs_until_breach_is_positive(falling_config):
    rates = [0.99, 0.96, 0.93, 0.90, 0.87]
    h = _history(rates)
    result = estimate_capacity(falling_config, h)
    assert result.will_breach is True
    assert result.runs_until_breach >= 1


def test_rising_direction_detects_breach():
    config = CapacityConfig(pipeline="pipe", metric="throughput", threshold=200.0, direction="rising")
    # throughput rising ~10/step from 150
    snaps = [_snap(i, 0.99, 150.0 + i * 10) for i in range(6)]
    h = PipelineHistory(pipeline="pipe")
    for s in snaps:
        h.add(s)
    result = estimate_capacity(config, h)
    assert result.will_breach is True
    assert result.runs_until_breach is not None


def test_to_dict_contains_expected_keys(falling_config):
    h = _history([0.99, 0.97, 0.95, 0.93, 0.91])
    result = estimate_capacity(falling_config, h)
    d = result.to_dict()
    for key in ("pipeline", "metric", "current_value", "threshold", "direction",
                "slope_per_run", "runs_until_breach", "will_breach"):
        assert key in d
