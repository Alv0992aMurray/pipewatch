"""Tests for pipewatch.window_aggregation."""
from datetime import datetime, timedelta

import pytest

from pipewatch.history import MetricSnapshot, PipelineHistory
from pipewatch.window_aggregation import (
    WindowAggregationConfig,
    aggregate_window,
)


def _ts(minutes_ago: int) -> datetime:
    return datetime(2024, 1, 1, 12, 0, 0) - timedelta(minutes=minutes_ago)


def _snap(minutes_ago: int, success_rate: float, throughput: float, healthy: bool) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline="pipe",
        timestamp=_ts(minutes_ago),
        success_rate=success_rate,
        throughput=throughput,
        is_healthy=healthy,
    )


NOW = datetime(2024, 1, 1, 12, 0, 0)
CONFIG = WindowAggregationConfig(window_minutes=60)


def _history(*snaps):
    h = PipelineHistory(pipeline="pipe")
    for s in snaps:
        h.snapshots.append(s)
    return h


def test_empty_history_returns_none():
    h = _history()
    assert aggregate_window(h, CONFIG, now=NOW) is None


def test_snapshots_outside_window_excluded():
    h = _history(
        _snap(90, 1.0, 100.0, True),
        _snap(10, 0.8, 80.0, True),
    )
    result = aggregate_window(h, CONFIG, now=NOW)
    assert result is not None
    assert result.snapshot_count == 1
    assert result.avg_success_rate == pytest.approx(0.8)


def test_all_snapshots_within_window_included():
    h = _history(
        _snap(10, 1.0, 100.0, True),
        _snap(20, 0.6, 60.0, False),
        _snap(30, 0.8, 80.0, True),
    )
    result = aggregate_window(h, CONFIG, now=NOW)
    assert result is not None
    assert result.snapshot_count == 3


def test_avg_success_rate_computed_correctly():
    h = _history(
        _snap(10, 1.0, 100.0, True),
        _snap(20, 0.5, 50.0, False),
    )
    result = aggregate_window(h, CONFIG, now=NOW)
    assert result.avg_success_rate == pytest.approx(0.75)


def test_min_max_success_rate():
    h = _history(
        _snap(5, 0.9, 90.0, True),
        _snap(15, 0.4, 40.0, False),
        _snap(25, 0.7, 70.0, True),
    )
    result = aggregate_window(h, CONFIG, now=NOW)
    assert result.min_success_rate == pytest.approx(0.4)
    assert result.max_success_rate == pytest.approx(0.9)


def test_healthy_unhealthy_counts():
    h = _history(
        _snap(5, 1.0, 100.0, True),
        _snap(15, 0.3, 30.0, False),
        _snap(25, 0.2, 20.0, False),
    )
    result = aggregate_window(h, CONFIG, now=NOW)
    assert result.healthy_count == 1
    assert result.unhealthy_count == 2


def test_health_ratio():
    h = _history(
        _snap(5, 1.0, 100.0, True),
        _snap(15, 0.3, 30.0, False),
    )
    result = aggregate_window(h, CONFIG, now=NOW)
    assert result.health_ratio == pytest.approx(0.5)


def test_to_dict_contains_expected_keys():
    h = _history(_snap(10, 0.9, 90.0, True))
    result = aggregate_window(h, CONFIG, now=NOW)
    d = result.to_dict()
    assert "pipeline" in d
    assert "avg_success_rate" in d
    assert "snapshot_count" in d
    assert "healthy_count" in d
