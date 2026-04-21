"""Tests for pipewatch.stagnation."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.stagnation import StagnationConfig, detect_stagnation


def _ts(offset: int = 0) -> datetime:
    return datetime(2024, 1, 1, 12, offset, 0, tzinfo=timezone.utc)


def _snap(rate: float, offset: int = 0) -> MetricSnapshot:
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
        h.add(_snap(r, offset=i))
    return h


def config(**kwargs) -> StagnationConfig:
    return StagnationConfig(pipeline="pipe", **kwargs)


def test_insufficient_data_returns_not_stagnant():
    h = _history([1.0, 1.0])
    result = detect_stagnation(h, config(min_snapshots=5))
    assert result is not None
    assert result.is_stagnant is False
    assert result.note == "insufficient data"


def test_all_identical_values_are_stagnant():
    h = _history([0.99] * 8)
    result = detect_stagnation(h, config(min_snapshots=5))
    assert result is not None
    assert result.is_stagnant is True
    assert result.variance == pytest.approx(0.0, abs=1e-9)
    assert "frozen" in result.note


def test_varying_values_are_not_stagnant():
    rates = [0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 1.0]
    h = _history(rates)
    result = detect_stagnation(h, config(min_snapshots=5, tolerance=0.0001))
    assert result is not None
    assert result.is_stagnant is False
    assert result.variance is not None
    assert result.variance > 0.0001


def test_unique_values_counted_correctly():
    h = _history([0.9, 0.9, 0.9, 0.91, 0.91, 0.92, 0.92, 0.92])
    result = detect_stagnation(h, config(min_snapshots=5))
    assert result is not None
    assert result.unique_values == 3


def test_snapshot_count_matches_history():
    h = _history([0.8] * 7)
    result = detect_stagnation(h, config(min_snapshots=5, _window=6))
    assert result is not None
    assert result.snapshot_count == 6


def test_to_dict_contains_expected_keys():
    h = _history([1.0] * 6)
    result = detect_stagnation(h, config(min_snapshots=5))
    d = result.to_dict()
    assert "pipeline" in d
    assert "is_stagnant" in d
    assert "variance" in d
    assert "unique_values" in d
    assert "snapshot_count" in d


def test_empty_history_returns_insufficient_data():
    h = PipelineHistory(pipeline="pipe")
    result = detect_stagnation(h, config(min_snapshots=3))
    assert result is not None
    assert result.is_stagnant is False
    assert result.variance is None
