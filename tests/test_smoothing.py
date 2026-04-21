"""Tests for pipewatch.smoothing."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.smoothing import smooth, SmoothingResult


def _ts(offset: int = 0) -> datetime:
    return datetime(2024, 1, 1, 12, offset, 0, tzinfo=timezone.utc)


def _snap(pipeline: str, success_rate: float, offset: int = 0) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline=pipeline,
        timestamp=_ts(offset),
        success_rate=success_rate,
        throughput=100.0,
        error_count=0,
        healthy=success_rate >= 0.95,
    )


def _history(snaps) -> PipelineHistory:
    h = PipelineHistory(pipeline_name=snaps[0].pipeline)
    for s in snaps:
        h.add(s)
    return h


def test_empty_history_returns_none():
    h = PipelineHistory(pipeline_name="pipe")
    assert smooth(h) is None


def test_insufficient_data_sets_flag():
    h = _history([_snap("pipe", 1.0)])
    result = smooth(h, min_points=2)
    assert result is not None
    assert result.insufficient_data is True
    assert result.points == []


def test_single_point_with_min_points_one():
    h = _history([_snap("pipe", 0.9)])
    result = smooth(h, min_points=1)
    assert result is not None
    assert not result.insufficient_data
    assert len(result.points) == 1
    assert result.points[0].smoothed == pytest.approx(0.9)


def test_stable_series_smoothed_equals_constant():
    snaps = [_snap("pipe", 1.0, i) for i in range(5)]
    h = _history(snaps)
    result = smooth(h, alpha=0.3)
    assert result is not None
    for pt in result.points:
        assert pt.smoothed == pytest.approx(1.0)


def test_ema_converges_toward_new_value():
    # Start at 1.0, then drop to 0.0 — EMA should decrease monotonically.
    snaps = [_snap("pipe", 1.0, 0)] + [_snap("pipe", 0.0, i) for i in range(1, 6)]
    h = _history(snaps)
    result = smooth(h, alpha=0.5)
    assert result is not None
    smoothed = [p.smoothed for p in result.points]
    assert smoothed[0] == pytest.approx(1.0)
    for i in range(1, len(smoothed)):
        assert smoothed[i] < smoothed[i - 1]


def test_latest_smoothed_returns_last_point():
    snaps = [_snap("pipe", 0.8, i) for i in range(4)]
    h = _history(snaps)
    result = smooth(h)
    assert result is not None
    assert result.latest_smoothed() == pytest.approx(result.points[-1].smoothed)


def test_result_pipeline_and_metric_set_correctly():
    h = _history([_snap("my_pipe", 0.99, i) for i in range(3)])
    result = smooth(h, metric="success_rate", alpha=0.2)
    assert result.pipeline == "my_pipe"
    assert result.metric == "success_rate"
    assert result.alpha == pytest.approx(0.2)


def test_to_dict_keys():
    h = _history([_snap("pipe", 0.9, i) for i in range(3)])
    result = smooth(h)
    d = result.to_dict()
    assert "pipeline" in d
    assert "metric" in d
    assert "alpha" in d
    assert "points" in d
    assert "latest_smoothed" in d
    assert "insufficient_data" in d
