"""Tests for pipewatch.tapering."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List

import pytest

from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.tapering import detect_tapering, _linear_slope


def _ts(offset_seconds: int = 0) -> datetime:
    return datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=offset_seconds)


def _snap(pipeline: str, success_rate: float, ts_offset: int = 0) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline=pipeline,
        timestamp=_ts(ts_offset),
        success_rate=success_rate,
        throughput=100.0,
        error_count=0,
        is_healthy=success_rate >= 0.95,
    )


def _history(snaps: List[MetricSnapshot]) -> PipelineHistory:
    h = PipelineHistory()
    for s in snaps:
        h.add(s)
    return h


# --- unit tests for slope helper ---

def test_slope_flat_series_is_zero():
    assert _linear_slope([0.95, 0.95, 0.95, 0.95, 0.95]) == pytest.approx(0.0)


def test_slope_perfectly_declining():
    values = [1.0, 0.9, 0.8, 0.7, 0.6]
    slope = _linear_slope(values)
    assert slope == pytest.approx(-0.1, abs=1e-9)


def test_slope_perfectly_rising():
    values = [0.6, 0.7, 0.8, 0.9, 1.0]
    slope = _linear_slope(values)
    assert slope == pytest.approx(0.1, abs=1e-9)


# --- detect_tapering ---

def test_empty_history_returns_none():
    h = PipelineHistory()
    result = detect_tapering(h, pipeline="pipe_a")
    assert result is None


def test_insufficient_data_sets_flag():
    snaps = [_snap("pipe_a", 0.98, i * 60) for i in range(3)]
    h = _history(snaps)
    result = detect_tapering(h, pipeline="pipe_a", min_snapshots=5)
    assert result is not None
    assert result.insufficient_data is True
    assert result.is_tapering is False


def test_stable_series_not_tapering():
    snaps = [_snap("pipe_a", 0.98, i * 60) for i in range(10)]
    h = _history(snaps)
    result = detect_tapering(h, pipeline="pipe_a", threshold=0.90, slope_threshold=-0.005)
    assert result is not None
    assert result.is_tapering is False
    assert result.insufficient_data is False


def test_declining_series_above_threshold_is_tapering():
    # Decline from 0.99 to 0.95 over 8 snapshots — still above 0.90 threshold
    rates = [0.99 - i * 0.005 for i in range(8)]
    snaps = [_snap("pipe_b", r, i * 60) for i, r in enumerate(rates)]
    h = _history(snaps)
    result = detect_tapering(h, pipeline="pipe_b", threshold=0.90, slope_threshold=-0.003)
    assert result is not None
    assert result.is_tapering is True
    assert result.slope is not None
    assert result.slope < 0


def test_already_breached_is_not_tapering():
    # Value already below threshold — tapering requires value > threshold
    rates = [0.85 - i * 0.005 for i in range(8)]
    snaps = [_snap("pipe_c", r, i * 60) for i, r in enumerate(rates)]
    h = _history(snaps)
    result = detect_tapering(h, pipeline="pipe_c", threshold=0.90, slope_threshold=-0.003)
    assert result is not None
    assert result.is_tapering is False


def test_projected_breach_is_positive_integer_when_tapering():
    rates = [0.99 - i * 0.01 for i in range(8)]
    snaps = [_snap("pipe_d", r, i * 60) for i, r in enumerate(rates)]
    h = _history(snaps)
    result = detect_tapering(h, pipeline="pipe_d", threshold=0.90, slope_threshold=-0.005)
    assert result is not None
    if result.is_tapering:
        assert result.projected_breach is not None
        assert result.projected_breach >= 1


def test_to_dict_contains_required_keys():
    snaps = [_snap("pipe_e", 0.98 - i * 0.005, i * 60) for i in range(8)]
    h = _history(snaps)
    result = detect_tapering(h, pipeline="pipe_e", threshold=0.90)
    assert result is not None
    d = result.to_dict()
    for key in ("pipeline", "metric", "is_tapering", "slope", "current_value", "threshold", "insufficient_data"):
        assert key in d
