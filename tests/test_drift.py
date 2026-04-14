"""Tests for pipewatch.drift."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import List

import pytest

from pipewatch.history import MetricSnapshot, PipelineHistory
from pipewatch.drift import detect_drift, DriftResult


def _ts(offset_seconds: int = 0) -> datetime:
    return datetime(2024, 1, 1, 12, 0, 0) + timedelta(seconds=offset_seconds)


def _snap(success_rate: float, offset: int = 0) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline="test_pipe",
        timestamp=_ts(offset),
        success_rate=success_rate,
        error_rate=1.0 - success_rate,
        throughput=100.0,
        healthy=success_rate >= 0.9,
    )


def _history(snaps: List[MetricSnapshot]) -> PipelineHistory:
    h = PipelineHistory(pipeline_name="test_pipe")
    for s in snaps:
        h.add(s)
    return h


def test_insufficient_data_returns_none():
    h = _history([_snap(0.95, i) for i in range(10)])  # need 15
    result = detect_drift(h, reference_window=10, recent_window=5)
    assert result is None


def test_no_drift_when_values_are_stable():
    snaps = [_snap(0.95, i) for i in range(15)]
    h = _history(snaps)
    result = detect_drift(h, reference_window=10, recent_window=5, threshold=0.10)
    assert result is not None
    assert result.drifted is False
    assert abs(result.delta) < 1e-9


def test_drift_detected_on_sudden_drop():
    # reference window: high success rate; recent window: low success rate
    ref_snaps = [_snap(0.95, i) for i in range(10)]
    recent_snaps = [_snap(0.50, i + 10) for i in range(5)]
    h = _history(ref_snaps + recent_snaps)
    result = detect_drift(h, reference_window=10, recent_window=5, threshold=0.10)
    assert result is not None
    assert result.drifted is True
    assert result.delta < 0
    assert result.relative_change is not None
    assert result.relative_change < -0.10


def test_drift_detected_on_improvement():
    ref_snaps = [_snap(0.50, i) for i in range(10)]
    recent_snaps = [_snap(0.95, i + 10) for i in range(5)]
    h = _history(ref_snaps + recent_snaps)
    result = detect_drift(h, reference_window=10, recent_window=5, threshold=0.10)
    assert result is not None
    assert result.drifted is True
    assert result.delta > 0


def test_result_fields_are_populated():
    snaps = [_snap(0.90, i) for i in range(10)] + [_snap(0.70, i + 10) for i in range(5)]
    h = _history(snaps)
    result = detect_drift(h, reference_window=10, recent_window=5)
    assert result is not None
    assert result.pipeline == "test_pipe"
    assert result.metric == "success_rate"
    assert result.reference_mean == pytest.approx(0.90)
    assert result.recent_mean == pytest.approx(0.70)
    assert result.threshold == 0.10


def test_to_dict_has_expected_keys():
    snaps = [_snap(0.95, i) for i in range(15)]
    h = _history(snaps)
    result = detect_drift(h)
    assert result is not None
    d = result.to_dict()
    for key in ("pipeline", "metric", "reference_mean", "recent_mean", "delta",
                "relative_change", "drifted", "threshold"):
        assert key in d


def test_custom_metric_error_rate():
    ref_snaps = [_snap(0.95, i) for i in range(10)]  # error_rate = 0.05
    recent_snaps = [_snap(0.50, i + 10) for i in range(5)]  # error_rate = 0.50
    h = _history(ref_snaps + recent_snaps)
    result = detect_drift(h, metric="error_rate", reference_window=10, recent_window=5, threshold=0.10)
    assert result is not None
    assert result.metric == "error_rate"
    assert result.drifted is True
    assert result.delta > 0  # error rate went up
