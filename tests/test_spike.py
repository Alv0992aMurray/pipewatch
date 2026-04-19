"""Tests for pipewatch.spike."""
import pytest
from datetime import datetime, timezone
from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.spike import detect_spike


def _ts(offset: int = 0) -> datetime:
    return datetime(2024, 1, 1, 0, offset, 0, tzinfo=timezone.utc)


def _snap(pipeline: str, error_rate: float, offset: int) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline=pipeline,
        timestamp=_ts(offset),
        success_rate=1.0 - error_rate,
        throughput=100.0,
        error_rate=error_rate,
        is_healthy=error_rate < 0.1,
    )


def _history(rates: list) -> PipelineHistory:
    h = PipelineHistory(pipeline_name="pipe")
    for i, r in enumerate(rates):
        h.add(_snap("pipe", r, i))
    return h


def test_empty_history_returns_none():
    h = PipelineHistory(pipeline_name="pipe")
    assert detect_spike(h) is None


def test_insufficient_data_returns_none():
    h = _history([0.05, 0.05, 0.05])
    assert detect_spike(h, min_snapshots=5) is None


def test_stable_series_no_spike():
    h = _history([0.05, 0.05, 0.05, 0.05, 0.05])
    result = detect_spike(h, metric="error_rate", ratio_threshold=2.0)
    assert result is not None
    assert not result.is_spike


def test_sudden_spike_detected():
    h = _history([0.05, 0.05, 0.05, 0.05, 0.50])
    result = detect_spike(h, metric="error_rate", ratio_threshold=2.0)
    assert result is not None
    assert result.is_spike
    assert result.ratio > 2.0


def test_spike_result_fields():
    h = _history([0.1, 0.1, 0.1, 0.1, 0.8])
    result = detect_spike(h, metric="error_rate", ratio_threshold=2.0)
    assert result.pipeline == "pipe"
    assert result.metric == "error_rate"
    assert result.current_value == pytest.approx(0.8)
    assert result.baseline_mean == pytest.approx(0.1)


def test_zero_baseline_no_spike():
    h = _history([0.0, 0.0, 0.0, 0.0, 0.0])
    result = detect_spike(h, metric="error_rate")
    assert result is not None
    assert not result.is_spike
    assert "zero" in result.note


def test_to_dict_has_expected_keys():
    h = _history([0.05, 0.05, 0.05, 0.05, 0.5])
    result = detect_spike(h)
    d = result.to_dict()
    for key in ("pipeline", "metric", "current_value", "baseline_mean", "ratio", "is_spike"):
        assert key in d
