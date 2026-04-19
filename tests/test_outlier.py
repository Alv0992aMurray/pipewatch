"""Tests for pipewatch.outlier."""
from __future__ import annotations
import pytest
from datetime import datetime, timezone
from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.outlier import detect_outlier


def _ts(offset: int = 0) -> datetime:
    return datetime(2024, 1, 1, tzinfo=timezone.utc).replace(second=offset)


def _snap(pipeline: str, success_rate: float, i: int = 0) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline=pipeline,
        timestamp=_ts(i),
        success_rate=success_rate,
        throughput=100.0,
        error_rate=1.0 - success_rate,
        latency_p99=0.5,
    )


def _history(rates: list[float]) -> PipelineHistory:
    h = PipelineHistory()
    for i, r in enumerate(rates):
        snap("pipe_a", r, i))
    return h


def test_empty_history_returns_none():
    h = PipelineHistory()
    assert detect_outlier(h) is None


def test_insufficient_data_returns_none():
    h = _history([0.9, 0.8, 0.95])
    assert detect_outlier(h, min_samples=5) is None


def test_stable_series_no_outlier():
    rates = [0.95, 0.96, 0.94, 0.95, 0.96, 0.95]
    result = detect_outlier(_history(rates))
    assert result is not None
    assert result.is_outlier is False
    assert result.direction is None


def test_sudden_drop_detected_as_low_outlier():
    rates = [0.95, 0.96, 0.94, 0.95, 0.96, 0.10]
    result = detect_outlier(_history(rates))
    assert result is not None
    assert result.is_outlier is True
    assert result.direction == "low"


def test_sudden_spike_detected_as_high_outlier():
    rates = [0.50, 0.51, 0.49, 0.50, 0.51, 0.99]
    result = detect_outlier(_history(rates))
    assert result is not None
    assert result.is_outlier is True
    assert result.direction == "high"


def test_result_contains_pipeline_name():
    rates = [0.9, 0.91, 0.89, 0.9, 0.91, 0.9]
    result = detect_outlier(_history(rates))
    assert result is not None
    assert result.pipeline == "pipe_a"


def test_to_dict_keys():
    rates = [0.9, 0.91, 0.89, 0.9, 0.91, 0.9]
    result = detect_outlier(_history(rates))
    d = result.to_dict()
    assert "pipeline" in d
    assert "is_outlier" in d
    assert "lower_fence" in d
    assert "upper_fence" in d


def test_mean_is_approximate():
    rates = [0.9, 0.9, 0.9, 0.9, 0.9, 0.9]
    result = detect_outlier(_history(rates))
    assert result is not None
    assert abs(result.mean - 0.9) < 1e-6
