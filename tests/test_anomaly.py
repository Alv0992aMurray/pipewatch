"""Tests for pipewatch.anomaly module."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipewatch.anomaly import AnomalyResult, detect_anomaly
from pipewatch.history import MetricSnapshot


def _snap(pipeline: str, success_rate: float, ts: float = 0.0) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline=pipeline,
        timestamp=datetime.fromtimestamp(ts, tz=timezone.utc),
        success_rate=success_rate,
        throughput=100.0,
        error_count=0,
        is_healthy=success_rate >= 0.9,
    )


def _history(rates, pipeline="pipe1"):
    return [_snap(pipeline, r, float(i)) for i, r in enumerate(rates)]


def test_insufficient_data_returns_none():
    snaps = _history([0.95, 0.90])
    assert detect_anomaly(snaps) is None


def test_stable_series_no_anomaly():
    snaps = _history([0.95, 0.96, 0.94, 0.95, 0.95])
    result = detect_anomaly(snaps)
    assert result is not None
    assert result.is_anomaly is False


def test_sudden_drop_detected_as_anomaly():
    snaps = _history([0.95, 0.96, 0.94, 0.95, 0.10])
    result = detect_anomaly(snaps, threshold=2.0)
    assert result is not None
    assert result.is_anomaly is True
    assert result.z_score < -2.0


def test_sudden_spike_detected_as_anomaly():
    snaps = _history([0.50, 0.51, 0.50, 0.49, 0.99])
    result = detect_anomaly(snaps, threshold=2.0)
    assert result is not None
    assert result.is_anomaly is True
    assert result.z_score > 2.0


def test_zero_std_dev_no_anomaly():
    snaps = _history([0.95, 0.95, 0.95, 0.95])
    result = detect_anomaly(snaps)
    assert result is not None
    assert result.z_score == 0.0
    assert result.is_anomaly is False


def test_throughput_metric():
    snaps = _history([0.95] * 5)
    for s in snaps:
        s.throughput = 100.0
    snaps[-1].throughput = 5.0
    result = detect_anomaly(snaps, metric="throughput", threshold=2.0)
    assert result is not None
    assert result.metric == "throughput"


def test_to_dict_keys():
    snaps = _history([0.95, 0.96, 0.94, 0.95, 0.10])
    result = detect_anomaly(snaps)
    d = result.to_dict()
    expected_keys = {
        "pipeline", "metric", "current_value", "mean",
        "std_dev", "z_score", "is_anomaly", "message",
    }
    assert expected_keys == set(d.keys())


def test_invalid_metric_raises():
    snaps = _history([0.95, 0.96, 0.94, 0.95, 0.95])
    with pytest.raises(ValueError, match="Unknown metric"):
        detect_anomaly(snaps, metric="nonexistent")
