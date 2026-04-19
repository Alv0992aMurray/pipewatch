"""Tests for pipewatch.latency."""
from datetime import datetime, timedelta

import pytest

from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.latency import detect_latency, LatencyResult


def _ts(offset_seconds: int) -> datetime:
    return datetime(2024, 1, 1, 12, 0, 0) + timedelta(seconds=offset_seconds)


def _snap(offset_seconds: int, success_rate: float = 1.0) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline="pipe",
        timestamp=_ts(offset_seconds),
        success_rate=success_rate,
        throughput=100.0,
        error_count=0,
        is_healthy=True,
    )


def _history(*snaps) -> PipelineHistory:
    h = PipelineHistory(pipeline="pipe")
    for s in snaps:
        h.add(s)
    return h


def test_empty_history_returns_none():
    h = _history()
    assert detect_latency(h) is None


def test_single_snapshot_returns_none():
    h = _history(_snap(0))
    assert detect_latency(h) is None


def test_two_snapshots_compute_interval():
    h = _history(_snap(0), _snap(60))
    result = detect_latency(h, threshold_seconds=300.0)
    assert result is not None
    assert result.sample_count == 1
    assert result.avg_seconds == pytest.approx(60.0)
    assert result.min_seconds == pytest.approx(60.0)
    assert result.max_seconds == pytest.approx(60.0)
    assert result.is_high is False


def test_high_latency_flagged():
    h = _history(_snap(0), _snap(600))
    result = detect_latency(h, threshold_seconds=300.0)
    assert result is not None
    assert result.is_high is True


def test_multiple_intervals_averaged():
    h = _history(_snap(0), _snap(100), _snap(300))
    result = detect_latency(h, threshold_seconds=500.0)
    assert result is not None
    assert result.sample_count == 2
    assert result.avg_seconds == pytest.approx(150.0)
    assert result.min_seconds == pytest.approx(100.0)
    assert result.max_seconds == pytest.approx(200.0)


def test_to_dict_has_expected_keys():
    h = _history(_snap(0), _snap(120))
    result = detect_latency(h)
    d = result.to_dict()
    for key in ("pipeline", "sample_count", "avg_seconds", "min_seconds", "max_seconds", "is_high", "threshold_seconds"):
        assert key in d


def test_pipeline_name_preserved():
    h = _history(_snap(0), _snap(60))
    result = detect_latency(h)
    assert result.pipeline == "pipe"
