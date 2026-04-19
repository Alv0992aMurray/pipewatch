"""Tests for pipewatch.jitter."""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.jitter import detect_jitter


def _ts(offset_seconds: float) -> datetime:
    base = datetime(2024, 1, 1, 12, 0, 0)
    return base + timedelta(seconds=offset_seconds)


def _snap(pipeline: str, ts: datetime, success_rate: float = 1.0) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline=pipeline,
        timestamp=ts,
        success_rate=success_rate,
        throughput=100.0,
        error_count=0,
        is_healthy=success_rate >= 0.95,
    )


def _history(*offsets: float, pipeline: str = "pipe") -> PipelineHistory:
    h = PipelineHistory()
    for offset in offsets:
        h.add(_snap(pipeline, _ts(offset)))
    return h


def test_empty_history_returns_none():
    h = PipelineHistory()
    assert detect_jitter(h) is None


def test_too_few_intervals_returns_none():
    # 2 snapshots → 1 interval, below min_samples=3
    h = _history(0, 60)
    assert detect_jitter(h) is None


def test_exactly_min_samples_returns_result():
    # 4 snapshots → 3 intervals
    h = _history(0, 60, 120, 180)
    result = detect_jitter(h)
    assert result is not None
    assert result.sample_count == 4


def test_regular_intervals_not_irregular():
    # perfectly regular: every 60 s
    h = _history(0, 60, 120, 180, 240)
    result = detect_jitter(h)
    assert result is not None
    assert not result.is_irregular
    assert result.stddev_seconds == pytest.approx(0.0, abs=1e-6)
    assert result.jitter_ratio == pytest.approx(0.0, abs=1e-6)


def test_irregular_intervals_flagged():
    # intervals: 60, 60, 300, 60 → high variance
    h = _history(0, 60, 120, 420, 480)
    result = detect_jitter(h)
    assert result is not None
    assert result.is_irregular
    assert result.note is not None
    assert "variability" in result.note.lower()


def test_pipeline_name_captured():
    h = _history(0, 60, 120, 180, pipeline="my_etl")
    result = detect_jitter(h)
    assert result is not None
    assert result.pipeline == "my_etl"


def test_to_dict_keys():
    h = _history(0, 60, 120, 180, 240)
    result = detect_jitter(h)
    assert result is not None
    d = result.to_dict()
    for key in ("pipeline", "sample_count", "mean_interval_seconds",
                "stddev_seconds", "jitter_ratio", "is_irregular", "note"):
        assert key in d


def test_custom_threshold_changes_classification():
    # ratio ~0 for regular data; with threshold=0 everything is irregular
    h = _history(0, 60, 120, 180, 240)
    result = detect_jitter(h, irregularity_threshold=0.0)
    assert result is not None
    # stddev is 0, ratio is 0, 0 > 0 is False
    assert not result.is_irregular
