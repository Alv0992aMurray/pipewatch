"""Tests for pipewatch.ceiling."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.ceiling import detect_ceiling, CeilingResult


def _ts(offset: int = 0) -> datetime:
    return datetime(2024, 1, 1, 12, offset, 0, tzinfo=timezone.utc)


def _snap(pipeline: str, success_rate: float, offset: int = 0) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline=pipeline,
        timestamp=_ts(offset),
        success_rate=success_rate,
        throughput=100.0,
        error_count=0,
        is_healthy=success_rate >= 0.95,
    )


def _history(snaps: list[MetricSnapshot]) -> PipelineHistory:
    h = PipelineHistory(pipeline_name=snaps[0].pipeline if snaps else "test")
    for s in snaps:
        h.add(s)
    return h


def test_empty_history_returns_none():
    h = PipelineHistory(pipeline_name="pipe")
    assert detect_ceiling(h) is None


def test_insufficient_data_sets_flag():
    snaps = [_snap("pipe", 0.99, i) for i in range(3)]  # fewer than default min 5
    result = detect_ceiling(_history(snaps), min_snapshots=5)
    assert result is not None
    assert result.insufficient_data is True
    assert result.at_ceiling is False


def test_stable_high_series_detected_as_ceiling():
    # all values at 0.99 — clearly at ceiling
    snaps = [_snap("pipe", 0.99, i) for i in range(10)]
    result = detect_ceiling(_history(snaps), threshold=0.98, required_ratio=0.8)
    assert result is not None
    assert result.insufficient_data is False
    assert result.at_ceiling is True
    assert result.ratio_at_ceiling == pytest.approx(1.0)


def test_low_series_not_at_ceiling():
    snaps = [_snap("pipe", 0.70, i) for i in range(10)]
    result = detect_ceiling(_history(snaps), threshold=0.98)
    assert result is not None
    assert result.at_ceiling is False


def test_mixed_series_below_required_ratio():
    # 4 out of 10 at ceiling — below default 0.8 ratio
    high = [_snap("pipe", 0.99, i) for i in range(4)]
    low = [_snap("pipe", 0.60, i + 4) for i in range(6)]
    result = detect_ceiling(_history(high + low), threshold=0.98, required_ratio=0.8)
    assert result is not None
    assert result.at_ceiling is False
    assert result.ratio_at_ceiling == pytest.approx(0.4)


def test_ceiling_value_is_max_of_series():
    snaps = [_snap("pipe", v, i) for i, v in enumerate([0.95, 0.97, 0.99, 0.98, 0.96])]
    result = detect_ceiling(_history(snaps))
    assert result is not None
    assert result.ceiling_value == pytest.approx(0.99)


def test_pipeline_name_preserved():
    snaps = [_snap("my_pipeline", 0.99, i) for i in range(6)]
    result = detect_ceiling(_history(snaps))
    assert result is not None
    assert result.pipeline == "my_pipeline"


def test_to_dict_contains_expected_keys():
    snaps = [_snap("pipe", 0.99, i) for i in range(6)]
    result = detect_ceiling(_history(snaps))
    assert result is not None
    d = result.to_dict()
    for key in ("pipeline", "metric", "at_ceiling", "ceiling_value",
                "ratio_at_ceiling", "sample_count", "insufficient_data", "note"):
        assert key in d


def test_note_set_when_at_ceiling():
    snaps = [_snap("pipe", 0.99, i) for i in range(10)]
    result = detect_ceiling(_history(snaps))
    assert result is not None
    assert result.at_ceiling is True
    assert result.note != ""


def test_note_empty_when_not_at_ceiling():
    snaps = [_snap("pipe", 0.70, i) for i in range(10)]
    result = detect_ceiling(_history(snaps))
    assert result is not None
    assert result.note == ""
