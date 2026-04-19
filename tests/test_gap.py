"""Tests for pipewatch.gap gap detection."""
from datetime import datetime, timedelta, timezone

import pytest

from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.gap import detect_gaps, GapResult, GapInterval


def _ts(offset_minutes: float) -> datetime:
    return datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc) + timedelta(
        minutes=offset_minutes
    )


def _snap(pipeline: str, offset_minutes: float, success_rate: float = 1.0) -> MetricSnapshot:
    from pipewatch.metrics import PipelineMetric
    m = PipelineMetric(
        pipeline=pipeline,
        rows_processed=100,
        rows_failed=int((1 - success_rate) * 100),
        duration_seconds=10.0,
    )
    s = MetricSnapshot.from_metric(m)
    s.timestamp = _ts(offset_minutes)
    return s


def _history(snaps):
    h = PipelineHistory()
    for s in snaps:
        h.add(s)
    return h


EXPECTED = timedelta(minutes=5)


def test_empty_history_returns_none():
    h = PipelineHistory()
    assert detect_gaps(h, EXPECTED) is None


def test_single_snapshot_returns_none():
    h = _history([_snap("p", 0)])
    assert detect_gaps(h, EXPECTED) is None


def test_no_gap_when_snapshots_on_time():
    snaps = [_snap("p", i * 5) for i in range(4)]
    result = detect_gaps(_history(snaps), EXPECTED)
    assert result is not None
    assert not result.has_gaps
    assert result.gap_count == 0  # via to_dict
    assert result.checked_snapshots == 4


def test_gap_detected_when_interval_exceeds_tolerance():
    # 0 -> 5 -> 5+20 (gap) -> 5+25
    snaps = [
        _snap("pipe", 0),
        _snap("pipe", 5),
        _snap("pipe", 25),  # 20-min gap after previous
        _snap("pipe", 30),
    ]
    result = detect_gaps(_history(snaps), EXPECTED, tolerance=1.5)
    assert result is not None
    assert result.has_gaps
    assert len(result.gaps) == 1
    assert result.gaps[0].duration_seconds == pytest.approx(20 * 60, rel=1e-3)


def test_multiple_gaps_all_detected():
    snaps = [
        _snap("pipe", 0),
        _snap("pipe", 20),   # gap 1
        _snap("pipe", 25),
        _snap("pipe", 50),   # gap 2
    ]
    result = detect_gaps(_history(snaps), EXPECTED)
    assert result is not None
    assert len(result.gaps) == 2


def test_total_gap_seconds_sums_all_gaps():
    snaps = [
        _snap("pipe", 0),
        _snap("pipe", 20),
        _snap("pipe", 45),
    ]
    result = detect_gaps(_history(snaps), EXPECTED)
    assert result is not None
    expected_total = (20 + 25) * 60
    assert result.total_gap_seconds == pytest.approx(expected_total, rel=1e-3)


def test_to_dict_contains_expected_keys():
    snaps = [_snap("pipe", 0), _snap("pipe", 20)]
    result = detect_gaps(_history(snaps), EXPECTED)
    d = result.to_dict()
    assert "pipeline" in d
    assert "gap_count" in d
    assert "total_gap_seconds" in d
    assert "gaps" in d
    assert isinstance(d["gaps"], list)
