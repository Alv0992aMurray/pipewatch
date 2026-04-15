"""Tests for pipewatch.flapping."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipewatch.history import MetricSnapshot, PipelineHistory
from pipewatch.flapping import detect_flapping, FlappingResult


def _ts(offset: int = 0) -> datetime:
    return datetime(2024, 1, 1, 12, offset, 0, tzinfo=timezone.utc)


def _snap(healthy: bool, offset: int = 0) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline="pipe_a",
        timestamp=_ts(offset),
        success_rate=1.0 if healthy else 0.2,
        throughput=100.0,
        error_rate=0.0 if healthy else 0.8,
        is_healthy=healthy,
    )


def _history(health_pattern: list[bool]) -> PipelineHistory:
    h = PipelineHistory(pipeline="pipe_a")
    for i, healthy in enumerate(health_pattern):
        h.add(_snap(healthy, offset=i))
    return h


def test_empty_history_returns_none():
    h = PipelineHistory(pipeline="pipe_a")
    assert detect_flapping(h) is None


def test_single_snapshot_returns_none():
    h = _history([True])
    assert detect_flapping(h) is None


def test_stable_healthy_not_flapping():
    h = _history([True, True, True, True, True])
    result = detect_flapping(h, window=5, min_transitions=3)
    assert result is not None
    assert result.is_flapping is False
    assert result.transitions == 0
    assert result.note is None


def test_stable_unhealthy_not_flapping():
    h = _history([False, False, False, False])
    result = detect_flapping(h, window=4, min_transitions=3)
    assert result is not None
    assert result.is_flapping is False


def test_alternating_pattern_is_flapping():
    h = _history([True, False, True, False, True, False])
    result = detect_flapping(h, window=6, min_transitions=3)
    assert result is not None
    assert result.is_flapping is True
    assert result.transitions == 5
    assert result.note is not None
    assert "5" in result.note


def test_transitions_exactly_at_threshold_is_flapping():
    # T F T → 2 transitions; min_transitions=2 → flapping
    h = _history([True, False, True])
    result = detect_flapping(h, window=3, min_transitions=2)
    assert result is not None
    assert result.is_flapping is True


def test_transitions_below_threshold_not_flapping():
    # T F T → 2 transitions; min_transitions=3 → not flapping
    h = _history([True, False, True])
    result = detect_flapping(h, window=3, min_transitions=3)
    assert result is not None
    assert result.is_flapping is False


def test_result_to_dict_keys():
    h = _history([True, False, True, False])
    result = detect_flapping(h, window=4, min_transitions=2)
    assert result is not None
    d = result.to_dict()
    assert set(d.keys()) == {
        "pipeline", "window_size", "transitions",
        "is_flapping", "health_sequence", "note",
    }


def test_window_limits_snapshots_considered():
    # 6 snapshots but window=2 → only last 2 examined
    h = _history([True, False, True, False, True, True])
    result = detect_flapping(h, window=2, min_transitions=2)
    assert result is not None
    # last 2 are [True, True] → 0 transitions
    assert result.transitions == 0
    assert result.is_flapping is False
