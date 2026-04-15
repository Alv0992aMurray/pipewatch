"""Tests for pipewatch.pattern."""
from __future__ import annotations

import pytest
from datetime import datetime, timezone

from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.pattern import detect_pattern, _is_alternating, _count_consecutive_failures


def _ts(offset: int = 0) -> datetime:
    return datetime(2024, 1, 1, 12, offset, tzinfo=timezone.utc)


def _snap(pipeline: str, healthy: bool, offset: int = 0) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline=pipeline,
        metric="success_rate",
        timestamp=_ts(offset),
        success_rate=1.0 if healthy else 0.5,
        throughput=100.0,
        error_count=0 if healthy else 10,
        is_healthy=healthy,
    )


def _history(snaps):
    h = PipelineHistory()
    for s in snaps:
        h.add(s)
    return h


def test_empty_history_returns_none():
    h = PipelineHistory()
    assert detect_pattern(h) is None


def test_all_healthy_is_stable():
    h = _history([_snap("pipe", True, i) for i in range(5)])
    result = detect_pattern(h)
    assert result is not None
    assert result.pattern_label == "stable_healthy"
    assert result.failure_runs == 0
    assert result.consecutive_failures == 0


def test_sustained_failure_detected():
    snaps = [_snap("pipe", True, 0)] + [_snap("pipe", False, i) for i in range(1, 5)]
    h = _history(snaps)
    result = detect_pattern(h)
    assert result.pattern_label == "sustained_failure"
    assert result.consecutive_failures == 4


def test_flapping_detected():
    snaps = [_snap("pipe", i % 2 == 0, i) for i in range(6)]
    h = _history(snaps)
    result = detect_pattern(h)
    assert result.pattern_label == "flapping"
    assert result.alternating is True


def test_occasional_failure_low_rate():
    snaps = [_snap("pipe", True, i) for i in range(9)] + [_snap("pipe", False, 9)]
    h = _history(snaps)
    result = detect_pattern(h)
    assert result.pattern_label == "occasional_failure"
    assert result.failure_runs == 1


def test_degraded_high_failure_rate():
    snaps = [_snap("pipe", i % 3 != 0, i) for i in range(9)]
    h = _history(snaps)
    result = detect_pattern(h)
    assert result.pattern_label in ("degraded", "sustained_failure", "flapping")


def test_result_fields_populated():
    h = _history([_snap("etl", True, i) for i in range(3)])
    r = detect_pattern(h)
    assert r.pipeline == "etl"
    assert r.metric == "success_rate"
    assert r.total_snapshots == 3


def test_is_alternating_true():
    assert _is_alternating([True, False, True, False]) is True


def test_is_alternating_false_for_uniform():
    assert _is_alternating([True, True, True, True]) is False


def test_count_consecutive_failures_from_tail():
    assert _count_consecutive_failures([True, False, False, False]) == 3
    assert _count_consecutive_failures([False, False, True, False]) == 1
    assert _count_consecutive_failures([True, True]) == 0
