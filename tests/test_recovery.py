"""Tests for pipewatch.recovery."""
from __future__ import annotations

import pytest
from datetime import datetime, timezone

from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.metrics import PipelineMetric
from pipewatch.recovery import detect_recovery


def _ts(offset: int = 0) -> datetime:
    return datetime(2024, 1, 1, 12, offset, 0, tzinfo=timezone.utc)


def _snap(pipeline: str, healthy: bool, offset: int = 0) -> MetricSnapshot:
    metric = PipelineMetric(
        pipeline_name=pipeline,
        rows_processed=100,
        rows_failed=0 if healthy else 50,
        duration_seconds=1.0,
        tags={},
    )
    return MetricSnapshot(
        pipeline=pipeline,
        timestamp=_ts(offset),
        success_rate=1.0 if healthy else 0.5,
        throughput=100.0,
        healthy=healthy,
    )


def _history(pipeline: str, health_seq: list[bool]) -> PipelineHistory:
    h = PipelineHistory(pipeline=pipeline)
    for i, healthy in enumerate(health_seq):
        h.snapshots.append(_snap(pipeline, healthy, offset=i))
    return h


def test_empty_history_returns_none():
    h = PipelineHistory(pipeline="p")
    assert detect_recovery(h) is None


def test_single_snapshot_returns_none():
    h = _history("p", [True])
    assert detect_recovery(h) is None


def test_no_recovery_when_latest_is_unhealthy():
    h = _history("p", [True, True, False])
    result = detect_recovery(h)
    assert result is not None
    assert result.recovered is False
    assert result.previous_failures == 0


def test_recovery_after_single_failure():
    h = _history("p", [True, False, True])
    result = detect_recovery(h, min_prior_failures=1)
    assert result is not None
    assert result.recovered is True
    assert result.previous_failures == 1
    assert result.recovery_snapshot_index == 2


def test_recovery_after_multiple_failures():
    h = _history("p", [True, False, False, False, True])
    result = detect_recovery(h, min_prior_failures=2)
    assert result is not None
    assert result.recovered is True
    assert result.previous_failures == 3


def test_no_recovery_when_prior_failures_below_threshold():
    h = _history("p", [True, False, True])
    result = detect_recovery(h, min_prior_failures=3)
    assert result is not None
    assert result.recovered is False
    assert result.previous_failures == 1


def test_always_healthy_is_not_a_recovery():
    h = _history("p", [True, True, True])
    result = detect_recovery(h, min_prior_failures=1)
    assert result is not None
    assert result.recovered is False
    assert result.previous_failures == 0


def test_recovery_note_contains_failure_count():
    h = _history("p", [False, False, True])
    result = detect_recovery(h, min_prior_failures=1)
    assert result is not None
    assert "2" in result.note
