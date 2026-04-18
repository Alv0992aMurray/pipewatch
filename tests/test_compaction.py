"""Tests for pipewatch.compaction."""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipewatch.history import MetricSnapshot, PipelineHistory
from pipewatch.compaction import compact_history, CompactionResult


def _ts(minutes_ago: int) -> datetime:
    return datetime(2024, 1, 1, 12, 0, 0) - timedelta(minutes=minutes_ago)


def _snap(minutes_ago: int, success_rate: float = 0.95, healthy: bool = True) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline="pipe_a",
        timestamp=_ts(minutes_ago),
        success_rate=success_rate,
        throughput=100.0,
        healthy=healthy,
    )


NOW = datetime(2024, 1, 1, 12, 0, 0)


@pytest.fixture
def history() -> PipelineHistory:
    h = PipelineHistory(pipeline="pipe_a")
    for snap in [
        _snap(120), _snap(105), _snap(90), _snap(75),  # older than 60 min
        _snap(45), _snap(30), _snap(15), _snap(5),     # within 60 min
    ]:
        h.add(snap)
    return h


def test_recent_snapshots_are_retained(history):
    result = compact_history(history, retain_minutes=60, bucket_minutes=15, now=NOW)
    assert len(result.retained_snapshots) == 4


def test_older_snapshots_are_bucketed(history):
    result = compact_history(history, retain_minutes=60, bucket_minutes=15, now=NOW)
    assert result.total_buckets > 0
    total_in_buckets = sum(b.snapshot_count for b in result.buckets)
    assert total_in_buckets == 4


def test_empty_history_returns_no_buckets():
    h = PipelineHistory(pipeline="empty")
    result = compact_history(h, retain_minutes=60, bucket_minutes=15, now=NOW)
    assert result.total_buckets == 0
    assert len(result.retained_snapshots) == 0


def test_all_recent_produces_no_buckets():
    h = PipelineHistory(pipeline="fresh")
    for m in [5, 10, 20]:
        h.add(_snap(m))
    result = compact_history(h, retain_minutes=60, bucket_minutes=15, now=NOW)
    assert result.total_buckets == 0
    assert len(result.retained_snapshots) == 3


def test_bucket_avg_success_rate_is_correct():
    h = PipelineHistory(pipeline="pipe_b")
    h.add(_snap(90, success_rate=0.8))
    h.add(_snap(91, success_rate=0.6))
    result = compact_history(h, retain_minutes=60, bucket_minutes=60, now=NOW)
    assert result.total_buckets == 1
    assert abs(result.buckets[0].avg_success_rate - 0.7) < 1e-6


def test_any_unhealthy_flag_set_when_unhealthy_snap_present():
    h = PipelineHistory(pipeline="pipe_c")
    h.add(_snap(90, healthy=True))
    h.add(_snap(91, healthy=False))
    result = compact_history(h, retain_minutes=60, bucket_minutes=60, now=NOW)
    assert result.buckets[0].any_unhealthy is True


def test_to_dict_contains_expected_keys():
    h = PipelineHistory(pipeline="pipe_d")
    h.add(_snap(90))
    result = compact_history(h, retain_minutes=60, bucket_minutes=15, now=NOW)
    d = result.to_dict()
    assert "pipeline" in d
    assert "buckets" in d
    assert "total_buckets" in d
    assert "retained_snapshots" in d
