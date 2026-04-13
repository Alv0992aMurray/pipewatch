"""Tests for pipewatch.retention — RetentionPolicy and pruning logic."""

import pytest
from datetime import datetime, timedelta, timezone

from pipewatch.metrics import PipelineMetric
from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.retention import RetentionPolicy, PruneResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts(minutes_ago: int) -> datetime:
    """Return a UTC datetime *minutes_ago* minutes in the past."""
    return datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)


def _snap(minutes_ago: int, success_rate: float = 0.95) -> MetricSnapshot:
    """Build a minimal MetricSnapshot at a given age."""
    return MetricSnapshot(
        pipeline_id="pipe-a",
        timestamp=_ts(minutes_ago),
        success_rate=success_rate,
        throughput=100.0,
        error_count=5,
        row_count=100,
        is_healthy=True,
    )


def _history_with_snaps(*minutes_ago_values: int) -> PipelineHistory:
    """Build a PipelineHistory containing snapshots at the given ages."""
    h = PipelineHistory(pipeline_id="pipe-a")
    for m in minutes_ago_values:
        h.snapshots.append(_snap(m))
    return h


# ---------------------------------------------------------------------------
# RetentionPolicy construction
# ---------------------------------------------------------------------------

def test_unlimited_policy_has_no_max_age():
    policy = RetentionPolicy()
    assert policy.is_unlimited()


def test_policy_with_max_age_is_not_unlimited():
    policy = RetentionPolicy(max_age_hours=24)
    assert not policy.is_unlimited()


def test_policy_with_max_count_is_not_unlimited():
    policy = RetentionPolicy(max_snapshots=100)
    assert not policy.is_unlimited()


# ---------------------------------------------------------------------------
# Pruning by age
# ---------------------------------------------------------------------------

def test_prune_by_age_removes_old_snapshots():
    # 3 snapshots: 10 min, 70 min, 130 min old; keep only those < 60 min
    history = _history_with_snaps(10, 70, 130)
    policy = RetentionPolicy(max_age_hours=1)  # 60 minutes
    result: PruneResult = policy.prune(history)

    assert result.removed == 2
    assert result.retained == 1
    assert len(history.snapshots) == 1
    assert history.snapshots[0].timestamp >= _ts(60)


def test_prune_by_age_keeps_all_when_within_window():
    history = _history_with_snaps(5, 10, 20)
    policy = RetentionPolicy(max_age_hours=24)
    result = policy.prune(history)

    assert result.removed == 0
    assert result.retained == 3


def test_prune_by_age_removes_all_when_all_expired():
    history = _history_with_snaps(120, 180, 240)
    policy = RetentionPolicy(max_age_hours=1)
    result = policy.prune(history)

    assert result.removed == 3
    assert result.retained == 0
    assert history.snapshots == []


# ---------------------------------------------------------------------------
# Pruning by count
# ---------------------------------------------------------------------------

def test_prune_by_count_keeps_most_recent():
    # 5 snapshots; keep only the 3 most recent (smallest minutes_ago)
    history = _history_with_snaps(50, 40, 30, 20, 10)
    policy = RetentionPolicy(max_snapshots=3)
    result = policy.prune(history)

    assert result.removed == 2
    assert result.retained == 3
    # Remaining snapshots should be the 3 most recent
    ages = [(_ts(0) - s.timestamp).total_seconds() / 60 for s in history.snapshots]
    assert all(a <= 31 for a in ages)  # 10, 20, 30 minutes old


def test_prune_by_count_no_op_when_under_limit():
    history = _history_with_snaps(10, 20)
    policy = RetentionPolicy(max_snapshots=10)
    result = policy.prune(history)

    assert result.removed == 0
    assert result.retained == 2


# ---------------------------------------------------------------------------
# Combined age + count policy
# ---------------------------------------------------------------------------

def test_prune_applies_age_then_count():
    # 5 snapshots: 5, 15, 25, 90, 150 min old
    # max_age_hours=1 removes the 90 and 150 min ones → 3 left
    # max_snapshots=2 then keeps the 2 most recent
    history = _history_with_snaps(5, 15, 25, 90, 150)
    policy = RetentionPolicy(max_age_hours=1, max_snapshots=2)
    result = policy.prune(history)

    assert result.removed == 3
    assert result.retained == 2


# ---------------------------------------------------------------------------
# PruneResult string representation
# ---------------------------------------------------------------------------

def test_prune_result_str():
    r = PruneResult(removed=4, retained=6)
    text = str(r)
    assert "4" in text
    assert "6" in text


def test_prune_result_str_no_removals():
    r = PruneResult(removed=0, retained=10)
    text = str(r)
    assert "0" in text or "no" in text.lower()
