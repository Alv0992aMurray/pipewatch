"""Tests for pipewatch.replay."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipewatch.alerts import AlertRule, AlertSeverity
from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.metrics import PipelineMetric
from pipewatch.replay import replay_history, ReplayEvent, ReplayResult, _snapshot_to_metric


def _ts(offset: int = 0) -> datetime:
    return datetime(2024, 1, 1, 12, offset, 0, tzinfo=timezone.utc)


def _snap(rows_processed: int = 100, rows_failed: int = 0, offset: int = 0) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline_name="pipe",
        timestamp=_ts(offset),
        rows_processed=rows_processed,
        rows_failed=rows_failed,
        duration_seconds=10.0,
        tags={},
    )


def _history(*snaps: MetricSnapshot) -> PipelineHistory:
    h = PipelineHistory(pipeline_name="pipe")
    for s in snaps:
        h.add(s)
    return h


@pytest.fixture
def low_success_rule() -> AlertRule:
    return AlertRule(
        name="low_success",
        metric="success_rate",
        operator="lt",
        threshold=0.8,
        severity=AlertSeverity.WARNING,
    )


def test_replay_empty_history_returns_no_events(low_success_rule):
    h = PipelineHistory(pipeline_name="pipe")
    result = replay_history(h, [low_success_rule])
    assert result.total_snapshots == 0
    assert result.total_alerts == 0


def test_replay_healthy_snapshots_produce_no_alerts(low_success_rule):
    h = _history(_snap(100, 0, 0), _snap(200, 5, 1))
    result = replay_history(h, [low_success_rule])
    assert result.total_snapshots == 2
    assert result.total_alerts == 0


def test_replay_failing_snapshot_triggers_alert(low_success_rule):
    h = _history(_snap(100, 50, 0))  # 50% success rate
    result = replay_history(h, [low_success_rule])
    assert result.total_alert_events == 1
    assert result.total_alerts == 1
    assert result.events[0].alerts[0].rule_name == "low_success"


def test_replay_mixed_history_counts_correctly(low_success_rule):
    h = _history(
        _snap(100, 0, 0),   # healthy
        _snap(100, 60, 1),  # failing
        _snap(100, 0, 2),   # healthy
        _snap(100, 90, 3),  # failing
    )
    result = replay_history(h, [low_success_rule])
    assert result.total_snapshots == 4
    assert result.total_alert_events == 2


def test_replay_last_n_limits_snapshots(low_success_rule):
    h = _history(*[_snap(100, 80, i) for i in range(5)])
    result = replay_history(h, [low_success_rule], last_n=2)
    assert result.total_snapshots == 2


def test_snapshot_to_metric_preserves_fields():
    snap = _snap(rows_processed=500, rows_failed=10, offset=3)
    metric = _snapshot_to_metric(snap)
    assert metric.pipeline_name == "pipe"
    assert metric.rows_processed == 500
    assert metric.rows_failed == 10


def test_replay_result_pipeline_name():
    h = _history(_snap())
    result = replay_history(h, [])
    assert result.pipeline_name == "pipe"


def test_replay_event_to_dict_no_alerts():
    snap = _snap()
    event = ReplayEvent(snapshot=snap, alerts=[])
    d = event.to_dict()
    assert d["pipeline"] == "pipe"
    assert d["alerts"] == []
