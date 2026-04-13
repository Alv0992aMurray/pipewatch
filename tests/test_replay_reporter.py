"""Tests for pipewatch.replay_reporter."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.history import MetricSnapshot
from pipewatch.replay import ReplayEvent, ReplayResult
from pipewatch.replay_reporter import format_replay_result, replay_to_json


def _ts() -> datetime:
    return datetime(2024, 6, 1, 9, 0, 0, tzinfo=timezone.utc)


def _snap() -> MetricSnapshot:
    return MetricSnapshot(
        pipeline_name="orders",
        timestamp=_ts(),
        rows_processed=100,
        rows_failed=0,
        duration_seconds=5.0,
        tags={},
    )


def _alert() -> Alert:
    return Alert(
        rule_name="low_success",
        severity=AlertSeverity.WARNING,
        message="success_rate 0.50 < 0.80",
        pipeline_name="orders",
        metric_value=0.5,
    )


@pytest.fixture
def empty_result() -> ReplayResult:
    return ReplayResult(pipeline_name="orders")


@pytest.fixture
def result_with_events() -> ReplayResult:
    r = ReplayResult(pipeline_name="orders")
    r.events.append(ReplayEvent(snapshot=_snap(), alerts=[]))
    r.events.append(ReplayEvent(snapshot=_snap(), alerts=[_alert()]))
    return r


def test_format_empty_result_mentions_no_snapshots(empty_result):
    out = format_replay_result(empty_result)
    assert "no snapshots" in out


def test_format_result_shows_pipeline_name(result_with_events):
    out = format_replay_result(result_with_events)
    assert "orders" in out


def test_format_result_shows_snapshot_count(result_with_events):
    out = format_replay_result(result_with_events)
    assert "snapshots=2" in out


def test_format_result_shows_alert_events(result_with_events):
    out = format_replay_result(result_with_events)
    assert "alert events=1" in out


def test_format_result_shows_rule_name(result_with_events):
    out = format_replay_result(result_with_events)
    assert "low_success" in out


def test_json_output_is_valid(result_with_events):
    raw = replay_to_json(result_with_events)
    data = json.loads(raw)
    assert data["pipeline"] == "orders"
    assert data["total_snapshots"] == 2
    assert data["total_alert_events"] == 1
    assert len(data["events"]) == 2


def test_json_empty_result(empty_result):
    raw = replay_to_json(empty_result)
    data = json.loads(raw)
    assert data["total_snapshots"] == 0
    assert data["events"] == []
