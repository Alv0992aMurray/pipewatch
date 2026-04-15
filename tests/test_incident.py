"""Tests for pipewatch.incident."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.incident import IncidentTracker, _incident_key


def _alert(pipeline: str = "orders", rule: str = "low_success", sev: AlertSeverity = AlertSeverity.WARNING) -> Alert:
    return Alert(
        pipeline=pipeline,
        rule_name=rule,
        severity=sev,
        message=f"{rule} triggered on {pipeline}",
        value=0.5,
        threshold=0.9,
    )


@pytest.fixture
def tracker() -> IncidentTracker:
    return IncidentTracker()


def test_no_alerts_produces_no_incidents(tracker):
    result = tracker.process([])
    assert result == []


def test_single_alert_opens_one_incident(tracker):
    incidents = tracker.process([_alert()])
    assert len(incidents) == 1
    assert incidents[0].is_open
    assert incidents[0].pipeline == "orders"


def test_duplicate_alert_appended_to_same_incident(tracker):
    tracker.process([_alert()])
    incidents = tracker.process([_alert()])
    assert len(incidents) == 1
    assert len(incidents[0].alerts) == 2


def test_different_rules_open_separate_incidents(tracker):
    incidents = tracker.process([_alert(rule="low_success"), _alert(rule="high_errors")])
    assert len(incidents) == 2


def test_different_pipelines_open_separate_incidents(tracker):
    incidents = tracker.process([_alert(pipeline="orders"), _alert(pipeline="payments")])
    assert len(incidents) == 2


def test_resolve_closes_incident(tracker):
    tracker.process([_alert()])
    resolved = tracker.resolve("orders", "low_success")
    assert resolved is not None
    assert not resolved.is_open
    assert resolved.resolved_at is not None


def test_resolve_unknown_pipeline_returns_none(tracker):
    result = tracker.resolve("nonexistent", "rule")
    assert result is None


def test_resolved_incident_moves_to_closed(tracker):
    tracker.process([_alert()])
    tracker.resolve("orders", "low_success")
    assert len(tracker.open_incidents()) == 0
    assert len(tracker.closed_incidents()) == 1


def test_to_dict_contains_required_keys(tracker):
    tracker.process([_alert()])
    d = tracker.open_incidents()[0].to_dict()
    for key in ("incident_id", "pipeline", "title", "severity", "opened_at", "resolved_at", "alert_count", "is_open"):
        assert key in d


def test_duration_increases_for_open_incident(tracker):
    tracker.process([_alert()])
    inc = tracker.open_incidents()[0]
    assert inc.duration_seconds is not None
    assert inc.duration_seconds >= 0


def test_incident_key_is_deterministic():
    k1 = _incident_key("orders", "low_success")
    k2 = _incident_key("orders", "low_success")
    assert k1 == k2


def test_incident_key_differs_for_different_inputs():
    k1 = _incident_key("orders", "low_success")
    k2 = _incident_key("payments", "low_success")
    assert k1 != k2
