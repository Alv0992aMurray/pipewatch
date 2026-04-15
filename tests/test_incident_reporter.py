"""Tests for pipewatch.incident_reporter."""

from __future__ import annotations

import json

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.incident import IncidentTracker
from pipewatch.incident_reporter import (
    format_incident,
    format_incident_report,
    incident_report_to_json,
)


def _alert(pipeline: str = "orders", rule: str = "low_success", sev: AlertSeverity = AlertSeverity.WARNING) -> Alert:
    return Alert(
        pipeline=pipeline,
        rule_name=rule,
        severity=sev,
        message=f"{rule} triggered",
        value=0.5,
        threshold=0.9,
    )


@pytest.fixture
def tracker_with_open() -> IncidentTracker:
    t = IncidentTracker()
    t.process([_alert()])
    return t


@pytest.fixture
def tracker_with_resolved() -> IncidentTracker:
    t = IncidentTracker()
    t.process([_alert()])
    t.resolve("orders", "low_success")
    return t


def test_format_incident_contains_pipeline(tracker_with_open):
    inc = tracker_with_open.open_incidents()[0]
    text = format_incident(inc)
    assert "orders" in text


def test_format_incident_shows_open_status(tracker_with_open):
    inc = tracker_with_open.open_incidents()[0]
    text = format_incident(inc)
    assert "OPEN" in text


def test_format_incident_shows_resolved_status(tracker_with_resolved):
    inc = tracker_with_resolved.closed_incidents()[0]
    text = format_incident(inc)
    assert "RESOLVED" in text


def test_format_incident_shows_alert_count(tracker_with_open):
    inc = tracker_with_open.open_incidents()[0]
    text = format_incident(inc)
    assert "1" in text


def test_format_report_no_open_shows_none_label():
    text = format_incident_report([], [])
    assert "(none)" in text


def test_format_report_with_open_incidents(tracker_with_open):
    open_inc = tracker_with_open.open_incidents()
    text = format_incident_report(open_inc, [])
    assert "Open Incidents" in text
    assert "orders" in text


def test_format_report_with_resolved_incidents(tracker_with_resolved):
    closed = tracker_with_resolved.closed_incidents()
    text = format_incident_report([], closed)
    assert "Resolved Incidents" in text


def test_incident_report_to_json_structure(tracker_with_open):
    open_inc = tracker_with_open.open_incidents()
    result = json.loads(incident_report_to_json(open_inc, []))
    assert "open" in result
    assert "closed" in result
    assert len(result["open"]) == 1


def test_incident_report_to_json_fields(tracker_with_open):
    open_inc = tracker_with_open.open_incidents()
    result = json.loads(incident_report_to_json(open_inc, []))
    entry = result["open"][0]
    assert entry["is_open"] is True
    assert entry["pipeline"] == "orders"
