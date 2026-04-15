"""Tests for pipewatch.triage."""

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.triage import (
    TriageEntry,
    TriageResult,
    _classify,
    _priority,
    triage_alerts,
)


def _alert(rule_name: str, severity: AlertSeverity = AlertSeverity.WARNING) -> Alert:
    return Alert(
        pipeline="test_pipeline",
        rule_name=rule_name,
        severity=severity,
        message=f"Alert: {rule_name}",
        value=0.5,
        threshold=0.8,
    )


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def test_classify_success_rate_is_data_quality():
    assert _classify(_alert("low_success_rate")) == "data_quality"


def test_classify_throughput_rule():
    assert _classify(_alert("low_throughput")) == "throughput"


def test_classify_latency_is_availability():
    assert _classify(_alert("high_latency")) == "availability"


def test_classify_unknown_rule_is_other():
    assert _classify(_alert("some_custom_rule")) == "other"


def test_classify_error_rate_is_data_quality():
    assert _classify(_alert("high_error_rate")) == "data_quality"


# ---------------------------------------------------------------------------
# Priority
# ---------------------------------------------------------------------------

def test_critical_availability_has_lowest_priority_number():
    alert = _alert("schedule_lag", AlertSeverity.CRITICAL)
    p = _priority(alert, "availability")
    assert p == 1


def test_critical_data_quality_priority():
    alert = _alert("low_success_rate", AlertSeverity.CRITICAL)
    p = _priority(alert, "data_quality")
    assert p == 2


def test_warning_other_has_highest_priority_number():
    alert = _alert("some_rule", AlertSeverity.WARNING)
    p = _priority(alert, "other")
    assert p == 4


# ---------------------------------------------------------------------------
# triage_alerts
# ---------------------------------------------------------------------------

def test_triage_empty_list_returns_empty_result():
    result = triage_alerts([])
    assert isinstance(result, TriageResult)
    assert result.entries == []


def test_triage_single_alert_produces_one_entry():
    result = triage_alerts([_alert("low_success_rate")])
    assert len(result.entries) == 1
    entry = result.entries[0]
    assert isinstance(entry, TriageEntry)
    assert entry.category == "data_quality"
    assert entry.suggested_action != ""


def test_sorted_entries_ordered_by_priority():
    alerts = [
        _alert("some_custom_rule", AlertSeverity.WARNING),   # priority 4
        _alert("schedule_lag", AlertSeverity.CRITICAL),       # priority 1
        _alert("low_success_rate", AlertSeverity.CRITICAL),   # priority 2
    ]
    result = triage_alerts(alerts)
    priorities = [e.priority for e in result.sorted_entries()]
    assert priorities == sorted(priorities)


def test_critical_entries_filters_correctly():
    alerts = [
        _alert("rule_a", AlertSeverity.CRITICAL),
        _alert("rule_b", AlertSeverity.WARNING),
    ]
    result = triage_alerts(alerts)
    critical = result.critical_entries()
    assert len(critical) == 1
    assert critical[0].alert.severity == AlertSeverity.CRITICAL


def test_to_dict_contains_triage_key():
    result = triage_alerts([_alert("low_throughput")])
    d = result.to_dict()
    assert "triage" in d
    assert len(d["triage"]) == 1
    assert d["triage"][0]["category"] == "throughput"
