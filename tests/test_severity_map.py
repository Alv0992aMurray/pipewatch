"""Tests for pipewatch.severity_map."""

import pytest
from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.severity_map import (
    filter_by_min_severity,
    max_severity,
    severity_level,
    summarise_severities,
)


def _alert(severity: AlertSeverity, pipeline: str = "pipe") -> Alert:
    return Alert(
        pipeline=pipeline,
        rule_name="test_rule",
        message="test message",
        severity=severity,
        value=0.5,
    )


def test_severity_level_ordering():
    assert severity_level(AlertSeverity.INFO) < severity_level(AlertSeverity.WARNING)
    assert severity_level(AlertSeverity.WARNING) < severity_level(AlertSeverity.CRITICAL)


def test_max_severity_empty_returns_none():
    assert max_severity([]) is None


def test_max_severity_single_alert():
    assert max_severity([_alert(AlertSeverity.WARNING)]) == AlertSeverity.WARNING


def test_max_severity_returns_highest():
    alerts = [
        _alert(AlertSeverity.INFO),
        _alert(AlertSeverity.CRITICAL),
        _alert(AlertSeverity.WARNING),
    ]
    assert max_severity(alerts) == AlertSeverity.CRITICAL


def test_filter_by_min_severity_keeps_at_and_above():
    alerts = [
        _alert(AlertSeverity.INFO),
        _alert(AlertSeverity.WARNING),
        _alert(AlertSeverity.CRITICAL),
    ]
    result = filter_by_min_severity(alerts, AlertSeverity.WARNING)
    severities = {a.severity for a in result}
    assert AlertSeverity.INFO not in severities
    assert AlertSeverity.WARNING in severities
    assert AlertSeverity.CRITICAL in severities


def test_filter_by_min_severity_critical_only():
    alerts = [
        _alert(AlertSeverity.INFO),
        _alert(AlertSeverity.WARNING),
        _alert(AlertSeverity.CRITICAL),
    ]
    result = filter_by_min_severity(alerts, AlertSeverity.CRITICAL)
    assert len(result) == 1
    assert result[0].severity == AlertSeverity.CRITICAL


def test_filter_by_min_severity_empty_input():
    assert filter_by_min_severity([], AlertSeverity.INFO) == []


def test_summarise_severities_empty():
    s = summarise_severities([])
    assert s.total() == 0
    assert s.info == 0
    assert s.warning == 0
    assert s.critical == 0


def test_summarise_severities_counts_correctly():
    alerts = [
        _alert(AlertSeverity.INFO),
        _alert(AlertSeverity.WARNING),
        _alert(AlertSeverity.WARNING),
        _alert(AlertSeverity.CRITICAL),
    ]
    s = summarise_severities(alerts)
    assert s.info == 1
    assert s.warning == 2
    assert s.critical == 1
    assert s.total() == 4


def test_summarise_to_dict_keys():
    s = summarise_severities([_alert(AlertSeverity.CRITICAL)])
    d = s.to_dict()
    assert set(d.keys()) == {"info", "warning", "critical", "total"}
