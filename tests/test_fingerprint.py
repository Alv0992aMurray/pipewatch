"""Tests for pipewatch.fingerprint and pipewatch.fingerprint_reporter."""

from __future__ import annotations

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.fingerprint import (
    Fingerprint,
    FingerprintedAlert,
    fingerprint_alert,
    fingerprint_alerts,
    group_by_fingerprint,
    _compute_digest,
)
from pipewatch.fingerprint_reporter import (
    format_fingerprinted_alert,
    format_fingerprint_report,
    fingerprint_report_to_json,
)


@pytest.fixture
def warning_alert() -> Alert:
    return Alert(
        pipeline="orders",
        rule_name="low_success_rate",
        severity=AlertSeverity.WARNING,
        message="Success rate 80.0% is below threshold 90.0%",
    )


@pytest.fixture
def critical_alert() -> Alert:
    return Alert(
        pipeline="payments",
        rule_name="high_error_rate",
        severity=AlertSeverity.CRITICAL,
        message="Error rate 25.0% exceeds threshold 10.0%",
    )


def test_fingerprint_has_correct_fields(warning_alert):
    fp = fingerprint_alert(warning_alert)
    assert fp.pipeline == "orders"
    assert fp.rule_name == "low_success_rate"
    assert fp.severity == "warning"
    assert len(fp.digest) == 12


def test_same_alert_produces_same_digest(warning_alert):
    fp1 = fingerprint_alert(warning_alert)
    fp2 = fingerprint_alert(warning_alert)
    assert fp1.digest == fp2.digest


def test_different_pipeline_produces_different_digest(warning_alert, critical_alert):
    fp1 = fingerprint_alert(warning_alert)
    fp2 = fingerprint_alert(critical_alert)
    assert fp1.digest != fp2.digest


def test_fingerprint_str_format(warning_alert):
    fp = fingerprint_alert(warning_alert)
    s = str(fp)
    assert s.startswith("orders/low_success_rate/warning#")
    assert len(s.split("#")[1]) == 12


def test_fingerprint_to_dict(warning_alert):
    fp = fingerprint_alert(warning_alert)
    d = fp.to_dict()
    assert d["pipeline"] == "orders"
    assert d["rule_name"] == "low_success_rate"
    assert "digest" in d


def test_fingerprint_alerts_returns_list(warning_alert, critical_alert):
    results = fingerprint_alerts([warning_alert, critical_alert])
    assert len(results) == 2
    assert all(isinstance(r, FingerprintedAlert) for r in results)


def test_group_by_fingerprint_groups_duplicates(warning_alert):
    groups = group_by_fingerprint([warning_alert, warning_alert, warning_alert])
    assert len(groups) == 1
    key = list(groups.keys())[0]
    assert len(groups[key]) == 3


def test_group_by_fingerprint_separates_distinct(warning_alert, critical_alert):
    groups = group_by_fingerprint([warning_alert, critical_alert])
    assert len(groups) == 2


def test_format_fingerprinted_alert(warning_alert):
    fa = fingerprint_alerts([warning_alert])[0]
    line = format_fingerprinted_alert(fa)
    assert "WARNING" in line
    assert "orders" in line
    assert "low_success_rate" in line


def test_format_fingerprint_report_empty():
    report = format_fingerprint_report([])
    assert "No alerts" in report


def test_format_fingerprint_report_contains_pipeline(warning_alert):
    report = format_fingerprint_report([warning_alert])
    assert "orders" in report
    assert "1 unique alert type" in report


def test_fingerprint_report_to_json_is_valid(warning_alert, critical_alert):
    import json
    result = fingerprint_report_to_json([warning_alert, critical_alert])
    data = json.loads(result)
    assert isinstance(data, list)
    assert len(data) == 2
    assert all("fingerprint" in item for item in data)
