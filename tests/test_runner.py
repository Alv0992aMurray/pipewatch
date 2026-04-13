"""Tests for pipewatch.runner module."""

from __future__ import annotations

import pytest

from pipewatch.alerts import AlertRule, AlertSeverity
from pipewatch.metrics import PipelineMetric
from pipewatch.runner import RunResult, run_checks


@pytest.fixture
def healthy_metric():
    return PipelineMetric(
        pipeline_name="test_pipe",
        rows_in=1000,
        rows_out=990,
        error_count=1,
        duration_seconds=1.0,
    )


@pytest.fixture
def failing_metric():
    return PipelineMetric(
        pipeline_name="test_pipe",
        rows_in=1000,
        rows_out=400,
        error_count=100,
        duration_seconds=30.0,
    )


@pytest.fixture
def critical_rule():
    return AlertRule(
        name="low_success",
        metric="success_rate",
        operator="lt",
        threshold=0.5,
        severity=AlertSeverity.CRITICAL,
        message="Success rate critically low",
    )


@pytest.fixture
def warning_rule():
    return AlertRule(
        name="high_errors",
        metric="error_count",
        operator="gt",
        threshold=50,
        severity=AlertSeverity.WARNING,
        message="High error count",
    )


def test_run_checks_no_alerts(healthy_metric, critical_rule):
    result = run_checks(healthy_metric, [critical_rule])
    assert isinstance(result, RunResult)
    assert not result.has_alerts
    assert not result.has_critical


def test_run_checks_triggers_alert(failing_metric, critical_rule):
    result = run_checks(failing_metric, [critical_rule])
    assert result.has_alerts
    assert result.has_critical
    assert len(result.alerts) == 1
    assert result.alerts[0].rule_name == "low_success"


def test_run_checks_multiple_rules(failing_metric, critical_rule, warning_rule):
    result = run_checks(failing_metric, [critical_rule, warning_rule])
    assert len(result.alerts) == 2


def test_run_checks_report_populated(failing_metric, critical_rule):
    result = run_checks(failing_metric, [critical_rule])
    assert "timestamp" in result.report
    assert result.report["alert_count"] == 1
    assert result.report["healthy"] is False


def test_run_checks_empty_rules(healthy_metric):
    result = run_checks(healthy_metric, [])
    assert not result.has_alerts
    assert result.report["alert_count"] == 0


def test_run_checks_warning_does_not_set_critical(failing_metric, warning_rule):
    """A triggered WARNING-severity alert should not mark the result as critical."""
    result = run_checks(failing_metric, [warning_rule])
    assert result.has_alerts
    assert not result.has_critical
    assert len(result.alerts) == 1
    assert result.alerts[0].rule_name == "high_errors"
