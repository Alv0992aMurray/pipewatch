"""Tests for alert rule evaluation logic."""

import pytest

from pipewatch.alerts import Alert, AlertRule, AlertSeverity, evaluate_rules
from pipewatch.metrics import PipelineMetric


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def healthy_metric():
    return PipelineMetric(
        pipeline_name="orders",
        rows_processed=1000,
        rows_failed=10,
        error_count=10,
        duration_seconds=50.0,
    )


@pytest.fixture
def failing_metric():
    return PipelineMetric(
        pipeline_name="orders",
        rows_processed=1000,
        rows_failed=300,
        error_count=300,
        duration_seconds=50.0,
    )


@pytest.fixture
def low_success_rule():
    return AlertRule(
        name="low_success_rate",
        metric="success_rate",
        threshold=0.95,
        operator="lt",
        severity=AlertSeverity.CRITICAL,
    )


@pytest.fixture
def high_error_rule():
    return AlertRule(
        name="high_error_count",
        metric="error_count",
        threshold=50,
        operator="gt",
        severity=AlertSeverity.WARNING,
    )


# ---------------------------------------------------------------------------
# AlertRule.evaluate tests
# ---------------------------------------------------------------------------

def test_rule_not_triggered_for_healthy_metric(low_success_rule, healthy_metric):
    assert low_success_rule.evaluate(healthy_metric) is None


def test_rule_triggered_for_failing_metric(low_success_rule, failing_metric):
    alert = low_success_rule.evaluate(failing_metric)
    assert alert is not None
    assert isinstance(alert, Alert)
    assert alert.severity == AlertSeverity.CRITICAL
    assert alert.pipeline == "orders"


def test_alert_value_matches_metric(low_success_rule, failing_metric):
    alert = low_success_rule.evaluate(failing_metric)
    assert alert.value == pytest.approx(0.7, rel=1e-3)


def test_rule_uses_custom_message():
    rule = AlertRule(
        name="custom_msg",
        metric="success_rate",
        threshold=1.0,
        operator="lt",
        message="Custom alert fired",
    )
    metric = PipelineMetric("p", 100, 1, 1, 10.0)
    alert = rule.evaluate(metric)
    assert alert.message == "Custom alert fired"


def test_unknown_metric_returns_none():
    rule = AlertRule(name="bad", metric="unknown_metric", threshold=1, operator="lt")
    metric = PipelineMetric("p", 100, 0, 0, 10.0)
    assert rule.evaluate(metric) is None


def test_high_error_rule_not_triggered(high_error_rule, healthy_metric):
    assert high_error_rule.evaluate(healthy_metric) is None


def test_high_error_rule_triggered(high_error_rule, failing_metric):
    alert = high_error_rule.evaluate(failing_metric)
    assert alert is not None
    assert alert.value == 300.0


# ---------------------------------------------------------------------------
# evaluate_rules tests
# ---------------------------------------------------------------------------

def test_evaluate_rules_returns_only_triggered(failing_metric, low_success_rule, high_error_rule):
    alerts = evaluate_rules(failing_metric, [low_success_rule, high_error_rule])
    assert len(alerts) == 2


def test_evaluate_rules_empty_for_healthy(healthy_metric, low_success_rule):
    alerts = evaluate_rules(healthy_metric, [low_success_rule])
    assert alerts == []


def test_alert_to_dict(low_success_rule, failing_metric):
    alert = low_success_rule.evaluate(failing_metric)
    d = alert.to_dict()
    assert d["rule"] == "low_success_rate"
    assert d["severity"] == "critical"
    assert d["pipeline"] == "orders"
    assert "value" in d
    assert "message" in d
